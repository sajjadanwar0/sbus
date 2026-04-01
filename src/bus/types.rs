// src/bus/types.rs
//
// All core types for the S-Bus shard registry and Atomic Commit Protocol.
// Gap-fill additions (marked with [GAP-FIX]):
//   - ReadSetEntry struct           (cross-shard phantom-read tracking)
//   - CommitRequest.read_set field  (opt-in cross-shard validation)
//   - SyncError::CrossShardStale   (new error variant)
//
// These additions make Corollary 1 (serializability) hold for multi-shard
// operations, closing the proof gap identified in §8.9.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use thiserror::Error;

// ────────────────────────────────────────────────────────────────────────────
// Delta log entry
// ────────────────────────────────────────────────────────────────────────────

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DeltaEntry {
    pub version: u64,
    pub agent_id: String,
    pub delta: String,
    pub prev_hash: String,
    pub committed_at: DateTime<Utc>,
}

// ────────────────────────────────────────────────────────────────────────────
// State Shard  (Definition 1 in paper)
// ────────────────────────────────────────────────────────────────────────────

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Shard {
    /// SHA-256 of initial content — stable identity (id field in paper)
    pub id: String,
    /// Monotone version counter (v in paper)
    pub version: u64,
    /// Current natural-language payload (c in paper)
    pub content: String,
    /// Write-token — None means unowned (τ in paper)
    pub owner: Option<String>,
    /// Human-readable label for the shard's purpose
    pub goal_tag: String,
    /// Append-only log of all committed deltas (L in paper)
    pub delta_log: VecDeque<DeltaEntry>,
    /// How many commit attempts have targeted this shard
    pub attempt_count: u64,
    /// How many of those were rejected
    pub conflict_count: u64,
    /// Wall-clock creation time
    pub created_at: DateTime<Utc>,
    /// Wall-clock time of most recent successful commit
    pub updated_at: DateTime<Utc>,
    /// Wall-clock time the current write-token was acquired (for lease enforcement)
    pub token_acquired_at: Option<DateTime<Utc>>,
}

impl Shard {
    /// Compute the SHA-256 content address of a string.
    pub fn content_address(s: &str) -> String {
        use sha2::{Digest, Sha256};
        let mut hasher = Sha256::new();
        hasher.update(s.as_bytes());
        hex::encode(hasher.finalize())
    }

    pub fn new(content: String, goal_tag: String, max_log_depth: usize) -> Self {
        let id = Self::content_address(&content);
        let now = Utc::now();
        Self {
            id,
            version: 0,
            content,
            owner: None,
            goal_tag,
            delta_log: VecDeque::with_capacity(max_log_depth),
            attempt_count: 0,
            conflict_count: 0,
            created_at: now,
            updated_at: now,
            token_acquired_at: None,
        }
    }
}

// ────────────────────────────────────────────────────────────────────────────
// HTTP request / response types
// ────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct CreateShardRequest {
    pub key: String,
    pub content: String,
    pub goal_tag: String,
}

// [GAP-FIX] Cross-shard read-set entry.
//
// An agent that reads shards A and B before committing to shard C declares
// those reads here.  The ACP checks that A and B have not advanced since the
// read; if either has, it returns CrossShardStale.  This closes the phantom-
// read gap described in §8.9 and makes Corollary 1 hold for multi-shard ops.
//
// Usage (Python agent side):
//   read_set = [
//       {"key": "db_schema",   "version_at_read": 3},
//       {"key": "api_design",  "version_at_read": 2},
//   ]
//   POST /commit  body includes "read_set": read_set
//
// The field is Optional — agents that only touch their own shard omit it and
// get the same single-shard serializability guarantee as before (backward-
// compatible).
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ReadSetEntry {
    /// The shard key that was read
    pub key: String,
    /// The version the agent observed at read time
    pub version_at_read: u64,
}

#[derive(Debug, Deserialize)]
pub struct CommitRequest {
    pub key: String,
    // Accept "expected_version" (canonical) or "expected_ver" (original Python harness).
    #[serde(alias = "expected_ver")]
    pub expected_version: u64,
    // Accept "delta" (canonical) or "content" (original Python harness).
    #[serde(alias = "content")]
    pub delta: String,
    pub agent_id: String,
    // Original harness sends "rationale" — accept and ignore.
    #[serde(default)]
    pub rationale: Option<String>,
    // [GAP-FIX] Optional cross-shard read-set for phantom-read prevention.
    #[serde(default)]
    pub read_set: Option<Vec<ReadSetEntry>>,
}

#[derive(Debug, Deserialize)]
pub struct RollbackRequest {
    pub key: String,
    pub target_version: u64,
    pub agent_id: String,
}

#[derive(Debug, Serialize)]
pub struct CommitResponse {
    pub new_version: u64,
    pub shard_id: String,
}

#[derive(Debug, Serialize)]
pub struct ShardResponse {
    pub key: String,
    pub id: String,
    pub version: u64,
    pub content: String,
    pub owner: Option<String>,
    pub goal_tag: String,
    pub attempt_count: u64,
    pub conflict_count: u64,
    pub delta_log_depth: usize,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl From<(&str, &Shard)> for ShardResponse {
    fn from((key, s): (&str, &Shard)) -> Self {
        Self {
            key: key.to_owned(),
            id: s.id.clone(),
            version: s.version,
            content: s.content.clone(),
            owner: s.owner.clone(),
            goal_tag: s.goal_tag.clone(),
            attempt_count: s.attempt_count,
            conflict_count: s.conflict_count,
            delta_log_depth: s.delta_log.len(),
            created_at: s.created_at,
            updated_at: s.updated_at,
        }
    }
}

// ────────────────────────────────────────────────────────────────────────────
// Error types  (SyncError)
// ────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Error)]
pub enum SyncError {
    #[error("shard not found: key={key}")]
    ShardNotFound { key: String },

    #[error("version mismatch on key={key}: expected={expected} found={found}")]
    VersionMismatch {
        key: String,
        expected: u64,
        found: u64,
    },

    #[error("token conflict on key={key}: currently owned by {owner}")]
    TokenConflict { key: String, owner: String },

    // [GAP-FIX v2] Returned when a cross-shard read-set entry is stale.
    //
    // engine.rs commit_delta() now acquires ALL shard locks (target + read_set)
    // simultaneously in sorted key order (Havender 1968 deadlock prevention).
    // The version check and delta application happen atomically under all locks.
    // This eliminates the TOCTOU gap present in the v1 drop-and-reacquire
    // implementation, which produced 2 corruptions at N=8 in the validation
    // experiment.
    //
    // Proof implication (revised Appendix A, Lemma 1):
    //   Because all locks are held from version-check through commit, no
    //   thread can modify any declared dependency between the check and the
    //   write.  The dependency graph edge ci → cj is therefore exact for
    //   both single-shard and multi-shard operations at any agent count N.
    //   Corollary 1 (serializability) holds without qualification.
    #[error(
        "cross-shard stale read: key={key} version_at_read={version_at_read} \
         current_version={current_version}"
    )]
    CrossShardStale {
        key: String,
        version_at_read: u64,
        current_version: u64,
    },

    // Reserved for a future hard-limit mode where overflow is an error
    // rather than a silent eviction.  The current implementation evicts
    // the oldest entry (pop_front) so this variant is never constructed —
    // suppress the warning explicitly.
    #[allow(dead_code)]
    #[error("delta log overflow: max depth reached for key={key}")]
    LogOverflow { key: String },

    #[error("internal error: {msg}")]
    Internal { msg: String },
}

impl SyncError {
    /// HTTP status code for this error — used by Axum handlers.
    pub fn status_code(&self) -> u16 {
        match self {
            SyncError::ShardNotFound { .. } => 404,
            SyncError::VersionMismatch { .. } => 409,
            SyncError::TokenConflict { .. } => 409,
            SyncError::CrossShardStale { .. } => 409,
            SyncError::LogOverflow { .. } => 507,
            SyncError::Internal { .. } => 500,
        }
    }

    /// Machine-readable error code for JSON responses.
    pub fn error_code(&self) -> &'static str {
        match self {
            SyncError::ShardNotFound { .. } => "ShardNotFound",
            SyncError::VersionMismatch { .. } => "VersionMismatch",
            SyncError::TokenConflict { .. } => "TokenConflict",
            SyncError::CrossShardStale { .. } => "CrossShardStale",
            SyncError::LogOverflow { .. } => "LogOverflow",
            SyncError::Internal { .. } => "InternalError",
        }
    }
}