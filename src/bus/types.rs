// All core types for the S-Bus shard registry and Atomic Commit Protocol.
//
// Additions vs original:
//   - AcpConfig struct           (ablation flags + retry budget, Table 11)
//   - ReadSetEntry struct         (cross-shard phantom-read tracking)
//   - CommitRequest.read_set      (opt-in cross-shard validation)
//   - SyncError::CrossShardStale  (new error variant)
//
// These additions make Corollary 2.1 (serializability) hold for multi-shard
// operations under Assumption A1, closing the proof gap in §8.9.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use thiserror::Error;

// ────────────────────────────────────────────────────────────────────────────
// AcpConfig  —  runtime ablation flags and retry budget
// ────────────────────────────────────────────────────────────────────────────

/// Runtime ACP configuration. Read from environment variables at startup.
///
/// Controls which ACP components are active (ablation study, Table 11)
/// and the retry budget per agent per step (Definition 6, Corollary 2.2).
///
/// Environment variables:
///   SBUS_TOKEN=0          → disable ownership token  (–token ablation)
///   SBUS_VERSION=0        → disable version check    (–version ablation)
///   SBUS_LOG=0            → disable delta log        (–log ablation)
///   SBUS_TOKEN=0 SBUS_VERSION=0 → both disabled      (–both ablation)
///   SBUS_RETRY_BUDGET=5   → increase retry budget (eliminates exhaustion at N=8)
///   SBUS_LEASE_TIMEOUT=30 → token lease timeout in seconds
#[derive(Clone, Debug, Serialize)]
pub struct AcpConfig {
    /// If false: skip token acquisition/release (–token ablation condition).
    pub enable_ownership_token: bool,
    /// If false: skip version mismatch check (–version ablation condition).
    pub enable_version_check: bool,
    /// If false: skip delta log append (–log ablation condition).
    pub enable_delta_log: bool,
    /// Retry budget B per agent per step (Definition 6, Corollary 2.2).
    /// Default: 1. Set SBUS_RETRY_BUDGET=5 to eliminate all exhaustion events.
    pub retry_budget: usize,
    /// Token lease timeout in seconds (default: 30).
    /// Drives the constructive liveness proof in Corollary 2.2.
    pub lease_timeout_secs: u64,
}

impl AcpConfig {
    pub fn from_env() -> Self {
        Self {
            enable_ownership_token: std::env::var("SBUS_TOKEN")
                .map(|v| v != "0")
                .unwrap_or(true),
            enable_version_check: std::env::var("SBUS_VERSION")
                .map(|v| v != "0")
                .unwrap_or(true),
            enable_delta_log: std::env::var("SBUS_LOG")
                .map(|v| v != "0")
                .unwrap_or(true),
            retry_budget: std::env::var("SBUS_RETRY_BUDGET")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(1),
            lease_timeout_secs: std::env::var("SBUS_LEASE_TIMEOUT")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(30),
        }
    }
}

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

/// Cross-shard read-set entry (Assumption A1, §3.3).
///
/// An agent that reads shards A and B before committing to shard C declares
/// those reads here. The ACP checks that A and B have not advanced since the
/// agent recorded their versions. If either has, it returns CrossShardStale
/// and the agent must re-read and re-reason.
///
/// The field is Optional on CommitRequest — agents that only touch their own
/// shard omit it and get the same single-shard serializability as before
/// (backward-compatible). Corollary 2.1 holds for multi-shard operations only
/// when all read shards are declared (Assumption A1).
///
/// Python usage:
///   read_set = [
///       {"key": "db_schema",  "version_at_read": 3},
///       {"key": "api_design", "version_at_read": 2},
///   ]
///   POST /commit/v2  body includes "read_set": read_set
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
    /// Accept "expected_version" (canonical) or "expected_ver" (Python harness).
    #[serde(alias = "expected_ver")]
    pub expected_version: u64,
    /// Accept "delta" (canonical) or "content" (Python harness).
    #[serde(alias = "content")]
    pub delta: String,
    pub agent_id: String,
    /// Original harness sends "rationale" — accepted and stored in delta log.
    #[serde(default)]
    pub rationale: Option<String>,
    /// Optional cross-shard read-set for phantom-read prevention (Assumption A1).
    /// When present and non-empty, the engine validates all listed shards have
    /// not advanced since the agent's read time before applying the delta.
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

    /// Returned when a cross-shard read-set entry has advanced since the
    /// agent recorded it. The engine now acquires ALL shard locks (target +
    /// read_set) simultaneously in sorted key order (Havender 1968) before
    /// performing any version check, eliminating the TOCTOU gap that existed
    /// in the v1 drop-and-reacquire implementation.
    ///
    /// Proof implication (Appendix A, Lemma 1):
    ///   All locks are held from version-check through commit. No thread can
    ///   modify any declared dependency in that window. The dependency graph
    ///   is therefore a DAG for both single- and multi-shard operations at
    ///   any agent count N. Corollary 2.1 holds without qualification under
    ///   Assumption A1.
    #[error(
        "cross-shard stale read: key={key} version_at_read={version_at_read} \
         current_version={current_version}"
    )]
    CrossShardStale {
        key: String,
        version_at_read: u64,
        current_version: u64,
    },

    #[allow(dead_code)]
    #[error("delta log overflow: max depth reached for key={key}")]
    LogOverflow { key: String },

    #[error("internal error: {msg}")]
    Internal { msg: String },
}

impl SyncError {
    pub fn status_code(&self) -> u16 {
        match self {
            SyncError::ShardNotFound { .. }    => 404,
            SyncError::VersionMismatch { .. }  => 409,
            SyncError::TokenConflict { .. }    => 409,
            SyncError::CrossShardStale { .. }  => 409,
            SyncError::LogOverflow { .. }      => 507,
            SyncError::Internal { .. }         => 500,
        }
    }

    pub fn error_code(&self) -> &'static str {
        match self {
            SyncError::ShardNotFound { .. }    => "ShardNotFound",
            SyncError::VersionMismatch { .. }  => "VersionMismatch",
            SyncError::TokenConflict { .. }    => "TokenConflict",
            SyncError::CrossShardStale { .. }  => "CrossShardStale",
            SyncError::LogOverflow { .. }      => "LogOverflow",
            SyncError::Internal { .. }         => "InternalError",
        }
    }
}