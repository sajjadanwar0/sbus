// src/bus/types.rs — S-Bus core types.
// Changes in this version:
//   - AcpConfig: added max_delta_chars (enforces Assumption B, closes proof gap)
//   - SyncError: added DeltaTooLarge variant (HTTP 413)
//   - Comments: removed stale DashMap references from CrossShardStale doc

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use thiserror::Error;

// ─────────────────────────────────────────────────────────────────────────────
// AcpConfig
// ─────────────────────────────────────────────────────────────────────────────

/// Runtime ACP configuration. Read from environment variables at startup.
///
/// Environment variables:
///   SBUS_TOKEN=0          → disable ownership token  (ablation)
///   SBUS_VERSION=0        → disable version check    (ablation)
///   SBUS_LOG=0            → disable delta log        (ablation)
///   SBUS_RETRY_BUDGET=3   → retry budget B per step  (default: 1)
///   SBUS_LEASE_TIMEOUT=30 → token lease timeout in seconds
///   SBUS_MAX_DELTA=2000   → max delta size in chars  (enforces Assumption B)
///
/// SBUS_MAX_DELTA enforces Assumption B from Theorem 1 (Coordination Cost
/// Complexity): shard size ≤ D_max.  Commits whose delta field exceeds this
/// limit are rejected with HTTP 413 / SyncError::DeltaTooLarge.
/// Default: 2000 chars (≈400 tokens at 5 chars/token), well above the
/// 300-token max_completion_tokens used in experiments.
/// Set to 0 to disable enforcement (restores old behaviour, breaks Theorem 1).
#[derive(Clone, Debug, Serialize)]
pub struct AcpConfig {
    pub enable_ownership_token: bool,
    pub enable_version_check:   bool,
    pub enable_delta_log:       bool,
    pub retry_budget:           usize,
    pub lease_timeout_secs:     u64,
    /// Maximum delta content length in characters.
    /// Enforces Assumption B (bounded shard size) so Theorem 1 (O(T) complexity)
    /// is unconditional rather than conditional on agent behaviour.
    /// 0 = enforcement disabled (Theorem 1 becomes conditional on agent discipline).
    pub max_delta_chars:        usize,
}

impl AcpConfig {
    pub fn from_env() -> Self {
        Self {
            enable_ownership_token: std::env::var("SBUS_TOKEN")
                .map(|v| v != "0").unwrap_or(true),
            enable_version_check: std::env::var("SBUS_VERSION")
                .map(|v| v != "0").unwrap_or(true),
            enable_delta_log: std::env::var("SBUS_LOG")
                .map(|v| v != "0").unwrap_or(true),
            retry_budget: std::env::var("SBUS_RETRY_BUDGET")
                .ok().and_then(|v| v.parse().ok()).unwrap_or(1),
            lease_timeout_secs: std::env::var("SBUS_LEASE_TIMEOUT")
                .ok().and_then(|v| v.parse().ok()).unwrap_or(30),
            max_delta_chars: std::env::var("SBUS_MAX_DELTA")
                .ok().and_then(|v| v.parse().ok()).unwrap_or(2000),
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Delta log entry
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DeltaEntry {
    pub version:      u64,
    pub agent_id:     String,
    pub delta:        String,
    pub prev_hash:    String,
    pub committed_at: DateTime<Utc>,
}

// ─────────────────────────────────────────────────────────────────────────────
// State Shard  (Definition 1 in paper)
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Shard {
    pub id:                 String,
    pub version:            u64,
    pub content:            String,
    pub owner:              Option<String>,
    pub goal_tag:           String,
    pub delta_log:          VecDeque<DeltaEntry>,
    pub attempt_count:      u64,
    pub conflict_count:     u64,
    pub created_at:         DateTime<Utc>,
    pub updated_at:         DateTime<Utc>,
    pub token_acquired_at:  Option<DateTime<Utc>>,
}

impl Shard {
    pub fn content_address(s: &str) -> String {
        use sha2::{Digest, Sha256};
        let mut h = Sha256::new();
        h.update(s.as_bytes());
        hex::encode(h.finalize())
    }
    pub fn new(content: String, goal_tag: String, max_log_depth: usize) -> Self {
        let id  = Self::content_address(&content);
        let now = Utc::now();
        Self {
            id, version: 0, content, owner: None, goal_tag,
            delta_log: VecDeque::with_capacity(max_log_depth),
            attempt_count: 0, conflict_count: 0,
            created_at: now, updated_at: now, token_acquired_at: None,
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// HTTP request / response types
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct CreateShardRequest {
    pub key:      String,
    pub content:  String,
    pub goal_tag: String,
}

/// Cross-shard read-set entry (Assumption A1).
/// Agents declare every shard read via GET /shard/:key before committing.
/// The ACP validates all declared shards have not advanced since read time.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ReadSetEntry {
    pub key:             String,
    pub version_at_read: u64,
}

#[derive(Debug, Deserialize)]
pub struct CommitRequest {
    pub key: String,
    #[serde(alias = "expected_ver")]
    pub expected_version: u64,
    #[serde(alias = "content")]
    pub delta:    String,
    pub agent_id: String,
    #[serde(default)]
    pub rationale: Option<String>,
    /// Optional cross-shard read-set for Type-II stale-read prevention (A1).
    #[serde(default)]
    pub read_set: Option<Vec<ReadSetEntry>>,
}

#[derive(Debug, Deserialize)]
pub struct RollbackRequest {
    #[serde(default)]
    pub check_token:    bool,
    pub key:            String,
    pub target_version: u64,
    pub agent_id:       String,
}

#[derive(Debug, Serialize)]
pub struct CommitResponse {
    pub new_version: u64,
    pub shard_id:    String,
}

#[derive(Debug, Serialize)]
pub struct ShardResponse {
    pub key:              String,
    pub id:               String,
    pub version:          u64,
    pub content:          String,
    pub owner:            Option<String>,
    pub goal_tag:         String,
    pub attempt_count:    u64,
    pub conflict_count:   u64,
    pub delta_log_depth:  usize,
    pub created_at:       DateTime<Utc>,
    pub updated_at:       DateTime<Utc>,
}

impl From<(&str, &Shard)> for ShardResponse {
    fn from((key, s): (&str, &Shard)) -> Self {
        Self {
            key: key.to_owned(), id: s.id.clone(), version: s.version,
            content: s.content.clone(), owner: s.owner.clone(),
            goal_tag: s.goal_tag.clone(), attempt_count: s.attempt_count,
            conflict_count: s.conflict_count, delta_log_depth: s.delta_log.len(),
            created_at: s.created_at, updated_at: s.updated_at,
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Error types
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Error)]
pub enum SyncError {
    #[error("shard already exists: key={key}")]
    ShardAlreadyExists { key: String },

    #[error("shard not found: key={key}")]
    ShardNotFound { key: String },

    #[error("version mismatch on key={key}: expected={expected} found={found}")]
    VersionMismatch { key: String, expected: u64, found: u64 },

    #[error("token conflict on key={key}: currently owned by {owner}")]
    TokenConflict { key: String, owner: String },

    #[error("cross-shard stale read: key={key} version_at_read={version_at_read} current_version={current_version}")]
    CrossShardStale { key: String, version_at_read: u64, current_version: u64 },

    /// Delta exceeds max_delta_chars (SBUS_MAX_DELTA).
    /// HTTP 413 — enforces Assumption B (bounded shard size) so that
    /// Theorem 1 (O(T) coordination cost complexity) holds unconditionally.
    #[error("delta too large: key={key} len={len} max={max}")]
    DeltaTooLarge { key: String, len: usize, max: usize },

    #[allow(dead_code)]
    #[error("delta log overflow: max depth reached for key={key}")]
    LogOverflow { key: String },

    #[error("rollback blocked: key={key} has active write-token owned by {owner}")]
    RollbackTokenConflict { key: String, owner: String },

    #[error("internal error: {msg}")]
    Internal { msg: String },
}

impl SyncError {
    pub fn status_code(&self) -> u16 {
        match self {
            SyncError::ShardAlreadyExists { .. }   => 409,
            SyncError::ShardNotFound { .. }        => 404,
            SyncError::VersionMismatch { .. }      => 409,
            SyncError::TokenConflict { .. }        => 409,
            SyncError::CrossShardStale { .. }      => 409,
            SyncError::DeltaTooLarge { .. }        => 413,
            SyncError::RollbackTokenConflict { .. }=> 409,
            SyncError::LogOverflow { .. }          => 507,
            SyncError::Internal { .. }             => 500,
        }
    }

    pub fn error_code(&self) -> &'static str {
        match self {
            SyncError::ShardAlreadyExists { .. }   => "ShardAlreadyExists",
            SyncError::ShardNotFound { .. }        => "ShardNotFound",
            SyncError::VersionMismatch { .. }      => "VersionMismatch",
            SyncError::TokenConflict { .. }        => "TokenConflict",
            SyncError::CrossShardStale { .. }      => "CrossShardStale",
            SyncError::DeltaTooLarge { .. }        => "DeltaTooLarge",
            SyncError::RollbackTokenConflict { .. }=> "RollbackTokenConflict",
            SyncError::LogOverflow { .. }          => "LogOverflow",
            SyncError::Internal { .. }             => "InternalError",
        }
    }
}