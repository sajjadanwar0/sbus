// src/bus/types.rs — S-Bus v29 (corrected)
//
// Changes:
//   - AcpConfig: ablation flags now SBUS_TOKEN=1/SBUS_LOG=1 to DISABLE (was =0)
//   - AcpConfig: retry_budget default raised to 5 (was 1)
//   - SyncError: added SessionExpired variant → HTTP 410 Gone (FIX-TTL)
//     TTL expiry now rejects commit instead of silently accepting stale reads.
//   - SyncError: DeltaTooLarge len: field (was size:)

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use thiserror::Error;

// ─────────────────────────────────────────────────────────────────────────────
// AcpConfig
// ─────────────────────────────────────────────────────────────────────────────

/// Runtime ACP configuration, read from environment at startup.
///
/// Ablation flags — set to "1" to DISABLE that component:
///   SBUS_TOKEN=1       disable ownership token  → DL-only mode
///   SBUS_VERSION=1     disable version check    → no OCC
///   SBUS_LOG=1         disable DeliveryLog      → token-only mode
///   SBUS_RETRY_BUDGET=K  retry budget (default 5)
///   SBUS_MAX_DELTA=N   max delta chars (default 2000, 0=disabled)
///   SBUS_SESSION_TTL=N DeliveryLog session TTL seconds (default 3600)
#[derive(Clone, Debug, Serialize)]
pub struct AcpConfig {
    pub enable_ownership_token: bool,
    pub enable_version_check:   bool,
    pub enable_delta_log:       bool,
    pub retry_budget:           usize,
    pub lease_timeout_secs:     u64,
    pub max_delta_chars:        usize,
}

impl AcpConfig {
    pub fn from_env() -> Self {
        Self {
            enable_ownership_token: std::env::var("SBUS_TOKEN")
                .map(|v| v != "1").unwrap_or(true),
            enable_version_check: std::env::var("SBUS_VERSION")
                .map(|v| v != "1").unwrap_or(true),
            enable_delta_log: std::env::var("SBUS_LOG")
                .map(|v| v != "1").unwrap_or(true),
            retry_budget: std::env::var("SBUS_RETRY_BUDGET")
                .ok().and_then(|v| v.parse().ok()).unwrap_or(5),
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
    pub id:                String,
    pub version:           u64,
    pub content:           String,
    pub owner:             Option<String>,
    pub goal_tag:          String,
    pub delta_log:         VecDeque<DeltaEntry>,
    pub attempt_count:     u64,
    pub conflict_count:    u64,
    pub created_at:        DateTime<Utc>,
    pub updated_at:        DateTime<Utc>,
    pub token_acquired_at: Option<DateTime<Utc>>,
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
    pub key:             String,
    pub id:              String,
    pub version:         u64,
    pub content:         String,
    pub owner:           Option<String>,
    pub goal_tag:        String,
    pub attempt_count:   u64,
    pub conflict_count:  u64,
    pub delta_log_depth: usize,
    pub created_at:      DateTime<Utc>,
    pub updated_at:      DateTime<Utc>,
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

    #[error("delta too large: key={key} len={len} max={max}")]
    DeltaTooLarge { key: String, len: usize, max: usize },

    /// FIX-TTL: DeliveryLog session expired. HTTP 410 Gone.
    /// Previously: commit succeeded silently without validating expired entries.
    /// Now: commit is rejected; agent must re-read all shards before retrying.
    /// Prevention: set SBUS_SESSION_TTL >= 2 * measured_wall_time.
    #[error("session expired for agent '{agent_id}': {message}")]
    SessionExpired { agent_id: String, message: String },

    #[error("unauthorized: agent_id={agent_id} — invalid or missing X-Agent-Token")]
    Unauthorized { agent_id: String },

    #[allow(dead_code)]
    #[error("delta log overflow: max depth reached for key={key}")]
    LogOverflow { key: String },

    #[error("rollback blocked: key={key} has active token owned by {owner}")]
    RollbackTokenConflict { key: String, owner: String },

    #[error("internal error: {msg}")]
    Internal { msg: String },
}

impl SyncError {
    pub fn status_code(&self) -> u16 {
        match self {
            Self::ShardAlreadyExists { .. }    => 409,
            Self::ShardNotFound { .. }         => 404,
            Self::VersionMismatch { .. }       => 409,
            Self::TokenConflict { .. }         => 409,
            Self::CrossShardStale { .. }       => 409,
            Self::DeltaTooLarge { .. }         => 413,
            Self::SessionExpired { .. }        => 410,   // HTTP 410 Gone
            Self::Unauthorized { .. }          => 401,
            Self::RollbackTokenConflict { .. } => 409,
            Self::LogOverflow { .. }           => 507,
            Self::Internal { .. }              => 500,
        }
    }

    pub fn error_code(&self) -> &'static str {
        match self {
            Self::ShardAlreadyExists { .. }    => "ShardAlreadyExists",
            Self::ShardNotFound { .. }         => "ShardNotFound",
            Self::VersionMismatch { .. }       => "VersionMismatch",
            Self::TokenConflict { .. }         => "TokenConflict",
            Self::CrossShardStale { .. }       => "CrossShardStale",
            Self::DeltaTooLarge { .. }         => "DeltaTooLarge",
            Self::SessionExpired { .. }        => "SessionExpired",
            Self::Unauthorized { .. }          => "Unauthorized",
            Self::RollbackTokenConflict { .. } => "RollbackTokenConflict",
            Self::LogOverflow { .. }           => "LogOverflow",
            Self::Internal { .. }              => "InternalError",
        }
    }
}