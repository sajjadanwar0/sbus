use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::VecDeque;

/// A content-addressed, versioned shard of natural language state.
/// Formally: s_i = (id, v, content, tau, delta_log)
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Shard {
    /// Unique shard identifier (content-addressed SHA-256)
    pub id: String,
    /// Monotone version counter — incremented on every committed delta
    pub version: u64,
    /// Current natural language content
    pub content: String,
    /// Current owner token (None = unowned, tau = bot)
    pub owner: Option<String>,
    /// Sub-goal tag this shard belongs to
    pub goal_tag: String,
    /// Append-only delta log for full rollback support
    pub delta_log: VecDeque<DeltaEntry>,
    /// Wall-clock creation time
    pub created_at: DateTime<Utc>,
    /// Wall-clock time of last commit
    pub updated_at: DateTime<Utc>,
    /// Number of rejected commits against this shard (for SCR metric)
    pub conflict_count: u64,
    /// Total commits attempted against this shard
    pub attempt_count: u64,
}

/// One entry in the append-only delta log
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DeltaEntry {
    pub version:    u64,
    pub agent_id:   String,
    pub delta:      String,
    pub prev_hash:  String,
    pub committed_at: DateTime<Utc>,
}

impl Shard {
    /// Create a new shard with initial content
    pub fn new(content: String, goal_tag: String) -> Self {
        let id = Self::content_address(&content);
        let now = Utc::now();
        Self {
            id,
            version: 0,
            content,
            owner: None,
            goal_tag,
            delta_log: VecDeque::new(),
            created_at: now,
            updated_at: now,
            conflict_count: 0,
            attempt_count: 0,
        }
    }

    /// SHA-256 content address of the payload
    pub fn content_address(content: &str) -> String {
        let mut hasher = Sha256::new();
        hasher.update(content.as_bytes());
        hex::encode(hasher.finalize())
    }
}

/// A mutation to apply to a shard's content
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Delta {
    /// New content value (full replacement for now; extend to patch ops later)
    pub content: String,
    /// Semantic description of what this mutation achieves
    pub rationale: String,
}

/// Errors produced by the Atomic Commit Protocol
#[derive(Debug, Clone, Serialize, Deserialize, thiserror::Error)]
pub enum SyncError {
    #[error("Version mismatch: expected {expected}, found {found}")]
    VersionMismatch { expected: u64, found: u64 },

    #[error("Token conflict: shard already owned by agent '{owner}'")]
    TokenConflict { owner: String },

    #[error("Shard not found: '{key}'")]
    ShardNotFound { key: String },

    #[error("Rollback target version {target} exceeds current version {current}")]
    RollbackInvalid { target: u64, current: u64 },

    #[error("Internal error: {msg}")]
    Internal { msg: String },
}

/// Bus-level statistics snapshot
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct BusStats {
    pub total_shards:       usize,
    pub total_commits:      u64,
    pub total_conflicts:    u64,
    pub total_rollbacks:    u64,
    pub global_scr:         f64,
    pub avg_version:        f64,
    pub owned_shards:       usize,
}
