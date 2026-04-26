use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use thiserror::Error;

#[derive(Clone, Debug, Serialize)]
pub struct AcpConfig {
    pub enable_ownership_token: bool,
    pub enable_version_check: bool,
    pub enable_delta_log: bool,
    pub max_delta_chars: usize,
}

impl AcpConfig {
    pub fn from_env() -> Self {
        let disabled = |name: &str| std::env::var(name).is_ok_and(|v| v == "1");
        let max_delta_chars: usize = std::env::var("SBUS_MAX_DELTA")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(2000);
        Self {
            enable_ownership_token: !disabled("SBUS_TOKEN"),
            enable_version_check: !disabled("SBUS_VERSION"),
            enable_delta_log: !disabled("SBUS_LOG"),
            max_delta_chars,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DeltaEntry {
    pub version: u64,
    pub agent_id: String,
    pub delta: String,
    pub prev_hash: String,
    pub committed_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Shard {
    pub id: String,
    pub version: u64,
    pub content: String,
    pub owner: Option<String>,
    pub goal_tag: String,
    pub delta_log: VecDeque<DeltaEntry>,
    pub attempt_count: u64,
    pub conflict_count: u64,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl Shard {
    pub fn content_address(s: &str) -> String {
        use sha2::{Digest, Sha256};
        let mut h = Sha256::new();
        h.update(s.as_bytes());
        hex::encode(h.finalize())
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
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CreateShardRequest {
    pub key: String,
    pub content: String,
    pub goal_tag: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ReadSetEntry {
    pub key: String,
    pub version_at_read: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CommitRequest {
    pub key: String,
    #[serde(alias = "expected_ver")]
    pub expected_version: u64,
    #[serde(alias = "content")]
    pub delta: String,
    pub agent_id: String,
    #[serde(default)]
    pub rationale: Option<String>,
    #[serde(default)]
    pub read_set: Option<Vec<ReadSetEntry>>,
}

#[derive(Debug, Serialize, Deserialize)]
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

#[derive(Debug, Error)]
pub enum SyncError {
    #[error("shard already exists: key={key}")]
    ShardAlreadyExists { key: String },

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

    #[error(
        "cross-shard stale read: key={key} version_at_read={version_at_read} current_version={current_version}"
    )]
    CrossShardStale {
        key: String,
        version_at_read: u64,
        current_version: u64,
    },

    #[error("delta too large: key={key} len={len} max={max}")]
    DeltaTooLarge { key: String, len: usize, max: usize },
    
    #[error("session expired for agent '{agent_id}': {message}")]
    SessionExpired { agent_id: String, message: String },

    #[error("internal error: {msg}")]
    Internal { msg: String },
}

impl SyncError {
    pub fn status_code(&self) -> u16 {
        match self {
            Self::ShardAlreadyExists { .. } => 409,
            Self::ShardNotFound { .. } => 404,
            Self::VersionMismatch { .. } => 409,
            Self::TokenConflict { .. } => 409,
            Self::CrossShardStale { .. } => 409,
            Self::DeltaTooLarge { .. } => 413,
            Self::SessionExpired { .. } => 410,
            Self::Internal { .. } => 500,
        }
    }

    pub fn error_code(&self) -> &'static str {
        match self {
            Self::ShardAlreadyExists { .. } => "ShardAlreadyExists",
            Self::ShardNotFound { .. } => "ShardNotFound",
            Self::VersionMismatch { .. } => "VersionMismatch",
            Self::TokenConflict { .. } => "TokenConflict",
            Self::CrossShardStale { .. } => "CrossShardStale",
            Self::DeltaTooLarge { .. } => "DeltaTooLarge",
            Self::SessionExpired { .. } => "SessionExpired",
            Self::Internal { .. } => "InternalError",
        }
    }
}