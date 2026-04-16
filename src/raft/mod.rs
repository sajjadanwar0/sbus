// src/raft/mod.rs — S-Bus v36 (openraft 0.8.4)
// Exports: SBusTypeConfig, SBusRaft, SBusAdaptor, CommitEntry,
//          CommitEntryResponse, build_raft_config

pub mod store;
pub mod network;

use openraft::{BasicNode, Config, Entry, SnapshotPolicy, storage::Adaptor};
use serde::{Deserialize, Serialize};
use std::io::Cursor;

use self::network::SBusNetworkFactory;
use self::store::SBusStore;

// ── CommitEntry: Raft log payload ─────────────────────────────────────────────
// init_content/init_goal_tag: if Some, the state machine creates the shard
// before applying the commit. This makes create_shard Raft-replicated.
// Both fields use serde(default) so old log entries without them still decode.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommitEntry {
    pub key:              String,
    pub expected_version: u64,
    pub delta:            String,
    pub agent_id:         String,
    pub read_set:         Vec<(String, u64)>,
    // Raft-replicated shard creation: if Some, create shard before commit
    #[serde(default)]
    pub init_content:     Option<String>,
    #[serde(default)]
    pub init_goal_tag:    Option<String>,
}

// ── CommitEntryResponse: state machine response ───────────────────────────────
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommitEntryResponse {
    pub new_version: Option<u64>,
    pub shard_id:    Option<String>,
    pub error:       Option<String>,
    pub error_code:  Option<String>,
}
impl CommitEntryResponse {
    pub fn ok(nv: u64, sid: String) -> Self {
        Self { new_version: Some(nv), shard_id: Some(sid), error: None, error_code: None }
    }
    pub fn err(code: &str, msg: &str) -> Self {
        Self { new_version: None, shard_id: None,
            error: Some(msg.to_owned()), error_code: Some(code.to_owned()) }
    }
}

// ── RaftTypeConfig ────────────────────────────────────────────────────────────
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SBusTypeConfig;

impl openraft::RaftTypeConfig for SBusTypeConfig {
    type D            = CommitEntry;
    type R            = CommitEntryResponse;
    type NodeId       = u64;
    type Node         = BasicNode;
    type Entry        = Entry<Self>;
    type SnapshotData = Cursor<Vec<u8>>;
}

// ── Concrete type aliases ─────────────────────────────────────────────────────
pub type SBusAdaptor = Adaptor<SBusTypeConfig, SBusStore>;
pub type SBusRaft    = openraft::Raft<SBusTypeConfig, SBusNetworkFactory, SBusAdaptor, SBusAdaptor>;

// ── Raft config ───────────────────────────────────────────────────────────────
pub fn build_raft_config() -> std::sync::Arc<Config> {
    std::sync::Arc::new(
        Config {
            cluster_name:                "sbus-cluster".into(),
            heartbeat_interval:          250,
            election_timeout_min:        500,
            election_timeout_max:        1000,
            max_in_snapshot_log_to_keep: 200,
            snapshot_policy:             SnapshotPolicy::LogsSinceLast(500),
            max_payload_entries:         64,
            ..Default::default()
        }
            .validate()
            .expect("valid raft config"),
    )
}