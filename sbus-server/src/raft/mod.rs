use std::io::Cursor;

use openraft::{BasicNode, Config, Entry, SnapshotPolicy, storage::Adaptor};
use serde::{Deserialize, Serialize};

use self::network::SBusNetworkFactory;
use self::store::SBusStore;

pub mod network;
pub mod store;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardCommitPayload {
    pub key:              String,
    pub expected_version: u64,
    pub delta:            String,
    pub agent_id:         String,
    pub read_set:         Vec<(String, u64)>,
    #[serde(default)]
    pub init_content:     Option<String>,
    #[serde(default)]
    pub init_goal_tag:    Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeliveryPayload {
    pub agent_id:  String,
    pub shard_key: String,
    pub version:   u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum CommitEntry {
    Shard(ShardCommitPayload),
    Delivery(DeliveryPayload),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommitEntryResponse {
    pub new_version: Option<u64>,
    pub shard_id:    Option<String>,
    pub error:       Option<String>,
    pub error_code:  Option<String>,
}

impl CommitEntryResponse {
    pub fn ok(new_version: u64, shard_id: String) -> Self {
        Self {
            new_version: Some(new_version),
            shard_id:    Some(shard_id),
            error:       None,
            error_code:  None,
        }
    }

    pub fn err(code: &str, msg: &str) -> Self {
        Self {
            new_version: None,
            shard_id:    None,
            error:       Some(msg.to_owned()),
            error_code:  Some(code.to_owned()),
        }
    }

    pub fn delivery_ok() -> Self {
        Self { new_version: None, shard_id: None, error: None, error_code: None }
    }
}

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

pub type SBusAdaptor = Adaptor<SBusTypeConfig, SBusStore>;
pub type SBusRaft    = openraft::Raft<SBusTypeConfig, SBusNetworkFactory, SBusAdaptor, SBusAdaptor>;

pub fn build_raft_config() -> std::sync::Arc<Config> {
    std::sync::Arc::new(Config {
        cluster_name:                "sbus-cluster".into(),
        heartbeat_interval:          250,
        election_timeout_min:        500,
        election_timeout_max:        1000,
        max_in_snapshot_log_to_keep: 200,
        snapshot_policy:             SnapshotPolicy::LogsSinceLast(500),
        max_payload_entries:         64,
        ..Default::default()
    })
}