use std::io::Cursor;

use async_trait::async_trait;
use openraft::{
    BasicNode, Entry, EntryPayload, LogId, LogState, RaftLogReader, RaftSnapshotBuilder, Snapshot,
    SnapshotMeta, StorageError, StorageIOError, StoredMembership, Vote, storage::RaftStorage,
};
use serde::{Deserialize, Serialize};
use sled::Tree;
use tracing::{info, warn};

use super::{CommitEntry, CommitEntryResponse, SBusTypeConfig};
use crate::bus::{
    engine::SBus,
    types::{CommitRequest, CreateShardRequest, ReadSetEntry, Shard},
};

pub fn open_db(path: &str) -> (Tree, Tree) {
    let db   = sled::open(path).unwrap_or_else(|e| panic!("sled open {path}: {e}"));
    let logs = db.open_tree("logs").expect("open logs tree");
    let meta = db.open_tree("meta").expect("open meta tree");
    (logs, meta)
}

/// Wrap any display-able error into an openraft `StorageError`.
fn to_err<E: std::fmt::Display>(e: E) -> StorageError<u64> {
    StorageIOError::read_snapshot(None, &std::io::Error::other(e.to_string())).into()
}

#[derive(Serialize, Deserialize, Clone)]
struct SnapData {
    last_log_id:     Option<LogId<u64>>,
    last_membership: StoredMembership<u64, BasicNode>,
    shards:          Vec<(String, Shard)>,
    #[serde(default)]
    delivery_log:    Vec<(String, String, u64)>,
}

#[derive(Clone)]
pub struct SBusStore {
    logs:            Tree,
    meta:            Tree,
    engine:          SBus,
    last_applied:    Option<LogId<u64>>,
    last_membership: StoredMembership<u64, BasicNode>,
    snapshot_idx:    u64,
}

impl SBusStore {
    pub fn new(engine: SBus, logs: Tree, meta: Tree) -> Self {
        let last_applied: Option<LogId<u64>> = meta
            .get(b"last_applied")
            .ok()
            .flatten()
            .and_then(|b| serde_json::from_slice(&b).ok());
        let last_membership: StoredMembership<u64, BasicNode> = meta
            .get(b"last_membership")
            .ok()
            .flatten()
            .and_then(|b| serde_json::from_slice(&b).ok())
            .unwrap_or_default();
        let snapshot_idx: u64 = meta
            .get(b"snapshot_idx")
            .ok()
            .flatten()
            .and_then(|b| serde_json::from_slice(&b).ok())
            .unwrap_or(0);

        if let Ok(Some(bytes)) = meta.get(b"snapshot_data") {
            if let Ok(snap) = serde_json::from_slice::<SnapData>(&bytes) {
                let n_shards = snap.shards.len();
                let n_dl = snap.delivery_log.len();
                for (key, shard) in snap.shards {
                    engine.restore_shard(key, shard);
                }
                for (agent_id, shard_key, version) in snap.delivery_log {
                    engine.touch_delivery_log(&agent_id);
                    engine.record_delivery(&agent_id, &shard_key, version);
                }
                info!("recovered {n_shards} shards + {n_dl} delivery entries from snapshot");
            }
        }

        Self { logs, meta, engine, last_applied, last_membership, snapshot_idx }
    }

    #[inline]
    fn log_key(index: u64) -> [u8; 8] {
        index.to_be_bytes()
    }

    fn get_meta<T: for<'de> Deserialize<'de>>(&self, key: &[u8]) -> Option<T> {
        let bytes = self.meta.get(key).ok()??;
        serde_json::from_slice(&bytes).ok()
    }

    fn put_meta<T: Serialize>(&self, key: &[u8], val: &T) {
        let bytes = serde_json::to_vec(val).expect("serialize meta");
        self.meta.insert(key, bytes).expect("sled meta insert");
    }

    fn last_purged_from_db(&self) -> Option<LogId<u64>> {
        self.get_meta(b"last_purged")
    }
}


#[async_trait]
impl RaftLogReader<SBusTypeConfig> for SBusStore {
    async fn get_log_state(&mut self) -> Result<LogState<SBusTypeConfig>, StorageError<u64>> {
        let last_purged = self.last_purged_from_db();
        let last = self
            .logs
            .last()
            .ok()
            .flatten()
            .and_then(|(_, v)| serde_json::from_slice::<Entry<SBusTypeConfig>>(&v).ok())
            .map(|e| e.log_id)
            .or(last_purged);
        Ok(LogState { last_purged_log_id: last_purged, last_log_id: last })
    }

    async fn try_get_log_entries<RB>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<SBusTypeConfig>>, StorageError<u64>>
    where
        RB: std::ops::RangeBounds<u64> + Clone + std::fmt::Debug + Send + Sync,
    {
        use std::ops::Bound;
        let start_idx = match range.start_bound() {
            Bound::Included(&s) => s,
            Bound::Excluded(&s) => s.saturating_add(1),
            Bound::Unbounded    => 0,
        };
        let end_idx: Option<u64> = match range.end_bound() {
            Bound::Included(&e) => Some(e),
            Bound::Excluded(&e) => Some(e.saturating_sub(1)),
            Bound::Unbounded    => None,
        };
        let start_key = Self::log_key(start_idx);
        let mut entries = Vec::new();
        for res in self.logs.range(start_key..) {
            let (k, v) = res.map_err(to_err)?;
            let idx = u64::from_be_bytes(k[..8].try_into().unwrap());
            if let Some(end) = end_idx {
                if idx > end {
                    break;
                }
            }
            let entry: Entry<SBusTypeConfig> = serde_json::from_slice(&v).map_err(to_err)?;
            entries.push(entry);
        }
        Ok(entries)
    }
}

// ── RaftSnapshotBuilder ──────────────────────────────────────────────────────

#[async_trait]
impl RaftSnapshotBuilder<SBusTypeConfig> for SBusStore {
    async fn build_snapshot(&mut self) -> Result<Snapshot<SBusTypeConfig>, StorageError<u64>> {
        self.snapshot_idx += 1;
        let delivery_log = self.engine.dump_delivery_log_flat();
        let shards = self.engine.snapshot_shards();

        let snap = SnapData {
            last_log_id:     self.last_applied,
            last_membership: self.last_membership.clone(),
            shards,
            delivery_log,
        };
        let bytes = serde_json::to_vec(&snap).map_err(|e| StorageIOError::read_snapshot(None, &e))?;

        self.put_meta(b"snapshot_data", &snap);
        self.put_meta(b"snapshot_idx",  &self.snapshot_idx);

        let snapshot_id = format!(
            "snap-{}-{}",
            self.last_applied.map(|l| l.index).unwrap_or(0),
            self.snapshot_idx,
        );
        let meta = SnapshotMeta {
            last_log_id:     self.last_applied,
            last_membership: self.last_membership.clone(),
            snapshot_id,
        };

        info!(
            "snapshot built: idx={}, shards={}, dl_entries={}",
            self.snapshot_idx,
            snap.shards.len(),
            snap.delivery_log.len(),
        );
        Ok(Snapshot { meta, snapshot: Box::new(Cursor::new(bytes)) })
    }
}

// ── RaftStorage ──────────────────────────────────────────────────────────────

#[async_trait]
impl RaftStorage<SBusTypeConfig> for SBusStore {
    type LogReader       = Self;
    type SnapshotBuilder = Self;

    async fn save_vote(&mut self, vote: &Vote<u64>) -> Result<(), StorageError<u64>> {
        self.put_meta(b"vote", vote);
        Ok(())
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<u64>>, StorageError<u64>> {
        Ok(self.get_meta(b"vote"))
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        // The log reader only touches the `logs` / `meta` trees. A default
        // (empty) engine is fine here because `try_get_log_entries` never
        // calls through to the state machine.
        SBusStore {
            logs: self.logs.clone(),
            meta: self.meta.clone(),
            engine: SBus::default(),
            last_applied: None,
            last_membership: StoredMembership::default(),
            snapshot_idx: 0,
        }
    }

    async fn append_to_log<I>(&mut self, entries: I) -> Result<(), StorageError<u64>>
    where
        I: IntoIterator<Item = Entry<SBusTypeConfig>> + Send,
    {
        for e in entries {
            let key = Self::log_key(e.log_id.index);
            let val = serde_json::to_vec(&e).map_err(to_err)?;
            self.logs.insert(key, val).map_err(to_err)?;
        }
        Ok(())
    }

    async fn delete_conflict_logs_since(
        &mut self,
        log_id: LogId<u64>,
    ) -> Result<(), StorageError<u64>> {
        let start = Self::log_key(log_id.index);
        let keys: Vec<sled::IVec> = self
            .logs
            .range(start..)
            .filter_map(|r| r.ok().map(|(k, _)| k))
            .collect();
        for k in keys {
            self.logs.remove(k).map_err(to_err)?;
        }
        Ok(())
    }

    async fn purge_logs_upto(&mut self, log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
        let end = Self::log_key(log_id.index.saturating_add(1));
        let keys: Vec<sled::IVec> = self
            .logs
            .range(..end)
            .filter_map(|r| r.ok().map(|(k, _)| k))
            .collect();
        for k in keys {
            self.logs.remove(k).map_err(to_err)?;
        }
        self.put_meta(b"last_purged", &log_id);
        Ok(())
    }

    async fn last_applied_state(
        &mut self,
    ) -> Result<(Option<LogId<u64>>, StoredMembership<u64, BasicNode>), StorageError<u64>> {
        Ok((self.last_applied, self.last_membership.clone()))
    }

    async fn apply_to_state_machine(
        &mut self,
        entries: &[Entry<SBusTypeConfig>],
    ) -> Result<Vec<CommitEntryResponse>, StorageError<u64>> {
        let mut responses = Vec::with_capacity(entries.len());

        for entry in entries {
            let log_id = entry.log_id;
            self.last_applied = Some(log_id);
            self.put_meta(b"last_applied", &log_id);

            let resp = match &entry.payload {
                EntryPayload::Blank => CommitEntryResponse::ok(0, String::new()),

                EntryPayload::Normal(ce) => match ce {
                    // Applies on ALL nodes — leader and followers. After a
                    // failover the new leader already has full DeliveryLog
                    // state without HTTP 410 recovery.
                    CommitEntry::Delivery(d) => {
                        self.engine.touch_delivery_log(&d.agent_id);
                        self.engine.record_delivery(&d.agent_id, &d.shard_key, d.version);
                        CommitEntryResponse::delivery_ok()
                    }

                    CommitEntry::Shard(sc) => {
                        if let Some(content) = &sc.init_content {
                            let req = CreateShardRequest {
                                key:      sc.key.clone(),
                                content:  content.clone(),
                                goal_tag: sc.init_goal_tag.clone().unwrap_or_default(),
                            };
                            let _ = self.engine.create_shard(req);
                            CommitEntryResponse::ok(0, sc.key.clone())
                        } else {
                            self.engine.touch_delivery_log(&sc.agent_id);
                            let rse: Vec<ReadSetEntry> = sc
                                .read_set
                                .iter()
                                .map(|(k, v)| ReadSetEntry {
                                    key: k.clone(),
                                    version_at_read: *v,
                                })
                                .collect();
                            let req = CommitRequest {
                                key:              sc.key.clone(),
                                expected_version: sc.expected_version,
                                delta:            sc.delta.clone(),
                                agent_id:         sc.agent_id.clone(),
                                rationale:        None,
                                read_set:         Some(rse),
                            };
                            match self.engine.commit_delta(req) {
                                Ok(r) => CommitEntryResponse::ok(r.new_version, r.shard_id),
                                Err(e) => {
                                    warn!("apply commit failed: {e}");
                                    CommitEntryResponse::err(e.error_code(), &e.to_string())
                                }
                            }
                        }
                    }
                },

                EntryPayload::Membership(m) => {
                    self.last_membership = StoredMembership::new(Some(log_id), m.clone());
                    self.put_meta(b"last_membership", &self.last_membership);
                    CommitEntryResponse::ok(0, String::new())
                }
            };
            responses.push(resp);
        }
        Ok(responses)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        SBusStore {
            logs: self.logs.clone(),
            meta: self.meta.clone(),
            engine: self.engine.clone(),
            last_applied: self.last_applied,
            last_membership: self.last_membership.clone(),
            snapshot_idx: self.snapshot_idx,
        }
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<Cursor<Vec<u8>>>, StorageError<u64>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta:     &SnapshotMeta<u64, BasicNode>,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError<u64>> {
        let bytes = snapshot.into_inner();
        let snap: SnapData = serde_json::from_slice(&bytes)
            .map_err(|e| StorageIOError::read_snapshot(Some(meta.signature()), &e))?;

        let n_shards = snap.shards.len();
        let n_dl = snap.delivery_log.len();

        // Persist first, consume second — avoids cloning the entire payload.
        self.put_meta(b"snapshot_data",   &snap);
        self.put_meta(b"last_applied",    &snap.last_log_id);
        self.put_meta(b"last_membership", &snap.last_membership);

        self.engine.reset_all();
        for (key, shard) in snap.shards {
            self.engine.restore_shard(key, shard);
        }
        for (agent_id, shard_key, version) in snap.delivery_log {
            self.engine.touch_delivery_log(&agent_id);
            self.engine.record_delivery(&agent_id, &shard_key, version);
        }

        self.last_applied    = snap.last_log_id;
        self.last_membership = snap.last_membership;

        info!("snapshot installed: {n_shards} shards + {n_dl} delivery entries");
        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<SBusTypeConfig>>, StorageError<u64>> {
        let Some(snap) = self.get_meta::<SnapData>(b"snapshot_data") else {
            return Ok(None);
        };
        let bytes = serde_json::to_vec(&snap).map_err(|e| StorageIOError::read_snapshot(None, &e))?;
        let snapshot_id = format!(
            "snap-persisted-{}",
            snap.last_log_id.map(|l| l.index).unwrap_or(0),
        );
        let meta = SnapshotMeta {
            last_log_id:     snap.last_log_id,
            last_membership: snap.last_membership,
            snapshot_id,
        };
        Ok(Some(Snapshot { meta, snapshot: Box::new(Cursor::new(bytes)) }))
    }
}
