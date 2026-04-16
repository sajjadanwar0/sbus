// src/raft/store.rs — openraft 0.8.4 + sled persistence (v37)
//
// ── Why sled instead of RocksDB ───────────────────────────────────────────────
// sled is a pure-Rust embedded key-value store with zero system dependencies.
// RocksDB requires clang and C++ headers to build from source, which breaks
// on systems without a full C++ toolchain. sled compiles with only rustc.
//
// ── Storage layout ────────────────────────────────────────────────────────────
// Two sled Trees (equivalent to RocksDB column families):
//   logs : key = u64 log index as 8-byte big-endian, value = JSON Entry
//   meta : key = UTF-8 string, value = JSON
//          known keys: "vote", "last_purged", "last_applied",
//                      "last_membership", "snapshot_idx", "snapshot_data"
//
// sled::Tree is Clone (Arc-backed) and Send + Sync — safe for async use.
//
// ── Bug fixes ─────────────────────────────────────────────────────────────────
// BUG-1  get_log_state: fall back to last_purged when logs tree is empty after
//        purge, so a fully-caught-up node correctly wins elections.
// BUG-2  apply_to_state_machine: touch_delivery_log before commit_delta_v2,
//        so followers (which never received POST /session) can apply commits.

use std::io::Cursor;
use async_trait::async_trait;
use openraft::{
    BasicNode,
    Entry, EntryPayload, LogId, LogState,
    Snapshot, SnapshotMeta, StoredMembership,
    RaftLogReader, RaftSnapshotBuilder,
    storage::RaftStorage,
    StorageError, StorageIOError, Vote,
};
use serde::{Deserialize, Serialize};
use sled::Tree;
use tracing::{info, warn};

use super::{CommitEntryResponse, SBusTypeConfig};
use crate::bus::engine::SBus;
use crate::bus::types::{CommitRequest, CreateShardRequest, ReadSetEntry, Shard};

// ── Open sled database ────────────────────────────────────────────────────────
/// Open (or create) the sled database at `path` and return the two trees
/// used by SBusStore.  sled::Tree is Arc-backed — cheap to clone.
pub fn open_db(path: &str) -> (Tree, Tree) {
    let db   = sled::open(path).unwrap_or_else(|e| panic!("sled open {path}: {e}"));
    let logs = db.open_tree("logs").expect("open logs tree");
    let meta = db.open_tree("meta").expect("open meta tree");
    (logs, meta)
}

// ── Error helper ──────────────────────────────────────────────────────────────
fn to_err(e: impl std::fmt::Display) -> StorageError<u64> {
    struct W(String);
    impl std::fmt::Debug   for W { fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result { write!(f, "{}", self.0) } }
    impl std::fmt::Display for W { fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result { write!(f, "{}", self.0) } }
    impl std::error::Error for W {}
    StorageIOError::read_snapshot(None, &W(e.to_string())).into()
}

// ── Snapshot payload ──────────────────────────────────────────────────────────
#[derive(Serialize, Deserialize, Clone)]
struct SnapData {
    last_log_id:     Option<LogId<u64>>,
    last_membership: StoredMembership<u64, BasicNode>,
    shards:          Vec<(String, Shard)>,
}

// ── SBusStore ─────────────────────────────────────────────────────────────────
/// Clone is cheap: sled::Tree is Arc-backed.
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
    /// Create a new store, restoring shard state from the latest snapshot
    /// if one exists in the sled database.
    pub fn new(engine: SBus, logs: Tree, meta: Tree) -> Self {
        let last_applied: Option<LogId<u64>> = meta.get(b"last_applied")
            .ok().flatten()
            .and_then(|b| serde_json::from_slice(&b).ok());

        let last_membership: StoredMembership<u64, BasicNode> = meta.get(b"last_membership")
            .ok().flatten()
            .and_then(|b| serde_json::from_slice(&b).ok())
            .unwrap_or_default();

        let snapshot_idx: u64 = meta.get(b"snapshot_idx")
            .ok().flatten()
            .and_then(|b| serde_json::from_slice(&b).ok())
            .unwrap_or(0);

        // Restore shards from persisted snapshot (crash recovery + new-node catch-up)
        if let Ok(Some(bytes)) = meta.get(b"snapshot_data") {
            if let Ok(snap) = serde_json::from_slice::<SnapData>(&bytes) {
                let n = snap.shards.len();
                for (key, shard) in snap.shards {
                    engine.restore_shard(key, shard);
                }
                info!("Recovered {n} shards from snapshot, last_applied={last_applied:?}");
            }
        }

        Self { logs, meta, engine, last_applied, last_membership, snapshot_idx }
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    #[inline]
    fn log_key(index: u64) -> [u8; 8] { index.to_be_bytes() }

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

// ── RaftLogReader ─────────────────────────────────────────────────────────────
#[async_trait]
impl RaftLogReader<SBusTypeConfig> for SBusStore {
    async fn get_log_state(&mut self) -> Result<LogState<SBusTypeConfig>, StorageError<u64>> {
        let last_purged = self.last_purged_from_db();

        // BUG-1 FIX: after purge_logs_upto the logs tree may be empty.
        // Fall back to last_purged so openRaft sees the correct effective
        // log end rather than None (= "empty log"), which would cause a
        // stale node to win elections over a fully caught-up purged node.
        let last = self.logs
            .last()
            .ok()
            .flatten()
            .and_then(|(_, v)| serde_json::from_slice::<Entry<SBusTypeConfig>>(&v).ok())
            .map(|e| e.log_id)
            .or(last_purged); // ← THE FIX

        Ok(LogState {
            last_purged_log_id: last_purged,
            last_log_id:        last,
        })
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
            let (k, v) = res.map_err(|e| to_err(e))?;
            let idx = u64::from_be_bytes(k[..8].try_into().unwrap());
            if let Some(end) = end_idx {
                if idx > end { break; }
            }
            let entry: Entry<SBusTypeConfig> = serde_json::from_slice(&v)
                .map_err(|e| to_err(e))?;
            entries.push(entry);
        }
        Ok(entries)
    }
}

// ── RaftSnapshotBuilder ───────────────────────────────────────────────────────
#[async_trait]
impl RaftSnapshotBuilder<SBusTypeConfig> for SBusStore {
    async fn build_snapshot(&mut self) -> Result<Snapshot<SBusTypeConfig>, StorageError<u64>> {
        self.snapshot_idx += 1;
        let snap = SnapData {
            last_log_id:     self.last_applied,
            last_membership: self.last_membership.clone(),
            shards:          self.engine.snapshot_shards(),
        };
        let bytes = serde_json::to_vec(&snap)
            .map_err(|e| StorageIOError::read_snapshot(None, &e))?;

        // Persist so get_current_snapshot can serve it to joining learners
        self.put_meta(b"snapshot_data", &snap);
        self.put_meta(b"snapshot_idx",  &self.snapshot_idx);

        let meta = SnapshotMeta {
            last_log_id:     self.last_applied,
            last_membership: self.last_membership.clone(),
            snapshot_id:     format!("snap-{}-{}",
                                     self.last_applied.map(|l| l.index).unwrap_or(0),
                                     self.snapshot_idx),
        };
        info!("Snapshot built: idx={}, shards={}", self.snapshot_idx, snap.shards.len());
        Ok(Snapshot { meta, snapshot: Box::new(Cursor::new(bytes)) })
    }
}

// ── RaftStorage ───────────────────────────────────────────────────────────────
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

    /// Log reader shares the same sled trees via Clone — cheap Arc clone.
    async fn get_log_reader(&mut self) -> Self::LogReader {
        SBusStore {
            logs:            self.logs.clone(),
            meta:            self.meta.clone(),
            engine:          SBus::default(),
            last_applied:    None,
            last_membership: StoredMembership::default(),
            snapshot_idx:    0,
        }
    }

    async fn append_to_log<I>(&mut self, entries: I) -> Result<(), StorageError<u64>>
    where I: IntoIterator<Item = Entry<SBusTypeConfig>> + Send,
    {
        for e in entries {
            let key  = Self::log_key(e.log_id.index);
            let val  = serde_json::to_vec(&e).map_err(|e| to_err(e))?;
            self.logs.insert(key, val).map_err(|e| to_err(e))?;
        }
        Ok(())
    }

    async fn delete_conflict_logs_since(
        &mut self, log_id: LogId<u64>,
    ) -> Result<(), StorageError<u64>> {
        // Delete all entries with index >= log_id.index
        let start = Self::log_key(log_id.index);
        let keys: Vec<sled::IVec> = self.logs
            .range(start..)
            .filter_map(|r| r.ok().map(|(k, _)| k))
            .collect();
        for k in keys {
            self.logs.remove(k).map_err(|e| to_err(e))?;
        }
        Ok(())
    }

    async fn purge_logs_upto(
        &mut self, log_id: LogId<u64>,
    ) -> Result<(), StorageError<u64>> {
        // Delete all entries with index <= log_id.index
        // sled range end is exclusive, so use index+1 as the end bound
        let end = Self::log_key(log_id.index.saturating_add(1));
        let keys: Vec<sled::IVec> = self.logs
            .range(..end)
            .filter_map(|r| r.ok().map(|(k, _)| k))
            .collect();
        for k in keys {
            self.logs.remove(k).map_err(|e| to_err(e))?;
        }
        // Persist the purge point — used by get_log_state BUG-1 fix
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

                EntryPayload::Normal(ce) => {
                    // ── Raft-replicated shard creation ─────────────────────────
                    if let Some(ref content) = ce.init_content {
                        let req = CreateShardRequest {
                            key:      ce.key.clone(),
                            content:  content.clone(),
                            goal_tag: ce.init_goal_tag.clone().unwrap_or_default(),
                        };
                        let _ = self.engine.create_shard(req); // idempotent
                        responses.push(CommitEntryResponse::ok(0, ce.key.clone()));
                        continue;
                    }

                    // ── Raft-replicated commit ─────────────────────────────────
                    // BUG-2 FIX: sessions are not Raft-replicated.
                    // On followers, the agent's session was never created via
                    // POST /session.  touch_delivery_log creates a fresh session
                    // so build_effective_read_set doesn't return SessionExpired.
                    self.engine.touch_delivery_log(&ce.agent_id);

                    let rse: Vec<ReadSetEntry> = ce.read_set.iter()
                        .map(|(k, v)| ReadSetEntry { key: k.clone(), version_at_read: *v })
                        .collect();
                    let req = CommitRequest {
                        key:              ce.key.clone(),
                        expected_version: ce.expected_version,
                        delta:            ce.delta.clone(),
                        agent_id:         ce.agent_id.clone(),
                        rationale:        None,
                        read_set:         Some(rse),
                    };
                    match self.engine.commit_delta_v2(req) {
                        Ok(r)  => CommitEntryResponse::ok(r.new_version, r.shard_id),
                        Err(e) => {
                            warn!("apply commit failed: {e}");
                            CommitEntryResponse::err(e.error_code(), &e.to_string())
                        }
                    }
                }

                EntryPayload::Membership(m) => {
                    self.last_membership = StoredMembership::new(Some(log_id), m.clone());
                    self.put_meta(b"last_membership", &self.last_membership.clone());
                    CommitEntryResponse::ok(0, String::new())
                }
            };
            responses.push(resp);
        }
        Ok(responses)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        SBusStore {
            logs:            self.logs.clone(),
            meta:            self.meta.clone(),
            engine:          self.engine.clone(),
            last_applied:    self.last_applied,
            last_membership: self.last_membership.clone(),
            snapshot_idx:    self.snapshot_idx,
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

        self.engine.reset_all();
        let n = snap.shards.len();
        for (key, shard) in snap.shards.clone() {
            self.engine.restore_shard(key, shard);
        }
        self.last_applied    = snap.last_log_id;
        self.last_membership = snap.last_membership.clone();

        // Persist so future restarts and get_current_snapshot can use it
        self.put_meta(b"snapshot_data",   &snap);
        self.put_meta(b"last_applied",    &snap.last_log_id);
        self.put_meta(b"last_membership", &snap.last_membership);

        info!("Snapshot installed: {n} shards, last_applied={:?}", self.last_applied);
        Ok(())
    }

    /// Returns the latest persisted snapshot so openRaft can transfer it to
    /// joining learners.  This is what makes dynamic node addition work:
    /// new nodes call add_learner → openRaft calls get_current_snapshot on
    /// the leader → sends the full shard state to the learner via
    /// install_snapshot → learner replays remaining log entries.
    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<SBusTypeConfig>>, StorageError<u64>> {
        let snap: SnapData = match self.get_meta(b"snapshot_data") {
            Some(s) => s,
            None    => return Ok(None),
        };
        let bytes = serde_json::to_vec(&snap)
            .map_err(|e| StorageIOError::read_snapshot(None, &e))?;
        let meta = SnapshotMeta {
            last_log_id:     snap.last_log_id,
            last_membership: snap.last_membership,
            snapshot_id:     format!("snap-persisted-{}",
                                     snap.last_log_id.map(|l| l.index).unwrap_or(0)),
        };
        Ok(Some(Snapshot { meta, snapshot: Box::new(Cursor::new(bytes)) }))
    }
}