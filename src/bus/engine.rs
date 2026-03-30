// S-Bus engine: implements the Atomic Commit Protocol (ACP)
// Theorem 1 guarantee: at most one agent holds a write-token per shard at any time.

use crate::bus::types::{BusStats, Delta, DeltaEntry, Shard, SyncError};
use chrono::Utc;
use dashmap::DashMap;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};
use tracing::{debug, info, warn};

/// The Semantic Bus — thread-safe, lock-free reads, serialized writes per shard.
///
/// Uses DashMap (shard-level locking) rather than a single global RwLock so that
/// commits to *different* shards can proceed in parallel. The Ownership Invariant
/// (Definition 4) is enforced per-shard by the `owner` field compare-and-set.
#[derive(Clone)]
pub struct SBus {
    /// Shard registry: content-addressed key → Shard
    registry: Arc<DashMap<String, Shard>>,
    /// Global monotone commit counter
    commit_counter: Arc<AtomicU64>,
    /// Global conflict counter
    conflict_counter: Arc<AtomicU64>,
    /// Global rollback counter
    rollback_counter: Arc<AtomicU64>,
    /// Max delta log entries per shard (configurable, default 1000)
    max_log_depth: usize,
}

impl SBus {
    /// Construct a new empty bus
    pub fn new() -> Self {
        Self {
            registry:         Arc::new(DashMap::new()),
            commit_counter:   Arc::new(AtomicU64::new(0)),
            conflict_counter: Arc::new(AtomicU64::new(0)),
            rollback_counter: Arc::new(AtomicU64::new(0)),
            max_log_depth:    1000,
        }
    }

    // ------------------------------------------------------------------ //
    //  CREATE                                                              //
    // ------------------------------------------------------------------ //

    /// Register a new shard. Returns the shard's key (content address of initial value).
    pub fn create_shard(&self, content: String, goal_tag: String) -> String {
        let shard = Shard::new(content, goal_tag);
        let key = shard.id.clone();
        self.registry.insert(key.clone(), shard);
        info!(key = %key, "shard created");
        key
    }

    /// Register a shard under an explicit user-defined key.
    pub fn create_shard_with_key(&self, key: String, content: String, goal_tag: String) -> String {
        let mut shard = Shard::new(content, goal_tag);
        shard.id = key.clone();
        self.registry.insert(key.clone(), shard);
        info!(key = %key, "shard created with explicit key");
        key
    }

    // ------------------------------------------------------------------ //
    //  READ  (non-blocking — multiple concurrent readers)                 //
    // ------------------------------------------------------------------ //

    /// Return a snapshot of the shard at its current version.
    /// This is the non-blocking read path — analogous to MVCC snapshot reads.
    pub fn read_shard(&self, key: &str) -> Result<Shard, SyncError> {
        self.registry
            .get(key)
            .map(|s| s.clone())
            .ok_or_else(|| SyncError::ShardNotFound { key: key.to_owned() })
    }

    /// List all shard keys
    pub fn list_shards(&self) -> Vec<String> {
        self.registry.iter().map(|e| e.key().clone()).collect()
    }

    // ------------------------------------------------------------------ //
    //  ATOMIC COMMIT PROTOCOL  (ACP)                                      //
    // ------------------------------------------------------------------ //
    //
    //  This is the core of the paper. Steps:
    //  1. Acquire per-shard entry (DashMap shard lock — NOT global lock)
    //  2. Check version matches expected (optimistic concurrency)
    //  3. Check no other agent holds the write-token (Ownership Invariant)
    //  4. Set write-token to caller
    //  5. Apply delta
    //  6. Increment version
    //  7. Append to delta log
    //  8. Release write-token (same atomic section)
    //
    //  Steps 3-8 are atomic under DashMap's per-entry lock.
    //  This gives us the Ownership Invariant proof from the paper.
    // ------------------------------------------------------------------ //

    pub fn commit_delta(
        &self,
        key:          &str,
        expected_ver: u64,
        delta:        Delta,
        agent_id:     &str,
    ) -> Result<u64, SyncError> {
        let mut entry = self
            .registry
            .get_mut(key)
            .ok_or_else(|| SyncError::ShardNotFound { key: key.to_owned() })?;

        let shard = entry.value_mut();

        // Always record the attempt
        shard.attempt_count += 1;

        // Step 2: Optimistic version check
        if shard.version != expected_ver {
            shard.conflict_count += 1;
            self.conflict_counter.fetch_add(1, Ordering::Relaxed);
            warn!(
                key    = %key,
                agent  = %agent_id,
                expected = expected_ver,
                found    = shard.version,
                "ACP: version mismatch"
            );
            return Err(SyncError::VersionMismatch {
                expected: expected_ver,
                found:    shard.version,
            });
        }

        // Step 3: Ownership Invariant check
        if let Some(ref owner) = shard.owner {
            shard.conflict_count += 1;
            self.conflict_counter.fetch_add(1, Ordering::Relaxed);
            warn!(key = %key, agent = %agent_id, owner = %owner, "ACP: token conflict");
            return Err(SyncError::TokenConflict { owner: owner.clone() });
        }

        // Step 4: Acquire write-token
        shard.owner = Some(agent_id.to_owned());

        // Step 5: Apply delta
        let prev_hash = Shard::content_address(&shard.content);
        shard.content = delta.content.clone();

        // Step 6: Increment version
        shard.version += 1;
        let new_version = shard.version;

        // Step 7: Append to delta log (bounded)
        let entry_rec = DeltaEntry {
            version:      new_version,
            agent_id:     agent_id.to_owned(),
            delta:        delta.content,
            prev_hash,
            committed_at: Utc::now(),
        };
        shard.delta_log.push_back(entry_rec);
        if shard.delta_log.len() > self.max_log_depth {
            shard.delta_log.pop_front();
        }

        // Step 8: Release write-token (atomic with this lock scope)
        shard.owner      = None;
        shard.updated_at = Utc::now();

        self.commit_counter.fetch_add(1, Ordering::Relaxed);
        debug!(key = %key, agent = %agent_id, version = new_version, "ACP: commit ok");

        Ok(new_version)
    }

    // ------------------------------------------------------------------ //
    //  ROLLBACK                                                            //
    // ------------------------------------------------------------------ //

    /// Atomically roll a shard back to a prior version.
    /// All delta_log entries after target_ver are discarded.
    pub fn rollback(&self, key: &str, target_ver: u64) -> Result<(), SyncError> {
        let mut entry = self
            .registry
            .get_mut(key)
            .ok_or_else(|| SyncError::ShardNotFound { key: key.to_owned() })?;

        let shard = entry.value_mut();

        if target_ver > shard.version {
            return Err(SyncError::RollbackInvalid {
                target:  target_ver,
                current: shard.version,
            });
        }

        // Truncate log to target version
        shard.delta_log.retain(|e| e.version <= target_ver);

        // Restore content from the last kept entry, or to empty if rolling
        // back before the first commit
        shard.content = shard
            .delta_log
            .back()
            .map(|e| e.delta.clone())
            .unwrap_or_default();

        shard.version    = target_ver;
        shard.owner      = None;
        shard.updated_at = Utc::now();

        self.rollback_counter.fetch_add(1, Ordering::Relaxed);
        info!(key = %key, target = target_ver, "ACP: rollback ok");
        Ok(())
    }

    // ------------------------------------------------------------------ //
    //  STATISTICS                                                          //
    // ------------------------------------------------------------------ //

    pub fn stats(&self) -> BusStats {
        let total_commits   = self.commit_counter.load(Ordering::Relaxed);
        let total_conflicts = self.conflict_counter.load(Ordering::Relaxed);
        let total_rollbacks = self.rollback_counter.load(Ordering::Relaxed);
        let total_shards    = self.registry.len();

        let (sum_version, owned) = self.registry.iter().fold((0u64, 0usize), |(sv, ow), e| {
            let o = if e.owner.is_some() { ow + 1 } else { ow };
            (sv + e.version, o)
        });

        let avg_version = if total_shards > 0 {
            sum_version as f64 / total_shards as f64
        } else {
            0.0
        };

        let total_attempts = total_commits + total_conflicts;
        let global_scr = if total_attempts > 0 {
            total_conflicts as f64 / total_attempts as f64
        } else {
            0.0
        };

        BusStats {
            total_shards,
            total_commits,
            total_conflicts,
            total_rollbacks,
            global_scr,
            avg_version,
            owned_shards: owned,
        }
    }
}

impl Default for SBus {
    fn default() -> Self {
        Self::new()
    }
}
