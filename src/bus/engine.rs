// src/bus/engine.rs
//
// S-Bus shard registry and Atomic Commit Protocol (ACP).
//
// Additions vs original:
//   1. AcpConfig field on SBus + ablation-aware single-shard commit path
//   2. apply_delta_inner_cfg() helper respecting all three ablation flags
//   3. commit_v2_naive() — unordered lock acquisition (Table 6 control)
//   4. acp_config block in stats() JSON
//   5. cross_shard_stale_counter exposed in stats() and prometheus_metrics()
//   6. Constructive liveness comment on spawn_lease_monitor() (Corollary 2.2)

use crate::bus::types::{
    AcpConfig, CommitRequest, CommitResponse, CreateShardRequest, DeltaEntry,
    RollbackRequest, Shard, ShardResponse, SyncError,
};
use chrono::Utc;
use dashmap::DashMap;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};
use tokio::time::{interval, Duration};
use tracing::{debug, info, warn};

// ────────────────────────────────────────────────────────────────────────────
// SBus  —  the top-level registry handle (clone-safe Arc wrapper)
// ────────────────────────────────────────────────────────────────────────────

/// Proposition 2 (Dual Ownership Enforcement):
/// (a) Compile-time: Rust's affine type system statically guarantees at most
///     one &mut Shard exists within the server process at any program point
///     [Jung et al. 2018].
/// (b) Runtime: DashMap's per-shard CAS (get_mut) enforces the same invariant
///     for external HTTP clients (Theorem 2).
/// Both are necessary; neither alone is sufficient.
#[derive(Clone)]
pub struct SBus {
    /// Runtime ACP configuration — ablation flags and retry budget.
    /// Read from environment at startup via AcpConfig::from_env().
    pub config: AcpConfig,
    /// Shard registry — 64-bucket DashMap for O(1) concurrent access.
    registry: Arc<DashMap<String, Shard>>,
    /// Global commit counter (monotonically increasing).
    commit_counter: Arc<AtomicU64>,
    /// Global conflict counter (VersionMismatch + TokenConflict combined).
    conflict_counter: Arc<AtomicU64>,
    /// Separate counter for cross-shard stale reads detected by the ACP.
    /// Exposed via /stats and /metrics so the cross-shard experiment can
    /// observe the metric directly without parsing log output.
    cross_shard_stale_counter: Arc<AtomicU64>,
    /// Maximum entries retained in each shard's delta log.
    max_log_depth: usize,
    /// Token lease timeout in seconds.
    /// Drives the constructive proof of Corollary 2.2 (liveness):
    ///   After an agent crashes while holding token τᵢ, the lease monitor
    ///   detects the orphan within lease_timeout_secs + 5 seconds and sets
    ///   τᵢ ← ⊥. Therefore at every logical time t + lease_timeout_secs
    ///   some agent can acquire τᵢ — condition (i) of Corollary 2.2 follows
    ///   constructively without assuming liveness.
    pub lease_timeout_secs: u64,
}

impl SBus {
    pub fn new() -> Self {
        Self::with_options(1_000, 30)
    }

    pub fn with_options(max_log_depth: usize, lease_timeout_secs: u64) -> Self {
        Self {
            config: AcpConfig::from_env(),
            registry: Arc::new(DashMap::with_capacity_and_shard_amount(64, 64)),
            commit_counter: Arc::new(AtomicU64::new(0)),
            conflict_counter: Arc::new(AtomicU64::new(0)),
            cross_shard_stale_counter: Arc::new(AtomicU64::new(0)),
            max_log_depth,
            lease_timeout_secs,
        }
    }

    // ─────────────────────────────────────────────────────────────────────
    // Shard CRUD
    // ─────────────────────────────────────────────────────────────────────

    pub fn create_shard(&self, req: CreateShardRequest) -> Result<ShardResponse, SyncError> {
        if self.registry.contains_key(&req.key) {
            return Err(SyncError::ShardNotFound {
                key: format!("shard '{}' already exists", req.key),
            });
        }
        let shard = Shard::new(req.content, req.goal_tag, self.max_log_depth);
        let resp = ShardResponse::from((req.key.as_str(), &shard));
        self.registry.insert(req.key, shard);
        Ok(resp)
    }

    pub fn read_shard(&self, key: &str) -> Result<ShardResponse, SyncError> {
        self.registry
            .get(key)
            .map(|s| ShardResponse::from((key, s.value())))
            .ok_or_else(|| SyncError::ShardNotFound { key: key.to_owned() })
    }

    pub fn list_shards(&self) -> Vec<String> {
        self.registry.iter().map(|r| r.key().clone()).collect()
    }

    // ─────────────────────────────────────────────────────────────────────
    // ACP — Algorithm 1 (paper §4.2), extended with ablation flags and
    //        cross-shard sorted-lock-order acquisition (Corollary 2.1)
    // ─────────────────────────────────────────────────────────────────────

    /// Atomic Commit Protocol (ACP).
    ///
    /// Two paths:
    ///
    /// Single-shard (req.read_set is None or empty):
    ///   Steps 1-5 are ablation-aware. Wrapping each step in the config
    ///   flag allows Table 11's four ablation conditions to run without
    ///   recompiling — set SBUS_TOKEN=0 / SBUS_VERSION=0 / SBUS_LOG=0
    ///   before starting the server.
    ///
    /// Multi-shard (req.read_set is Some with entries):
    ///   Acquires all locks simultaneously in sorted lexicographic key order
    ///   (Lemma 2 / Havender 1968) before any version check, eliminating
    ///   the TOCTOU window. Corollary 2.1 holds for any N under Assumption A1.
    pub fn commit_delta(&self, req: CommitRequest) -> Result<CommitResponse, SyncError> {
        let cfg = &self.config;

        // ── Multi-shard path (sorted-lock-order, Corollary 2.1) ──────────
        if let Some(ref read_set) = req.read_set {
            if !read_set.is_empty() {
                // Drop-and-reacquire not used here — we build the sorted key
                // list first, then acquire all locks simultaneously.
                let mut all_keys: Vec<String> = read_set
                    .iter()
                    .filter(|rs| rs.key != req.key)
                    .map(|rs| rs.key.clone())
                    .collect();
                all_keys.push(req.key.clone());
                // LEXICOGRAPHIC SORT — Lemma 2 (Havender 1968):
                // Consistent total order prevents circular wait → no deadlock.
                all_keys.sort();
                all_keys.dedup();

                // Acquire all locks in sorted order.
                // Each get_mut() holds the DashMap per-bucket RwLock until
                // the RefMut drops. Rust RAII guarantees release on all paths.
                let mut guards: Vec<dashmap::mapref::one::RefMut<String, Shard>> =
                    Vec::with_capacity(all_keys.len());
                for k in &all_keys {
                    let g = self.registry.get_mut(k).ok_or_else(|| {
                        SyncError::ShardNotFound { key: k.clone() }
                    })?;
                    guards.push(g);
                }

                // All locks held — validate cross-shard read-set.
                // No thread can modify any declared shard in this window.
                for rs_entry in read_set {
                    if rs_entry.key == req.key {
                        continue;
                    }
                    let idx = all_keys
                        .iter()
                        .position(|k| k == &rs_entry.key)
                        .ok_or_else(|| SyncError::ShardNotFound {
                            key: rs_entry.key.clone(),
                        })?;
                    let current_ver = guards[idx].value().version;
                    if current_ver != rs_entry.version_at_read {
                        self.cross_shard_stale_counter.fetch_add(1, Ordering::Relaxed);
                        return Err(SyncError::CrossShardStale {
                            key:             rs_entry.key.clone(),
                            version_at_read: rs_entry.version_at_read,
                            current_version: current_ver,
                        });
                    }
                }

                // Validate and apply delta on target shard (all locks still held).
                let target_idx = all_keys
                    .iter()
                    .position(|k| k == &req.key)
                    .ok_or_else(|| SyncError::ShardNotFound {
                        key: req.key.clone(),
                    })?;
                let s_target = guards[target_idx].value_mut();
                s_target.attempt_count += 1;

                if s_target.version != req.expected_version {
                    s_target.conflict_count += 1;
                    self.conflict_counter.fetch_add(1, Ordering::Relaxed);
                    return Err(SyncError::VersionMismatch {
                        key:      req.key.clone(),
                        expected: req.expected_version,
                        found:    s_target.version,
                    });
                }
                if let Some(ref owner) = s_target.owner {
                    s_target.conflict_count += 1;
                    self.conflict_counter.fetch_add(1, Ordering::Relaxed);
                    return Err(SyncError::TokenConflict {
                        key:   req.key.clone(),
                        owner: owner.clone(),
                    });
                }

                return self.apply_delta_inner(
                    s_target, &req.agent_id, &req.delta, &req.key,
                );
                // guards dropped here — all locks released via RAII
            }
        }

        // ── Single-shard path (ablation-aware) ───────────────────────────
        // Acquire the single target shard lock.
        let mut entry = self.registry.get_mut(&req.key).ok_or_else(|| {
            SyncError::ShardNotFound { key: req.key.clone() }
        })?;
        let s = entry.value_mut();
        s.attempt_count += 1;

        // Step 2 (ablation-aware): version check.
        // Disabled by SBUS_VERSION=0 — corresponds to –version condition
        // in Table 11. Without this, agents commit against stale content.
        if cfg.enable_version_check && s.version != req.expected_version {
            s.conflict_count += 1;
            self.conflict_counter.fetch_add(1, Ordering::Relaxed);
            return Err(SyncError::VersionMismatch {
                key:      req.key.clone(),
                expected: req.expected_version,
                found:    s.version,
            });
        }

        // Step 3 (ablation-aware): ownership token (Ownership Invariant, Def. 5).
        // Disabled by SBUS_TOKEN=0 — corresponds to –token condition in Table 11.
        // Without this, two agents can both pass version check if their commits
        // arrive within the same DashMap lock window.
        if cfg.enable_ownership_token {
            if let Some(ref owner) = s.owner {
                s.conflict_count += 1;
                self.conflict_counter.fetch_add(1, Ordering::Relaxed);
                return Err(SyncError::TokenConflict {
                    key:   req.key.clone(),
                    owner: owner.clone(),
                });
            }
        }

        // Steps 4-5: apply delta with ablation-aware log append.
        self.apply_delta_inner_cfg(s, &req.agent_id, &req.delta, &req.key, cfg)
    }

    /// commit_v2_naive — UNORDERED multi-shard commit.
    ///
    /// Control condition for the cross-shard validation experiment (Table 6).
    ///
    /// Identical to the multi-shard path in commit_delta() EXCEPT that
    /// `all_keys.sort()` is omitted, so locks are acquired in
    /// request-insertion order. This can deadlock under adversarial
    /// concurrency and produces corruptions under concurrent injection.
    ///
    /// Purpose: demonstrates empirically that Lemma 2's sorted-lock-order
    /// is specifically necessary — read-set declaration alone is insufficient.
    ///
    /// DO NOT call from production agents.
    pub fn commit_v2_naive(&self, req: CommitRequest) -> Result<CommitResponse, SyncError> {
        let read_set = match &req.read_set {
            Some(rs) if !rs.is_empty() => rs.clone(),
            // No read_set — fall through to standard single-shard path
            _ => return self.commit_delta(req),
        };

        // Collect keys in REQUEST-INSERTION ORDER — intentionally NOT sorted.
        // This is the only functional difference from the sorted path.
        let mut all_keys: Vec<String> = read_set
            .iter()
            .filter(|rs| rs.key != req.key)
            .map(|rs| rs.key.clone())
            .collect();
        all_keys.push(req.key.clone());
        // Note: no all_keys.sort() — intentional, this is the control condition
        all_keys.dedup();

        // Acquire locks in insertion order (may deadlock under concurrent load)
        let mut guards: Vec<dashmap::mapref::one::RefMut<String, Shard>> =
            Vec::with_capacity(all_keys.len());
        for k in &all_keys {
            let g = self.registry.get_mut(k).ok_or_else(|| {
                SyncError::ShardNotFound { key: k.clone() }
            })?;
            guards.push(g);
        }

        // Cross-shard read-set validation (same logic as sorted path)
        for rs_entry in &read_set {
            if rs_entry.key == req.key {
                continue;
            }
            let idx = all_keys
                .iter()
                .position(|k| k == &rs_entry.key)
                .ok_or_else(|| SyncError::ShardNotFound {
                    key: rs_entry.key.clone(),
                })?;
            let current_ver = guards[idx].value().version;
            if current_ver != rs_entry.version_at_read {
                self.cross_shard_stale_counter.fetch_add(1, Ordering::Relaxed);
                return Err(SyncError::CrossShardStale {
                    key:             rs_entry.key.clone(),
                    version_at_read: rs_entry.version_at_read,
                    current_version: current_ver,
                });
            }
        }

        // Validate and apply delta on target shard
        let target_idx = all_keys
            .iter()
            .position(|k| k == &req.key)
            .ok_or_else(|| SyncError::ShardNotFound {
                key: req.key.clone(),
            })?;
        let s_target = guards[target_idx].value_mut();
        s_target.attempt_count += 1;

        if s_target.version != req.expected_version {
            s_target.conflict_count += 1;
            self.conflict_counter.fetch_add(1, Ordering::Relaxed);
            return Err(SyncError::VersionMismatch {
                key:      req.key.clone(),
                expected: req.expected_version,
                found:    s_target.version,
            });
        }
        if let Some(ref owner) = s_target.owner {
            s_target.conflict_count += 1;
            self.conflict_counter.fetch_add(1, Ordering::Relaxed);
            return Err(SyncError::TokenConflict {
                key:   req.key.clone(),
                owner: owner.clone(),
            });
        }

        self.apply_delta_inner(s_target, &req.agent_id, &req.delta, &req.key)
        // guards dropped here — all locks released via RAII
    }

    /// Inner helper: apply a delta to a shard that has already passed all ACP
    /// checks. Always enables all ACP features (used by multi-shard paths).
    /// Called with the DashMap entry lock held.
    fn apply_delta_inner(
        &self,
        s:        &mut Shard,
        agent_id: &str,
        delta:    &str,
        key:      &str,
    ) -> Result<CommitResponse, SyncError> {
        s.owner = Some(agent_id.to_owned());
        s.token_acquired_at = Some(Utc::now());

        let prev_hash = Shard::content_address(&s.content);
        s.content = delta.to_owned();
        s.version += 1;
        s.updated_at = Utc::now();

        s.delta_log.push_back(DeltaEntry {
            version:      s.version,
            agent_id:     agent_id.to_owned(),
            delta:        delta.to_owned(),
            prev_hash,
            committed_at: Utc::now(),
        });
        if s.delta_log.len() > self.max_log_depth {
            s.delta_log.pop_front();
        }

        let new_version = s.version;
        let shard_id   = s.id.clone();

        s.owner = None;
        s.token_acquired_at = None;

        self.commit_counter.fetch_add(1, Ordering::Relaxed);
        debug!(key, agent = agent_id, new_version, "ACP commit succeeded");

        Ok(CommitResponse { new_version, shard_id })
    }

    /// Ablation-aware inner helper. Respects AcpConfig flags for token,
    /// version, and log steps. Used by the single-shard path only.
    fn apply_delta_inner_cfg(
        &self,
        s:        &mut Shard,
        agent_id: &str,
        delta:    &str,
        key:      &str,
        cfg:      &AcpConfig,
    ) -> Result<CommitResponse, SyncError> {
        // Acquire write-token (conditional — disabled in –token ablation)
        if cfg.enable_ownership_token {
            s.owner = Some(agent_id.to_owned());
            s.token_acquired_at = Some(Utc::now());
        }

        let prev_hash = Shard::content_address(&s.content);
        s.content = delta.to_owned();
        s.version += 1;
        s.updated_at = Utc::now();

        // Append to delta log (conditional — disabled in –log ablation).
        // –log condition: skip append entirely. The shard is still updated;
        // rollback capability is lost but correctness is not affected.
        if cfg.enable_delta_log {
            s.delta_log.push_back(DeltaEntry {
                version:      s.version,
                agent_id:     agent_id.to_owned(),
                delta:        delta.to_owned(),
                prev_hash,
                committed_at: Utc::now(),
            });
            if s.delta_log.len() > self.max_log_depth {
                s.delta_log.pop_front();
            }
        }

        let new_version = s.version;
        let shard_id   = s.id.clone();

        // Release write-token (conditional — disabled in –token ablation)
        if cfg.enable_ownership_token {
            s.owner = None;
            s.token_acquired_at = None;
        }

        self.commit_counter.fetch_add(1, Ordering::Relaxed);
        debug!(key, agent = agent_id, new_version, "ACP commit succeeded (cfg-aware)");

        Ok(CommitResponse { new_version, shard_id })
    }

    // ─────────────────────────────────────────────────────────────────────
    // Rollback
    // ─────────────────────────────────────────────────────────────────────

    pub fn rollback(&self, req: RollbackRequest) -> Result<ShardResponse, SyncError> {
        let mut entry = self.registry.get_mut(&req.key).ok_or_else(|| {
            SyncError::ShardNotFound { key: req.key.clone() }
        })?;
        let s = entry.value_mut();

        let target_entry = s
            .delta_log
            .iter()
            .find(|e| e.version == req.target_version)
            .ok_or_else(|| SyncError::Internal {
                msg: format!(
                    "version {} not found in delta log for key={}",
                    req.target_version, req.key
                ),
            })?
            .clone();

        let prev_hash = Shard::content_address(&s.content);
        s.content = target_entry.delta.clone();
        s.version += 1;
        s.updated_at = Utc::now();
        s.delta_log.push_back(DeltaEntry {
            version:      s.version,
            agent_id:     format!("rollback:{}", req.agent_id),
            delta:        target_entry.delta,
            prev_hash,
            committed_at: Utc::now(),
        });
        if s.delta_log.len() > self.max_log_depth {
            s.delta_log.pop_front();
        }

        info!(
            key = req.key,
            target_version = req.target_version,
            new_version = s.version,
            "rollback completed"
        );

        let resp = ShardResponse::from((req.key.as_str(), &*s));
        drop(entry);
        Ok(resp)
    }

    // ─────────────────────────────────────────────────────────────────────
    // Statistics
    // ─────────────────────────────────────────────────────────────────────

    pub fn stats(&self) -> serde_json::Value {
        let total_shards    = self.registry.len();
        let total_commits   = self.commit_counter.load(Ordering::Relaxed);
        let total_conflicts = self.conflict_counter.load(Ordering::Relaxed);
        let cross_stale     = self.cross_shard_stale_counter.load(Ordering::Relaxed);
        let total_attempts  = total_commits + total_conflicts;
        let scr = if total_attempts > 0 {
            total_conflicts as f64 / total_attempts as f64
        } else {
            0.0
        };

        serde_json::json!({
            "total_shards": total_shards,
            "total_commits": total_commits,
            "total_conflicts": total_conflicts,
            "cross_shard_stale_count": cross_stale,
            "total_attempts": total_attempts,
            "scr": scr,
            "lease_timeout_secs": self.lease_timeout_secs,
            // Expose ACP config so Python experiments can verify which
            // ablation condition is active without parsing log output.
            "acp_config": {
                "enable_ownership_token": self.config.enable_ownership_token,
                "enable_version_check":   self.config.enable_version_check,
                "enable_delta_log":       self.config.enable_delta_log,
                "retry_budget":           self.config.retry_budget,
                "lease_timeout_secs":     self.config.lease_timeout_secs,
            },
        })
    }

    // ─────────────────────────────────────────────────────────────────────
    // Prometheus metrics endpoint (GET /metrics)
    // ─────────────────────────────────────────────────────────────────────

    pub fn prometheus_metrics(&self) -> String {
        let commits     = self.commit_counter.load(Ordering::Relaxed);
        let conflicts   = self.conflict_counter.load(Ordering::Relaxed);
        let cross_stale = self.cross_shard_stale_counter.load(Ordering::Relaxed);
        let shards      = self.registry.len();
        format!(
            "# HELP sbus_commits_total Total successful ACP commits\n\
             # TYPE sbus_commits_total counter\n\
             sbus_commits_total {commits}\n\
             # HELP sbus_conflicts_total Total ACP conflicts (VersionMismatch + TokenConflict)\n\
             # TYPE sbus_conflicts_total counter\n\
             sbus_conflicts_total {conflicts}\n\
             # HELP sbus_cross_shard_stale_total Cross-shard stale reads detected\n\
             # TYPE sbus_cross_shard_stale_total counter\n\
             sbus_cross_shard_stale_total {cross_stale}\n\
             # HELP sbus_shards_total Number of registered shards\n\
             # TYPE sbus_shards_total gauge\n\
             sbus_shards_total {shards}\n"
        )
    }

    // ─────────────────────────────────────────────────────────────────────
    // Lease monitor — constructive liveness for Corollary 2.2
    //
    // Corollary 2.2 (Liveness under Bounded Contention) requires:
    //   (i)  retry budget B ≥ 1 (set via SBUS_RETRY_BUDGET, default 1)
    //   (ii) the lease monitor polls every p = 5 seconds
    //
    // This function makes condition (ii) constructive: after an agent crashes
    // while holding token τᵢ, the monitor detects the orphan within
    // lease_timeout_secs + 5 seconds and releases τᵢ ← ⊥.
    // Therefore at every logical time t + lease_timeout_secs some agent can
    // acquire τᵢ — global progress is guaranteed.
    // ─────────────────────────────────────────────────────────────────────
    pub fn spawn_lease_monitor(self) {
        let lease_secs = self.lease_timeout_secs;
        tokio::spawn(async move {
            let mut ticker = interval(Duration::from_secs(5));
            loop {
                ticker.tick().await;
                let threshold = chrono::Duration::seconds(lease_secs as i64);
                let now = Utc::now();

                for mut entry in self.registry.iter_mut() {
                    let key_str = entry.key().clone();
                    let shard = entry.value_mut();
                    if shard.owner.is_none() {
                        continue;
                    }
                    let acquired_at = match shard.token_acquired_at {
                        Some(t) => t,
                        None    => continue,
                    };
                    if now - acquired_at >= threshold {
                        warn!(
                            key   = key_str.as_str(),
                            owner = ?shard.owner,
                            "lease expired — releasing write-token and rolling back"
                        );
                        if let Some(prev) = shard.delta_log.back() {
                            shard.content = prev.delta.clone();
                        }
                        shard.owner = None;
                        shard.token_acquired_at = None;
                    }
                }
            }
        });
    }
}

impl Default for SBus {
    fn default() -> Self {
        Self::new()
    }
}