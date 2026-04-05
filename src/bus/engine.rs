// src/bus/engine.rs — S-Bus ACP v19.
//
// Changes from v18:
//  FIX-1: commit_v2_naive now uses genuine per-shard Mutex locks in INSERTION
//          ORDER (not sorted), enabling deadlock at N>=2 with overlapping shards.
//          This re-enables the deadlock demonstration retracted in the paper.
//  FIX-2: rollback() performs token check inside a single with_map_write closure
//          (eliminates TOCTOU window between token snapshot and version restore).
//          COMPILE FIX: with_map_write returns R (not Option<R>); removed the
//          incorrect match { None=>..., Some(r)=>r } pattern.
//          BORROW FIX: extract tok_reg Arc clone before entering with_map_write
//          so the closure does not double-borrow `self`.
//  FIX-3: WAL stub wired in via Wal struct — enable with SBUS_WAL_PATH env var.
//  All other guarantees unchanged. Zero unsafe blocks.

use std::{
    collections::HashMap,
    io::{BufWriter, Write},
    sync::{Arc, Mutex, RwLock},
    sync::atomic::{AtomicU64, Ordering},
};
use chrono::{DateTime, Utc};
use tokio::time::{interval, Duration};
use tracing::{debug, info, warn};

use crate::bus::{
    registry::{SafeMap, SimpleMap},
    types::{
        AcpConfig, CommitRequest, CommitResponse, CreateShardRequest,
        DeltaEntry, RollbackRequest, Shard, ShardResponse, SyncError,
    },
};

// ── Token entry ───────────────────────────────────────────────────────────────

#[derive(Clone, Debug)]
pub struct TokenEntry {
    pub owner:       String,
    pub acquired_at: DateTime<Utc>,
}

// ── Per-shard naive lock map (FIX-1) ─────────────────────────────────────────
//
// Used ONLY by commit_v2_naive. Keys are acquired in INSERTION ORDER (not
// sorted) to demonstrate the deadlock the sorted-lock design prevents.

type NaiveLockMap = Arc<RwLock<HashMap<String, Arc<Mutex<()>>>>>;

fn get_or_create_naive_lock(map: &NaiveLockMap, key: &str) -> Arc<Mutex<()>> {
    { // Fast read path
        let r = map.read().unwrap();
        if let Some(lk) = r.get(key) { return lk.clone(); }
    }
    // Slow write path
    let mut w = map.write().unwrap();
    w.entry(key.to_owned())
        .or_insert_with(|| Arc::new(Mutex::new(())))
        .clone()
}

// ── WAL stub (FIX-3) ─────────────────────────────────────────────────────────
// Enable: SBUS_WAL_PATH=/path/to/wal.log
// Format: newline-delimited JSON per delta.

struct Wal {
    writer: Option<Mutex<BufWriter<std::fs::File>>>,
}

impl Wal {
    fn from_env() -> Self {
        let writer = std::env::var("SBUS_WAL_PATH").ok().and_then(|path| {
            std::fs::OpenOptions::new()
                .create(true).append(true).open(&path).ok()
                .map(|f| Mutex::new(BufWriter::new(f)))
        });
        Self { writer }
    }

    fn append(&self, key: &str, version: u64, agent_id: &str, delta: &str) {
        if let Some(ref mx) = self.writer {
            let entry = serde_json::json!({
                "key": key, "version": version,
                "agent_id": agent_id, "delta": delta,
                "ts": Utc::now().to_rfc3339(),
            });
            if let Ok(mut w) = mx.lock() {
                let _ = writeln!(w, "{entry}");
                let _ = w.flush();
            }
        }
    }

    fn is_enabled(&self) -> bool { self.writer.is_some() }
}

// ── SBus ─────────────────────────────────────────────────────────────────────

#[derive(Clone)]
pub struct SBus {
    pub config:                AcpConfig,
    registry:                  Arc<SafeMap<Shard>>,
    token_registry:            Arc<SimpleMap<TokenEntry>>,
    naive_lock_map:            NaiveLockMap,
    commit_counter:            Arc<AtomicU64>,
    conflict_counter:          Arc<AtomicU64>,
    cross_shard_stale_counter: Arc<AtomicU64>,
    max_log_depth:             usize,
    pub lease_timeout_secs:    u64,
    wal:                       Arc<Wal>,
}

impl SBus {
    pub fn new() -> Self { Self::with_options(1_000, 30) }

    pub fn with_options(max_log_depth: usize, lease_timeout_secs: u64) -> Self {
        Self {
            config:                    AcpConfig::from_env(),
            registry:                  Arc::new(SafeMap::new()),
            token_registry:            Arc::new(SimpleMap::new()),
            naive_lock_map:            Arc::new(RwLock::new(HashMap::new())),
            commit_counter:            Arc::new(AtomicU64::new(0)),
            conflict_counter:          Arc::new(AtomicU64::new(0)),
            cross_shard_stale_counter: Arc::new(AtomicU64::new(0)),
            max_log_depth,
            lease_timeout_secs,
            wal:                       Arc::new(Wal::from_env()),
        }
    }

    // ── CRUD ──────────────────────────────────────────────────────────────────

    pub fn create_shard(&self, req: CreateShardRequest) -> Result<ShardResponse, SyncError> {
        let shard = Shard::new(req.content, req.goal_tag, self.max_log_depth);
        if !self.registry.insert_if_absent(req.key.clone(), shard) {
            return Err(SyncError::ShardAlreadyExists { key: req.key });
        }
        self.registry
            .with_entry_read(&req.key, |s| ShardResponse::from((req.key.as_str(), s)))
            .ok_or_else(|| SyncError::ShardNotFound { key: req.key })
    }

    pub fn read_shard(&self, key: &str) -> Result<ShardResponse, SyncError> {
        self.registry
            .with_entry_read(key, |s| ShardResponse::from((key, s)))
            .ok_or_else(|| SyncError::ShardNotFound { key: key.to_owned() })
    }

    pub fn list_shards(&self) -> Vec<String> { self.registry.keys() }

    // ── Token helpers ─────────────────────────────────────────────────────────

    fn acquire_token(&self, key: &str, agent_id: &str) -> Result<(), SyncError> {
        self.token_registry
            .insert_if_absent(key.to_owned(), TokenEntry {
                owner: agent_id.to_owned(), acquired_at: Utc::now(),
            })
            .map_err(|existing| SyncError::TokenConflict {
                key: key.to_owned(), owner: existing.owner,
            })
    }

    fn release_token(&self, key: &str) { self.token_registry.remove(key); }

    // ── ACP commit — single RwLock, deadlock-free ─────────────────────────────

    pub fn commit_delta(&self, req: CommitRequest) -> Result<CommitResponse, SyncError> {
        let cfg = &self.config;

        if cfg.max_delta_chars > 0 && req.delta.len() > cfg.max_delta_chars {
            return Err(SyncError::DeltaTooLarge {
                key: req.key.clone(), len: req.delta.len(), max: cfg.max_delta_chars,
            });
        }

        // ── Multi-shard path ──────────────────────────────────────────────────
        if let Some(ref read_set) = req.read_set {
            if !read_set.is_empty() {
                let req_key  = req.key.clone();
                let agent_id = req.agent_id.clone();
                let delta    = req.delta.clone();
                let exp_ver  = req.expected_version;
                let rs       = read_set.clone();
                let stale    = self.cross_shard_stale_counter.clone();
                let conflict = self.conflict_counter.clone();
                let commits  = self.commit_counter.clone();
                let tok_reg  = self.token_registry.clone();
                let wal      = self.wal.clone();
                let max_log  = self.max_log_depth;
                let use_tok  = cfg.enable_ownership_token;

                // with_map_write acquires ONE exclusive RwLock write lock.
                // Returns Result<CommitResponse, SyncError> directly (not Option).
                return self.registry.with_map_write(|map| {
                    for rs_entry in &rs {
                        if rs_entry.key == req_key { continue; }
                        let s = map.get(&rs_entry.key).ok_or_else(|| {
                            SyncError::ShardNotFound { key: rs_entry.key.clone() }
                        })?;
                        if s.version != rs_entry.version_at_read {
                            stale.fetch_add(1, Ordering::Relaxed);
                            return Err(SyncError::CrossShardStale {
                                key:             rs_entry.key.clone(),
                                version_at_read: rs_entry.version_at_read,
                                current_version: s.version,
                            });
                        }
                    }

                    let shard = map.get_mut(&req_key).ok_or_else(|| {
                        SyncError::ShardNotFound { key: req_key.clone() }
                    })?;
                    shard.attempt_count += 1;

                    if shard.version != exp_ver {
                        shard.conflict_count += 1;
                        conflict.fetch_add(1, Ordering::Relaxed);
                        return Err(SyncError::VersionMismatch {
                            key: req_key.clone(), expected: exp_ver, found: shard.version,
                        });
                    }

                    if use_tok {
                        tok_reg.insert_if_absent(req_key.clone(), TokenEntry {
                            owner: agent_id.clone(), acquired_at: Utc::now(),
                        }).map_err(|e| SyncError::TokenConflict {
                            key: req_key.clone(), owner: e.owner,
                        })?;
                    }

                    wal.append(&req_key, shard.version + 1, &agent_id, &delta);
                    apply_delta(shard, &agent_id, &delta, max_log);
                    let (nv, sid) = (shard.version, shard.id.clone());
                    if use_tok { tok_reg.remove(&req_key); }
                    commits.fetch_add(1, Ordering::Relaxed);
                    debug!(key = req_key.as_str(), new_version = nv, "multi-shard commit");
                    Ok(CommitResponse { new_version: nv, shard_id: sid })
                });
            }
        }

        // ── Single-shard path — Phase 1: version pre-check ───────────────────
        // with_entry returns Option<Result<...>>; None means key not found.
        {
            let r = self.registry.with_entry(&req.key, |s| {
                s.attempt_count += 1;
                if cfg.enable_version_check && s.version != req.expected_version {
                    s.conflict_count += 1;
                    self.conflict_counter.fetch_add(1, Ordering::Relaxed);
                    return Err(SyncError::VersionMismatch {
                        key: req.key.clone(),
                        expected: req.expected_version,
                        found: s.version,
                    });
                }
                Ok(())
            });
            match r {
                None    => return Err(SyncError::ShardNotFound { key: req.key.clone() }),
                Some(r) => r?,
            }
        }

        // Phase 2: token acquisition.
        if cfg.enable_ownership_token {
            self.acquire_token(&req.key, &req.agent_id).map_err(|e| {
                self.conflict_counter.fetch_add(1, Ordering::Relaxed);
                e
            })?;
        }

        // Phase 3: re-check + apply. with_entry returns Option<Result<...>>.
        let res = self.registry.with_entry(&req.key, |s| {
            s.attempt_count += 1;
            if cfg.enable_version_check && s.version != req.expected_version {
                s.conflict_count += 1;
                self.conflict_counter.fetch_add(1, Ordering::Relaxed);
                return Err(SyncError::VersionMismatch {
                    key: req.key.clone(),
                    expected: req.expected_version,
                    found: s.version,
                });
            }
            self.wal.append(&req.key, s.version + 1, &req.agent_id, &req.delta);
            apply_delta_cfg(s, &req.agent_id, &req.delta, cfg, self.max_log_depth);
            let (nv, sid) = (s.version, s.id.clone());
            self.commit_counter.fetch_add(1, Ordering::Relaxed);
            debug!(key = req.key.as_str(), new_version = nv, "single-shard commit");
            Ok(CommitResponse { new_version: nv, shard_id: sid })
        });

        if cfg.enable_ownership_token { self.release_token(&req.key); }
        match res {
            None    => Err(SyncError::ShardNotFound { key: req.key.clone() }),
            Some(r) => r,
        }
    }

    // ── commit_v2_naive — FIX-1: genuine insertion-order per-shard locking ────
    //
    // Acquires one Arc<Mutex<()>> per shard in the INSERTION ORDER of the
    // read-set (not sorted). When two transactions overlap shards in opposite
    // orders, each holds one Mutex and blocks waiting for the other → deadlock.
    //
    // Example (deadlock at N=2):
    //   Thread A: key="x", read_set=[{key:"y"}] → acquires x_lock, waits y_lock
    //   Thread B: key="y", read_set=[{key:"x"}] → acquires y_lock, waits x_lock
    //
    // Use naive_deadlock_test.py to reproduce. DO NOT use in production.

    pub fn commit_v2_naive(&self, req: CommitRequest) -> Result<CommitResponse, SyncError> {
        let cfg = &self.config;

        if cfg.max_delta_chars > 0 && req.delta.len() > cfg.max_delta_chars {
            return Err(SyncError::DeltaTooLarge {
                key: req.key.clone(), len: req.delta.len(), max: cfg.max_delta_chars,
            });
        }

        let read_set = req.read_set.clone().unwrap_or_default();

        // Build insertion-order key list — deliberately NOT sorted.
        let mut ordered_keys: Vec<String> = read_set.iter()
            .filter(|r| r.key != req.key)
            .map(|r| r.key.clone())
            .collect();
        ordered_keys.push(req.key.clone());

        // Collect Arc<Mutex<()>> for each key (fast, no contention here).
        let lock_arcs: Vec<Arc<Mutex<()>>> = ordered_keys.iter()
            .map(|k| get_or_create_naive_lock(&self.naive_lock_map, k))
            .collect();

        // Acquire each Mutex sequentially in insertion order.
        // Deadlock possible if another thread holds a complementary subset.
        // _guards keeps all MutexGuards alive until end of function.
        let _guards: Vec<std::sync::MutexGuard<'_, ()>> = lock_arcs.iter()
            .map(|lk| lk.lock().unwrap())
            .collect();

        // Cross-shard validation (with naive locks held).
        for rs_entry in &read_set {
            if rs_entry.key == req.key { continue; }
            let ver = self.registry
                .with_entry_read(&rs_entry.key, |s| s.version)
                .ok_or_else(|| SyncError::ShardNotFound { key: rs_entry.key.clone() })?;
            if ver != rs_entry.version_at_read {
                self.cross_shard_stale_counter.fetch_add(1, Ordering::Relaxed);
                return Err(SyncError::CrossShardStale {
                    key:             rs_entry.key.clone(),
                    version_at_read: rs_entry.version_at_read,
                    current_version: ver,
                });
            }
        }

        // Version check + apply. with_entry returns Option<Result<...>>.
        let res = self.registry.with_entry(&req.key, |s| {
            s.attempt_count += 1;
            if s.version != req.expected_version {
                s.conflict_count += 1;
                self.conflict_counter.fetch_add(1, Ordering::Relaxed);
                return Err(SyncError::VersionMismatch {
                    key: req.key.clone(),
                    expected: req.expected_version,
                    found: s.version,
                });
            }
            apply_delta(s, &req.agent_id, &req.delta, self.max_log_depth);
            let (nv, sid) = (s.version, s.id.clone());
            self.commit_counter.fetch_add(1, Ordering::Relaxed);
            Ok(CommitResponse { new_version: nv, shard_id: sid })
        });
        // _guards dropped here — all naive locks released.
        match res {
            None    => Err(SyncError::ShardNotFound { key: req.key.clone() }),
            Some(r) => r,
        }
    }

    // ── Rollback — FIX-2: TOCTOU-safe + compile-correct ──────────────────────
    //
    // BUG IN V18: rollback() called token_registry.snapshot() THEN acquired the
    // registry write lock separately. An agent could commit in that window,
    // making the rollback silently overwrite an in-flight commit.
    //
    // COMPILE BUG IN engine_v19 first draft: with_map_write returns R (not
    // Option<R>). The old `match res { None => ..., Some(r) => r }` pattern
    // from with_entry does NOT apply to with_map_write. Fixed: return
    // with_map_write(...) directly.
    //
    // BORROW BUG: calling self.token_registry inside a closure passed to
    // self.registry.with_map_write() causes double-borrow of self. Fixed:
    // extract tok_reg = self.token_registry.clone() before the closure.

    pub fn rollback(&self, req: RollbackRequest) -> Result<ShardResponse, SyncError> {
        let key     = req.key.clone();
        let ml      = self.max_log_depth;
        // Clone Arc BEFORE with_map_write to avoid double-borrowing self.
        let tok_reg = self.token_registry.clone();

        // with_map_write returns Result<ShardResponse, SyncError> directly.
        // Do NOT wrap in match { None=>..., Some(r)=>r } — that is with_entry pattern.
        self.registry.with_map_write(|map| {
            // Token check is now INSIDE the write lock — no TOCTOU window.
            if req.check_token {
                let snap = tok_reg.snapshot();
                if let Some((_, tok)) = snap.iter().find(|(k, _)| k.as_str() == key.as_str()) {
                    return Err(SyncError::RollbackTokenConflict {
                        key:   key.clone(),
                        owner: tok.owner.clone(),
                    });
                }
            }

            let shard = map.get_mut(&key)
                .ok_or_else(|| SyncError::ShardNotFound { key: key.clone() })?;

            let tgt = shard.delta_log.iter()
                .find(|e| e.version == req.target_version)
                .ok_or_else(|| SyncError::Internal {
                    msg: format!("version {} not in delta log for key={}", req.target_version, key),
                })?
                .clone();

            let prev_hash    = Shard::content_address(&shard.content);
            shard.content    = tgt.delta.clone();
            shard.version   += 1;
            shard.updated_at = Utc::now();
            shard.delta_log.push_back(DeltaEntry {
                version:      shard.version,
                agent_id:     format!("rollback:{}", req.agent_id),
                delta:        tgt.delta,
                prev_hash,
                committed_at: Utc::now(),
            });
            if shard.delta_log.len() > ml { shard.delta_log.pop_front(); }
            info!(key = key.as_str(), target = req.target_version, "rollback applied");
            Ok(ShardResponse::from((key.as_str(), &*shard)))
        })
        // with_map_write returns the closure return value directly — Result<ShardResponse, SyncError>.
    }

    // ── Stats ─────────────────────────────────────────────────────────────────

    pub fn stats(&self) -> serde_json::Value {
        let tc = self.commit_counter.load(Ordering::Relaxed);
        let tf = self.conflict_counter.load(Ordering::Relaxed);
        let cs = self.cross_shard_stale_counter.load(Ordering::Relaxed);
        let ta = tc + tf;
        serde_json::json!({
            "total_shards":            self.registry.len(),
            "total_commits":           tc,
            "total_conflicts":         tf,
            "cross_shard_stale_count": cs,
            "total_attempts":          ta,
            "scr": if ta > 0 { tf as f64 / ta as f64 } else { 0.0 },
            "tokens_held":             self.token_registry.len(),
            "lease_timeout_secs":      self.lease_timeout_secs,
            "wal_enabled":             self.wal.is_enabled(),
            "acp_config": {
                "enable_ownership_token": self.config.enable_ownership_token,
                "enable_version_check":   self.config.enable_version_check,
                "enable_delta_log":       self.config.enable_delta_log,
                "retry_budget":           self.config.retry_budget,
            },
        })
    }

    pub fn prometheus_metrics(&self) -> String {
        let c = self.commit_counter.load(Ordering::Relaxed);
        let f = self.conflict_counter.load(Ordering::Relaxed);
        let s = self.cross_shard_stale_counter.load(Ordering::Relaxed);
        format!(
            "# TYPE sbus_commits_total counter\nsbus_commits_total {c}\n\
             # TYPE sbus_conflicts_total counter\nsbus_conflicts_total {f}\n\
             # TYPE sbus_cross_shard_stale_total counter\nsbus_cross_shard_stale_total {s}\n\
             # TYPE sbus_shards_total gauge\nsbus_shards_total {}\n\
             # TYPE sbus_tokens_held gauge\nsbus_tokens_held {}\n",
            self.registry.len(), self.token_registry.len()
        )
    }

    pub fn spawn_lease_monitor(&self) {
        let tr = self.token_registry.clone();
        let ls = self.lease_timeout_secs;
        tokio::spawn(async move {
            let mut tick = interval(Duration::from_secs(5));
            loop {
                tick.tick().await;
                let threshold = chrono::Duration::seconds(ls as i64);
                let now = Utc::now();
                tr.retain(|key, entry| {
                    if entry.acquired_at + threshold <= now {
                        warn!(key, owner = entry.owner.as_str(), "lease expired — token released");
                        false
                    } else { true }
                });
            }
        });
    }
}

impl Default for SBus { fn default() -> Self { Self::new() } }

// ── Private helpers ───────────────────────────────────────────────────────────

fn apply_delta(s: &mut Shard, agent_id: &str, delta: &str, max_log: usize) {
    let prev = Shard::content_address(&s.content);
    s.content = delta.to_owned(); s.version += 1; s.updated_at = Utc::now();
    s.delta_log.push_back(DeltaEntry {
        version: s.version, agent_id: agent_id.to_owned(),
        delta: delta.to_owned(), prev_hash: prev, committed_at: Utc::now(),
    });
    if s.delta_log.len() > max_log { s.delta_log.pop_front(); }
}

fn apply_delta_cfg(s: &mut Shard, agent_id: &str, delta: &str, cfg: &AcpConfig, max_log: usize) {
    let prev = Shard::content_address(&s.content);
    s.content = delta.to_owned(); s.version += 1; s.updated_at = Utc::now();
    if cfg.enable_delta_log {
        s.delta_log.push_back(DeltaEntry {
            version: s.version, agent_id: agent_id.to_owned(),
            delta: delta.to_owned(), prev_hash: prev, committed_at: Utc::now(),
        });
        if s.delta_log.len() > max_log { s.delta_log.pop_front(); }
    }
}