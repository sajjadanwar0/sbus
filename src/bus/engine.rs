// src/bus/engine.rs — S-Bus v29 (corrected)
//
// All fixes applied:
//   FIX-1: removed accidentally embedded main.rs content
//   FIX-2: DeltaTooLarge uses len: field
//   FIX-3: ReadSetEntry → (String,u64) before build_effective_read_set
//   FIX-4: WAL crash recovery (rebuild_from_wal)
//   FIX-5: acquire_token/release_token marked dead_code
//   FIX-6: WAL uses direct File::write_all (no BufWriter — SIGKILL-safe)
//   FIX-TTL: SessionExpiredError from build_effective_read_set → HTTP 410

use std::{
    collections::HashMap,
    io::{BufRead, BufReader, Write},
    sync::{Arc, Mutex, RwLock},
    sync::atomic::{AtomicU64, Ordering},
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tokio::time::{interval, Duration};
use tracing::{debug, info, warn};

use crate::bus::{
    registry::{DeliveryLog, SafeMap, SimpleMap, SessionExpiredError},
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

// ── Per-shard naive lock map (deadlock demo) ──────────────────────────────────

type NaiveLockMap = Arc<RwLock<HashMap<String, Arc<Mutex<()>>>>>;

fn get_or_create_naive_lock(map: &NaiveLockMap, key: &str) -> Arc<Mutex<()>> {
    {
        let r = map.read().unwrap();
        if let Some(lk) = r.get(key) { return lk.clone(); }
    }
    let mut w = map.write().unwrap();
    w.entry(key.to_owned()).or_insert_with(|| Arc::new(Mutex::new(()))).clone()
}

// ── WAL — direct File write, no BufWriter ─────────────────────────────────────
//
// FIX-6: BufWriter + SIGKILL loses buffered data.
// Direct write_all() calls write(2) immediately — data in OS page cache survives
// process crash. Power-failure durability requires sync_data() / O_SYNC (omitted).

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct WalEntry {
    pub key:      String,
    pub version:  u64,
    pub agent_id: String,
    pub delta:    String,
    pub goal_tag: String,
    pub ts:       String,
}

pub struct Wal {
    path:   Option<String>,
    writer: Option<Mutex<std::fs::File>>,  // FIX-6: direct File, no BufWriter
}

impl Wal {
    pub fn from_env() -> Self {
        let path = std::env::var("SBUS_WAL_PATH").ok();
        let writer = path.as_deref().and_then(|p| {
            match std::fs::OpenOptions::new().create(true).append(true).open(p) {
                Ok(f)  => { info!("WAL opened for append: {p}"); Some(Mutex::new(f)) }
                Err(e) => { warn!("WAL open failed at {p}: {e} — WAL disabled"); None }
            }
        });
        Self { path, writer }
    }

    /// Append one committed delta. Direct write_all — SIGKILL-safe.
    pub fn append(&self, key: &str, version: u64, agent_id: &str,
                  delta: &str, goal_tag: &str) {
        let Some(ref mx) = self.writer else { return; };
        let entry = WalEntry {
            key: key.to_owned(), version,
            agent_id: agent_id.to_owned(),
            delta: delta.to_owned(),
            goal_tag: goal_tag.to_owned(),
            ts: Utc::now().to_rfc3339(),
        };
        let line = match serde_json::to_string(&entry) {
            Ok(s)  => s + "\n",
            Err(e) => { warn!("WAL serialize error key={key}: {e}"); return; }
        };
        let Ok(mut f) = mx.lock() else {
            warn!("WAL mutex poisoned — skipping write for key={key}");
            return;
        };
        if let Err(e) = f.write_all(line.as_bytes()) {
            warn!("WAL write_all failed key={key} version={version}: {e}");
        }
    }

    /// Read all valid WalEntry lines (earliest first). Skips blank/corrupt lines.
    pub fn replay(path: &str) -> Vec<WalEntry> {
        let file = match std::fs::File::open(path) {
            Ok(f)  => f,
            Err(e) => { info!("WAL not found at {path}: {e} — starting fresh"); return vec![]; }
        };
        let reader = BufReader::new(file);
        let mut entries = Vec::new();
        let mut corrupt = 0usize;
        for (lineno, line_result) in reader.lines().enumerate() {
            match line_result {
                Err(e) => { warn!("WAL I/O error at line {lineno}: {e}"); corrupt += 1; }
                Ok(s) if s.trim().is_empty() => continue,
                Ok(s) => match serde_json::from_str::<WalEntry>(&s) {
                    Ok(entry) => entries.push(entry),
                    Err(e)    => { warn!("WAL parse error at line {lineno}: {e}"); corrupt += 1; }
                },
            }
        }
        info!("WAL replay: {} valid entries, {} skipped", entries.len(), corrupt);
        entries
    }

    pub fn is_enabled(&self) -> bool { self.writer.is_some() }
    pub fn path(&self) -> Option<&str> { self.path.as_deref() }
}

// ── SBus ──────────────────────────────────────────────────────────────────────

#[derive(Clone)]
pub struct SBus {
    pub config:                AcpConfig,
    registry:                  Arc<SafeMap<Shard>>,
    token_registry:            Arc<SimpleMap<TokenEntry>>,
    delivery_log:              Arc<DeliveryLog>,
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
            delivery_log:              Arc::new(DeliveryLog::new()),
            naive_lock_map:            Arc::new(RwLock::new(HashMap::new())),
            commit_counter:            Arc::new(AtomicU64::new(0)),
            conflict_counter:          Arc::new(AtomicU64::new(0)),
            cross_shard_stale_counter: Arc::new(AtomicU64::new(0)),
            max_log_depth,
            lease_timeout_secs,
            wal:                       Arc::new(Wal::from_env()),
        }
    }

    // ── WAL crash recovery ────────────────────────────────────────────────────
    // Call from main() BEFORE binding the HTTP listener.

    pub fn rebuild_from_wal(&self) {
        let path = match self.wal.path() {
            Some(p) => p.to_owned(),
            None    => { info!("WAL disabled — no replay"); return; }
        };
        let entries = Wal::replay(&path);
        if entries.is_empty() {
            info!("WAL replay: no entries — registry starts fresh");
            return;
        }
        let mut latest: HashMap<String, WalEntry> = HashMap::new();
        for entry in &entries {
            let should_update = latest.get(&entry.key)
                .map(|e| entry.version > e.version).unwrap_or(true);
            if should_update { latest.insert(entry.key.clone(), entry.clone()); }
        }
        let mut recovered = 0usize;
        for (key, entry) in &latest {
            let ml = self.max_log_depth;
            if self.registry.with_entry_read(key, |_| ()).is_none() {
                let shard = Shard::new(entry.delta.clone(), entry.goal_tag.clone(), ml);
                self.registry.insert_if_absent(key.clone(), shard);
            }
            self.registry.with_entry(key, |s| {
                if entry.version > s.version {
                    s.content    = entry.delta.clone();
                    s.version    = entry.version;
                    s.updated_at = Utc::now();
                    recovered   += 1;
                    info!(key = key.as_str(), version = entry.version, "WAL: shard recovered");
                }
            });
        }
        info!("WAL rebuild: {} distinct shards, {} updated", latest.len(), recovered);
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

    /// Read a shard. Records delivery in the DeliveryLog when agent_id is non-empty.
    pub fn read_shard(&self, key: &str, agent_id: &str) -> Result<ShardResponse, SyncError> {
        let resp = self.registry
            .with_entry_read(key, |s| ShardResponse::from((key, s)))
            .ok_or_else(|| SyncError::ShardNotFound { key: key.to_owned() })?;
        if !agent_id.is_empty() {
            self.delivery_log.record(agent_id, key, resp.version);
            debug!(key, agent_id, version = resp.version, "delivery recorded");
        }
        Ok(resp)
    }

    pub fn list_shards(&self) -> Vec<String> { self.registry.keys() }

    #[allow(dead_code)]
    fn acquire_token(&self, key: &str, agent_id: &str) -> Result<(), SyncError> {
        self.token_registry
            .insert_if_absent(key.to_owned(), TokenEntry {
                owner: agent_id.to_owned(), acquired_at: Utc::now(),
            })
            .map_err(|existing| SyncError::TokenConflict {
                key: key.to_owned(), owner: existing.owner,
            })
    }

    #[allow(dead_code)]
    fn release_token(&self, key: &str) { self.token_registry.remove(key); }

    // ── ACP commit — Conditional Write-Snapshot Serializability (WSS) ────────
    //
    // Algorithm (write lock held continuously through steps 2-8):
    //   1. Build effective R_hat from DeliveryLog ∪ explicit read_set.
    //      FIX-TTL: if session expired, return SessionExpired (HTTP 410).
    //   2. CrossShardStale: validate all (key,version) in R_hat.
    //   3. VersionMismatch: check primary key version.
    //   4. TokenConflict: check/set ownership token.
    //   5. apply_delta_cfg: update shard content/version/log.
    //   6. WAL append (inside write lock — crash-safe).
    //   7. Release token (same lock scope).

    pub fn commit_delta(&self, req: CommitRequest) -> Result<CommitResponse, SyncError> {
        let cfg = &self.config;

        // Delta size guard (Assumption B, Theorem 1 coordination cost).
        if cfg.max_delta_chars > 0 && req.delta.len() > cfg.max_delta_chars {
            return Err(SyncError::DeltaTooLarge {
                key: req.key.clone(),
                len: req.delta.len(),
                max: cfg.max_delta_chars,
            });
        }

        // Convert ReadSetEntry → (String, u64) for build_effective_read_set.
        let explicit_pairs: Vec<(String, u64)> = req
            .read_set.as_deref().unwrap_or(&[])
            .iter()
            .map(|e| (e.key.clone(), e.version_at_read))
            .collect();

        // Step 1: Build effective read-set BEFORE acquiring write lock.
        // FIX-TTL: returns Err(SessionExpiredError) if session expired.
        let effective_rs: Vec<(String, u64)> = match self.delivery_log
            .build_effective_read_set(&req.agent_id, &req.key, &explicit_pairs)
        {
            Ok(rs) => rs,
            Err(e) => {
                warn!(
                    agent_id = req.agent_id.as_str(),
                    "Commit rejected: DeliveryLog session expired. \
                     Agent must re-read all shards before committing."
                );
                return Err(SyncError::SessionExpired {
                    agent_id: req.agent_id.clone(),
                    message:  e.to_string(),
                });
            }
        };

        // Acquire write lock — held through all steps below.
        self.registry.with_map_write(|map| {

            // Step 2: CrossShardStale validation on entire R_hat.
            for (k, v) in &effective_rs {
                if k == &req.key { continue; }
                let shard = map.get(k.as_str())
                    .ok_or_else(|| SyncError::ShardNotFound { key: k.clone() })?;
                if shard.version != *v {
                    self.cross_shard_stale_counter.fetch_add(1, Ordering::Relaxed);
                    return Err(SyncError::CrossShardStale {
                        key:             k.clone(),
                        version_at_read: *v,
                        current_version: shard.version,
                    });
                }
            }

            // Step 3: Version check on primary key.
            let shard = map.get_mut(req.key.as_str())
                .ok_or_else(|| SyncError::ShardNotFound { key: req.key.clone() })?;

            shard.attempt_count += 1;

            if cfg.enable_version_check && shard.version != req.expected_version {
                shard.conflict_count += 1;
                self.conflict_counter.fetch_add(1, Ordering::Relaxed);
                return Err(SyncError::VersionMismatch {
                    key:      req.key.clone(),
                    expected: req.expected_version,
                    found:    shard.version,
                });
            }

            // Step 4: Ownership token (inline, same lock scope).
            if cfg.enable_ownership_token {
                if let Some(ref owner) = shard.owner {
                    shard.conflict_count += 1;
                    self.conflict_counter.fetch_add(1, Ordering::Relaxed);
                    return Err(SyncError::TokenConflict {
                        key:   req.key.clone(),
                        owner: owner.clone(),
                    });
                }
                shard.owner = Some(req.agent_id.clone());
            }

            // Step 5: Apply delta (increments shard.version).
            apply_delta_cfg(shard, &req.agent_id, &req.delta, cfg, self.max_log_depth);
            let new_version = shard.version;
            let shard_id    = shard.id.clone();
            let goal_tag    = shard.goal_tag.clone();

            // Step 6: WAL append (inside write lock — FIX-6: direct write_all).
            self.wal.append(&req.key, new_version, &req.agent_id, &req.delta, &goal_tag);

            // Step 7: Release token.
            if cfg.enable_ownership_token { shard.owner = None; }

            self.commit_counter.fetch_add(1, Ordering::Relaxed);
            Ok(CommitResponse { new_version, shard_id })
        })
    }

    /// commit_delta_v2: unified with commit_delta.
    pub fn commit_delta_v2(&self, req: CommitRequest) -> Result<CommitResponse, SyncError> {
        self.commit_delta(req)
    }

    /// commit_delta_v2_naive: deadlock demo — insertion-order locking.
    /// Deadlocks at N≥8 under concurrent cross-shard commits without sort.
    pub fn commit_delta_v2_naive(&self, req: CommitRequest) -> Result<CommitResponse, SyncError> {
        let read_set = req.read_set.clone().unwrap_or_default();
        let cfg      = &self.config;

        let mut ordered_keys: Vec<String> = read_set.iter()
            .map(|rs| rs.key.clone())
            .chain(std::iter::once(req.key.clone()))
            .collect::<std::collections::HashSet<_>>()
            .into_iter()
            .collect();
        ordered_keys.sort();  // Remove this sort to reproduce N≥8 deadlock

        let lock_arcs: Vec<Arc<Mutex<()>>> = ordered_keys.iter()
            .map(|k| get_or_create_naive_lock(&self.naive_lock_map, k))
            .collect();
        let _guards: Vec<std::sync::MutexGuard<'_, ()>> = lock_arcs.iter()
            .map(|lk| lk.lock().unwrap())
            .collect();

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

        let res = self.registry.with_entry(&req.key, |s| {
            s.attempt_count += 1;
            if s.version != req.expected_version {
                s.conflict_count += 1;
                self.conflict_counter.fetch_add(1, Ordering::Relaxed);
                return Err(SyncError::VersionMismatch {
                    key: req.key.clone(), expected: req.expected_version, found: s.version,
                });
            }
            apply_delta(s, &req.agent_id, &req.delta, self.max_log_depth);
            let (nv, sid) = (s.version, s.id.clone());
            self.commit_counter.fetch_add(1, Ordering::Relaxed);
            Ok(CommitResponse { new_version: nv, shard_id: sid })
        });
        match res {
            None    => Err(SyncError::ShardNotFound { key: req.key.clone() }),
            Some(r) => r,
        }
    }

    // ── Rollback ──────────────────────────────────────────────────────────────

    pub fn rollback(&self, req: RollbackRequest) -> Result<ShardResponse, SyncError> {
        let key     = req.key.clone();
        let ml      = self.max_log_depth;
        let tok_reg = self.token_registry.clone();

        self.registry.with_map_write(|map| {
            if req.check_token {
                let snap = tok_reg.snapshot();
                if let Some((_, tok)) = snap.iter().find(|(k, _)| k.as_str() == key.as_str()) {
                    return Err(SyncError::RollbackTokenConflict {
                        key: key.clone(), owner: tok.owner.clone(),
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
            "wal_path":                self.wal.path().unwrap_or("disabled"),
            "delivery_log": {
                "tracked_agents":   self.delivery_log.agent_count(),
                "total_deliveries": self.delivery_log.total_entries(),
            },
            "acp_config": {
                "enable_ownership_token": self.config.enable_ownership_token,
                "enable_version_check":   self.config.enable_version_check,
                "enable_delta_log":       self.config.enable_delta_log,
                "retry_budget":           self.config.retry_budget,
                "max_delta_chars":        self.config.max_delta_chars,
            },
        })
    }

    pub fn prometheus_metrics(&self) -> String {
        let c  = self.commit_counter.load(Ordering::Relaxed);
        let f  = self.conflict_counter.load(Ordering::Relaxed);
        let s  = self.cross_shard_stale_counter.load(Ordering::Relaxed);
        let da = self.delivery_log.agent_count();
        let de = self.delivery_log.total_entries();
        format!(
            "# TYPE sbus_commits_total counter\nsbus_commits_total {c}\n\
             # TYPE sbus_conflicts_total counter\nsbus_conflicts_total {f}\n\
             # TYPE sbus_cross_shard_stale_total counter\nsbus_cross_shard_stale_total {s}\n\
             # TYPE sbus_shards_total gauge\nsbus_shards_total {}\n\
             # TYPE sbus_tokens_held gauge\nsbus_tokens_held {}\n\
             # TYPE sbus_delivery_tracked_agents gauge\nsbus_delivery_tracked_agents {da}\n\
             # TYPE sbus_delivery_total_entries gauge\nsbus_delivery_total_entries {de}\n",
            self.registry.len(), self.token_registry.len()
        )
    }

    // ── Lease monitor ─────────────────────────────────────────────────────────

    pub fn spawn_lease_monitor(&self) {
        let tr = self.token_registry.clone();
        let dl = self.delivery_log.clone();
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
                dl.evict_stale();
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

fn apply_delta_cfg(s: &mut Shard, agent_id: &str, delta: &str,
                   cfg: &AcpConfig, max_log: usize) {
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