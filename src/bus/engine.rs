// src/bus/engine.rs — S-Bus v35 distributed
// ORI within-node: per-shard Mutex (ShardRegistry), cross-shard via global lock
// ORI: per-shard write lock, correct under Raft total order

use std::{
    collections::HashMap,
    io::{BufRead, BufReader, Write},
    sync::{Arc, atomic::{AtomicU64, Ordering}},
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::time::{interval, Duration};
use tracing::{debug, info, warn};

use crate::bus::{
    registry::{DeliveryLog, ShardRegistry, SimpleMap},
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

// ── WAL ───────────────────────────────────────────────────────────────────────
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct WalEntry {
    pub key: String, pub version: u64, pub agent_id: String,
    pub delta: String, pub goal_tag: String, pub ts: String,
}
pub struct Wal {
    path:   Option<String>,
    writer: Option<std::sync::Mutex<std::fs::File>>,
}
impl Wal {
    pub fn from_env() -> Self {
        let path = std::env::var("SBUS_WAL_PATH").ok();
        let writer = path.as_deref().and_then(|p| {
            match std::fs::OpenOptions::new().create(true).append(true).open(p) {
                Ok(f)  => { info!("WAL: {}", p); Some(std::sync::Mutex::new(f)) }
                Err(e) => { warn!("WAL open failed {}: {}", p, e); None }
            }
        });
        Self { path, writer }
    }
    pub fn append(&self, key: &str, version: u64, agent_id: &str, delta: &str, goal_tag: &str) {
        let Some(ref mx) = self.writer else { return; };
        let e = WalEntry {
            key: key.to_owned(), version, agent_id: agent_id.to_owned(),
            delta: delta.to_owned(), goal_tag: goal_tag.to_owned(),
            ts: Utc::now().to_rfc3339(),
        };
        if let Ok(mut line) = serde_json::to_string(&e).map(|s| s + "\n") {
            if let Ok(mut f) = mx.lock() { let _ = f.write_all(line.as_bytes()); }
        }
    }
    pub fn replay(path: &str) -> Vec<WalEntry> {
        let file = match std::fs::File::open(path) {
            Ok(f) => f, Err(_) => return vec![],
        };
        BufReader::new(file).lines()
            .filter_map(|l| l.ok())
            .filter(|s| !s.trim().is_empty())
            .filter_map(|s| serde_json::from_str::<WalEntry>(&s).ok())
            .collect()
    }
    pub fn is_enabled(&self) -> bool { self.writer.is_some() }
    pub fn path(&self) -> Option<&str> { self.path.as_deref() }
}

// ── SBus ──────────────────────────────────────────────────────────────────────
#[derive(Clone)]
pub struct SBus {
    pub config:       AcpConfig,
    registry:         Arc<ShardRegistry>,
    token_registry:   Arc<SimpleMap<TokenEntry>>,
    delivery_log:     Arc<DeliveryLog>,
    commits:          Arc<AtomicU64>,
    conflicts:        Arc<AtomicU64>,
    cross_stale:      Arc<AtomicU64>,
    max_log_depth:    usize,
    pub lease_secs:   u64,
    wal:              Arc<Wal>,
}

impl SBus {
    pub fn new() -> Self { Self::with_options(1_000, 3600) }
    pub fn with_options(max_log_depth: usize, lease_secs: u64) -> Self {
        Self {
            config: AcpConfig::from_env(),
            registry: Arc::new(ShardRegistry::new()),
            token_registry: Arc::new(SimpleMap::new()),
            delivery_log: Arc::new(DeliveryLog::new()),
            commits: Arc::new(AtomicU64::new(0)),
            conflicts: Arc::new(AtomicU64::new(0)),
            cross_stale: Arc::new(AtomicU64::new(0)),
            max_log_depth, lease_secs,
            wal: Arc::new(Wal::from_env()),
        }
    }

    // ── WAL recovery ──────────────────────────────────────────────────────────
    pub fn rebuild_from_wal(&self) {
        let path = match self.wal.path() { Some(p) => p.to_owned(), None => return };
        let entries = Wal::replay(&path);
        if entries.is_empty() { return; }
        let mut latest: HashMap<String, WalEntry> = HashMap::new();
        for e in &entries {
            if latest.get(&e.key).map(|l| e.version > l.version).unwrap_or(true) {
                latest.insert(e.key.clone(), e.clone());
            }
        }
        let mut recovered = 0usize;
        for (key, entry) in &latest {
            let ml = self.max_log_depth;
            let shard = Shard::new(entry.delta.clone(), entry.goal_tag.clone(), ml);
            self.registry.insert_if_absent(key.clone(), shard);
            self.registry.with_write(key, |s| {
                if entry.version > s.version {
                    s.content = entry.delta.clone();
                    s.version = entry.version;
                    s.updated_at = Utc::now();
                    recovered += 1;
                }
            });
        }
        info!("WAL rebuilt: {} shards, {} updated", latest.len(), recovered);
    }

    // ── CRUD ──────────────────────────────────────────────────────────────────
    pub fn create_shard(&self, req: CreateShardRequest) -> Result<ShardResponse, SyncError> {
        let shard = Shard::new(req.content, req.goal_tag, self.max_log_depth);
        if !self.registry.insert_if_absent(req.key.clone(), shard) {
            return Err(SyncError::ShardAlreadyExists { key: req.key });
        }
        self.registry.with_read(&req.key, |s| ShardResponse::from((req.key.as_str(), s)))
            .ok_or_else(|| SyncError::ShardNotFound { key: req.key })
    }

    pub fn read_shard(&self, key: &str, agent_id: &str) -> Result<ShardResponse, SyncError> {
        let resp = self.registry.with_read(key, |s| ShardResponse::from((key, s)))
            .ok_or_else(|| SyncError::ShardNotFound { key: key.to_owned() })?;
        if !agent_id.is_empty() {
            self.delivery_log.record(agent_id, key, resp.version);
            debug!(key, agent_id, version = resp.version, "delivery recorded");
        }
        Ok(resp)
    }

    pub fn list_shards(&self) -> Vec<String> { self.registry.keys() }

    // ── ACP commit ────────────────────────────────────────────────────────────
    pub fn commit_delta(&self, req: CommitRequest) -> Result<CommitResponse, SyncError> {
        let cfg = &self.config;
        if cfg.max_delta_chars > 0 && req.delta.len() > cfg.max_delta_chars {
            return Err(SyncError::DeltaTooLarge {
                key: req.key.clone(), len: req.delta.len(), max: cfg.max_delta_chars,
            });
        }
        let explicit: Vec<(String,u64)> = req.read_set.as_deref().unwrap_or(&[])
            .iter().map(|e| (e.key.clone(), e.version_at_read)).collect();
        let eff_rs = match self.delivery_log
            .build_effective_read_set(&req.agent_id, &req.key, &explicit) {
            Ok(rs) => rs,
            Err(e) => return Err(SyncError::SessionExpired {
                agent_id: req.agent_id.clone(), message: e.to_string(),
            }),
        };
        // Cross-shard validation: check each read-set shard version
        for (k, v) in &eff_rs {
            if k == &req.key { continue; }
            let cur = self.registry.with_read(k, |s| s.version)
                .ok_or_else(|| SyncError::ShardNotFound { key: k.clone() })?;
            if cur != *v {
                self.cross_stale.fetch_add(1, Ordering::Relaxed);
                return Err(SyncError::CrossShardStale {
                    key: k.clone(), version_at_read: *v, current_version: cur,
                });
            }
        }
        // Primary commit
        let result = self.registry.with_write(&req.key, |s| {
            s.attempt_count += 1;
            if cfg.enable_version_check && s.version != req.expected_version {
                s.conflict_count += 1;
                self.conflicts.fetch_add(1, Ordering::Relaxed);
                return Err(SyncError::VersionMismatch {
                    key: req.key.clone(), expected: req.expected_version, found: s.version,
                });
            }
            if cfg.enable_ownership_token {
                if let Some(ref owner) = s.owner {
                    if owner.starts_with("2pc:") {
                        s.owner = None; // stale lock cleanup
                    } else {
                        s.conflict_count += 1;
                        self.conflicts.fetch_add(1, Ordering::Relaxed);
                        return Err(SyncError::TokenConflict {
                            key: req.key.clone(), owner: owner.clone(),
                        });
                    }
                }
                s.owner = Some(req.agent_id.clone());
            }
            apply_delta(s, &req.agent_id, &req.delta, cfg, self.max_log_depth);
            let nv = s.version; let sid = s.id.clone(); let goal = s.goal_tag.clone();
            if cfg.enable_ownership_token { s.owner = None; }
            self.wal.append(&req.key, nv, &req.agent_id, &req.delta, &goal);
            self.commits.fetch_add(1, Ordering::Relaxed);
            Ok(CommitResponse { new_version: nv, shard_id: sid })
        });
        result.unwrap_or_else(|| Err(SyncError::ShardNotFound { key: req.key }))
    }

    pub fn commit_delta_v2(&self, req: CommitRequest) -> Result<CommitResponse, SyncError> {
        self.commit_delta(req)
    }

    // ── Rollback ──────────────────────────────────────────────────────────────
    pub fn rollback(&self, req: RollbackRequest) -> Result<ShardResponse, SyncError> {
        let key = req.key.clone();
        let ml = self.max_log_depth;
        let result = self.registry.with_write(&key, |s| {
            let tgt = s.delta_log.iter()
                .find(|e| e.version == req.target_version)
                .ok_or_else(|| SyncError::Internal {
                    msg: format!("version {} not in log for {key}", req.target_version),
                })?.clone();
            let prev = Shard::content_address(&s.content);
            s.content = tgt.delta.clone(); s.version += 1; s.updated_at = Utc::now();
            s.delta_log.push_back(DeltaEntry {
                version: s.version, agent_id: format!("rollback:{}", req.agent_id),
                delta: tgt.delta, prev_hash: prev, committed_at: Utc::now(),
            });
            if s.delta_log.len() > ml { s.delta_log.pop_front(); }
            Ok(ShardResponse::from((key.as_str(), &*s)))
        });
        result.unwrap_or_else(|| Err(SyncError::ShardNotFound { key }))
    }

    // ── Stats ─────────────────────────────────────────────────────────────────
    pub fn stats(&self) -> serde_json::Value {
        let tc = self.commits.load(Ordering::Relaxed);
        let tf = self.conflicts.load(Ordering::Relaxed);
        let cs = self.cross_stale.load(Ordering::Relaxed);
        let ta = tc + tf;
        json!({
            "total_shards":   self.registry.len(),
            "total_commits":  tc,
            "total_conflicts": tf,
            "cross_shard_stale": cs,
            "total_attempts": ta,
            "scr": if ta>0 { tf as f64/ta as f64 } else { 0.0 },
            "wal_enabled":    self.wal.is_enabled(),
            "delivery_log": {
                "agents":      self.delivery_log.agent_count(),
                "deliveries":  self.delivery_log.total_entries(),
            },
        })
    }

    pub fn prometheus_metrics(&self) -> String {
        let c = self.commits.load(Ordering::Relaxed);
        let f = self.conflicts.load(Ordering::Relaxed);
        let s = self.cross_stale.load(Ordering::Relaxed);
        format!(
            "# TYPE sbus_commits_total counter\nsbus_commits_total {c}\n             # TYPE sbus_conflicts_total counter\nsbus_conflicts_total {f}\n             # TYPE sbus_cross_stale_total counter\nsbus_cross_stale_total {s}\n             # TYPE sbus_shards_total gauge\nsbus_shards_total {}\n",
            self.registry.len()
        )
    }

    // ── Admin helpers ─────────────────────────────────────────────────────────
    pub fn touch_delivery_log(&self, agent_id: &str) {
        self.delivery_log.touch(agent_id);
    }
    pub fn inject_stale_delivery(&self, agent_id: &str, key: &str, v: u64) {
        self.delivery_log.record(agent_id, key, v);
    }
    pub fn get_shard_version(&self, key: &str) -> Option<u64> {
        self.registry.with_read(key, |s| s.version)
    }
    pub fn dump_delivery_log(&self) -> serde_json::Value {
        let snap = self.delivery_log.snapshot_all();
        let agents: serde_json::Map<String, serde_json::Value> = snap.into_iter().map(|(a, es)| {
            let m: serde_json::Map<String, serde_json::Value> = es.into_iter().map(|(k, e)| {
                (k, json!({"version": e.version, "age_secs": e.delivered_at.elapsed().as_secs_f64()}))
            }).collect();
            (a, serde_json::Value::Object(m))
        }).collect();
        let n = agents.len();
        let d: usize = agents.values().filter_map(|v| v.as_object()).map(|o| o.len()).sum();
        json!({"agents": agents, "total_agents": n, "total_deliveries": d})
    }
    pub fn reset_all(&self) -> usize {
        let n = self.registry.len();
        self.registry.clear();
        self.token_registry.clear();
        self.delivery_log.clear();
        self.commits.store(0, Ordering::Relaxed);
        self.conflicts.store(0, Ordering::Relaxed);
        self.cross_stale.store(0, Ordering::Relaxed);
        n
    }
    pub fn admin_health(&self) -> serde_json::Value {
        let tc = self.commits.load(Ordering::Relaxed);
        let tf = self.conflicts.load(Ordering::Relaxed);
        json!({
            "status": "ok", "version": "v35-dist",
            "total_shards": self.registry.len(),
            "total_commits": tc, "total_conflicts": tf,
            "delivery_agents": self.delivery_log.agent_count(),
        })
    }
    pub fn spawn_lease_monitor(&self) {
        let dl = self.delivery_log.clone();
        let ls = self.lease_secs;
        tokio::spawn(async move {
            let mut tick = interval(Duration::from_secs(5));
            loop {
                tick.tick().await;
                dl.evict_stale();
            }
        });
    }
}

impl Default for SBus { fn default() -> Self { Self::new() } }

impl SBus {
    // ── Raft state machine helpers ──────────────────────────────────────────

    /// Restore a shard from a Raft snapshot. Used by install_snapshot.
    pub fn restore_shard(&self, key: String, shard: crate::bus::types::Shard) {
        self.registry.insert_if_absent(key.clone(), shard.clone());
        self.registry.with_write(&key, |s| {
            s.version    = shard.version;
            s.content    = shard.content;
            s.updated_at = shard.updated_at;
        });
    }

    /// Snapshot all shards for Raft log compaction.
    pub fn snapshot_shards(&self) -> Vec<(String, crate::bus::types::Shard)> {
        self.registry.snapshot_all()
    }

    /// Total number of shards (for logging).
    pub fn shard_count(&self) -> usize {
        self.registry.len()
    }
}

fn apply_delta(s: &mut Shard, agent_id: &str, delta: &str,
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