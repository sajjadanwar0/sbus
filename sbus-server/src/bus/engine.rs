use std::{
    collections::HashMap,
    io::{BufRead, BufReader, Write},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};

use chrono::Utc;
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::time::{Duration, interval};
use tracing::{debug, info, warn};

use crate::bus::{
    registry::{DeliveryLog, ShardRegistry},
    types::{
        AcpConfig, CommitRequest, CommitResponse, CreateShardRequest, DeltaEntry, RollbackRequest,
        Shard, ShardResponse, SyncError,
    },
};
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct WalEntry {
    pub key: String,
    pub version: u64,
    pub agent_id: String,
    pub delta: String,
    pub goal_tag: String,
    pub ts: String,
}

pub struct Wal {
    path: Option<String>,
    writer: Option<std::sync::Mutex<std::fs::File>>,
}

impl Wal {
    pub fn from_env() -> Self {
        let path = std::env::var("SBUS_WAL_PATH").ok();
        let writer = path.as_deref().and_then(|p| {
            match std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(p)
            {
                Ok(f) => {
                    info!("WAL: {}", p);
                    Some(std::sync::Mutex::new(f))
                }
                Err(e) => {
                    warn!("WAL open failed {p}: {e}");
                    None
                }
            }
        });
        Self { path, writer }
    }

    pub fn append(&self, key: &str, version: u64, agent_id: &str, delta: &str, goal_tag: &str) {
        let Some(ref w) = self.writer else { return };
        let e = WalEntry {
            key: key.to_owned(),
            version,
            agent_id: agent_id.to_owned(),
            delta: delta.to_owned(),
            goal_tag: goal_tag.to_owned(),
            ts: Utc::now().to_rfc3339(),
        };
        let Ok(line) = serde_json::to_string(&e) else { return };
        if let Ok(mut f) = w.lock() {
            let _ = writeln!(f, "{line}");
            let _ = f.flush();
        }
    }

    pub fn replay(path: &str) -> Vec<WalEntry> {
        let Ok(f) = std::fs::File::open(path) else {
            return Vec::new();
        };
        BufReader::new(f)
            .lines()
            .map_while(Result::ok)
            .filter_map(|l| serde_json::from_str(&l).ok())
            .collect()
    }

    pub fn is_enabled(&self) -> bool {
        self.writer.is_some()
    }

    pub fn path(&self) -> Option<&str> {
        self.path.as_deref()
    }
}

#[derive(Clone)]
pub struct SBus {
    pub config: AcpConfig,
    registry: Arc<ShardRegistry>,
    delivery_log: Arc<DeliveryLog>,
    commits: Arc<AtomicU64>,
    conflicts: Arc<AtomicU64>,
    cross_stale: Arc<AtomicU64>,
    /// View-divergence counter, INDEPENDENT of `ori_enabled`.
    /// Increments every time a commit's effective read-set has a sibling
    /// version that differs from the registry's current version, REGARDLESS
    /// of whether the commit was rejected (ori_on) or allowed through
    /// (ori_off). This is the workload-B outcome-quality signal: under
    /// ori_off, view_divergent_commits counts commits that succeeded
    /// despite being structurally stale — exactly the failure mode ORI
    /// is supposed to prevent.
    view_divergent_commits: Arc<AtomicU64>,
    /// Total commits where the cross-shard view was checked (denominator
    /// for view-divergence rate).
    view_checked_commits: Arc<AtomicU64>,
    /// Runtime-toggleable cross-shard staleness check. When `false`, the
    /// commit path skips the DeliveryLog cross-shard validation and degrades
    /// to per-shard OCC only (last-writer-wins on cross-shard reads).
    /// Used by Workload-B (data-pipeline planning) to provide an ORI-OFF
    /// baseline. Defaults to `true` (ORI enforced) on every SBus instance;
    /// the harness flips it via `POST /admin/config` per trial.
    ///
    /// Per-shard version checking and ownership-token semantics are NOT
    /// affected by this flag — only the cross-shard staleness check
    /// (`commit_delta` lines that read `eff_rs`) is gated.
    ori_enabled: Arc<AtomicBool>,
    max_log_depth: usize,
    wal: Arc<Wal>,
}

impl SBus {
    const DEFAULT_LOG_DEPTH: usize = 1_000;

    pub fn new() -> Self {
        Self::with_log_depth(Self::DEFAULT_LOG_DEPTH)
    }

    pub fn with_log_depth(max_log_depth: usize) -> Self {
        Self {
            config: AcpConfig::from_env(),
            registry: Arc::new(ShardRegistry::new()),
            delivery_log: Arc::new(DeliveryLog::new()),
            commits: Arc::new(AtomicU64::new(0)),
            conflicts: Arc::new(AtomicU64::new(0)),
            cross_stale: Arc::new(AtomicU64::new(0)),
            view_divergent_commits: Arc::new(AtomicU64::new(0)),
            view_checked_commits: Arc::new(AtomicU64::new(0)),
            ori_enabled: Arc::new(AtomicBool::new(true)),
            max_log_depth,
            wal: Arc::new(Wal::from_env()),
        }
    }

    /// Toggle the cross-shard staleness check at runtime. Called from the
    /// `POST /admin/config` admin handler. Default is `true` (ORI enforced).
    /// When `false`, `commit_delta` skips the DeliveryLog cross-shard
    /// validation, leaving only per-shard version-checking and ownership-
    /// token semantics in place.
    pub fn set_ori_enabled(&self, enabled: bool) {
        self.ori_enabled.store(enabled, Ordering::SeqCst);
    }

    /// Read the current ORI-enabled flag value. Used by `admin_get_config`
    /// for harness verification before each trial.
    pub fn is_ori_enabled(&self) -> bool {
        self.ori_enabled.load(Ordering::SeqCst)
    }

    pub fn record_delivery(&self, agent_id: &str, shard_key: &str, version: u64) {
        self.delivery_log.record(agent_id, shard_key, version);
    }

    /// NEW in v50.1 — returns true if `agent_id` already has a live
    /// DeliveryLog entry for `shard_key`. Used by the proxy-register
    /// handler to implement skip-if-exists semantics. See
    /// registry::DeliveryLog::has_entry for the safety rationale.
    pub fn has_delivery(&self, agent_id: &str, shard_key: &str) -> bool {
        self.delivery_log.has_entry(agent_id, shard_key)
    }

    pub fn dump_delivery_log_flat(&self) -> Vec<(String, String, u64)> {
        self.delivery_log
            .snapshot_all()
            .into_iter()
            .flat_map(|(agent_id, entries)| {
                entries
                    .into_iter()
                    .map(move |(shard_key, entry)| (agent_id.clone(), shard_key, entry.version))
            })
            .collect()
    }

    pub fn rebuild_from_wal(&self) {
        let Some(path) = self.wal.path().map(str::to_owned) else {
            return;
        };
        let entries = Wal::replay(&path);
        if entries.is_empty() {
            return;
        }
        let mut latest: HashMap<String, WalEntry> = HashMap::new();
        for e in &entries {
            if latest.get(&e.key).is_none_or(|l| e.version > l.version) {
                latest.insert(e.key.clone(), e.clone());
            }
        }
        let mut recovered = 0usize;
        for (key, entry) in &latest {
            let shard = Shard::new(
                entry.delta.clone(),
                entry.goal_tag.clone(),
                self.max_log_depth,
            );
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
        info!("WAL rebuilt: {} shards, {recovered} updated", latest.len());
    }

    pub fn create_shard(&self, req: CreateShardRequest) -> Result<ShardResponse, SyncError> {
        let shard = Shard::new(req.content, req.goal_tag, self.max_log_depth);
        if !self.registry.insert_if_absent(req.key.clone(), shard) {
            return Err(SyncError::ShardAlreadyExists { key: req.key });
        }
        self.registry
            .with_read(&req.key, |s| ShardResponse::from((req.key.as_str(), s)))
            .ok_or(SyncError::ShardNotFound { key: req.key })
    }

    pub fn read_shard(&self, key: &str, agent_id: &str) -> Result<ShardResponse, SyncError> {
        let resp = self
            .registry
            .with_read(key, |s| ShardResponse::from((key, s)))
            .ok_or_else(|| SyncError::ShardNotFound {
                key: key.to_owned(),
            })?;
        if !agent_id.is_empty() {
            self.delivery_log.record(agent_id, key, resp.version);
            debug!(key, agent_id, version = resp.version, "delivery recorded");
        }
        Ok(resp)
    }

    pub fn list_shards(&self) -> Vec<String> {
        self.registry.keys()
    }

    pub fn commit_delta(&self, req: CommitRequest) -> Result<CommitResponse, SyncError> {
        let cfg = &self.config;

        if cfg.max_delta_chars > 0 && req.delta.len() > cfg.max_delta_chars {
            return Err(SyncError::DeltaTooLarge {
                key: req.key.clone(),
                len: req.delta.len(),
                max: cfg.max_delta_chars,
            });
        }

        let explicit: Vec<(String, u64)> = req
            .read_set
            .as_deref()
            .unwrap_or(&[])
            .iter()
            .map(|e| (e.key.clone(), e.version_at_read))
            .collect();

        let eff_rs = self
            .delivery_log
            .build_effective_read_set(&req.agent_id, &req.key, &explicit)
            .map_err(|e| SyncError::SessionExpired {
                agent_id: req.agent_id.clone(),
                message: e.to_string(),
            })?;

        // Cross-shard view-divergence detection. The detection itself runs
        // under BOTH conditions (ori_on and ori_off) so the workload-B
        // outcome-quality experiment can measure how often agents commit
        // with stale cross-shard views. ORI's safety guarantee is enforced
        // by REJECTING (HTTP-409) on divergence; ori_off allows the commit
        // through but still counts the divergence in view_divergent_commits.
        //
        // Under ori_on: rejected commits → CrossShardStale; non-rejected
        //   commits all had fresh views (zero divergence).
        // Under ori_off: divergent commits succeed silently; the divergence
        //   counter measures how many got through.
        let mut had_divergence = false;
        for (k, v) in &eff_rs {
            if k == &req.key {
                continue;
            }
            let cur = self
                .registry
                .with_read(k, |s| s.version)
                .ok_or_else(|| SyncError::ShardNotFound { key: k.clone() })?;
            if cur != *v {
                had_divergence = true;
                if self.ori_enabled.load(Ordering::Relaxed) {
                    self.cross_stale.fetch_add(1, Ordering::Relaxed);
                    return Err(SyncError::CrossShardStale {
                        key: k.clone(),
                        version_at_read: *v,
                        current_version: cur,
                    });
                }
                // ori_off: don't reject, but keep iterating to confirm we
                // know about this divergence. Don't double-count: one
                // divergent commit, regardless of how many siblings stale.
                break;
            }
        }
        // Count once per (committed-or-not) attempt that passed through
        // the cross-shard check.
        if !eff_rs.is_empty() {
            self.view_checked_commits.fetch_add(1, Ordering::Relaxed);
            if had_divergence {
                self.view_divergent_commits.fetch_add(1, Ordering::Relaxed);
            }
        }

        let result = self.registry.with_write(&req.key, |s| {
            s.attempt_count += 1;

            if cfg.enable_version_check && s.version != req.expected_version {
                s.conflict_count += 1;
                self.conflicts.fetch_add(1, Ordering::Relaxed);
                return Err(SyncError::VersionMismatch {
                    key: req.key.clone(),
                    expected: req.expected_version,
                    found: s.version,
                });
            }

            if cfg.enable_ownership_token {
                if let Some(ref owner) = s.owner {
                    s.conflict_count += 1;
                    self.conflicts.fetch_add(1, Ordering::Relaxed);
                    return Err(SyncError::TokenConflict {
                        key: req.key.clone(),
                        owner: owner.clone(),
                    });
                }
                s.owner = Some(req.agent_id.clone());
            }

            apply_delta(s, &req.agent_id, &req.delta, cfg, self.max_log_depth);
            let nv = s.version;
            let sid = s.id.clone();
            let goal = s.goal_tag.clone();
            if cfg.enable_ownership_token {
                s.owner = None;
            }
            self.wal
                .append(&req.key, nv, &req.agent_id, &req.delta, &goal);
            self.commits.fetch_add(1, Ordering::Relaxed);
            Ok(CommitResponse {
                new_version: nv,
                shard_id: sid,
            })
        });

        result.unwrap_or(Err(SyncError::ShardNotFound { key: req.key }))
    }

    pub fn rollback(&self, req: RollbackRequest) -> Result<ShardResponse, SyncError> {
        let key = req.key.clone();
        let max_log = self.max_log_depth;
        let result = self.registry.with_write(&key, |s| {
            let tgt = s
                .delta_log
                .iter()
                .find(|e| e.version == req.target_version)
                .ok_or_else(|| SyncError::Internal {
                    msg: format!("version {} not in log for {key}", req.target_version),
                })?
                .clone();
            let prev = Shard::content_address(&s.content);
            s.content = tgt.delta.clone();
            s.version += 1;
            s.updated_at = Utc::now();
            s.delta_log.push_back(DeltaEntry {
                version: s.version,
                agent_id: format!("rollback:{}", req.agent_id),
                delta: tgt.delta,
                prev_hash: prev,
                committed_at: Utc::now(),
            });
            if s.delta_log.len() > max_log {
                s.delta_log.pop_front();
            }
            Ok(ShardResponse::from((key.as_str(), &*s)))
        });
        result.unwrap_or(Err(SyncError::ShardNotFound { key }))
    }

    pub fn stats(&self) -> serde_json::Value {
        let tc = self.commits.load(Ordering::Relaxed);
        let tf = self.conflicts.load(Ordering::Relaxed);
        let cs = self.cross_stale.load(Ordering::Relaxed);
        let vd = self.view_divergent_commits.load(Ordering::Relaxed);
        let vc = self.view_checked_commits.load(Ordering::Relaxed);
        let ta = tc + tf;
        json!({
            "total_shards":           self.registry.len(),
            "total_commits":          tc,
            "total_conflicts":        tf,
            "cross_shard_stale":      cs,
            "view_divergent_commits": vd,
            "view_checked_commits":   vc,
            "view_divergence_rate":   if vc > 0 { vd as f64 / vc as f64 } else { 0.0 },
            "ori_enabled":            self.is_ori_enabled(),
            "total_attempts":         ta,
            "scr":                    if ta > 0 { tf as f64 / ta as f64 } else { 0.0 },
            "wal_enabled":            self.wal.is_enabled(),
            "delivery_log": {
                "agents":     self.delivery_log.agent_count(),
                "deliveries": self.delivery_log.total_entries(),
            },
        })
    }

    pub fn prometheus_metrics(&self) -> String {
        let c = self.commits.load(Ordering::Relaxed);
        let f = self.conflicts.load(Ordering::Relaxed);
        let s = self.cross_stale.load(Ordering::Relaxed);
        let n = self.registry.len();
        format!(
            "# TYPE sbus_commits_total counter\n\
             sbus_commits_total {c}\n\
             # TYPE sbus_conflicts_total counter\n\
             sbus_conflicts_total {f}\n\
             # TYPE sbus_cross_stale_total counter\n\
             sbus_cross_stale_total {s}\n\
             # TYPE sbus_shards_total gauge\n\
             sbus_shards_total {n}\n"
        )
    }

    pub fn touch_delivery_log(&self, agent_id: &str) {
        self.delivery_log.touch(agent_id);
    }

    pub fn reset_session(&self, agent_id: &str) {
        self.delivery_log.reset_session(agent_id);
    }

    pub fn inject_stale_delivery(&self, agent_id: &str, key: &str, v: u64) {
        self.delivery_log.record(agent_id, key, v);
    }

    pub fn get_shard_version(&self, key: &str) -> Option<u64> {
        self.registry.with_read(key, |s| s.version)
    }

    pub fn dump_delivery_log(&self) -> serde_json::Value {
        let snap = self.delivery_log.snapshot_all();
        let agents: serde_json::Map<String, serde_json::Value> = snap
            .into_iter()
            .map(|(a, es)| {
                let m: serde_json::Map<String, serde_json::Value> = es
                    .into_iter()
                    .map(|(k, e)| {
                        (
                            k,
                            json!({
                                "version":  e.version,
                                "age_secs": e.delivered_at.elapsed().as_secs_f64(),
                            }),
                        )
                    })
                    .collect();
                (a, serde_json::Value::Object(m))
            })
            .collect();
        let total_agents = agents.len();
        let total_deliveries: usize = agents
            .values()
            .filter_map(|v| v.as_object())
            .map(|o| o.len())
            .sum();
        json!({
            "agents":            agents,
            "total_agents":      total_agents,
            "total_deliveries":  total_deliveries,
        })
    }

    pub fn reset_all(&self) -> usize {
        let n = self.registry.len();
        self.registry.clear();
        self.delivery_log.clear();
        self.commits.store(0, Ordering::Relaxed);
        self.conflicts.store(0, Ordering::Relaxed);
        self.cross_stale.store(0, Ordering::Relaxed);
        self.view_divergent_commits.store(0, Ordering::Relaxed);
        self.view_checked_commits.store(0, Ordering::Relaxed);
        n
    }

    /// Workload-B server-side view-divergence counters. Returns
    /// `(view_checked_commits, view_divergent_commits)` — how many commits
    /// had their cross-shard view checked, and how many of those had at
    /// least one stale sibling. Under ORI-ON, the divergent ones were
    /// rejected; under ORI-OFF, they succeeded silently.
    pub fn view_divergence_counters(&self) -> (u64, u64) {
        (
            self.view_checked_commits.load(Ordering::Relaxed),
            self.view_divergent_commits.load(Ordering::Relaxed),
        )
    }

    pub fn admin_health(&self) -> serde_json::Value {
        json!({
            "status":           "ok",
            "total_shards":     self.registry.len(),
            "total_commits":    self.commits.load(Ordering::Relaxed),
            "total_conflicts":  self.conflicts.load(Ordering::Relaxed),
            "delivery_agents":  self.delivery_log.agent_count(),
        })
    }

    pub fn spawn_lease_monitor(&self) {
        let dl = self.delivery_log.clone();
        tokio::spawn(async move {
            let mut tick = interval(Duration::from_secs(5));
            loop {
                tick.tick().await;
                dl.evict_stale();
            }
        });
    }

    pub fn restore_shard(&self, key: String, shard: Shard) {
        self.registry.insert_if_absent(key.clone(), shard.clone());
        self.registry.with_write(&key, |s| {
            s.version = shard.version;
            s.content = shard.content;
            s.updated_at = shard.updated_at;
        });
    }

    pub fn snapshot_shards(&self) -> Vec<(String, Shard)> {
        self.registry.snapshot_all()
    }
}

impl Default for SBus {
    fn default() -> Self {
        Self::new()
    }
}

fn apply_delta(s: &mut Shard, agent_id: &str, delta: &str, cfg: &AcpConfig, max_log: usize) {
    let prev = Shard::content_address(&s.content);
    s.content = delta.to_owned();
    s.version += 1;
    s.updated_at = Utc::now();
    if cfg.enable_delta_log {
        s.delta_log.push_back(DeltaEntry {
            version: s.version,
            agent_id: agent_id.to_owned(),
            delta: delta.to_owned(),
            prev_hash: prev,
            committed_at: Utc::now(),
        });
        if s.delta_log.len() > max_log {
            s.delta_log.pop_front();
        }
    }
}
