// src/cluster/ramp.rs — RAMP-lite Two-Phase Commit
//
// This implements the coordinator side of RAMP 2PC for cross-node commits.
//
// PROTOCOL:
//   Phase 1 — Prepare:
//     For each node that owns shards in {primary_key} ∪ read_set:
//       POST /internal/prepare  → PrepareRequest
//       Node validates versions, marks shard as "2pc:locked", stores PreparedTxn.
//       Returns PrepareOk{new_version} or PrepareErr{error_code}.
//
//   Phase 2a — Commit (all prepares succeeded):
//     POST /internal/commit to every prepared node.
//     Each node applies the delta, clears the 2pc lock.
//
//   Phase 2b — Abort (any prepare failed):
//     POST /internal/abort to every node that returned PrepareOk.
//     Each node discards PreparedTxn, clears the 2pc lock.
//
// ORI CORRECTNESS ACROSS NODES:
//   All nodes prepare (validate + lock) before any node commits.
//   No concurrent commit can interleave because:
//     (a) prepare sets shard.owner = "2pc:{txn_id}" (blocks concurrent commits)
//     (b) commit only succeeds if owner still matches (prevents races)
//   => Property 1 (ORI) holds across nodes under RAMP-lite.
//
// FAILURE MODEL (prototype scope):
//   Network partition after prepare but before commit: shard stays locked.
//   Recovery: PrepareStore.evict_stale() runs every 5 min (30 min TTL).
//   Production: use WAL + coordinator crash recovery (noted as future work).

use uuid::Uuid;
use std::{
    collections::HashMap,
    sync::Mutex,
    time::Instant,
};
use serde::{Deserialize, Serialize};
use tracing::{info, warn};
use super::{ClusterConfig, post_json};

// ── Wire types ────────────────────────────────────────────────────────────────

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PrepareRequest {
    pub txn_id:           String,
    /// The shard key being written (must be owned by the receiving node).
    pub key:              String,
    /// Expected version for OCC check (0 if this node only validates read-set).
    pub expected_version: u64,
    /// Delta to apply at commit (empty if read-set-only node).
    pub delta:            String,
    pub agent_id:         String,
    /// Entries from the effective read-set that live on this node.
    pub local_read_set:   Vec<(String, u64)>,
    /// True if this node owns the primary shard (the write target).
    pub is_write_node:    bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PrepareOk {
    pub txn_id:      String,
    pub key:         String,
    pub new_version: u64,
    pub shard_id:    String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PrepareErr {
    pub txn_id:     String,
    pub key:        String,
    pub error_code: String,
    pub detail:     String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CommitRequest {
    pub txn_id: String,
    pub key:    String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AbortRequest {
    pub txn_id: String,
    pub key:    String,
}

// ── PrepareStore: in-memory 2PC state ────────────────────────────────────────

pub struct PreparedTxn {
    pub txn_id:      String,
    pub key:         String,
    pub delta:       String,
    pub agent_id:    String,
    pub new_version: u64,
    pub shard_id:    String,
    pub prepared_at: Instant,
}

pub struct PrepareStore {
    inner: Mutex<HashMap<String, PreparedTxn>>,
}

impl PrepareStore {
    pub fn new() -> Self { Self { inner: Mutex::new(HashMap::new()) } }
    pub fn insert(&self, t: PreparedTxn) {
        self.inner.lock().unwrap().insert(t.txn_id.clone(), t);
    }
    pub fn take(&self, txn_id: &str) -> Option<PreparedTxn> {
        self.inner.lock().unwrap().remove(txn_id)
    }
    pub fn count(&self) -> usize { self.inner.lock().unwrap().len() }
    pub fn evict_stale(&self, timeout_secs: u64) -> usize {
        let timeout = std::time::Duration::from_secs(timeout_secs);
        let now = Instant::now();
        let mut m = self.inner.lock().unwrap();
        let before = m.len();
        m.retain(|id, t| {
            let age = now.duration_since(t.prepared_at);
            if age > timeout {
                warn!(txn_id = id.as_str(), "evicting stale 2PC prepared txn");
                false
            } else { true }
        });
        before - m.len()
    }
}
impl Default for PrepareStore { fn default() -> Self { Self::new() } }

// ── 2PC Coordinator ───────────────────────────────────────────────────────────

pub enum TwoPcResult {
    Committed { new_version: u64, shard_id: String },
    Aborted   { error_code: String, detail: String },
}

/// Coordinate a cross-node commit via RAMP-lite 2PC.
///
/// Called by handlers.rs when `cfg.needs_2pc(primary_key, rs_keys)` is true.
/// The coordinator is whichever node received the original commit request.
///
/// Correctness: all nodes prepare (validate + lock) before any commits.
/// If any node rejects the prepare, all prepared nodes receive abort.
pub async fn coordinate(
    cfg:              &ClusterConfig,
    primary_key:      &str,
    delta:            &str,
    agent_id:         &str,
    expected_version: u64,
    full_read_set:    &[(String, u64)],   // all (key, version_at_read) in R_hat
) -> TwoPcResult {
    let txn_id = Uuid::new_v4().to_string();

    // Partition read-set by owning node
    let mut node_rs: HashMap<usize, Vec<(String, u64)>> = HashMap::new();
    for (k, v) in full_read_set {
        node_rs.entry(cfg.owning_node(k)).or_default().push((k.clone(), *v));
    }
    let primary_node = cfg.owning_node(primary_key);
    node_rs.entry(primary_node).or_default(); // ensure entry exists

    // Determine which nodes participate
    let participating: Vec<usize> = {
        let mut p: Vec<usize> = node_rs.keys().cloned().collect();
        p.sort();
        p
    };

    info!(txn_id = txn_id.as_str(), primary_key, nodes = ?participating, "2PC: starting");

    // ── Phase 1: Prepare ──────────────────────────────────────────────────────
    let mut prepared: Vec<(usize, PrepareOk)> = vec![];
    let mut aborted_reason: Option<(String, String)> = None;

    for &nid in &participating {
        let url = format!("{}/internal/prepare", cfg.url_for(nid));
        let local_rs = node_rs.get(&nid).cloned().unwrap_or_default();
        let is_write  = nid == primary_node;

        let req = PrepareRequest {
            txn_id:           txn_id.clone(),
            key:              primary_key.to_owned(),
            expected_version: if is_write { expected_version } else { 0 },
            delta:            if is_write { delta.to_owned() } else { String::new() },
            agent_id:         agent_id.to_owned(),
            local_read_set:   local_rs,
            is_write_node:    is_write,
        };

        match post_json(&cfg.client, &url, &req).await {
            Ok((200, body)) => {
                match serde_json::from_value::<PrepareOk>(body) {
                    Ok(ok)  => { prepared.push((nid, ok)); }
                    Err(e)  => {
                        aborted_reason = Some(("ParseError".into(), e.to_string()));
                        break;
                    }
                }
            }
            Ok((_, body)) => {
                let err = serde_json::from_value::<PrepareErr>(body.clone())
                    .unwrap_or(PrepareErr {
                        txn_id: txn_id.clone(), key: primary_key.to_owned(),
                        error_code: "RemoteError".into(),
                        detail: body.to_string(),
                    });
                aborted_reason = Some((err.error_code, err.detail));
                break;
            }
            Err(e) => {
                aborted_reason = Some(("NetworkError".into(), e));
                break;
            }
        }
    }

    // ── Phase 2b: Abort (if any prepare failed) ───────────────────────────────
    if let Some((code, detail)) = aborted_reason {
        for (nid, ok) in &prepared {
            let url = format!("{}/internal/abort", cfg.url_for(*nid));
            let req = AbortRequest { txn_id: txn_id.clone(), key: ok.key.clone() };
            if let Err(e) = post_json(&cfg.client, &url, &req).await {
                warn!("abort failed node {nid}: {e}");
            }
        }
        warn!(txn_id = txn_id.as_str(), error_code = code.as_str(), "2PC: aborted");
        return TwoPcResult::Aborted { error_code: code, detail };
    }

    // ── Phase 2a: Commit all ──────────────────────────────────────────────────
    let mut final_version = 0u64;
    let mut final_shard_id = String::new();

    for (nid, ok) in &prepared {
        if cfg.owning_node(primary_key) == *nid {
            final_version  = ok.new_version;
            final_shard_id = ok.shard_id.clone();
        }
        let url = format!("{}/internal/commit", cfg.url_for(*nid));
        let req = CommitRequest { txn_id: txn_id.clone(), key: ok.key.clone() };
        if let Err(e) = post_json(&cfg.client, &url, &req).await {
            // Post-prepare commit failure is a consistency hazard.
            // Logged here; WAL-based recovery is future work.
            warn!("COMMIT FAILED node {nid}: {e}. Manual WAL recovery needed.");
        }
    }

    info!(txn_id = txn_id.as_str(), new_version = final_version, "2PC: committed");
    TwoPcResult::Committed { new_version: final_version, shard_id: final_shard_id }
}