// src/api/handlers.rs — S-Bus v36 Raft-backed handlers
//
// Key change: create_shard now routes through Raft client_write so that
// shard creation is replicated to all nodes. Single-node mode is unchanged.

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::sync::Arc;

use crate::{
    bus::{
        engine::SBus,
        types::{CommitRequest, CreateShardRequest, RollbackRequest},
    },
    cluster::{ClusterConfig, post_json},
    raft::{CommitEntry, SBusRaft},
};

// ── AppState ──────────────────────────────────────────────────────────────────
pub struct AppState {
    pub bus:           SBus,
    pub cluster:       ClusterConfig,
    pub raft:          Option<SBusRaft>,
    pub node_id:       u64,
    pub this_url:      String,
    pub admin_enabled: bool,
}

impl AppState {
    /// URL of the current leader node, or None if WE are the leader.
    pub fn leader_url(&self) -> Option<String> {
        let raft = self.raft.as_ref()?;
        let m    = raft.metrics().borrow().clone();
        let lid  = m.current_leader?;
        if lid == self.node_id { return None; }
        let cfg  = m.membership_config;
        cfg.membership().get_node(&lid).map(|n| n.addr.clone())
    }

    pub fn is_leader(&self) -> bool {
        // Explicit type on closure param avoids E0282
        self.raft.as_ref().map(|r: &SBusRaft| {
            r.metrics().borrow().current_leader == Some(self.node_id)
        }).unwrap_or(true)
    }
}

// ── POST /shard ───────────────────────────────────────────────────────────────
// In single-node mode: direct write (same as before).
// In Raft mode: route through client_write so ALL nodes replicate the creation.
// This fixes the bug where create_shard only wrote to the local registry.
pub async fn create_shard(
    State(state): State<Arc<AppState>>,
    Json(req):    Json<CreateShardRequest>,
) -> impl IntoResponse {
    // ── Single-node mode: direct write ───────────────────────────────────────
    let Some(ref raft) = state.raft else {
        return match state.bus.create_shard(req) {
            Ok(r)  => (StatusCode::CREATED, Json(serde_json::to_value(r).unwrap())),
            Err(e) => {
                let sc = StatusCode::from_u16(e.status_code())
                    .unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
                (sc, Json(json!({"error": e.error_code(), "detail": e.to_string()})))
            }
        };
    };

    // ── Raft mode: forward to leader if we are not leader ────────────────────
    if !state.is_leader() {
        if let Some(url) = state.leader_url() {
            let full = format!("{url}/shard");
            return match post_json(&state.cluster.client, &full, &req).await {
                Ok((s, b)) => {
                    let sc = StatusCode::from_u16(s).unwrap_or(StatusCode::BAD_GATEWAY);
                    (sc, Json(b))
                }
                Err(e) => (StatusCode::BAD_GATEWAY,
                           Json(json!({"error": "leader_forward_failed", "detail": e}))),
            };
        }
        return (StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({"error": "no_leader", "detail": "cluster has no leader yet"})));
    }

    // ── We are leader: propose CreateShard through Raft log ──────────────────
    // CommitEntry with init_content signals the state machine to call
    // create_shard on every node that applies this entry. The delta and
    // expected_version fields are unused for creation (set to sentinel values).
    let entry = CommitEntry {
        key:              req.key.clone(),
        expected_version: 0,
        delta:            String::new(),      // unused for creation
        agent_id:         "system".to_string(),
        read_set:         vec![],
        init_content:     Some(req.content.clone()),
        init_goal_tag:    Some(req.goal_tag.clone()),
    };

    match raft.client_write(entry).await {
        Ok(_) => (StatusCode::CREATED, Json(json!({
            "key":    req.key,
            "status": "created",
            "via":    "raft",
        }))),
        Err(e) => {
            let msg = format!("{e}");
            (StatusCode::INTERNAL_SERVER_ERROR,
             Json(json!({"error": "raft_write_failed", "detail": msg})))
        }
    }
}

// ── GET /shard/:key ───────────────────────────────────────────────────────────
#[derive(Deserialize)]
pub struct AgentQuery { #[serde(default)] pub agent_id: String }

pub async fn get_shard(
    State(state): State<Arc<AppState>>,
    Path(key):    Path<String>,
    Query(q):     Query<AgentQuery>,
) -> impl IntoResponse {
    match state.bus.read_shard(&key, &q.agent_id) {
        Ok(r)  => (StatusCode::OK, Json(serde_json::to_value(r).unwrap())),
        Err(e) => {
            let sc = StatusCode::from_u16(e.status_code())
                .unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
            (sc, Json(json!({"error": e.error_code(), "detail": e.to_string()})))
        }
    }
}

pub async fn list_shards(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let keys = state.bus.list_shards();
    (StatusCode::OK, Json(json!({"shards": keys, "count": keys.len()})))
}

// ── POST /commit/v2 ───────────────────────────────────────────────────────────
pub async fn commit_v2(
    State(state): State<Arc<AppState>>,
    Json(req):    Json<CommitRequest>,
) -> impl IntoResponse {
    // ── Single-node: no Raft ──────────────────────────────────────────────────
    let Some(ref raft) = state.raft else {
        return match state.bus.commit_delta_v2(req) {
            Ok(r)  => (StatusCode::OK, Json(serde_json::to_value(r).unwrap())),
            Err(e) => {
                let sc = StatusCode::from_u16(e.status_code())
                    .unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
                (sc, Json(json!({"error": e.error_code(), "detail": e.to_string()})))
            }
        };
    };

    // ── Raft: forward to leader if needed ─────────────────────────────────────
    if !state.is_leader() {
        if let Some(url) = state.leader_url() {
            let full = format!("{url}/commit/v2");
            return match post_json(&state.cluster.client, &full, &req).await {
                Ok((s, b)) => {
                    let sc = StatusCode::from_u16(s).unwrap_or(StatusCode::BAD_GATEWAY);
                    (sc, Json(b))
                }
                Err(e) => (StatusCode::BAD_GATEWAY,
                           Json(json!({"error": "leader_forward_failed", "detail": e}))),
            };
        }
        return (StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({"error": "no_leader"})));
    }

    // ── We are leader: propose via Raft log ───────────────────────────────────
    let entry = CommitEntry {
        key:              req.key.clone(),
        expected_version: req.expected_version,
        delta:            req.delta.clone(),
        agent_id:         req.agent_id.clone(),
        read_set:         req.read_set.as_deref().unwrap_or(&[])
            .iter().map(|e| (e.key.clone(), e.version_at_read)).collect(),
        init_content:  None,
        init_goal_tag: None,
    };

    match raft.client_write(entry).await {
        Ok(resp) => {
            let data = resp.data;
            if data.error.is_none() {
                (StatusCode::OK, Json(json!({
                    "new_version": data.new_version,
                    "shard_id":   data.shard_id,
                    "via":        "raft",
                })))
            } else {
                let sc = match data.error_code.as_deref() {
                    Some("VersionMismatch") | Some("CrossShardStale")
                    | Some("TokenConflict") => StatusCode::CONFLICT,
                    Some("ShardNotFound")   => StatusCode::NOT_FOUND,
                    _                       => StatusCode::INTERNAL_SERVER_ERROR,
                };
                (sc, Json(json!({
                    "error":  data.error_code,
                    "detail": data.error,
                    "via":    "raft",
                })))
            }
        }
        Err(e) => {
            let msg = format!("{e}");
            (StatusCode::INTERNAL_SERVER_ERROR,
             Json(json!({"error": "raft_write_failed", "detail": msg})))
        }
    }
}

pub async fn commit_v1(
    State(state): State<Arc<AppState>>,
    Json(req):    Json<CommitRequest>,
) -> impl IntoResponse {
    commit_v2(State(state), Json(req)).await
}

// ── POST /rollback ────────────────────────────────────────────────────────────
pub async fn rollback(
    State(state): State<Arc<AppState>>,
    Json(req):    Json<RollbackRequest>,
) -> impl IntoResponse {
    match state.bus.rollback(req) {
        Ok(r)  => (StatusCode::OK, Json(serde_json::to_value(r).unwrap())),
        Err(e) => {
            let sc = StatusCode::from_u16(e.status_code())
                .unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
            (sc, Json(json!({"error": e.error_code(), "detail": e.to_string()})))
        }
    }
}

// ── GET /stats ────────────────────────────────────────────────────────────────
pub async fn stats(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let mut s = state.bus.stats();
    if let Some(ref raft) = state.raft {
        let m = raft.metrics().borrow().clone();
        s["raft"] = json!({
            "state":          format!("{:?}", m.state),
            "current_leader": m.current_leader,
            "current_term":   m.current_term,
            "last_log_index": m.last_log_index,
            "last_applied":   m.last_applied,
        });
    }
    (StatusCode::OK, Json(s))
}

pub async fn metrics(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    (StatusCode::OK, state.bus.prometheus_metrics())
}

// ── Admin endpoints ───────────────────────────────────────────────────────────

#[derive(Serialize, Deserialize)]
pub struct AddNodeRequest {
    pub node_id:     u64,
    pub addr:        String,
    pub new_members: Vec<u64>,  // complete new voter set including the new node
}

/// Convenience endpoint: add a new node to the Raft cluster in one call.
/// Combines add_learner (sends snapshot, waits for catch-up) then
/// change_membership (promotes to voter).
/// Requires SBUS_ADMIN_ENABLED=1 and must be called on the current leader.
/// Python: POST /admin/add-node {"node_id":3,"addr":"http://localhost:7003","new_members":[0,1,2,3]}
pub async fn admin_add_node(
    State(state): State<Arc<AppState>>,
    Json(req):    Json<AddNodeRequest>,
) -> impl IntoResponse {
    if !state.admin_enabled {
        return (StatusCode::FORBIDDEN, Json(json!({"error": "admin disabled"})));
    }
    let Some(ref raft) = state.raft else {
        return (StatusCode::BAD_REQUEST, Json(json!({"error": "not in Raft mode"})));
    };
    if !state.is_leader() {
        if let Some(url) = state.leader_url() {
            // Forward to leader
            let full = format!("{url}/admin/add-node");
            return match post_json(&state.cluster.client, &full, &req).await {
                Ok((s, b)) => {
                    let sc = StatusCode::from_u16(s).unwrap_or(StatusCode::BAD_GATEWAY);
                    (sc, Json(b))
                }
                Err(e) => (StatusCode::BAD_GATEWAY,
                           Json(json!({"error": "leader_forward_failed", "detail": e}))),
            };
        }
        return (StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({"error": "no_leader"})));
    }

    // Step 1: add_learner — openRaft sends snapshot to new node and waits for catch-up
    let node = openraft::BasicNode { addr: req.addr.clone() };
    if let Err(e) = raft.add_learner(req.node_id, node, true).await {
        let m = format!("{e}");
        return (StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "add_learner failed", "detail": m})));
    }

    // Step 2: change_membership — promote to full voter
    let members: std::collections::BTreeSet<u64> = req.new_members.iter().cloned().collect();
    match raft.change_membership(members, false).await {
        Ok(_)  => (StatusCode::OK, Json(json!({
            "status":      "added",
            "node_id":     req.node_id,
            "addr":        req.addr,
            "new_members": req.new_members,
        }))),
        Err(e) => {
            let m = format!("{e}");
            (StatusCode::INTERNAL_SERVER_ERROR,
             Json(json!({"error": "change_membership failed", "detail": m})))
        }
    }
}

pub async fn admin_create_shard(
    State(state): State<Arc<AppState>>,
    Json(req):    Json<CreateShardRequest>,
) -> impl IntoResponse {
    // Direct write to local state.bus — bypasses Raft entirely.
    // Used ONLY for test setup in DR-7 (SBUS_ADMIN_ENABLED=1 required).
    // The shard is seeded on each node so DR-7 can test election without
    // depending on Raft replication to every node.
    if !state.admin_enabled {
        return (StatusCode::FORBIDDEN, Json(json!({"error": "admin disabled"})));
    }
    match state.bus.create_shard(req) {
        Ok(r)  => (StatusCode::CREATED, Json(serde_json::to_value(r).unwrap())),
        Err(e) => {
            // ShardAlreadyExists is OK (idempotent seed)
            if e.error_code() == "ShardAlreadyExists" {
                return (StatusCode::OK, Json(json!({"status": "already_exists"})));
            }
            let sc = StatusCode::from_u16(e.status_code())
                .unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
            (sc, Json(json!({"error": e.error_code(), "detail": e.to_string()})))
        }
    }
}

pub async fn admin_commit(
    State(state): State<Arc<AppState>>,
    Json(req):    Json<CommitRequest>,
) -> impl IntoResponse {
    // Direct commit to local state.bus — bypasses Raft entirely.
    // Used ONLY for admin sync (SBUS_ADMIN_ENABLED=1 required).
    // Allows syncing follower state after post-election when Raft
    // AppendEntries are delayed in the in-memory prototype.
    if !state.admin_enabled {
        return (StatusCode::FORBIDDEN, Json(json!({"error": "admin disabled"})));
    }
    state.bus.touch_delivery_log(&req.agent_id);
    match state.bus.commit_delta_v2(req) {
        Ok(r)  => (StatusCode::OK, Json(serde_json::to_value(r).unwrap())),
        Err(e) => {
            let sc = StatusCode::from_u16(e.status_code())
                .unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
            (sc, Json(json!({"error": e.error_code(), "detail": e.to_string()})))
        }
    }
}

pub async fn admin_health(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let mut h = state.bus.admin_health();
    if let Some(ref raft) = state.raft {
        let m = raft.metrics().borrow().clone();
        h["raft_state"]  = json!(format!("{:?}", m.state));
        h["raft_leader"] = json!(m.current_leader);
        h["raft_term"]   = json!(m.current_term);
    }
    (StatusCode::OK, Json(h))
}

pub async fn admin_reset(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    if !state.admin_enabled {
        return (StatusCode::FORBIDDEN, Json(json!({"error": "admin disabled"})));
    }
    let n = state.bus.reset_all();
    (StatusCode::OK, Json(json!({"cleared": n})))
}

pub async fn admin_delivery_log(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    if !state.admin_enabled {
        return (StatusCode::FORBIDDEN, Json(json!({"error": "admin disabled"})));
    }
    (StatusCode::OK, Json(state.bus.dump_delivery_log()))
}

#[derive(Deserialize)]
pub struct InjectStaleRequest {
    pub agent_id: String, pub key: String, pub stale_version: u64,
}
pub async fn admin_inject_stale(
    State(state): State<Arc<AppState>>,
    Json(req):    Json<InjectStaleRequest>,
) -> impl IntoResponse {
    if !state.admin_enabled {
        return (StatusCode::FORBIDDEN, Json(json!({"error": "admin disabled"})));
    }
    state.bus.inject_stale_delivery(&req.agent_id, &req.key, req.stale_version);
    (StatusCode::OK, Json(json!({"injected": true})))
}

#[derive(Deserialize)]
pub struct SessionRequest { pub agent_id: String }
pub async fn create_session(
    State(state): State<Arc<AppState>>,
    Json(req):    Json<SessionRequest>,
) -> impl IntoResponse {
    state.bus.touch_delivery_log(&req.agent_id);
    (StatusCode::OK, Json(json!({"agent_id": req.agent_id, "status": "created"})))
}