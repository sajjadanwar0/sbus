use std::sync::Arc;

use axum::{
    Json,
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
};
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::{
    bus::{
        engine::SBus,
        types::{CommitRequest, CreateShardRequest, RollbackRequest, SyncError},
    },
    cluster::{ClusterConfig, post_json},
    raft::{CommitEntry, DeliveryPayload, SBusRaft, ShardCommitPayload},
};

pub struct AppState {
    pub bus: SBus,
    pub cluster: ClusterConfig,
    pub raft: Option<SBusRaft>,
    pub node_id: u64,
    pub this_url: String,
    pub admin_enabled: bool,
}

impl AppState {
    pub fn leader_url(&self) -> Option<String> {
        let raft = self.raft.as_ref()?;
        let m = raft.metrics().borrow().clone();
        let lid = m.current_leader?;
        if lid == self.node_id {
            return None;
        }
        m.membership_config
            .membership()
            .get_node(&lid)
            .map(|n| n.addr.clone())
    }

    pub fn is_leader(&self) -> bool {
        self.raft
            .as_ref()
            .map(|r| r.metrics().borrow().current_leader == Some(self.node_id))
            .unwrap_or(true)
    }
}

fn err_response(e: &SyncError) -> (StatusCode, Json<serde_json::Value>) {
    let sc = StatusCode::from_u16(e.status_code()).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    (
        sc,
        Json(json!({ "error": e.error_code(), "detail": e.to_string() })),
    )
}

async fn forward_to_leader<B: Serialize>(
    state: &AppState,
    path: &str,
    body: &B,
) -> Option<(StatusCode, Json<serde_json::Value>)> {
    let leader = state.leader_url()?;
    let full = format!("{leader}{path}");
    Some(match post_json(&state.cluster.client, &full, body).await {
        Ok((s, b)) => (
            StatusCode::from_u16(s).unwrap_or(StatusCode::BAD_GATEWAY),
            Json(b),
        ),
        Err(e) => (
            StatusCode::BAD_GATEWAY,
            Json(json!({ "error": "leader_forward_failed", "detail": e })),
        ),
    })
}

fn no_leader() -> (StatusCode, Json<serde_json::Value>) {
    (
        StatusCode::SERVICE_UNAVAILABLE,
        Json(json!({ "error": "no_leader", "detail": "cluster has no leader yet" })),
    )
}

pub async fn create_shard(
    State(state): State<Arc<AppState>>,
    Json(req): Json<CreateShardRequest>,
) -> impl IntoResponse {
    let Some(ref raft) = state.raft else {
        return match state.bus.create_shard(req) {
            Ok(r) => (StatusCode::CREATED, Json(serde_json::to_value(r).unwrap())),
            Err(e) => err_response(&e),
        };
    };

    if !state.is_leader() {
        return forward_to_leader(&state, "/shard", &req)
            .await
            .unwrap_or_else(|| no_leader());
    }

    let entry = CommitEntry::Shard(ShardCommitPayload {
        key: req.key.clone(),
        expected_version: 0,
        delta: String::new(),
        agent_id: "system".to_owned(),
        read_set: Vec::new(),
        init_content: Some(req.content.clone()),
        init_goal_tag: Some(req.goal_tag.clone()),
    });

    match raft.client_write(entry).await {
        Ok(_) => (
            StatusCode::CREATED,
            Json(json!({
                "key":    req.key,
                "status": "created",
                "via":    "raft",
            })),
        ),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": "raft_write_failed", "detail": format!("{e}") })),
        ),
    }
}

#[derive(Deserialize)]
pub struct AgentQuery {
    #[serde(default)]
    pub agent_id: String,
}

pub async fn get_shard(
    State(state): State<Arc<AppState>>,
    Path(key): Path<String>,
    Query(q): Query<AgentQuery>,
) -> impl IntoResponse {
    match state.bus.read_shard(&key, &q.agent_id) {
        Ok(r) => {
            if !q.agent_id.is_empty() {
                if let Some(raft) = state.raft.clone() {
                    let delivery = CommitEntry::Delivery(DeliveryPayload {
                        agent_id: q.agent_id.clone(),
                        shard_key: key.clone(),
                        version: r.version,
                    });
                    tokio::spawn(async move {
                        if let Err(e) = raft.client_write(delivery).await {
                            tracing::debug!("delivery replication failed (non-fatal): {e}");
                        }
                    });
                }
            }
            (StatusCode::OK, Json(serde_json::to_value(r).unwrap()))
        }
        Err(e) => err_response(&e),
    }
}

pub async fn list_shards(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let keys = state.bus.list_shards();
    let count = keys.len();
    (
        StatusCode::OK,
        Json(json!({ "shards": keys, "count": count })),
    )
}

pub async fn commit_v2(
    State(state): State<Arc<AppState>>,
    Json(req): Json<CommitRequest>,
) -> impl IntoResponse {
    // Single-node: no Raft.
    let Some(ref raft) = state.raft else {
        return match state.bus.commit_delta(req) {
            Ok(r) => (StatusCode::OK, Json(serde_json::to_value(r).unwrap())),
            Err(e) => err_response(&e),
        };
    };

    if !state.is_leader() {
        return forward_to_leader(&state, "/commit/v2", &req)
            .await
            .unwrap_or_else(|| no_leader());
    }

    let entry = CommitEntry::Shard(ShardCommitPayload {
        key: req.key.clone(),
        expected_version: req.expected_version,
        delta: req.delta.clone(),
        agent_id: req.agent_id.clone(),
        read_set: req
            .read_set
            .as_deref()
            .unwrap_or(&[])
            .iter()
            .map(|e| (e.key.clone(), e.version_at_read))
            .collect(),
        init_content: None,
        init_goal_tag: None,
    });

    match raft.client_write(entry).await {
        Ok(resp) => {
            let data = resp.data;
            if data.error.is_none() {
                (
                    StatusCode::OK,
                    Json(json!({
                        "new_version": data.new_version,
                        "shard_id":    data.shard_id,
                        "via":         "raft",
                    })),
                )
            } else {
                let sc = match data.error_code.as_deref() {
                    Some("VersionMismatch" | "CrossShardStale" | "TokenConflict") => {
                        StatusCode::CONFLICT
                    }
                    Some("ShardNotFound") => StatusCode::NOT_FOUND,
                    _ => StatusCode::INTERNAL_SERVER_ERROR,
                };
                (
                    sc,
                    Json(json!({
                        "error":  data.error_code,
                        "detail": data.error,
                        "via":    "raft",
                    })),
                )
            }
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": "raft_write_failed", "detail": format!("{e}") })),
        ),
    }
}

pub async fn commit_v1(
    State(state): State<Arc<AppState>>,
    Json(req): Json<CommitRequest>,
) -> impl IntoResponse {
    commit_v2(State(state), Json(req)).await
}

pub async fn rollback(
    State(state): State<Arc<AppState>>,
    Json(req): Json<RollbackRequest>,
) -> impl IntoResponse {
    match state.bus.rollback(req) {
        Ok(r) => (StatusCode::OK, Json(serde_json::to_value(r).unwrap())),
        Err(e) => err_response(&e),
    }
}

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

#[derive(Serialize, Deserialize)]
pub struct AddNodeRequest {
    pub node_id: u64,
    pub addr: String,
    pub new_members: Vec<u64>,
}

pub async fn admin_add_node(
    State(state): State<Arc<AppState>>,
    Json(req): Json<AddNodeRequest>,
) -> impl IntoResponse {
    if !state.admin_enabled {
        return (
            StatusCode::FORBIDDEN,
            Json(json!({ "error": "admin disabled" })),
        );
    }
    let Some(ref raft) = state.raft else {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "not in Raft mode" })),
        );
    };
    if !state.is_leader() {
        return forward_to_leader(&state, "/admin/add-node", &req)
            .await
            .unwrap_or_else(|| no_leader());
    }

    let node = openraft::BasicNode {
        addr: req.addr.clone(),
    };
    if let Err(e) = raft.add_learner(req.node_id, node, true).await {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": "add_learner failed", "detail": format!("{e}") })),
        );
    }

    let members: std::collections::BTreeSet<u64> = req.new_members.iter().copied().collect();
    match raft.change_membership(members, false).await {
        Ok(_) => (
            StatusCode::OK,
            Json(json!({
                "status":      "added",
                "node_id":     req.node_id,
                "addr":        req.addr,
                "new_members": req.new_members,
            })),
        ),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": "change_membership failed", "detail": format!("{e}") })),
        ),
    }
}

pub async fn admin_create_shard(
    State(state): State<Arc<AppState>>,
    Json(req): Json<CreateShardRequest>,
) -> impl IntoResponse {
    if !state.admin_enabled {
        return (
            StatusCode::FORBIDDEN,
            Json(json!({ "error": "admin disabled" })),
        );
    }
    match state.bus.create_shard(req) {
        Ok(r) => (StatusCode::CREATED, Json(serde_json::to_value(r).unwrap())),
        Err(e) => {
            if e.error_code() == "ShardAlreadyExists" {
                return (StatusCode::OK, Json(json!({ "status": "already_exists" })));
            }
            err_response(&e)
        }
    }
}

pub async fn admin_commit(
    State(state): State<Arc<AppState>>,
    Json(req): Json<CommitRequest>,
) -> impl IntoResponse {
    if !state.admin_enabled {
        return (
            StatusCode::FORBIDDEN,
            Json(json!({ "error": "admin disabled" })),
        );
    }
    state.bus.touch_delivery_log(&req.agent_id);
    match state.bus.commit_delta(req) {
        Ok(r) => (StatusCode::OK, Json(serde_json::to_value(r).unwrap())),
        Err(e) => err_response(&e),
    }
}

pub async fn admin_health(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let mut h = state.bus.admin_health();
    if let Some(ref raft) = state.raft {
        let m = raft.metrics().borrow().clone();
        h["raft_state"] = json!(format!("{:?}", m.state));
        h["raft_leader"] = json!(m.current_leader);
        h["raft_term"] = json!(m.current_term);
    }
    (StatusCode::OK, Json(h))
}

pub async fn admin_reset(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let n = state.bus.reset_all();
    (StatusCode::OK, Json(json!({ "cleared": n })))
}

#[derive(Deserialize)]
pub struct ConfigRequest {
    pub ori_enabled: bool,
}

pub async fn admin_set_config(
    State(state): State<Arc<AppState>>,
    Json(req): Json<ConfigRequest>,
) -> impl IntoResponse {
    state.bus.set_ori_enabled(req.ori_enabled);
    (
        StatusCode::OK,
        Json(json!({
            "ok": true,
            "ori_enabled": state.bus.is_ori_enabled(),
        })),
    )
}


pub async fn admin_get_config(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let (checked, divergent) = state.bus.view_divergence_counters();
    (
        StatusCode::OK,
        Json(json!({
            "ori_enabled":             state.bus.is_ori_enabled(),
            "view_checked_commits":    checked,
            "view_divergent_commits":  divergent,
        })),
    )
}

pub async fn admin_delivery_log(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    if !state.admin_enabled {
        return (
            StatusCode::FORBIDDEN,
            Json(json!({ "error": "admin disabled" })),
        );
    }
    (StatusCode::OK, Json(state.bus.dump_delivery_log()))
}

#[derive(Deserialize)]
pub struct InjectStaleRequest {
    pub agent_id: String,
    pub key: String,
    pub stale_version: u64,
}

pub async fn admin_inject_stale(
    State(state): State<Arc<AppState>>,
    Json(req): Json<InjectStaleRequest>,
) -> impl IntoResponse {
    if !state.admin_enabled {
        return (
            StatusCode::FORBIDDEN,
            Json(json!({ "error": "admin disabled" })),
        );
    }
    state
        .bus
        .inject_stale_delivery(&req.agent_id, &req.key, req.stale_version);
    (StatusCode::OK, Json(json!({ "injected": true })))
}

#[derive(Deserialize)]
pub struct SessionRequest {
    pub agent_id: String,
}

pub async fn create_session(
    State(state): State<Arc<AppState>>,
    Json(req): Json<SessionRequest>,
) -> impl IntoResponse {
    state.bus.reset_session(&req.agent_id);
    (
        StatusCode::OK,
        Json(json!({ "agent_id": req.agent_id, "status": "created" })),
    )
}

#[derive(Deserialize)]
pub struct ProxyRegisterRequest {
    pub agent_id: String,
    #[serde(default)]
    pub session_id: String,
    pub shards_used: Vec<String>,
    #[serde(default)]
    pub source: String,
}

#[derive(Serialize)]
pub struct ProxyRegisterResponse {
    pub ok: bool,
    pub registered: usize,
    pub skipped_existing: Vec<String>,
    pub skipped_no_shard: Vec<String>,
}

pub async fn delivery_log_register(
    State(state): State<Arc<AppState>>,
    Json(req): Json<ProxyRegisterRequest>,
) -> impl IntoResponse {
    if req.agent_id.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "agent_id is empty" })),
        )
            .into_response();
    }

    if req.shards_used.is_empty() {
        return (
            StatusCode::OK,
            Json(ProxyRegisterResponse {
                ok: true,
                registered: 0,
                skipped_existing: Vec::new(),
                skipped_no_shard: Vec::new(),
            }),
        )
            .into_response();
    }

    let mut registered: usize = 0;
    let mut skipped_existing: Vec<String> = Vec::new();
    let mut skipped_no_shard: Vec<String> = Vec::new();

    for shard_key in &req.shards_used {
        if state.bus.has_delivery(&req.agent_id, shard_key) {
            skipped_existing.push(shard_key.clone());
            tracing::debug!(
                agent = %req.agent_id,
                shard = %shard_key,
                "DeliveryLog: proxy reference skipped — agent already has entry (preserving read-time version)"
            );
            continue;
        }
        
        match state.bus.get_shard_version(shard_key) {
            Some(v) => {
                state.bus.record_delivery(&req.agent_id, shard_key, v);
                registered += 1;
                tracing::debug!(
                    agent   = %req.agent_id,
                    session = %req.session_id,
                    shard   = %shard_key,
                    version = v,
                    source  = %req.source,
                    "DeliveryLog: proxy-originated entry recorded (new)"
                );
            }
            None => {
                skipped_no_shard.push(shard_key.clone());
                tracing::debug!(
                    agent = %req.agent_id,
                    shard = %shard_key,
                    "DeliveryLog: proxy reference skipped — shard not in registry"
                );
            }
        }
    }

    tracing::info!(
        agent             = %req.agent_id,
        session           = %req.session_id,
        registered,
        skipped_existing  = skipped_existing.len(),
        skipped_no_shard  = skipped_no_shard.len(),
        source            = %req.source,
        "DeliveryLog: proxy batch registration complete"
    );

    (
        StatusCode::OK,
        Json(ProxyRegisterResponse {
            ok: true,
            registered,
            skipped_existing,
            skipped_no_shard,
        }),
    )
        .into_response()
}
