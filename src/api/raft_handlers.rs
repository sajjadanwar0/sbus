// src/api/raft_handlers.rs — Raft RPC endpoints + management
// All error handling uses format!("{e}") to avoid E0282 type inference issues.

use axum::{extract::State, http::StatusCode, response::IntoResponse, Json};
use openraft::{BasicNode, raft::{AppendEntriesRequest, InstallSnapshotRequest, VoteRequest}};
use serde::Deserialize;
use serde_json::json;
use std::{collections::BTreeMap, sync::Arc};

use crate::api::handlers::AppState;
use crate::raft::SBusTypeConfig;

// ── Raft RPCs (node-to-node) ──────────────────────────────────────────────────

pub async fn append_entries(
    State(s): State<Arc<AppState>>,
    Json(req): Json<AppendEntriesRequest<SBusTypeConfig>>,
) -> impl IntoResponse {
    match s.raft.as_ref().expect("raft not init").append_entries(req).await {
        Ok(r)  => (StatusCode::OK, Json(serde_json::to_value(r).unwrap())),
        Err(e) => { let m = format!("{e}");
            (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": m}))) }
    }
}

pub async fn vote(
    State(s): State<Arc<AppState>>,
    Json(req): Json<VoteRequest<u64>>,
) -> impl IntoResponse {
    match s.raft.as_ref().expect("raft not init").vote(req).await {
        Ok(r)  => (StatusCode::OK, Json(serde_json::to_value(r).unwrap())),
        Err(e) => { let m = format!("{e}");
            (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": m}))) }
    }
}

pub async fn install_snapshot(
    State(s): State<Arc<AppState>>,
    Json(req): Json<InstallSnapshotRequest<SBusTypeConfig>>,
) -> impl IntoResponse {
    match s.raft.as_ref().expect("raft not init").install_snapshot(req).await {
        Ok(r)  => (StatusCode::OK, Json(serde_json::to_value(r).unwrap())),
        Err(e) => { let m = format!("{e}");
            (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": m}))) }
    }
}

// ── Cluster management ────────────────────────────────────────────────────────

pub async fn raft_init(State(s): State<Arc<AppState>>) -> impl IntoResponse {
    let mut members = BTreeMap::new();
    members.insert(s.node_id, BasicNode { addr: s.this_url.clone() });
    match s.raft.as_ref().expect("raft not init").initialize(members).await {
        Ok(_)  => (StatusCode::OK, Json(json!({"status": "initialized", "node_id": s.node_id}))),
        Err(e) => { let m = format!("{e}");
            (StatusCode::CONFLICT, Json(json!({"error": m}))) }
    }
}

#[derive(Deserialize)]
pub struct AddLearnerReq { pub node_id: u64, pub addr: String }

pub async fn add_learner(
    State(s): State<Arc<AppState>>,
    Json(req): Json<AddLearnerReq>,
) -> impl IntoResponse {
    match s.raft.as_ref().expect("raft not init")
        .add_learner(req.node_id, BasicNode { addr: req.addr }, true).await
    {
        Ok(r)  => (StatusCode::OK, Json(serde_json::to_value(r).unwrap())),
        Err(e) => { let m = format!("{e}");
            (StatusCode::BAD_REQUEST, Json(json!({"error": m}))) }
    }
}

#[derive(Deserialize)]
pub struct ChangeMembershipReq { pub members: Vec<u64> }

pub async fn change_membership(
    State(s): State<Arc<AppState>>,
    Json(req): Json<ChangeMembershipReq>,
) -> impl IntoResponse {
    let members: std::collections::BTreeSet<u64> = req.members.into_iter().collect();
    match s.raft.as_ref().expect("raft not init")
        .change_membership(members, false).await
    {
        Ok(r)  => (StatusCode::OK, Json(serde_json::to_value(r).unwrap())),
        Err(e) => { let m = format!("{e}");
            (StatusCode::BAD_REQUEST, Json(json!({"error": m}))) }
    }
}

pub async fn raft_metrics(State(s): State<Arc<AppState>>) -> impl IntoResponse {
    let m = s.raft.as_ref().expect("raft not init").metrics().borrow().clone();
    (StatusCode::OK, Json(json!({
        "node_id":        m.id,
        "state":          format!("{:?}", m.state),
        "current_term":   m.current_term,
        "current_leader": m.current_leader,
        "last_log_index": m.last_log_index,
        "last_applied":   m.last_applied,
    })))
}

pub async fn raft_leader(State(s): State<Arc<AppState>>) -> impl IntoResponse {
    let m = s.raft.as_ref().expect("raft not init").metrics().borrow().clone();
    (StatusCode::OK, Json(json!({
        "current_leader": m.current_leader,
        "this_node_id":   s.node_id,
        "is_leader":      m.current_leader == Some(s.node_id),
        "current_term":   m.current_term,
        "state":          format!("{:?}", m.state),
    })))
}