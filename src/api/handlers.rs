use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Json, Response},
};
use serde::{Deserialize, Serialize};
use crate::bus::{engine::SBus, types::{Delta, SyncError}};
use crate::metrics::collector::MetricsCollector;

/// Shared application state
#[derive(Clone)]
pub struct AppState {
    pub bus:     SBus,
    pub metrics: MetricsCollector,
}

// ── request/response DTOs ────────────────────────────────────────────────────

#[derive(Deserialize)]
pub struct CreateShardReq {
    pub content:  String,
    pub goal_tag: String,
    pub key:      Option<String>,   // optional explicit key
}

#[derive(Serialize)]
pub struct CreateShardResp {
    pub key: String,
}

#[derive(Deserialize)]
pub struct CommitDeltaReq {
    pub key:          String,
    pub expected_ver: u64,
    pub content:      String,
    pub rationale:    String,
    pub agent_id:     String,
}

#[derive(Serialize)]
pub struct CommitDeltaResp {
    pub new_version: u64,
}

#[derive(Deserialize)]
pub struct RollbackReq {
    pub key:        String,
    pub target_ver: u64,
}

// ── error response helper ────────────────────────────────────────────────────

struct ApiError(SyncError);

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let code = match &self.0 {
            SyncError::ShardNotFound { .. }   => StatusCode::NOT_FOUND,
            SyncError::VersionMismatch { .. } => StatusCode::CONFLICT,
            SyncError::TokenConflict { .. }   => StatusCode::LOCKED,
            SyncError::RollbackInvalid { .. } => StatusCode::BAD_REQUEST,
            SyncError::Internal { .. }        => StatusCode::INTERNAL_SERVER_ERROR,
        };
        let body = serde_json::json!({ "error": self.0.to_string() });
        (code, Json(body)).into_response()
    }
}

// ── handlers ─────────────────────────────────────────────────────────────────

/// POST /shard — create a new shard
pub async fn create_shard(
    State(state): State<AppState>,
    Json(req):    Json<CreateShardReq>,
) -> impl IntoResponse {
    let key = match req.key {
        Some(k) => state.bus.create_shard_with_key(k, req.content, req.goal_tag),
        None    => state.bus.create_shard(req.content, req.goal_tag),
    };
    (StatusCode::CREATED, Json(CreateShardResp { key }))
}

/// GET /shard/:key — read current shard
pub async fn read_shard(
    State(state): State<AppState>,
    Path(key):    Path<String>,
) -> Response {
    match state.bus.read_shard(&key) {
        Ok(shard) => Json(shard).into_response(),
        Err(e)    => ApiError(e).into_response(),
    }
}

/// GET /shards — list all shard keys
pub async fn list_shards(State(state): State<AppState>) -> impl IntoResponse {
    Json(state.bus.list_shards())
}

/// POST /commit — Atomic Commit Protocol
pub async fn commit_delta(
    State(state): State<AppState>,
    Json(req):    Json<CommitDeltaReq>,
) -> Response {
    let delta = Delta {
        content:   req.content,
        rationale: req.rationale,
    };
    match state.bus.commit_delta(&req.key, req.expected_ver, delta, &req.agent_id) {
        Ok(new_version) => {
            Json(CommitDeltaResp { new_version }).into_response()
        }
        Err(e) => ApiError(e).into_response(),
    }
}

/// POST /rollback — roll shard back to a prior version
pub async fn rollback(
    State(state): State<AppState>,
    Json(req):    Json<RollbackReq>,
) -> Response {
    match state.bus.rollback(&req.key, req.target_ver) {
        Ok(())  => StatusCode::OK.into_response(),
        Err(e)  => ApiError(e).into_response(),
    }
}

/// GET /stats — bus-level statistics
pub async fn bus_stats(State(state): State<AppState>) -> impl IntoResponse {
    Json(state.bus.stats())
}

/// GET /metrics — Prometheus exposition format
pub async fn prometheus_metrics(State(state): State<AppState>) -> impl IntoResponse {
    (
        [(axum::http::header::CONTENT_TYPE, "text/plain; version=0.0.4")],
        state.metrics.prometheus_text(),
    )
}

/// GET /results/csv — export all run metrics as CSV
pub async fn export_csv(State(state): State<AppState>) -> impl IntoResponse {
    (
        [(axum::http::header::CONTENT_TYPE, "text/csv")],
        state.metrics.export_csv(),
    )
}

/// GET /results/aggregate — aggregated statistics as JSON
pub async fn aggregate_results(State(state): State<AppState>) -> impl IntoResponse {
    Json(state.metrics.aggregate())
}

/// POST /metrics/run — record a completed experiment run
pub async fn record_run(
    State(state): State<AppState>,
    Json(run):    Json<crate::metrics::collector::RunMetrics>,
) -> impl IntoResponse {
    state.metrics.record(run);
    StatusCode::CREATED
}