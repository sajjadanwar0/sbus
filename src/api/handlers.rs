// src/api/handlers.rs
//
// Axum HTTP handlers for all S-Bus endpoints.
//
// [GAP-FIX] New endpoint:
//   POST /commit/v2  — accepts CommitRequest with optional read_set field.
//                      Backward-compatible: /commit still works unchanged.
//                      v2 is the endpoint documented in the revised paper §4.2.

use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use serde_json::json;

use crate::bus::{
    engine::SBus,
    types::{CommitRequest, CreateShardRequest, RollbackRequest, SyncError},
};

// ── helper: convert SyncError to an Axum response ──────────────────────────

fn sync_err_to_response(e: SyncError) -> impl IntoResponse {
    let status = StatusCode::from_u16(e.status_code()).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    let body = json!({
        "error": e.error_code(),
        "detail": e.to_string(),
    });
    (status, Json(body))
}

// ── POST /shard ─────────────────────────────────────────────────────────────

pub async fn create_shard(
    State(bus): State<SBus>,
    Json(req): Json<CreateShardRequest>,
) -> impl IntoResponse {
    match bus.create_shard(req) {
        Ok(resp) => (StatusCode::CREATED, Json(json!(resp))).into_response(),
        Err(e) => sync_err_to_response(e).into_response(),
    }
}

// ── GET /shard/:key ─────────────────────────────────────────────────────────

pub async fn read_shard(
    State(bus): State<SBus>,
    Path(key): Path<String>,
) -> impl IntoResponse {
    match bus.read_shard(&key) {
        Ok(resp) => (StatusCode::OK, Json(json!(resp))).into_response(),
        Err(e) => sync_err_to_response(e).into_response(),
    }
}

// ── GET /shards ─────────────────────────────────────────────────────────────

pub async fn list_shards(State(bus): State<SBus>) -> impl IntoResponse {
    let keys = bus.list_shards();
    (StatusCode::OK, Json(json!({ "shards": keys })))
}

// ── POST /commit  (original, single-shard, backward-compatible) ─────────────

pub async fn commit(
    State(bus): State<SBus>,
    Json(req): Json<CommitRequest>,
) -> impl IntoResponse {
    match bus.commit_delta(req) {
        Ok(resp) => (StatusCode::OK, Json(json!(resp))).into_response(),
        Err(e) => sync_err_to_response(e).into_response(),
    }
}

// ── POST /commit/v2  [GAP-FIX] ─────────────────────────────────────────────
//
// Extended ACP endpoint that accepts an optional `read_set` field.
// Agents that want cross-shard snapshot consistency include a list of
// { "key": "...", "version_at_read": N } entries.
//
// If any listed shard has advanced since the agent recorded its version,
// the server returns 409 CrossShardStale and the agent re-reads that shard.
//
// Agents that omit `read_set` (or set it to null) get exactly the same
// behavior as POST /commit — no behavioral change, no performance cost.
//
// This endpoint is what the cross_shard_validation.py experiment calls.
// The paper's revised §4.2 documents it as the recommended commit endpoint
// for multi-shard tasks.
pub async fn commit_v2(
    State(bus): State<SBus>,
    Json(req): Json<CommitRequest>,
) -> impl IntoResponse {
    // CommitRequest already has read_set: Option<Vec<ReadSetEntry>>.
    // The engine's commit_delta handles both cases — no branching needed here.
    match bus.commit_delta(req) {
        Ok(resp) => (StatusCode::OK, Json(json!(resp))).into_response(),
        Err(e) => sync_err_to_response(e).into_response(),
    }
}

// ── POST /rollback ──────────────────────────────────────────────────────────

pub async fn rollback(
    State(bus): State<SBus>,
    Json(req): Json<RollbackRequest>,
) -> impl IntoResponse {
    match bus.rollback(req) {
        Ok(resp) => (StatusCode::OK, Json(json!(resp))).into_response(),
        Err(e) => sync_err_to_response(e).into_response(),
    }
}

// ── GET /stats ──────────────────────────────────────────────────────────────

pub async fn stats(State(bus): State<SBus>) -> impl IntoResponse {
    (StatusCode::OK, Json(bus.stats()))
}

// ── GET /metrics (Prometheus) ───────────────────────────────────────────────

pub async fn metrics(State(bus): State<SBus>) -> impl IntoResponse {
    (
        StatusCode::OK,
        [(axum::http::header::CONTENT_TYPE, "text/plain; version=0.0.4")],
        bus.prometheus_metrics(),
    )
}