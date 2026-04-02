// src/api/handlers.rs
//
// Axum HTTP handlers for all S-Bus endpoints.
//
// Additions vs original:
//   - commit_v2_naive handler — unordered lock acquisition (Table 6 control)
//
// All existing handlers are unchanged.

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

// ── helper: convert SyncError to an Axum response ───────────────────────────

fn sync_err_to_response(e: SyncError) -> impl IntoResponse {
    let status = StatusCode::from_u16(e.status_code())
        .unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    let body = json!({
        "error":  e.error_code(),
        "detail": e.to_string(),
    });
    (status, Json(body))
}

// ── POST /shard ──────────────────────────────────────────────────────────────

pub async fn create_shard(
    State(bus): State<SBus>,
    Json(req): Json<CreateShardRequest>,
) -> impl IntoResponse {
    match bus.create_shard(req) {
        Ok(resp) => (StatusCode::CREATED, Json(json!(resp))).into_response(),
        Err(e)   => sync_err_to_response(e).into_response(),
    }
}

// ── GET /shard/:key ──────────────────────────────────────────────────────────

pub async fn read_shard(
    State(bus): State<SBus>,
    Path(key): Path<String>,
) -> impl IntoResponse {
    match bus.read_shard(&key) {
        Ok(resp) => (StatusCode::OK, Json(json!(resp))).into_response(),
        Err(e)   => sync_err_to_response(e).into_response(),
    }
}

// ── GET /shards ──────────────────────────────────────────────────────────────

pub async fn list_shards(State(bus): State<SBus>) -> impl IntoResponse {
    let keys = bus.list_shards();
    (StatusCode::OK, Json(json!({ "shards": keys })))
}

// ── POST /commit  (original single-shard, backward-compatible) ───────────────

pub async fn commit(
    State(bus): State<SBus>,
    Json(req): Json<CommitRequest>,
) -> impl IntoResponse {
    match bus.commit_delta(req) {
        Ok(resp) => (StatusCode::OK, Json(json!(resp))).into_response(),
        Err(e)   => sync_err_to_response(e).into_response(),
    }
}

// ── POST /commit/v2 ──────────────────────────────────────────────────────────
//
// Extended ACP endpoint with optional read_set field for cross-shard
// phantom-read prevention (Assumption A1, Corollary 2.1).
//
// Agents that include read_set trigger the sorted-lock-order multi-shard
// path in engine::commit_delta (Lemma 2 / Havender 1968).
//
// Agents that omit read_set get exactly the same behaviour as POST /commit
// — fully backward-compatible, no performance cost.
//
// CommitRequest already has read_set: Option<Vec<ReadSetEntry>>.
// The engine handles both cases; no branching needed here.

pub async fn commit_v2(
    State(bus): State<SBus>,
    Json(req): Json<CommitRequest>,
) -> impl IntoResponse {
    match bus.commit_delta(req) {
        Ok(resp) => (StatusCode::OK, Json(json!(resp))).into_response(),
        Err(e)   => sync_err_to_response(e).into_response(),
    }
}

// ── POST /commit/v2_naive ────────────────────────────────────────────────────
//
// Control condition for the cross-shard validation experiment (Table 6).
//
// Accepts the same CommitRequest body as /commit/v2 (including read_set).
// The difference is inside the engine: locks are acquired in
// request-insertion order rather than sorted lexicographic order.
//
// This endpoint exists solely to demonstrate experimentally that
// sorted-lock-order (Lemma 2 / Havender 1968) is specifically necessary —
// read-set declaration alone does not prevent corruption under concurrent
// multi-shard operations.
//
// DO NOT use in production agents. This is an intentionally unsafe
// control condition for the experiment harness.

pub async fn commit_v2_naive(
    State(bus): State<SBus>,
    Json(req): Json<CommitRequest>,
) -> impl IntoResponse {
    match bus.commit_v2_naive(req) {
        Ok(resp) => (StatusCode::OK, Json(json!(resp))).into_response(),
        Err(e)   => sync_err_to_response(e).into_response(),
    }
}

// ── POST /rollback ───────────────────────────────────────────────────────────

pub async fn rollback(
    State(bus): State<SBus>,
    Json(req): Json<RollbackRequest>,
) -> impl IntoResponse {
    match bus.rollback(req) {
        Ok(resp) => (StatusCode::OK, Json(json!(resp))).into_response(),
        Err(e)   => sync_err_to_response(e).into_response(),
    }
}

// ── GET /stats ───────────────────────────────────────────────────────────────
//
// Returns total_shards, total_commits, total_conflicts, cross_shard_stale_count,
// scr, lease_timeout_secs, and acp_config (ablation flags + retry_budget).
// The acp_config block lets the Python experiment harness verify which
// ablation condition is active via `curl /stats` without parsing logs.

pub async fn stats(State(bus): State<SBus>) -> impl IntoResponse {
    (StatusCode::OK, Json(bus.stats()))
}

// ── GET /metrics (Prometheus) ────────────────────────────────────────────────

pub async fn metrics(State(bus): State<SBus>) -> impl IntoResponse {
    (
        StatusCode::OK,
        [(axum::http::header::CONTENT_TYPE, "text/plain; version=0.0.4")],
        bus.prometheus_metrics(),
    )
}