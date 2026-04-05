// src/api/handlers.rs  — spawn_blocking fix for Tokio thread starvation
use axum::{extract::{Path, State}, http::StatusCode, response::IntoResponse, Json};
use serde_json::json;
use crate::bus::{engine::SBus, types::{CommitRequest, CreateShardRequest, RollbackRequest, SyncError}};

fn err_resp(e: SyncError) -> impl IntoResponse {
    let status = StatusCode::from_u16(e.status_code()).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    (status, Json(json!({"error": e.error_code(), "detail": e.to_string()}))).into_response()
}

macro_rules! blocking {
    ($bus:expr, $op:expr) => {
        match tokio::task::spawn_blocking(move || $op).await {
            Ok(Ok(r))  => (StatusCode::OK, Json(json!(r))).into_response(),
            Ok(Err(e)) => err_resp(e).into_response(),
            Err(_)     => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
        }
    };
}

pub async fn create_shard(State(bus): State<SBus>, Json(req): Json<CreateShardRequest>) -> impl IntoResponse {
    match tokio::task::spawn_blocking(move || bus.create_shard(req)).await {
        Ok(Ok(r))  => (StatusCode::CREATED, Json(json!(r))).into_response(),
        Ok(Err(e)) => err_resp(e).into_response(),
        Err(_)     => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}
pub async fn read_shard(State(bus): State<SBus>, Path(key): Path<String>) -> impl IntoResponse {
    blocking!(bus, bus.read_shard(&key))
}
pub async fn list_shards(State(bus): State<SBus>) -> impl IntoResponse {
    let keys = tokio::task::spawn_blocking(move || bus.list_shards()).await.unwrap_or_default();
    (StatusCode::OK, Json(json!({"shards": keys})))
}
pub async fn commit(State(bus): State<SBus>, Json(req): Json<CommitRequest>) -> impl IntoResponse {
    blocking!(bus, bus.commit_delta(req))
}
pub async fn commit_v2(State(bus): State<SBus>, Json(req): Json<CommitRequest>) -> impl IntoResponse {
    blocking!(bus, bus.commit_delta(req))
}
pub async fn commit_v2_naive(State(bus): State<SBus>, Json(req): Json<CommitRequest>) -> impl IntoResponse {
    blocking!(bus, bus.commit_v2_naive(req))
}
pub async fn rollback(State(bus): State<SBus>, Json(req): Json<RollbackRequest>) -> impl IntoResponse {
    blocking!(bus, bus.rollback(req))
}
pub async fn stats(State(bus): State<SBus>) -> impl IntoResponse {
    let s = tokio::task::spawn_blocking(move || bus.stats()).await
        .unwrap_or_else(|_| json!({"error":"internal"}));
    (StatusCode::OK, Json(s))
}
pub async fn metrics(State(bus): State<SBus>) -> impl IntoResponse {
    let m = tokio::task::spawn_blocking(move || bus.prometheus_metrics()).await.unwrap_or_default();
    (StatusCode::OK, [(axum::http::header::CONTENT_TYPE, "text/plain; version=0.0.4")], m)
}