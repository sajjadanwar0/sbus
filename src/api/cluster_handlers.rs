// src/api/cluster_handlers.rs
// Cluster status endpoint only.
// RAMP 2PC handlers are removed — Raft consensus handles replication.

use axum::{extract::State, http::StatusCode, response::IntoResponse, Json};
use std::sync::Arc;

use crate::api::handlers::AppState;
use crate::cluster::cluster_status;

pub async fn cluster_status_handler(
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    let cluster_info = cluster_status(&state.cluster).await;
    let stats        = state.bus.stats();
    (StatusCode::OK, Json(serde_json::json!({
        "cluster": cluster_info,
        "local_stats": stats,
    })))
}