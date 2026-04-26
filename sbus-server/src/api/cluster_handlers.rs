use std::sync::Arc;

use axum::{Json, extract::State, http::StatusCode, response::IntoResponse};
use serde_json::json;

use crate::api::handlers::AppState;
use crate::cluster::cluster_status;

pub async fn cluster_status_handler(
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    let cluster_info = cluster_status(&state.cluster).await;
    let stats        = state.bus.stats();
    (
        StatusCode::OK,
        Json(json!({
            "cluster":     cluster_info,
            "local_stats": stats,
        })),
    )
}
