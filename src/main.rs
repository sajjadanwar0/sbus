mod api;
mod bus;
mod metrics;

use api::handlers::{
    aggregate_results, bus_stats, commit_delta, create_shard, export_csv,
    list_shards, prometheus_metrics, read_shard, record_run, rollback, AppState,
};
use axum::{
    routing::{get, post},
    Router,
};
use bus::engine::SBus;
use metrics::collector::MetricsCollector;
use std::net::SocketAddr;
use tower_http::{cors::CorsLayer, trace::TraceLayer};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

pub mod bus_mod {}

#[tokio::main]
async fn main() {
    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::new(
            std::env::var("RUST_LOG").unwrap_or_else(|_| "sbus=info".to_string()),
        ))
        .with(tracing_subscriber::fmt::layer())
        .init();

    let state = AppState {
        bus:     SBus::new(),
        metrics: MetricsCollector::new(),
    };

    let app = Router::new()
        // Shard CRUD
        .route("/shard",         post(create_shard))
        .route("/shard/{key}",   get(read_shard))
        .route("/shards",        get(list_shards))
        // ACP
        .route("/commit",        post(commit_delta))
        .route("/rollback",      post(rollback))
        // Observability
        .route("/stats",         get(bus_stats))
        .route("/metrics",       get(prometheus_metrics))
        // Experiment result collection
        .route("/results/run",   post(record_run))
        .route("/results/csv",   get(export_csv))
        .route("/results/agg",   get(aggregate_results))
        .layer(CorsLayer::permissive())
        .layer(TraceLayer::new_for_http())
        .with_state(state);

    let addr = SocketAddr::from(([127, 0, 0, 1], 3000));
    tracing::info!("S-Bus server listening on {addr}");
    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}