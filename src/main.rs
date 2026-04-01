// src/main.rs
//
// S-Bus HTTP server — Axum + Tokio.
//
// [GAP-FIX] Changes from original:
//   - POST /commit/v2 route added (cross-shard read-set endpoint)
//   - lease monitor spawned with constructive timeout argument
//   - CORS kept permissive for experiment harness compatibility

use axum::{
    routing::{get, post},
    Router,
};
use tower_http::{cors::CorsLayer, trace::TraceLayer};
use tracing_subscriber::{fmt, EnvFilter};

mod api {
    pub mod handlers;
}
mod bus {
    pub mod engine;
    pub mod types;
}

use api::handlers;
use bus::engine::SBus;

#[tokio::main]
async fn main() {
    // Logging
    fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| "info,sbus=debug".parse().unwrap()),
        )
        .init();

    let bus = SBus::with_options(1_000, 30); // max_log_depth=1000, lease=30s

    // [GAP-FIX] spawn lease monitor — makes Corollary 2 constructive
    bus.clone().spawn_lease_monitor();

    let app = Router::new()
        // ── shard CRUD ──────────────────────────────────────────────────
        .route("/shard",      post(handlers::create_shard))
        .route("/shard/{key}", get(handlers::read_shard))
        .route("/shards",     get(handlers::list_shards))
        // ── ACP commit (original — backward-compatible) ─────────────────
        .route("/commit",     post(handlers::commit))
        // ── ACP commit v2 [GAP-FIX] — with optional read_set field ─────
        .route("/commit/v2",  post(handlers::commit_v2))
        // ── Other ────────────────────────────────────────────────────────
        .route("/rollback",   post(handlers::rollback))
        .route("/stats",      get(handlers::stats))
        .route("/metrics",    get(handlers::metrics))
        .layer(CorsLayer::permissive())
        .layer(TraceLayer::new_for_http())
        .with_state(bus);

    let addr = "0.0.0.0:3000";
    tracing::info!("S-Bus server listening on {addr}");
    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}