// src/main.rs
//
// S-Bus HTTP server — Axum + Tokio.
//
// Additions vs original:
//   - POST /commit/v2_naive route (Table 6 control condition)
//   - Startup log showing active ACP config flags (ablation visibility)

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
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,sbus=debug".parse().unwrap()),
        )
        .init();

    let bus = SBus::with_options(1_000, 30); // max_log_depth=1000, lease=30s

    // Log active ACP config at startup.
    // This makes the current ablation condition immediately visible in server
    // output and in `curl /stats` without needing to check env vars manually.
    //
    // Ablation usage:
    //   SBUS_TOKEN=0                cargo run   →  –token condition   (Table 11)
    //   SBUS_VERSION=0              cargo run   →  –version condition (Table 11)
    //   SBUS_LOG=0                  cargo run   →  –log condition     (Table 11)
    //   SBUS_TOKEN=0 SBUS_VERSION=0 cargo run   →  –both condition    (Table 11)
    //   SBUS_RETRY_BUDGET=5         cargo run   →  eliminate retry exhaustion
    tracing::info!(
        token        = bus.config.enable_ownership_token,
        version      = bus.config.enable_version_check,
        log          = bus.config.enable_delta_log,
        retry_budget = bus.config.retry_budget,
        lease_secs   = bus.config.lease_timeout_secs,
        "ACP config loaded — set SBUS_TOKEN/VERSION/LOG/RETRY_BUDGET for ablation"
    );

    // Spawn lease monitor (Corollary 2.2 — constructive liveness).
    // Releases orphaned tokens within lease_timeout_secs + 5 seconds.
    bus.clone().spawn_lease_monitor();

    let app = Router::new()
        // ── Shard CRUD ──────────────────────────────────────────────────
        .route("/shard",       post(handlers::create_shard))
        .route("/shard/{key}", get(handlers::read_shard))
        .route("/shards",      get(handlers::list_shards))
        // ── ACP commit endpoints ────────────────────────────────────────
        // /commit      — original single-shard ACP (backward-compatible)
        // /commit/v2   — opt-in cross-shard read_set, sorted lock order
        //                (Corollary 2.1, Lemma 2, Assumption A1)
        // /commit/v2_naive — same body as v2, UNORDERED lock acquisition
        //                (Table 6 control condition — do not use in production)
        .route("/commit",          post(handlers::commit))
        .route("/commit/v2",       post(handlers::commit_v2))
        .route("/commit/v2_naive", post(handlers::commit_v2_naive))
        // ── Other ───────────────────────────────────────────────────────
        .route("/rollback", post(handlers::rollback))
        .route("/stats",    get(handlers::stats))
        .route("/metrics",  get(handlers::metrics))
        .layer(CorsLayer::permissive())
        .layer(TraceLayer::new_for_http())
        .with_state(bus);

    let addr = "0.0.0.0:3000";
    tracing::info!("S-Bus server listening on {addr}");
    tracing::info!(
        "Routes: POST /shard  GET /shard/:key  GET /shards  \
         POST /commit  POST /commit/v2  POST /commit/v2_naive  \
         POST /rollback  GET /stats  GET /metrics"
    );

    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}