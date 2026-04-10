// src/main.rs — S-Bus v29 (corrected)
//
// Changes from v22:
//   FIX-4 (WAL crash recovery): calls bus.rebuild_from_wal() BEFORE binding
//     the HTTP listener. Registry is restored before the first request.
//   FIX-5 (HMAC status): logs whether HMAC is enabled at startup.
//   FIX-Tmax (corrected): warns if SBUS_SESSION_TTL is below the CORRECT
//     formula threshold: 1.5 × steps × t_step (NO N multiplier).
//     Wall time ≈ max(agents) ≈ steps × t_step, not N × steps × t_step.
//     Default 3600s provides ≥8× safety margin for 50-step tasks.
//
// QUICK START:
//   # Dev (no HMAC, no WAL):
//   cargo run --release
//
//   # Full (WAL + HMAC):
//   export SBUS_WAL_PATH=results/wal.jsonl
//   export SBUS_SESSION_TTL=3600    # 1 hr: 8× safety margin for 50 steps
//   cargo run --release
//
//   # TTL formula (corrected): TTL ≥ 1.5 × steps × t_step
//   # For 50 steps, t_step=5s → TTL ≥ 375s. Default 3600s is safe.
//   # For long tasks (>500 steps or slow APIs): set TTL = 2 × measured_wall_time
//
// ABLATION FLAGS (no rebuild needed):
//   SBUS_TOKEN=1       disable ownership token check
//   SBUS_VERSION=1     disable version mismatch check
//   SBUS_LOG=1         disable delta log (token-only mode)
//   SBUS_RETRY_BUDGET=K  set max retries (default 5)

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
    pub mod registry;
    pub mod types;
}

use api::handlers;
use bus::engine::SBus;

#[tokio::main]
async fn main() {
    fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,sbus=debug".parse().unwrap()),
        )
        .init();

    // ── Environment ───────────────────────────────────────────────────────────
    let max_log_depth      = std::env::var("SBUS_MAX_LOG_DEPTH")
        .ok().and_then(|s| s.parse().ok()).unwrap_or(1_000usize);
    let lease_timeout_secs = std::env::var("SBUS_SESSION_TTL")
        .ok().and_then(|s| s.parse().ok()).unwrap_or(3600u64);
    let wal_path           = std::env::var("SBUS_WAL_PATH").ok();
    let hmac_enabled       = std::env::var("SBUS_HMAC_KEY").is_ok();

    // ── Tmax safety check (CORRECTED formula: no N multiplier) ───────────────
    // Wall time ≈ max_i(agent_i) ≈ steps × t_step  (NOT N × steps × t_step).
    // For 50 steps, t_step=5s: T_max ≤ 375s. Default TTL=3600s provides ≥8× margin.
    // Only warn if TTL is genuinely low (< 375s for 50-step tasks).
    // For very long tasks: set TTL = 2 × measured_wall_time.
    const TMAX_50_STEPS_5S: u64 = 375; // 1.5 × 50 × 5
    if lease_timeout_secs < TMAX_50_STEPS_5S {
        tracing::warn!(
            ttl = lease_timeout_secs,
            "SBUS_SESSION_TTL={} is below the safe Tmax for 50-step tasks \
             (Tmax ≈ 375s = 1.5 × 50 steps × 5s/step). \
             TTL expiry causes silent false negatives: stale reads slip through \
             without error. Increase TTL or set SBUS_SESSION_TTL=2×measured_wall_time.",
            lease_timeout_secs
        );
    } else {
        tracing::info!(
            ttl = lease_timeout_secs,
            "SBUS_SESSION_TTL={} provides ≥{}× safety margin over Tmax≈375s for 50-step tasks.",
            lease_timeout_secs,
            lease_timeout_secs / TMAX_50_STEPS_5S
        );
    }

    // ── Construct bus ─────────────────────────────────────────────────────────
    let bus = SBus::with_options(max_log_depth, lease_timeout_secs);

    tracing::info!(
        token        = bus.config.enable_ownership_token,
        version      = bus.config.enable_version_check,
        log          = bus.config.enable_delta_log,
        retry_budget = bus.config.retry_budget,
        lease_secs   = lease_timeout_secs,
        wal          = wal_path.as_deref().unwrap_or("disabled"),
        hmac         = hmac_enabled,
        "S-Bus v29 — ACP config loaded"
    );
    tracing::info!(
        "DeliveryLog: active (A1 server-enforced for HTTP reads). \
         WSS: write-write + R_obs stale-read elimination."
    );

    // Log ablation flag status (0=enabled, 1=disabled)
    let tok_flag = std::env::var("SBUS_TOKEN").unwrap_or("0".into());
    let ver_flag = std::env::var("SBUS_VERSION").unwrap_or("0".into());
    let log_flag = std::env::var("SBUS_LOG").unwrap_or("0".into());
    tracing::info!(
        "Ablation flags: SBUS_TOKEN={tok_flag} | SBUS_VERSION={ver_flag} | \
         SBUS_LOG={log_flag} | SBUS_RETRY_BUDGET={}",
        std::env::var("SBUS_RETRY_BUDGET").unwrap_or("N".into())
    );

    if log_flag != "0" {
        tracing::warn!(
            "SBUS_LOG=1: DeliveryLog DISABLED — running token-only mode. \
             Cross-shard stale reads not tracked. Use only for ablation studies."
        );
    }

    if hmac_enabled {
        tracing::info!("HMAC agent authentication: ENABLED (T1/T2 threat mitigated)");
    } else {
        tracing::warn!(
            "HMAC agent authentication: DISABLED (single-tenant / dev mode). \
             Set SBUS_HMAC_KEY for multi-tenant or untrusted deployments."
        );
    }

    // ── FIX-4: WAL crash recovery — BEFORE binding listener ──────────────────
    if wal_path.is_some() {
        tracing::info!("WAL crash recovery: starting replay...");
        bus.rebuild_from_wal();
        tracing::info!("WAL crash recovery: complete.");
    }

    // ── Lease monitor ─────────────────────────────────────────────────────────
    bus.clone().spawn_lease_monitor();

    // ── Router ────────────────────────────────────────────────────────────────
    let app = Router::new()
        .route("/shard",           post(handlers::create_shard))
        .route("/shard/{key}",     get(handlers::read_shard))      // ?agent_id=X for DeliveryLog
        .route("/shards",          get(handlers::list_shards))
        .route("/commit",          post(handlers::commit))
        .route("/commit/v2",       post(handlers::commit_v2))       // WSS path
        .route("/commit/v2_naive", post(handlers::commit_v2_naive)) // deadlock demo
        .route("/rollback",        post(handlers::rollback))
        .route("/stats",           get(handlers::stats))
        .route("/metrics",         get(handlers::metrics))
        .layer(CorsLayer::permissive())
        .layer(TraceLayer::new_for_http())
        .with_state(bus);

    let port = std::env::var("SBUS_PORT").unwrap_or_else(|_| "7000".into());
    let addr = format!("0.0.0.0:{port}");
    tracing::info!("S-Bus v29 listening on {addr}");

    let listener = tokio::net::TcpListener::bind(&addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}