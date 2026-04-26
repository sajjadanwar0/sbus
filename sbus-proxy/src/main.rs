// ───────────────────────────────────────────────────────────────────────────
// main.rs — binary entrypoint for sbus-proxy
//
// All real logic lives in lib.rs and the module files. This binary does
// three things:
//   1. Initialise tracing
//   2. Load ProxyConfig from env
//   3. Build the router and serve with graceful shutdown
// ───────────────────────────────────────────────────────────────────────────

use std::net::SocketAddr;

use anyhow::{Context, Result};
use tokio::signal;
use tracing::{info, warn};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

use sbus_proxy::{build_router, ProxyConfig};

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();

    let config = ProxyConfig::from_env().context("failed to load proxy config")?;
    info!(?config, "sbus-proxy starting");

    let port = config.listen_port;
    let app  = build_router(config)?;

    let addr: SocketAddr = ([0, 0, 0, 0], port).into();
    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .with_context(|| format!("failed to bind :{}", port))?;
    info!(%addr, "sbus-proxy listening");

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .context("server error")?;

    info!("sbus-proxy shutting down cleanly");
    Ok(())
}

fn init_tracing() {
    tracing_subscriber::registry()
        .with(EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| EnvFilter::new("info,sbus_proxy=debug")))
        .with(tracing_subscriber::fmt::layer())
        .init();
}

async fn shutdown_signal() {
    let ctrl_c = async {
        signal::ctrl_c().await.expect("install Ctrl-C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c   => { warn!("received SIGINT"); },
        _ = terminate => { warn!("received SIGTERM"); },
    }
}
