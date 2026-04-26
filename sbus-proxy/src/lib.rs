// ───────────────────────────────────────────────────────────────────────────
// lib.rs — exposes the proxy's internals as a library
//
// Why have a lib in addition to the binary? Because integration tests in
// `tests/` can only link against a library, not a binary. By moving all the
// real logic into lib.rs and keeping main.rs as a thin launcher, we get:
//   - Unit tests in each module (still work the same)
//   - Integration tests under tests/ that can wire up a real proxy instance
//     against mock servers
//   - A clean path for a future embedding use case (running the proxy
//     in-process inside another Rust program)
// ───────────────────────────────────────────────────────────────────────────

pub mod config;
pub mod delivery_log;
pub mod extractor;
pub mod proxy;
pub mod vocabulary;

use std::sync::Arc;

use anyhow::{Context, Result};

pub use config::ProxyConfig;
pub use proxy::AppState;
pub use vocabulary::Vocabulary;

/// Build an `axum::Router` configured with the given `ProxyConfig`.
///
/// The caller is responsible for binding and serving the router. This is
/// the integration-test entry point: tests can construct a router against
/// mock-server URLs, bind to port 0, and drive real HTTP requests.
pub fn build_router(config: ProxyConfig) -> Result<axum::Router> {
    let vocab = Vocabulary::from_config(&config)
        .context("failed to build shard vocabulary")?;

    let upstream_client = reqwest::Client::builder()
        .tcp_nodelay(true)
        .build()
        .context("failed to build upstream HTTP client")?;

    let sbus_client = reqwest::Client::builder()
        .tcp_nodelay(true)
        .timeout(std::time::Duration::from_secs(5))
        .build()
        .context("failed to build S-Bus client")?;

    let state = Arc::new(AppState {
        config,
        vocab: Arc::new(vocab),
        upstream_client,
        sbus_client,
    });

    Ok(proxy::router(state))
}
