// src/main.rs — S-Bus v37 (Raft + RocksDB + dynamic membership)
//
// Environment variables:
//   SBUS_PORT=7000                       HTTP port
//   SBUS_RAFT_NODE_ID=0                  Raft node ID (omit for single-node mode)
//   SBUS_RAFT_PEERS="0=http://localhost:7000,1=http://localhost:7001,..."
//   SBUS_DATA_DIR=./data/node0           RocksDB data directory (default: ./data/node{id})
//   SBUS_ADMIN_ENABLED=1                 Enable admin endpoints
//
// Example 3-node cluster:
//   SBUS_PORT=7000 SBUS_RAFT_NODE_ID=0 \
//   SBUS_RAFT_PEERS="0=http://localhost:7000,1=http://localhost:7001,2=http://localhost:7002" \
//   SBUS_DATA_DIR=./data/node0 SBUS_ADMIN_ENABLED=1 ./target/release/sbus-server

use axum::{routing::{get, post}, Router};
use openraft::storage::Adaptor;
use std::sync::Arc;
use tower_http::{cors::CorsLayer, trace::TraceLayer};
use tracing_subscriber::{fmt, EnvFilter};

mod raft;
mod cluster;
mod bus {
    pub mod engine;
    pub mod registry;
    pub mod types;
}
mod api {
    pub mod cluster_handlers;
    pub mod handlers;
    pub mod raft_handlers;
}

use api::{
    cluster_handlers::cluster_status_handler,
    handlers::{
        AppState,
        admin_add_node, admin_commit, admin_create_shard,
        admin_delivery_log, admin_health, admin_inject_stale, admin_reset,
        commit_v1, commit_v2, create_session, create_shard,
        get_shard, list_shards, metrics, rollback, stats,
    },
    raft_handlers::{
        add_learner, append_entries, change_membership, install_snapshot,
        raft_init, raft_leader, raft_metrics, vote,
    },
};
use bus::engine::SBus;
use cluster::ClusterConfig;
use raft::{
    SBusTypeConfig,
    build_raft_config,
    network::SBusNetworkFactory,
    store::{SBusStore, open_db},
};

#[tokio::main]
async fn main() {
    fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,sbus=debug,openraft=info".parse().unwrap()),
        )
        .init();

    let port          = std::env::var("SBUS_PORT").unwrap_or_else(|_| "7000".into());
    let admin_enabled = std::env::var("SBUS_ADMIN_ENABLED")
        .map(|v| v == "1").unwrap_or(false);

    // Parse Raft identity
    let raft_node_id: Option<u64> = std::env::var("SBUS_RAFT_NODE_ID")
        .ok().and_then(|s| s.parse().ok());
    let raft_peers_str = std::env::var("SBUS_RAFT_PEERS").unwrap_or_default();

    let this_url = raft_node_id.and_then(|id| {
        raft_peers_str.split(',').find_map(|s| {
            let (nid, addr) = s.split_once('=')?;
            if nid.trim().parse::<u64>().ok()? == id {
                Some(addr.trim().to_owned())
            } else { None }
        })
    }).unwrap_or_else(|| format!("http://localhost:{port}"));

    let bus     = SBus::new();
    let cluster = ClusterConfig::from_env();
    bus.rebuild_from_wal();
    bus.spawn_lease_monitor();

    // ── RocksDB init ──────────────────────────────────────────────────────────
    let data_dir = std::env::var("SBUS_DATA_DIR")
        .unwrap_or_else(|_| format!("./data/node{}", raft_node_id.unwrap_or(0)));
    std::fs::create_dir_all(&data_dir)
        .unwrap_or_else(|e| panic!("Cannot create data dir {data_dir}: {e}"));
    let (logs_tree, meta_tree) = open_db(&data_dir);
    tracing::info!("sled database opened at {data_dir}");

    // ── Raft init ─────────────────────────────────────────────────────────────
    let sbus_raft = if let Some(node_id) = raft_node_id {
        let config = build_raft_config();
        // SBusStore restores state from RocksDB snapshot on startup
        let store  = SBusStore::new(bus.clone(), logs_tree.clone(), meta_tree.clone());
        let (log_store, state_machine) = Adaptor::<SBusTypeConfig, SBusStore>::new(store);

        let raft = openraft::Raft::<
            SBusTypeConfig,
            SBusNetworkFactory,
            Adaptor<SBusTypeConfig, SBusStore>,
            Adaptor<SBusTypeConfig, SBusStore>,
        >::new(
            node_id,
            config,
            SBusNetworkFactory::new(),
            log_store,
            state_machine,
        )
            .await
            .expect("raft node");

        tracing::info!(node_id, url = this_url.as_str(), data_dir = data_dir.as_str(), "Raft node started");
        Some(raft)
    } else {
        tracing::info!("Single-node mode (set SBUS_RAFT_NODE_ID for Raft cluster)");
        None
    };

    let node_id = raft_node_id.unwrap_or(0);
    let state = Arc::new(AppState {
        bus, cluster, admin_enabled,
        raft: sbus_raft, node_id, this_url,
    });

    let app = Router::new()
        // Core
        .route("/shard",            post(create_shard))
        .route("/shard/{key}",      get(get_shard))
        .route("/shards",           get(list_shards))
        .route("/commit",           post(commit_v1))
        .route("/commit/v2",        post(commit_v2))
        .route("/rollback",         post(rollback))
        .route("/stats",            get(stats))
        .route("/metrics",          get(metrics))
        .route("/session",          post(create_session))
        // Admin
        .route("/admin/health",       get(admin_health))
        .route("/admin/reset",        post(admin_reset))
        .route("/admin/delivery-log", get(admin_delivery_log))
        .route("/admin/inject-stale", post(admin_inject_stale))
        .route("/admin/shard",        post(admin_create_shard))
        .route("/admin/commit",       post(admin_commit))
        .route("/admin/add-node",     post(admin_add_node))
        // Cluster
        .route("/cluster/status",     get(cluster_status_handler))
        // Raft RPCs
        .route("/raft/append-entries",    post(append_entries))
        .route("/raft/vote",              post(vote))
        .route("/raft/install-snapshot",  post(install_snapshot))
        // Raft management
        .route("/raft/init",              post(raft_init))
        .route("/raft/add-learner",       post(add_learner))
        .route("/raft/change-membership", post(change_membership))
        .route("/raft/metrics",           get(raft_metrics))
        .route("/raft/leader",            get(raft_leader))
        .layer(CorsLayer::permissive())
        .layer(TraceLayer::new_for_http())
        .with_state(state);

    let addr = format!("0.0.0.0:{port}");
    tracing::info!("S-Bus v37 (Raft + RocksDB) on {}", addr);
    let listener = tokio::net::TcpListener::bind(&addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}