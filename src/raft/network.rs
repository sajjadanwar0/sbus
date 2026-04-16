// src/raft/network.rs — openraft 0.8.4 RaftNetwork
// Uses reqwest 0.12 (compatible with hyper 1.x used by axum 0.8).
// Verified method signatures against openraft-0.8.4/src/network/network.rs

use std::sync::Arc;
use async_trait::async_trait;
use openraft::{
    BasicNode,
    error::{InstallSnapshotError, RPCError, RaftError, NetworkError},
    network::{RaftNetwork, RaftNetworkFactory, RPCOption},
    raft::{
        AppendEntriesRequest, AppendEntriesResponse,
        InstallSnapshotRequest, InstallSnapshotResponse,
        VoteRequest, VoteResponse,
    },
};
use reqwest::Client;
use serde::{de::DeserializeOwned, Serialize};

use super::SBusTypeConfig;

// ── Factory ───────────────────────────────────────────────────────────────────

pub struct SBusNetworkFactory {
    client: Arc<Client>,
}

impl SBusNetworkFactory {
    pub fn new() -> Self {
        Self {
            client: Arc::new(
                Client::builder()
                    .timeout(std::time::Duration::from_secs(5))
                    .build()
                    .expect("reqwest client"),
            ),
        }
    }
}

impl Default for SBusNetworkFactory {
    fn default() -> Self { Self::new() }
}

#[async_trait]
impl RaftNetworkFactory<SBusTypeConfig> for SBusNetworkFactory {
    type Network = SBusNetwork;

    async fn new_client(&mut self, _target: u64, node: &BasicNode) -> Self::Network {
        SBusNetwork { node_url: node.addr.clone(), client: self.client.clone() }
    }
}

// ── Per-peer connection ───────────────────────────────────────────────────────

pub struct SBusNetwork {
    node_url: String,
    client:   Arc<Client>,
}

impl SBusNetwork {
    async fn post<Req: Serialize, Resp: DeserializeOwned>(
        &self, path: &str, req: &Req,
    ) -> Result<Resp, String> {
        let url = format!("{}{}", self.node_url, path);
        let bytes = self.client
            .post(&url)
            .json(req)
            .send()
            .await
            .map_err(|e| e.to_string())?
            .bytes()
            .await
            .map_err(|e| e.to_string())?;
        serde_json::from_slice(&bytes).map_err(|e| e.to_string())
    }

    fn net_err<E: std::error::Error + Send + Sync + 'static>(
        msg: String,
    ) -> RPCError<u64, BasicNode, E> {
        RPCError::Network(NetworkError::new(
            &std::io::Error::new(std::io::ErrorKind::Other, msg),
        ))
    }
}

// ── RaftNetwork — verified against openraft 0.8.4 trait signatures ────────────
// Method order: append_entries, install_snapshot, vote
#[async_trait]
impl RaftNetwork<SBusTypeConfig> for SBusNetwork {

    async fn append_entries(
        &mut self,
        rpc:  AppendEntriesRequest<SBusTypeConfig>,
        _opt: RPCOption,
    ) -> Result<AppendEntriesResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>> {
        self.post("/raft/append-entries", &rpc).await
            .map_err(|e| Self::net_err::<RaftError<u64>>(e))
    }

    async fn install_snapshot(
        &mut self,
        rpc:  InstallSnapshotRequest<SBusTypeConfig>,
        _opt: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<u64>,
        RPCError<u64, BasicNode, RaftError<u64, InstallSnapshotError>>,
    > {
        self.post("/raft/install-snapshot", &rpc).await
            .map_err(|e| Self::net_err::<RaftError<u64, InstallSnapshotError>>(e))
    }

    async fn vote(
        &mut self,
        rpc:  VoteRequest<u64>,
        _opt: RPCOption,
    ) -> Result<VoteResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>> {
        self.post("/raft/vote", &rpc).await
            .map_err(|e| Self::net_err::<RaftError<u64>>(e))
    }
}