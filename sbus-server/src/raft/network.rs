use std::sync::Arc;

use async_trait::async_trait;
use openraft::{
    BasicNode,
    error::{InstallSnapshotError, NetworkError, RPCError, RaftError},
    network::{RPCOption, RaftNetwork, RaftNetworkFactory},
    raft::{
        AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest,
        InstallSnapshotResponse, VoteRequest, VoteResponse,
    },
};
use reqwest::Client;
use serde::{Serialize, de::DeserializeOwned};

use super::SBusTypeConfig;

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
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl RaftNetworkFactory<SBusTypeConfig> for SBusNetworkFactory {
    type Network = SBusNetwork;

    async fn new_client(&mut self, _target: u64, node: &BasicNode) -> Self::Network {
        SBusNetwork {
            node_url: node.addr.clone(),
            client: self.client.clone(),
        }
    }
}

pub struct SBusNetwork {
    node_url: String,
    client: Arc<Client>,
}

impl SBusNetwork {
    async fn post<Req, Resp>(&self, path: &str, req: &Req) -> Result<Resp, String>
    where
        Req: Serialize,
        Resp: DeserializeOwned,
    {
        let url = format!("{}{}", self.node_url, path);
        let bytes = self
            .client
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

    fn net_err<E>(msg: String) -> RPCError<u64, BasicNode, E>
    where
        E: std::error::Error + Send + Sync + 'static,
    {
        RPCError::Network(NetworkError::new(&std::io::Error::other(msg)))
    }
}

#[async_trait]
impl RaftNetwork<SBusTypeConfig> for SBusNetwork {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<SBusTypeConfig>,
        _opt: RPCOption,
    ) -> Result<AppendEntriesResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>> {
        self.post("/raft/append-entries", &rpc)
            .await
            .map_err(Self::net_err::<RaftError<u64>>)
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<SBusTypeConfig>,
        _opt: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<u64>,
        RPCError<u64, BasicNode, RaftError<u64, InstallSnapshotError>>,
    > {
        self.post("/raft/install-snapshot", &rpc)
            .await
            .map_err(Self::net_err::<RaftError<u64, InstallSnapshotError>>)
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<u64>,
        _opt: RPCOption,
    ) -> Result<VoteResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>> {
        self.post("/raft/vote", &rpc)
            .await
            .map_err(Self::net_err::<RaftError<u64>>)
    }
}
