use std::sync::Arc;

use reqwest::Client;
use serde::Serialize;
use serde_json::{Value, json};
use tracing::info;

#[derive(Clone, Debug)]
pub struct ClusterConfig {
    pub enabled:  bool,
    pub node_id:  usize,
    pub nodes:    Vec<String>,
    pub this_url: String,
    pub client:   Arc<Client>,
}

impl ClusterConfig {
    pub fn from_env() -> Self {
        let raw = std::env::var("SBUS_CLUSTER_NODES").unwrap_or_default();
        let node_id: usize = std::env::var("SBUS_NODE_ID")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        let port = std::env::var("SBUS_PORT").unwrap_or_else(|_| "7000".into());

        let client = Arc::new(
            Client::builder()
                .timeout(std::time::Duration::from_secs(30))
                .build()
                .expect("reqwest client"),
        );

        if raw.trim().is_empty() {
            return Self {
                enabled: false,
                node_id: 0,
                nodes: Vec::new(),
                this_url: format!("http://localhost:{port}"),
                client,
            };
        }

        let nodes: Vec<String> = raw
            .split(',')
            .map(|s| s.trim().to_owned())
            .filter(|s| !s.is_empty())
            .collect();
        let this_url = nodes
            .get(node_id)
            .cloned()
            .unwrap_or_else(|| format!("http://localhost:{port}"));

        info!(
            node_id,
            url = this_url.as_str(),
            n = nodes.len(),
            "cluster mode"
        );

        Self { enabled: true, node_id, nodes, this_url, client }
    }
}
pub async fn post_json<B: Serialize>(
    client: &Client,
    url:    &str,
    body:   &B,
) -> Result<(u16, Value), String> {
    let resp = client
        .post(url)
        .json(body)
        .send()
        .await
        .map_err(|e| format!("network: {e}"))?;
    let status = resp.status().as_u16();
    let value: Value = resp.json().await.map_err(|e| format!("parse: {e}"))?;
    Ok((status, value))
}

pub async fn cluster_status(cfg: &ClusterConfig) -> Value {
    let mut peers = Vec::with_capacity(cfg.nodes.len());
    for (i, url) in cfg.nodes.iter().enumerate() {
        let is_self = i == cfg.node_id;
        let status = if is_self {
            "self".to_owned()
        } else {
            let health = format!("{url}/admin/health");
            match cfg
                .client
                .get(&health)
                .timeout(std::time::Duration::from_secs(3))
                .send()
                .await
            {
                Ok(r) if r.status().is_success() => "ok".to_owned(),
                Ok(r) => format!("http_{}", r.status().as_u16()),
                Err(e) => format!("unreachable: {e}"),
            }
        };
        peers.push(json!({
            "node_id": i,
            "url":     url,
            "status":  status,
            "is_self": is_self,
        }));
    }
    json!({
        "cluster_enabled": cfg.enabled,
        "this_node_id":    cfg.node_id,
        "this_url":        cfg.this_url,
        "num_nodes":       cfg.nodes.len(),
        "peers":           peers,
        "replication":     "Raft (openraft)",
        "isolation":       "ORI via Raft total order",
    })
}
