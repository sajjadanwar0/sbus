// src/cluster/mod.rs — S-Bus v35 Distributed
// Shard routing: FNV-1a(key) % num_nodes
// HTTP helpers use reqwest with json feature for serialization.
// Add to Cargo.toml: reqwest = { version = "0.12", features = ["json"] }

pub mod ramp;

use std::sync::Arc;
use reqwest::Client;
use serde::Serialize;
use serde_json::Value;
use tracing::info;

// ── ClusterConfig ─────────────────────────────────────────────────────────────

#[derive(Clone, Debug)]
pub struct ClusterConfig {
    pub enabled:  bool,
    pub node_id:  usize,
    pub nodes:    Vec<String>,
    pub this_url: String,
    pub client:   Arc<Client>,
}

impl ClusterConfig {
    /// Build from env:
    ///   SBUS_CLUSTER_NODES = "http://h0:7000,http://h1:7001"
    ///   SBUS_NODE_ID       = 0  (0-based index of this node)
    pub fn from_env() -> Self {
        let raw = std::env::var("SBUS_CLUSTER_NODES").unwrap_or_default();
        let node_id: usize = std::env::var("SBUS_NODE_ID")
            .ok().and_then(|s| s.parse().ok()).unwrap_or(0);
        let port = std::env::var("SBUS_PORT").unwrap_or_else(|_| "7000".into());
        let client = Arc::new(
            Client::builder()
                .timeout(std::time::Duration::from_secs(30))
                .build()
                .expect("reqwest client"),
        );
        if raw.trim().is_empty() {
            return Self {
                enabled: false, node_id: 0, nodes: vec![],
                this_url: format!("http://localhost:{port}"), client,
            };
        }
        let nodes: Vec<String> = raw.split(',')
            .map(|s| s.trim().to_owned())
            .filter(|s| !s.is_empty())
            .collect();
        let this_url = nodes.get(node_id).cloned()
            .unwrap_or_else(|| format!("http://localhost:{port}"));
        info!(node_id, url = this_url.as_str(), n = nodes.len(), "cluster mode");
        Self { enabled: true, node_id, nodes, this_url, client }
    }

    // ── Routing ───────────────────────────────────────────────────────────────

    pub fn owning_node(&self, key: &str) -> usize {
        if self.nodes.is_empty() { return 0; }
        (fnv1a(key.as_bytes()) % self.nodes.len() as u64) as usize
    }
    pub fn is_local(&self, key: &str) -> bool {
        !self.enabled || self.owning_node(key) == self.node_id
    }
    pub fn owning_url(&self, key: &str) -> &str {
        let i = self.owning_node(key);
        self.nodes.get(i).map(|s| s.as_str()).unwrap_or(&self.this_url)
    }
    pub fn needs_2pc(&self, primary: &str, rs_keys: &[String]) -> bool {
        if !self.enabled { return false; }
        let pn = self.owning_node(primary);
        rs_keys.iter().any(|k| self.owning_node(k) != pn)
    }
    pub fn url_for(&self, nid: usize) -> &str {
        self.nodes.get(nid).map(|s| s.as_str()).unwrap_or(&self.this_url)
    }
}

// ── HTTP helpers ──────────────────────────────────────────────────────────────
// Requires reqwest with features = ["json"] in Cargo.toml.

/// POST JSON body to url, return (status_code, response_body).
pub async fn post_json<B: Serialize>(
    client: &Client,
    url:    &str,
    body:   &B,
) -> Result<(u16, Value), String> {
    let resp = client
        .post(url)
        .json(body)                           // reqwest json feature
        .send()
        .await
        .map_err(|e| format!("network: {e}"))?;
    let status = resp.status().as_u16();
    let value: Value = resp
        .json()                               // reqwest json feature
        .await
        .map_err(|e| format!("parse: {e}"))?;
    Ok((status, value))
}

/// GET url, return (status_code, response_body).
pub async fn get_json(
    client: &Client,
    url:    &str,
) -> Result<(u16, Value), String> {
    let resp = client
        .get(url)
        .send()
        .await
        .map_err(|e| format!("network: {e}"))?;
    let status = resp.status().as_u16();
    let value: Value = resp
        .json()
        .await
        .map_err(|e| format!("parse: {e}"))?;
    Ok((status, value))
}

// ── Cluster status dashboard ──────────────────────────────────────────────────

pub async fn cluster_status(cfg: &ClusterConfig) -> Value {
    let mut peers = vec![];
    for (i, url) in cfg.nodes.iter().enumerate() {
        let is_self = i == cfg.node_id;
        let status = if is_self {
            "self".to_owned()
        } else {
            let health = format!("{url}/admin/health");
            match cfg.client
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
        peers.push(serde_json::json!({
            "node_id": i, "url": url,
            "status": status, "is_self": is_self,
        }));
    }
    serde_json::json!({
        "cluster_enabled": cfg.enabled,
        "this_node_id":    cfg.node_id,
        "this_url":        cfg.this_url,
        "num_nodes":       cfg.nodes.len(),
        "peers":           peers,
        "routing":         "fnv1a(key) % num_nodes",
        "isolation":       "ORI via RAMP-lite 2PC",
    })
}

// ── FNV-1a hash (64-bit, no external dependency) ─────────────────────────────

fn fnv1a(b: &[u8]) -> u64 {
    b.iter().fold(14695981039346656037u64,
                  |h, &x| h.wrapping_mul(1099511628211) ^ x as u64)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn routing_deterministic() {
        assert_eq!(fnv1a(b"key_a"), fnv1a(b"key_a"));
        assert_ne!(fnv1a(b"key_a"), fnv1a(b"key_b"));
    }
    #[test]
    fn routing_distributes_uniformly() {
        let mut counts = [0usize; 2];
        for i in 0..1000 {
            counts[(fnv1a(format!("shard_{i}").as_bytes()) % 2) as usize] += 1;
        }
        assert!(counts[0] > 400 && counts[0] < 600, "uneven: {counts:?}");
    }
}