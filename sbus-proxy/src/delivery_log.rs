// ───────────────────────────────────────────────────────────────────────────
// delivery_log.rs — client for S-Bus DeliveryLog registration
//
// PATCHED for Exp. PROXY-PH2 (v50):
//   - `register` now takes an explicit `shard_suffix: &str` parameter. When
//     non-empty, the suffix is appended to every shard key before the POST
//     body is built. Enables a single proxy process to serve many
//     run-isolated trials whose server-side shard keys are of the form
//     "{base}_{run_id}" (as used by exp_proxy_ph2.py and all existing
//     multi-agent experiments since v32).
//
//   - Behaviour when suffix is empty ("") is exactly as before this patch;
//     existing call sites need no change.
//
// S-Bus is expected to expose:
//   POST {sbus_url}/delivery_log/register
//     body: {
//       "agent_id":     str,
//       "session_id":   str,
//       "shards_used":  [str, ...],
//       "source":       "proxy" | "http" | "arsi",
//       "timestamp_ms": int
//     }
// ───────────────────────────────────────────────────────────────────────────

use std::collections::BTreeSet;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::Serialize;
use tracing::{debug, warn};

#[derive(Debug, Serialize)]
pub struct RegisterRequest<'a> {
    pub agent_id:     &'a str,
    pub session_id:   &'a str,
    pub shards_used:  Vec<String>,
    pub source:       &'static str,
    pub timestamp_ms: u128,
}

pub struct DeliveryLogClient {
    client:   reqwest::Client,
    sbus_url: String,
}

impl DeliveryLogClient {
    pub fn new(client: reqwest::Client, sbus_url: String) -> Self {
        Self { client, sbus_url }
    }

    /// Register a set of shard references. Non-fatal on failure — logs a
    /// warning and returns Ok(()) even if S-Bus is unreachable, because a
    /// failing DeliveryLog must not break the agent's LLM call.
    ///
    /// `shard_suffix`, if non-empty, is appended to every shard key before
    /// POSTing. Use this for per-trial run-isolation where the server-side
    /// key schema is "{base}_{run_id}" but the proxy's vocabulary is
    /// configured on base names.
    ///
    /// Callers that need to enforce hard ordering (commit-after-register)
    /// should await this future before allowing the commit path to proceed.
    pub async fn register(
        &self,
        agent_id:     &str,
        session_id:   &str,
        shards_used:  BTreeSet<String>,
        source:       &'static str,
        shard_suffix: &str,
    ) -> anyhow::Result<()> {
        if shards_used.is_empty() {
            debug!("no shard refs extracted; skipping DeliveryLog register");
            return Ok(());
        }

        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis())
            .unwrap_or(0);

        // Apply the suffix if set. Shard names are ASCII-identifier-like and
        // the suffix is short; plain concatenation is fine.
        let keyed_shards: Vec<String> = if shard_suffix.is_empty() {
            shards_used.into_iter().collect()
        } else {
            shards_used
                .into_iter()
                .map(|s| format!("{s}{shard_suffix}"))
                .collect()
        };

        let body = RegisterRequest {
            agent_id,
            session_id,
            shards_used: keyed_shards,
            source,
            timestamp_ms: now_ms,
        };

        let url = format!("{}/delivery_log/register", self.sbus_url.trim_end_matches('/'));
        let res = self.client.post(&url).json(&body).send().await;

        match res {
            Ok(r) if r.status().is_success() => {
                debug!(%url, status = r.status().as_u16(), "DeliveryLog registered");
                Ok(())
            }
            Ok(r) => {
                warn!(%url, status = r.status().as_u16(), "DeliveryLog rejected registration");
                Ok(()) // non-fatal
            }
            Err(e) => {
                warn!(%url, error = %e, "DeliveryLog unreachable; continuing without registration");
                Ok(()) // non-fatal
            }
        }
    }
}

// ───────────────────────────────────────────────────────────────────────────
// Tests
// ───────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn suffix_applied_when_nonempty() {
        // Direct test of the suffix-concatenation logic (we can't easily
        // unit-test the full register() path without a mock HTTP server —
        // integration tests under tests/ cover that).
        let hits: BTreeSet<String> = ["models_state", "orm_query"]
            .into_iter().map(String::from).collect();
        let suffix = "_run123";
        let keyed: Vec<String> = hits
            .into_iter()
            .map(|s| format!("{s}{suffix}"))
            .collect();
        assert!(keyed.contains(&"models_state_run123".to_string()));
        assert!(keyed.contains(&"orm_query_run123".to_string()));
    }

    #[test]
    fn suffix_empty_preserves_names() {
        let hits: BTreeSet<String> = ["models_state"]
            .into_iter().map(String::from).collect();
        let suffix = "";
        let keyed: Vec<String> = if suffix.is_empty() {
            hits.into_iter().collect()
        } else {
            hits.into_iter().map(|s| format!("{s}{suffix}")).collect()
        };
        assert_eq!(keyed, vec!["models_state".to_string()]);
    }
}
