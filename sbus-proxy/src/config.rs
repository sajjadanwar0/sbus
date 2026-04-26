// ───────────────────────────────────────────────────────────────────────────
// config.rs — environment-driven configuration
//
// Twelve-factor style: all config comes from environment variables. Missing
// required vars cause startup to fail loudly. This keeps the binary trivially
// reproducible — no config files, no hidden state.
// ───────────────────────────────────────────────────────────────────────────

use std::fmt;

use anyhow::{bail, Result};
use serde::Deserialize;

/// Proxy configuration loaded from environment at startup.
///
/// Env vars read (with defaults):
///   SBUS_PROXY_PORT                    u16  (default 9000)
///   SBUS_PROXY_UPSTREAM_URL            str  (default "https://api.openai.com")
///                                           Used for OpenAI-format paths
///                                           (/v1/chat/completions).
///   SBUS_PROXY_UPSTREAM_URL_ANTHROPIC  str  (default "https://api.anthropic.com")
///                                           Used for Anthropic-format paths
///                                           (/v1/messages).
///   SBUS_PROXY_UPSTREAM_URL_GOOGLE     str  (default
///                                           "https://generativelanguage.googleapis.com")
///                                           Used for Google Gemini-format paths
///                                           (/v1beta/models/{model}:generateContent).
///   SBUS_URL                           str  (default "http://localhost:7000")
///   SBUS_PROXY_VOCAB                   str  comma-separated shard names (required)
///   SBUS_PROXY_AGENT_HEADER            str  HTTP header carrying agent_id
///                                           (default "X-SBus-Agent-Id")
///   SBUS_PROXY_SESSION_HEADER          str  HTTP header carrying session_id
///                                           (default "X-SBus-Session-Id")
///   SBUS_PROXY_DEBUG_PASSTHROUGH       bool (default false)
///                                           If true, proxy forwards requests without
///                                           extracting references — useful to isolate
///                                           extraction overhead in benchmarks.
#[derive(Clone, Deserialize)]
pub struct ProxyConfig {
    #[serde(default = "default_port")]
    pub listen_port: u16,

    #[serde(default = "default_upstream_url")]
    pub upstream_url: String,

    /// Upstream URL for Anthropic API paths (/v1/messages).
    /// Allows multi-upstream routing: OpenAI-format requests hit
    /// `upstream_url`; Anthropic-format requests hit `upstream_url_anthropic`.
    #[serde(default = "default_upstream_url_anthropic")]
    pub upstream_url_anthropic: String,

    /// Upstream URL for Google Gemini API paths (/v1beta/models/...).
    /// Default `https://generativelanguage.googleapis.com` is the public
    /// Google AI for Developers endpoint. For Vertex AI use, override to
    /// `https://{location}-aiplatform.googleapis.com`.
    #[serde(default = "default_upstream_url_google")]
    pub upstream_url_google: String,

    #[serde(default = "default_sbus_url", rename = "sbus_url")]
    pub sbus_url: String,

    /// Comma-separated list of shard base names the proxy will look for.
    /// e.g. "models_state,orm_query,test_fixture,review_notes"
    #[serde(rename = "sbus_proxy_vocab")]
    pub vocab_raw: String,

    #[serde(default = "default_agent_header")]
    pub agent_header: String,

    #[serde(default = "default_session_header")]
    pub session_header: String,

    #[serde(default)]
    pub debug_passthrough: bool,
}

fn default_port() -> u16 { 9000 }
fn default_upstream_url() -> String { "https://api.openai.com".to_owned() }
fn default_upstream_url_anthropic() -> String { "https://api.anthropic.com".to_owned() }
fn default_upstream_url_google() -> String {
    "https://generativelanguage.googleapis.com".to_owned()
}
fn default_sbus_url() -> String { "http://localhost:7000".to_owned() }
fn default_agent_header() -> String { "X-SBus-Agent-Id".to_owned() }
fn default_session_header() -> String { "X-SBus-Session-Id".to_owned() }

impl ProxyConfig {
    /// Load from environment. Returns Err on missing required vars.
    pub fn from_env() -> Result<Self> {
        // envy reads from std::env with its own prefix mapping. We accept the
        // SBUS_PROXY_* prefix for our own vars, and SBUS_URL (unprefixed) for
        // the S-Bus server URL to match existing S-Bus conventions.
        let cfg: Self = envy::prefixed("SBUS_PROXY_")
            .from_env::<Self>()
            .or_else(|_| envy::from_env::<Self>())
            .map_err(|e| anyhow::anyhow!("config error: {e}"))?;
        if cfg.vocab_raw.trim().is_empty() {
            bail!("SBUS_PROXY_VOCAB is empty; specify comma-separated shard names");
        }
        Ok(cfg)
    }

    pub fn vocab(&self) -> Vec<String> {
        self.vocab_raw
            .split(',')
            .map(|s| s.trim().to_owned())
            .filter(|s| !s.is_empty())
            .collect()
    }
}

impl fmt::Debug for ProxyConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ProxyConfig")
            .field("listen_port",            &self.listen_port)
            .field("upstream_url",           &self.upstream_url)
            .field("upstream_url_anthropic", &self.upstream_url_anthropic)
            .field("upstream_url_google",    &self.upstream_url_google)
            .field("sbus_url",               &self.sbus_url)
            .field("vocab_size",             &self.vocab().len())
            .field("agent_header",           &self.agent_header)
            .field("session_header",         &self.session_header)
            .field("debug_passthrough",      &self.debug_passthrough)
            .finish()
    }
}