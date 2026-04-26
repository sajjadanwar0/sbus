# sbus-proxy

Transparent LLM-API proxy that intercepts agent ↔ model traffic, scans
responses for shard references, and registers them with S-Bus's
DeliveryLog via a skip-if-exists protocol.

This crate is part of the [S-Bus workspace](../). For full project
context, see the [top-level README](../README.md).

---

## What it does

The proxy sits between an agent's LLM client (OpenAI SDK, Anthropic
SDK, Google generative-ai SDK) and the upstream provider's API. For
every request:

1. **Path-routes to the correct upstream** based on URL pattern:
   - `/v1/chat/completions` → OpenAI (`api.openai.com`)
   - `/v1/messages` → Anthropic (`api.anthropic.com`)
   - `/v1beta/models/.../generateContent` → Google
     (`generativelanguage.googleapis.com`)
2. **Forwards the request unchanged** (transparent — no header
   rewriting, no body modification, supports streaming SSE).
3. **Scans the response body** against a configured shard vocabulary
   (`SBUS_PROXY_VOCAB`).
4. **For each hit**, POSTs to S-Bus's
   `POST /delivery_log/register` with skip-if-exists semantics. The
   server-side handler in `sbus-server` will not overwrite an existing
   DeliveryLog entry — it preserves the agent's true read-time
   version (paper §VII-K, Limitation 10).

The proxy is the production path the paper proposes for closing the
`R_hidden` coverage gap (paper §IX.B.d): agents reference shard
content from conversation memory without re-issuing HTTP `GET`s, and
the proxy recovers those references by scanning the LLM's actual
output rather than relying on agent self-reports.

---

## Status

This is the proxy used to produce paper Exp. PROXY-PH2:

- Cross-backbone safety parity (0 / 26,400 Type-I corruptions) across
  GPT-4o-mini, Anthropic Haiku 4.5, Google Gemini 2.5 Flash
- Total observable coverage of self-reported references ∈ [0.997, 0.999]
  on all three backbones
- Throughput cost scaling with vocabulary size (−2.1pp commit rate at
  V=4; −47.2pp at V=12) — the proxy is safety-preserving but
  monotonically throughput-negative at realistic vocabulary sizes
  (paper Table — PROXY-PH2 vocabulary scaling)

The keyword-scan extraction mechanism is documented as
**necessary-but-not-sufficient** for `R_hidden` coverage closure
(paper Limitation 16). Semantic-extraction-at-proxy (using a
dedicated analyst LLM to parse references rather than keyword
matching) is the natural next mechanism, scoped to future work.

The current code is research-grade: it works, it's tested against
mock servers (`tests/integration.rs`), and it produced the paper's
PROXY-PH2 numbers. It is **not** production-grade: there's no
authentication, no rate limiting, no circuit breaker, and no metrics
export. Use behind your own gateway in any deployed scenario.

---

## Building

```bash
# from the workspace root
cargo build --release -p sbus-proxy

# from this directory
cargo build --release
```

Requires Rust edition 2024 (stable 1.85+).

---

## Running

```bash
SBUS_PROXY_VOCAB="models_state,query_compiler,test_fixture,review_notes" \
SBUS_URL=http://localhost:7000 \
  cargo run --release
# → http://localhost:9000
```

`SBUS_PROXY_VOCAB` is required. Everything else has sensible defaults.

### Pointing an SDK at the proxy

For OpenAI clients, set the base URL:

```python
import openai
client = openai.OpenAI(
    api_key="sk-...",
    base_url="http://localhost:9000/v1",
)
client.chat.completions.create(...)   # routed through the proxy
```

For Anthropic:

```python
import anthropic
client = anthropic.Anthropic(
    api_key="sk-ant-...",
    base_url="http://localhost:9000",
)
```

For Google:

```python
import google.generativeai as genai
import os
os.environ["GOOGLE_API_BASE"] = "http://localhost:9000"
genai.configure(api_key="...")
```

The `agent_id` (and optional `session_id`) is carried in HTTP headers,
configurable via `SBUS_PROXY_AGENT_HEADER` and
`SBUS_PROXY_SESSION_HEADER` (defaults: `X-SBus-Agent-Id`,
`X-SBus-Session-Id`). The OpenAI Python SDK supports custom headers
via `default_headers={...}`.

---

## Configuration

| Variable | Default | Purpose |
|---|---|---|
| `SBUS_PROXY_PORT` | `9000` | HTTP listen port |
| `SBUS_URL` | `http://localhost:7000` | Where to register DeliveryLog entries |
| `SBUS_PROXY_VOCAB` | required | Comma-separated shard names |
| `SBUS_PROXY_UPSTREAM_URL` | `https://api.openai.com` | OpenAI-format upstream |
| `SBUS_PROXY_UPSTREAM_URL_ANTHROPIC` | `https://api.anthropic.com` | Anthropic upstream |
| `SBUS_PROXY_UPSTREAM_URL_GOOGLE` | `https://generativelanguage.googleapis.com` | Google upstream |
| `SBUS_PROXY_AGENT_HEADER` | `X-SBus-Agent-Id` | Header carrying agent_id |
| `SBUS_PROXY_SESSION_HEADER` | `X-SBus-Session-Id` | Header carrying session_id |
| `SBUS_PROXY_DEBUG_PASSTHROUGH` | `false` | If true, skip extraction (for benchmarking the proxy's overhead in isolation) |

For Vertex AI deployments (instead of public Google AI):

```bash
SBUS_PROXY_UPSTREAM_URL_GOOGLE="https://us-central1-aiplatform.googleapis.com" \
  cargo run --release
```

---

## Code layout

```
src/
├── main.rs           Binary entry point (~73 lines)
├── lib.rs            Library entry point — exposes build_router() (~56 lines)
├── config.rs         12-factor env-driven ProxyConfig (~125 lines)
├── proxy.rs          HTTP forwarder + path routing (~453 lines)
├── extractor.rs      Keyword-scan reference detection (~1030 lines)
├── vocabulary.rs     Vocabulary + tokenisation (~142 lines)
└── delivery_log.rs   Client for POST /delivery_log/register (~155 lines)

tests/
└── integration.rs    Integration tests against mock upstreams (~164 lines)
```

Total: ~2,200 lines of safe Rust, zero `unsafe`. Uses `wiremock` for
mock-server testing in `tests/integration.rs`.

---

## Testing

```bash
cargo test --release
```

The integration tests spin up `wiremock` mock servers for OpenAI /
Anthropic / Google upstreams and a mock S-Bus server, then drive the
proxy through real HTTP requests. They verify: routing logic,
streaming-SSE preservation, header forwarding, and the
DeliveryLog-register skip-if-exists semantics.

---

## Limitations

1. **Keyword-scan extraction only.** Misses references that don't
   contain the literal shard-name string. Cross-validated by paper
   Limitation 16: keyword scan recall is 0.073 against PH-3
   self-report ground truth (paper §VII-H, Table XII). The
   semantic-extraction-at-proxy variant (analyst LLM at the proxy
   layer) is the natural next mechanism.

2. **Throughput-negative at realistic vocabularies.** Each scanned
   response adds proxy-side latency proportional to vocabulary size.
   At V=12 the throughput cost is −47.2pp commit rate (paper
   PROXY-PH2 vocabulary sweep). Production deployments should
   compile the vocabulary to a single Aho-Corasick automaton; the
   current code uses a regex per term, which is the bottleneck.

3. **No authentication.** The proxy forwards upstream API keys
   verbatim from the request headers. Don't expose this on a public
   network without a gateway in front.

4. **No retries on upstream timeout.** Upstream HTTP errors propagate
   to the client unchanged. The proxy does **not** retry — that's
   the SDK's responsibility.

5. **Does not handle the OpenAI Assistants API.** Only the
   `/v1/chat/completions` endpoint is routed; `/v1/threads/...` and
   `/v1/assistants/...` will 404. The paper's experiments only use
   chat completions.

---

## License

MIT.
