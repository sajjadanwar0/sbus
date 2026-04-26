// ───────────────────────────────────────────────────────────────────────────
// proxy.rs — HTTP handler that forwards LLM API requests with instrumentation
//
// PATCHED for Exp. PROXY-PH2 (v50):
//   - Added X-SBus-Shard-Suffix header support. When present, the proxy
//     appends this suffix to every matched shard name before POSTing to
//     S-Bus /delivery_log/register. This lets a single proxy instance
//     serve many concurrent run-isolated trials (shards keyed as
//     "{base}_{run_id}" on the server) without reconfiguring vocab.
//
//   - Left the extraction pipeline and streaming SSE tee unchanged; only
//     the delivery-log registration call carries the suffix.
//
// Flow for a non-streaming request:
//   1. Agent POSTs /v1/chat/completions to proxy
//   2. Proxy buffers the full request body
//   3. Input-side extraction: scan_request → set of shard refs
//   4. Forward request to upstream (OpenAI/Anthropic/etc.)
//   5. Buffer the full response body
//   6. Output-side extraction: scan_response → set of shard refs
//   7. POST (input ∪ output) to S-Bus DeliveryLog (suffix applied here)
//   8. Return response body to agent
//
// Flow for a streaming request (stream: true):
//   1. Same as above through step 4
//   2. Instead of buffering response, we pipe the SSE event stream through
//      an extractor that accumulates shard refs from each `data: {...}` chunk
//   3. When stream completes, POST accumulated refs to DeliveryLog (fire-
//      and-forget so we don't delay the agent getting the final chunk)
//
// DESIGN NOTE: The Phase 1 ordering here is "register AFTER the response is
// returned" for streaming. This is a known weakness — an agent that commits
// to S-Bus immediately after seeing the stream finish might commit before
// DeliveryLog is updated. Phase 3 fixes this by routing commits through the
// proxy or by forcing the agent SDK to await a proxy-supplied ack.
// ───────────────────────────────────────────────────────────────────────────

use std::sync::Arc;

use axum::{
    body::Body,
    extract::State,
    http::{HeaderMap, HeaderName, StatusCode},
    response::{IntoResponse, Response},
    routing::{any, post},
    Router,
};
use bytes::Bytes;
use futures_util::StreamExt;
use tokio_stream::wrappers::ReceiverStream;
use tracing::{debug, error, info, warn};

use crate::config::ProxyConfig;
use crate::delivery_log::DeliveryLogClient;
use crate::extractor::{ApiFormat, scan_request, scan_response, scan_stream_chunk};
use crate::vocabulary::Vocabulary;

/// Header carrying the agent-ID that owns this LLM call. Registered with
/// the DeliveryLog so cross-shard validation can match on commit.
pub const HEADER_AGENT_ID: &str = "X-SBus-Agent-Id";

/// Header carrying the session-ID. Included in the register payload for
/// logging, not used for routing.
pub const HEADER_SESSION_ID: &str = "X-SBus-Session-Id";

/// NEW in v50: header carrying a shard-key suffix to append before
/// registration. Allows run-isolated trials (shards keyed as
/// "{base}_{run_id}") to share one proxy process.
///
/// Example: agent sets `X-SBus-Shard-Suffix: _a1b2c3d4`. Proxy scans for
/// "models_state" in the LLM traffic, then registers "models_state_a1b2c3d4"
/// on the DeliveryLog.
pub const HEADER_SHARD_SUFFIX: &str = "X-SBus-Shard-Suffix";

pub struct AppState {
    pub config:          ProxyConfig,
    pub vocab:           Arc<Vocabulary>,
    pub upstream_client: reqwest::Client,
    pub sbus_client:     reqwest::Client,
}

pub fn router(state: Arc<AppState>) -> Router {
    Router::new()
        // OpenAI (canonical and Azure-compat path)
        .route("/v1/chat/completions", post(chat_completions))
        // Anthropic
        .route("/v1/messages",         post(chat_completions))
        // Google Gemini — dynamic path includes model + method.
        // We register a wildcard catcher under /v1beta/models and /v1/models;
        // the handler inspects the URI to pick the format and forwards the
        // incoming path verbatim to the upstream.
        .route("/v1beta/models/{*rest}", post(chat_completions))
        .route("/v1/models/{*rest}",     post(chat_completions))
        .route("/health",              any(health))
        .with_state(state)
}

async fn health() -> &'static str { "ok\n" }

// ───────────────────────────────────────────────────────────────────────────
// Main handler
// ───────────────────────────────────────────────────────────────────────────

async fn chat_completions(
    State(state): State<Arc<AppState>>,
    uri:          axum::http::Uri,
    headers:      HeaderMap,
    body:         Bytes,
) -> Response {
    let agent_id   = header_str(&headers, &state.config.agent_header)
                        .unwrap_or("unknown-agent")
                        .to_owned();
    let session_id = header_str(&headers, &state.config.session_header)
                        .unwrap_or("unknown-session")
                        .to_owned();

    // NEW: per-request shard-name suffix (appended at register time).
    let shard_suffix = header_str(&headers, HEADER_SHARD_SUFFIX)
                        .unwrap_or("")
                        .to_owned();

    // Determine which upstream + format to use based on the request path.
    let format = ApiFormat::from_path(uri.path());

    // ─── 1. Input-side extraction ─────────────────────────────────────
    let input_hits = if state.config.debug_passthrough {
        Default::default()
    } else {
        scan_request(&body, &state.vocab, format)
    };
    debug!(agent = %agent_id, format = ?format, shards = ?input_hits, "input-side extraction");

    // ─── 2. Build upstream request ────────────────────────────────────
    // Pick the correct upstream URL for this format. For OpenAI and
    // Anthropic the upstream path is fixed and we ignore the incoming
    // path component. For Google, the path encodes the model name and
    // the method (`:generateContent` vs `:streamGenerateContent`), both
    // of which vary per request — so we forward the incoming path
    // (and query string) verbatim to Google's upstream.
    let upstream_base = match format {
        ApiFormat::OpenAi    => &state.config.upstream_url,
        ApiFormat::Anthropic => &state.config.upstream_url_anthropic,
        ApiFormat::Google    => &state.config.upstream_url_google,
    };
    let upstream_url = match format.upstream_path() {
        Some(fixed_path) => {
            // OpenAI / Anthropic — fixed upstream path.
            format!(
                "{}{}",
                upstream_base.trim_end_matches('/'),
                fixed_path
            )
        }
        None => {
            // Google — use the incoming path (and query) verbatim. The
            // path-and-query of a parsed URI handles both
            // `/v1beta/models/{m}:generateContent` and
            // `/v1beta/models/{m}:generateContent?alt=json` etc.
            let path_and_query = uri
                .path_and_query()
                .map(|pq| pq.as_str())
                .unwrap_or(uri.path());
            format!(
                "{}{}",
                upstream_base.trim_end_matches('/'),
                path_and_query
            )
        }
    };

    let mut upstream_req = state.upstream_client
        .post(&upstream_url)
        .body(body.clone());

    // Forward client-supplied headers except hop-by-hop and our own S-Bus
    // control headers (which upstream shouldn't see).
    //
    // The permissive passthrough naturally carries format-specific auth
    // headers: OpenAI uses `authorization: Bearer ...`, Anthropic uses
    // `x-api-key: ...` and `anthropic-version: ...`. The client supplies
    // whichever matches its intended upstream.
    for (name, value) in headers.iter() {
        if should_forward_header(name, &state.config) {
            upstream_req = upstream_req.header(name, value);
        }
    }

    // Detect streaming based on the request body.
    let is_streaming = detect_streaming(&body);

    // ─── 3. Send to upstream ──────────────────────────────────────────
    let upstream_res = match upstream_req.send().await {
        Ok(r) => r,
        Err(e) => {
            error!(upstream = %upstream_url, error = %e, "upstream request failed");
            return (StatusCode::BAD_GATEWAY, format!("upstream error: {e}")).into_response();
        }
    };

    let status = upstream_res.status();
    let upstream_headers = upstream_res.headers().clone();
    info!(
        agent = %agent_id,
        format = ?format,
        status = status.as_u16(),
        streaming = is_streaming,
        "forwarded to upstream"
    );

    let sbus_client = DeliveryLogClient::new(
        state.sbus_client.clone(),
        state.config.sbus_url.clone(),
    );

    // ─── 4. Handle streaming vs non-streaming ─────────────────────────
    if is_streaming {
        stream_and_extract(
            upstream_res,
            state.clone(),
            sbus_client,
            agent_id,
            session_id,
            shard_suffix,
            input_hits,
            upstream_headers,
            status,
            format,
        ).await
    } else {
        buffer_and_extract(
            upstream_res,
            state.clone(),
            sbus_client,
            agent_id,
            session_id,
            shard_suffix,
            input_hits,
            upstream_headers,
            status,
            format,
        ).await
    }
}

// ───────────────────────────────────────────────────────────────────────────
// Non-streaming path
// ───────────────────────────────────────────────────────────────────────────

async fn buffer_and_extract(
    upstream_res: reqwest::Response,
    state:        Arc<AppState>,
    sbus_client:  DeliveryLogClient,
    agent_id:     String,
    session_id:   String,
    shard_suffix: String,
    input_hits:   std::collections::BTreeSet<String>,
    upstream_headers: HeaderMap,
    status:       StatusCode,
    format:       ApiFormat,
) -> Response {
    let body_bytes = match upstream_res.bytes().await {
        Ok(b) => b,
        Err(e) => {
            error!(error = %e, "failed to buffer upstream response");
            return (StatusCode::BAD_GATEWAY, "upstream body error").into_response();
        }
    };

    // Output-side extraction only on 2xx (no sense scanning error bodies).
    let output_hits = if status.is_success() && !state.config.debug_passthrough {
        scan_response(&body_bytes, &state.vocab, format)
    } else {
        Default::default()
    };

    let mut all_hits = input_hits;
    all_hits.extend(output_hits);

    // Register with S-Bus before returning to caller (non-streaming path
    // guarantees the agent cannot start a commit before DeliveryLog is
    // updated — this is the ordering property we want).
    if !all_hits.is_empty() {
        let _ = sbus_client
            .register(&agent_id, &session_id, all_hits, "proxy", &shard_suffix)
            .await;
    }

    build_response(status, upstream_headers, body_bytes)
}

// ───────────────────────────────────────────────────────────────────────────
// Streaming path
// ───────────────────────────────────────────────────────────────────────────

async fn stream_and_extract(
    upstream_res: reqwest::Response,
    state:        Arc<AppState>,
    sbus_client:  DeliveryLogClient,
    agent_id:     String,
    session_id:   String,
    shard_suffix: String,
    input_hits:   std::collections::BTreeSet<String>,
    upstream_headers: HeaderMap,
    status:       StatusCode,
    format:       ApiFormat,
) -> Response {
    use eventsource_stream::Eventsource;

    // Take the upstream byte stream and split it two ways: the agent still
    // sees the raw bytes (we pass them through unchanged), and in parallel
    // we parse them as SSE events to extract shard refs from each chunk.
    //
    // To do this cleanly we use an mpsc channel: the upstream stream feeds
    // both a scanner (that accumulates hits) and the outgoing Body.
    use tokio::sync::mpsc;

    let (tx, rx) = mpsc::channel::<Result<Bytes, reqwest::Error>>(16);

    let vocab = state.vocab.clone();
    let agent_id_c   = agent_id.clone();
    let session_id_c = session_id.clone();
    let suffix_c     = shard_suffix.clone();
    let input_hits_c = input_hits;

    // Background task: read upstream, forward bytes to client, accumulate
    // hits, and register with S-Bus on stream completion.
    tokio::spawn(async move {
        let byte_stream = upstream_res.bytes_stream();
        // Tee: clone bytes to both the outgoing channel and to the parser.
        let mut event_stream = byte_stream.eventsource();
        let mut output_hits = std::collections::BTreeSet::new();

        while let Some(event) = event_stream.next().await {
            match event {
                Ok(ev) => {
                    // OpenAI end sentinel — re-emit and skip parsing.
                    // Anthropic uses typed events (message_stop) which are
                    // valid JSON and parse fine, so we only special-case
                    // the OpenAI sentinel.
                    if format == ApiFormat::OpenAi && ev.data == "[DONE]" {
                        let done = Bytes::from("data: [DONE]\n\n");
                        let _ = tx.send(Ok(done)).await;
                        continue;
                    }

                    // Extract shard refs from this chunk's JSON.
                    let chunk_hits = scan_stream_chunk(ev.data.as_bytes(), &vocab, format);
                    output_hits.extend(chunk_hits);

                    // Re-encode as SSE and forward. Anthropic SSE carries
                    // `event: <kind>` lines before the `data:` line; the
                    // eventsource parser strips the event kind, so we
                    // reconstruct it from `ev.event` when present.
                    let wire = if !ev.event.is_empty() {
                        format!("event: {}\ndata: {}\n\n", ev.event, ev.data)
                    } else {
                        format!("data: {}\n\n", ev.data)
                    };
                    if tx.send(Ok(Bytes::from(wire))).await.is_err() {
                        break; // receiver dropped (client disconnected)
                    }
                }
                Err(e) => {
                    warn!(error = %e, "SSE parse error");
                    break;
                }
            }
        }

        // Stream finished — register with S-Bus.
        let mut all_hits = input_hits_c;
        all_hits.extend(output_hits);
        if !all_hits.is_empty() {
            let _ = sbus_client
                .register(&agent_id_c, &session_id_c, all_hits, "proxy", &suffix_c)
                .await;
        }
        debug!(agent = %agent_id_c, format = ?format, "streaming complete; DeliveryLog registered");
    });

    // Build a streaming response body from the channel.
    let out_stream = ReceiverStream::new(rx);
    let body = Body::from_stream(out_stream);

    let mut resp = Response::builder().status(status);
    for (k, v) in upstream_headers.iter() {
        // Copy all non-hop-by-hop headers; SSE needs content-type preserved.
        if !is_hop_by_hop(k) {
            resp = resp.header(k, v);
        }
    }
    // Make sure we keep the SSE content-type if upstream set it.
    resp.body(body).unwrap_or_else(|_| {
        (StatusCode::INTERNAL_SERVER_ERROR, "failed to build response").into_response()
    })
}

// ───────────────────────────────────────────────────────────────────────────
// Helpers
// ───────────────────────────────────────────────────────────────────────────

fn header_str<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
    headers.get(name).and_then(|v| v.to_str().ok())
}

/// Decide whether an incoming header should be forwarded upstream.
///
/// We strip:
///   - Hop-by-hop headers (per RFC 9110) — `Connection`, `Transfer-Encoding`, etc.
///   - `Host`             (reqwest fills this based on the upstream URL)
///   - `Content-Length`   (reqwest recomputes)
///   - Our own S-Bus control headers
fn should_forward_header(name: &HeaderName, config: &ProxyConfig) -> bool {
    let lower = name.as_str().to_ascii_lowercase();
    if matches!(lower.as_str(),
        "host" | "content-length" | "connection" | "proxy-connection"
        | "transfer-encoding" | "te" | "upgrade" | "keep-alive" | "trailer"
    ) {
        return false;
    }
    if lower == config.agent_header.to_ascii_lowercase()
        || lower == config.session_header.to_ascii_lowercase()
        || lower == HEADER_SHARD_SUFFIX.to_ascii_lowercase()
    {
        return false;
    }
    true
}

fn is_hop_by_hop(name: &HeaderName) -> bool {
    matches!(name.as_str().to_ascii_lowercase().as_str(),
        "connection" | "proxy-connection" | "transfer-encoding" | "te"
        | "upgrade" | "keep-alive" | "trailer" | "content-length"
    )
}

fn detect_streaming(body: &[u8]) -> bool {
    // Avoid full JSON parse just to check one field — a substring probe is
    // fine here. We look for `"stream":true` or `"stream": true`.
    let s = std::str::from_utf8(body).unwrap_or("");
    s.contains("\"stream\":true") || s.contains("\"stream\": true")
}

fn build_response(status: StatusCode, headers: HeaderMap, body: Bytes) -> Response {
    let mut builder = Response::builder().status(status);
    for (k, v) in headers.iter() {
        if !is_hop_by_hop(k) {
            builder = builder.header(k, v);
        }
    }
    builder.body(Body::from(body)).unwrap_or_else(|_| {
        (StatusCode::INTERNAL_SERVER_ERROR, "failed to build response").into_response()
    })
}