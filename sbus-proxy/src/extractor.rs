// ───────────────────────────────────────────────────────────────────────────
// extractor.rs — scan OpenAI and Anthropic request/response bodies
//                for shard refs
//
// Input-side extraction:
//   OpenAI format: scans every `content` field in the `messages` array.
//   Anthropic format: scans the top-level `system` field (string or array)
//                     and every `content` in `messages[]`. Anthropic's
//                     `content` can be a plain string or an array of blocks
//                     with {type: "text", text: "..."}.
//
// Output-side extraction:
//   OpenAI non-streaming: scans `choices[*].message.content`.
//   Anthropic non-streaming: scans `content[*]` for blocks of type "text"
//                            and "tool_use" (tool_use.input is a JSON object;
//                            we stringify it and scan the whole thing).
//   OpenAI streaming: scans `choices[*].delta.content`.
//   Anthropic streaming: SSE events carry {type: "content_block_delta",
//                        delta: {type: "text_delta", text: "..."}}.
//
// Why union across messages? An LLM's "effective read set" for a single
// completion is everything in the context window, not just the latest user
// turn. Scanning all messages in a request captures the full context the
// model reasons over.
//
// What we do NOT do here:
//   - Semantic extraction via a second LLM (that's Phase 2)
//   - Causality attribution (did the model actually USE this reference, vs.
//     merely have it in context?) — Phase 3
// ───────────────────────────────────────────────────────────────────────────

use std::collections::BTreeSet;

use serde::Deserialize;
use serde_json::Value as JsonValue;

use crate::vocabulary::Vocabulary;

/// Which upstream API format a given HTTP path corresponds to.
/// Determined by the URI path at the proxy layer:
///   /v1/chat/completions      → OpenAI
///   /v1/messages              → Anthropic
///   /v1beta/models/...        → Google (Gemini)
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ApiFormat {
    OpenAi,
    Anthropic,
    Google,
}

impl ApiFormat {
    /// Pick an API format from the incoming request path.
    pub fn from_path(path: &str) -> Self {
        if path.starts_with("/v1/messages") {
            ApiFormat::Anthropic
        } else if path.starts_with("/v1beta/models/") || path.starts_with("/v1/models/") {
            // Google Gemini: /v1beta/models/{model}:generateContent
            //                /v1beta/models/{model}:streamGenerateContent
            //                /v1/models/{model}:generateContent (Vertex AI)
            ApiFormat::Google
        } else {
            ApiFormat::OpenAi
        }
    }

    /// The upstream path to forward to. For OpenAI and Anthropic this is a
    /// fixed string. For Google the path includes the model name and the
    /// method, both of which vary per-request — so for Google we forward
    /// the *exact* incoming path unchanged (handled by caller, not here).
    /// This method returns `None` for Google to signal "use incoming path".
    pub fn upstream_path(self) -> Option<&'static str> {
        match self {
            ApiFormat::OpenAi    => Some("/v1/chat/completions"),
            ApiFormat::Anthropic => Some("/v1/messages"),
            ApiFormat::Google    => None,  // forward the incoming path
        }
    }
}

// ───────────────────────────────────────────────────────────────────────────
// OpenAI request format
// ───────────────────────────────────────────────────────────────────────────

/// A minimal OpenAI chat-completions request body, just enough to pull out
/// the message contents. We deserialise with `serde(other)` so that unknown
/// fields don't break parsing.
#[derive(Deserialize, Debug)]
pub struct ChatRequest {
    #[serde(default)]
    pub messages: Vec<Message>,
}

/// OpenAI messages have `role` + `content`. The `content` field can be
/// either a string (legacy) or an array of content parts (multimodal). We
/// handle both.
#[derive(Deserialize, Debug)]
pub struct Message {
    #[serde(default)]
    pub role: String,
    #[serde(default)]
    pub content: Content,
}

#[derive(Deserialize, Debug)]
#[serde(untagged)]
pub enum Content {
    Text(String),
    Parts(Vec<ContentPart>),
    #[serde(skip)]
    None,
}

impl Default for Content {
    fn default() -> Self { Content::None }
}

#[derive(Deserialize, Debug)]
pub struct ContentPart {
    #[serde(rename = "type", default)]
    pub kind: String,
    #[serde(default)]
    pub text: String,
}

/// Scan a request body (OpenAI, Anthropic, or Google format) for shard
/// references. Returns the union of shards found across all messages.
///
/// The format dispatch:
///   - OpenAI:    `messages: [{role, content}]`. `content` is a string OR
///                an array of content parts.
///   - Anthropic: `messages: [{role, content}]` (same shape) PLUS a
///                top-level `system` field (string or array of blocks).
///   - Google:    `contents: [{role, parts: [{text}]}]` PLUS a top-level
///                `systemInstruction: {parts: [{text}]}` field. The shape
///                is structurally similar but uses `contents` (not
///                `messages`), `parts` (not `content`), and `text` only
///                inside parts (no top-level string content variant).
pub fn scan_request(raw_body: &[u8], vocab: &Vocabulary, format: ApiFormat) -> BTreeSet<String> {
    match format {
        ApiFormat::OpenAi    => scan_request_openai(raw_body, vocab),
        ApiFormat::Anthropic => scan_request_anthropic(raw_body, vocab),
        ApiFormat::Google    => scan_request_google(raw_body, vocab),
    }
}

fn scan_request_openai(raw_body: &[u8], vocab: &Vocabulary) -> BTreeSet<String> {
    let Ok(req): Result<ChatRequest, _> = serde_json::from_slice(raw_body) else {
        // Malformed or non-JSON body — return empty rather than error.
        // The proxy must never break agent requests on extraction failure.
        return BTreeSet::new();
    };
    let mut hits = BTreeSet::new();
    for msg in &req.messages {
        match &msg.content {
            Content::Text(s) => hits.extend(vocab.scan(s)),
            Content::Parts(parts) => {
                for part in parts {
                    if part.kind == "text" {
                        hits.extend(vocab.scan(&part.text));
                    }
                }
            }
            Content::None => {}
        }
    }
    hits
}

// ───────────────────────────────────────────────────────────────────────────
// Anthropic request format
// ───────────────────────────────────────────────────────────────────────────
//
// Example body:
//   {
//     "model": "claude-haiku-4-5-20251001",
//     "system": "You are a helpful assistant. Shards: models_state, ...",
//     "messages": [
//       {"role": "user", "content": "..."},
//       {"role": "assistant", "content": [
//           {"type": "text", "text": "..."},
//           {"type": "tool_use", "name": "...", "input": {...}}
//       ]}
//     ]
//   }
//
// The `system` field can be a plain string OR an array of content blocks
// (for prompt caching). Both handled.

#[derive(Deserialize, Debug)]
struct AnthropicRequest {
    #[serde(default)]
    system: Option<JsonValue>,
    #[serde(default)]
    messages: Vec<Message>,  // reuse OpenAI Message — same shape works
}

fn scan_request_anthropic(raw_body: &[u8], vocab: &Vocabulary) -> BTreeSet<String> {
    let Ok(req): Result<AnthropicRequest, _> = serde_json::from_slice(raw_body) else {
        return BTreeSet::new();
    };
    let mut hits = BTreeSet::new();

    // Scan `system` field. Anthropic allows string or array of blocks.
    if let Some(sys) = &req.system {
        match sys {
            JsonValue::String(s) => hits.extend(vocab.scan(s)),
            JsonValue::Array(arr) => {
                for block in arr {
                    if let Some(t) = block.get("text").and_then(|v| v.as_str()) {
                        hits.extend(vocab.scan(t));
                    }
                }
            }
            _ => {}
        }
    }

    // Scan `messages[]` — same shape as OpenAI so reuse the helper logic.
    for msg in &req.messages {
        match &msg.content {
            Content::Text(s) => hits.extend(vocab.scan(s)),
            Content::Parts(parts) => {
                for part in parts {
                    // Anthropic blocks: type=text carries .text;
                    // type=tool_result has a nested .content string/array.
                    if part.kind == "text" && !part.text.is_empty() {
                        hits.extend(vocab.scan(&part.text));
                    }
                }
            }
            Content::None => {}
        }
    }
    hits
}

// ───────────────────────────────────────────────────────────────────────────
// Response parsers
// ───────────────────────────────────────────────────────────────────────────

/// Scan a response body (OpenAI, Anthropic, or Google format,
/// non-streaming).
pub fn scan_response(raw_body: &[u8], vocab: &Vocabulary, format: ApiFormat) -> BTreeSet<String> {
    match format {
        ApiFormat::OpenAi    => scan_response_openai(raw_body, vocab),
        ApiFormat::Anthropic => scan_response_anthropic(raw_body, vocab),
        ApiFormat::Google    => scan_response_google(raw_body, vocab),
    }
}

#[derive(Deserialize, Debug)]
struct ChatResponse {
    #[serde(default)]
    choices: Vec<Choice>,
}
#[derive(Deserialize, Debug)]
struct Choice {
    #[serde(default)]
    message: ResponseMessage,
}
#[derive(Deserialize, Debug, Default)]
struct ResponseMessage {
    #[serde(default)]
    content: String,
}

fn scan_response_openai(raw_body: &[u8], vocab: &Vocabulary) -> BTreeSet<String> {
    let Ok(resp): Result<ChatResponse, _> = serde_json::from_slice(raw_body) else {
        return BTreeSet::new();
    };
    let mut hits = BTreeSet::new();
    for c in &resp.choices {
        hits.extend(vocab.scan(&c.message.content));
    }
    hits
}

// ───────────────────────────────────────────────────────────────────────────
// Anthropic response format
// ───────────────────────────────────────────────────────────────────────────
//
// Example body:
//   {
//     "id": "msg_...",
//     "type": "message",
//     "role": "assistant",
//     "content": [
//       {"type": "text", "text": "I'll update the models_state."},
//       {"type": "tool_use", "id": "toolu_...", "name": "report_change",
//        "input": {"change": "...", "shards_used": ["models_state"]}}
//     ],
//     "stop_reason": "end_turn",
//     ...
//   }
//
// We scan the `text` field of every block, AND we JSON-stringify each
// `tool_use.input` value and scan that — the Haiku experiment uses
// tool-use output, where `shards_used` is an array of literal shard names
// inside the tool input JSON.

#[derive(Deserialize, Debug)]
struct AnthropicResponse {
    #[serde(default)]
    content: Vec<AnthropicBlock>,
}

#[derive(Deserialize, Debug)]
struct AnthropicBlock {
    #[serde(rename = "type", default)]
    kind: String,
    #[serde(default)]
    text: String,
    #[serde(default)]
    input: JsonValue,
}

fn scan_response_anthropic(raw_body: &[u8], vocab: &Vocabulary) -> BTreeSet<String> {
    let Ok(resp): Result<AnthropicResponse, _> = serde_json::from_slice(raw_body) else {
        return BTreeSet::new();
    };
    let mut hits = BTreeSet::new();
    for block in &resp.content {
        match block.kind.as_str() {
            "text" => {
                if !block.text.is_empty() {
                    hits.extend(vocab.scan(&block.text));
                }
            }
            "tool_use" => {
                // Serialise the input JSON and scan it. This catches shard
                // names appearing as values in tool arguments (e.g. the
                // Haiku experiment's `shards_used: ["models_state", ...]`).
                if !block.input.is_null() {
                    let as_str = block.input.to_string();
                    hits.extend(vocab.scan(&as_str));
                }
            }
            _ => {}
        }
    }
    hits
}

// ───────────────────────────────────────────────────────────────────────────
// Streaming chunk parsers
// ───────────────────────────────────────────────────────────────────────────

/// Scan an SSE streaming chunk. Format determines the event shape:
///   OpenAI:    `{choices: [{delta: {content: "..."}}]}` per chunk.
///   Anthropic: events are typed, e.g.
///              `{type: "content_block_delta",
///                delta: {type: "text_delta", text: "..."}}`
///   Google:    each SSE chunk is a complete `GenerateContentResponse`
///              with one or more candidates and parts. Same shape as the
///              non-streaming response, scanned the same way.
pub fn scan_stream_chunk(chunk_json: &[u8], vocab: &Vocabulary, format: ApiFormat) -> BTreeSet<String> {
    match format {
        ApiFormat::OpenAi    => scan_stream_chunk_openai(chunk_json, vocab),
        ApiFormat::Anthropic => scan_stream_chunk_anthropic(chunk_json, vocab),
        ApiFormat::Google    => scan_stream_chunk_google(chunk_json, vocab),
    }
}

#[derive(Deserialize, Debug)]
struct StreamChunk {
    #[serde(default)]
    choices: Vec<StreamChoice>,
}
#[derive(Deserialize, Debug)]
struct StreamChoice {
    #[serde(default)]
    delta: Delta,
}
#[derive(Deserialize, Debug, Default)]
struct Delta {
    #[serde(default)]
    content: String,
}

fn scan_stream_chunk_openai(chunk_json: &[u8], vocab: &Vocabulary) -> BTreeSet<String> {
    let Ok(chunk): Result<StreamChunk, _> = serde_json::from_slice(chunk_json) else {
        return BTreeSet::new();
    };
    let mut hits = BTreeSet::new();
    for c in &chunk.choices {
        if !c.delta.content.is_empty() {
            hits.extend(vocab.scan(&c.delta.content));
        }
    }
    hits
}

#[derive(Deserialize, Debug)]
struct AnthropicStreamEvent {
    #[serde(rename = "type", default)]
    kind: String,
    #[serde(default)]
    delta: AnthropicDelta,
    #[serde(default)]
    content_block: AnthropicBlock,
}

#[derive(Deserialize, Debug, Default)]
struct AnthropicDelta {
    #[serde(rename = "type", default)]
    kind: String,
    #[serde(default)]
    text: String,
    /// For `input_json_delta` events (streaming tool_use), the partial JSON
    /// text is accumulated here as a raw string.
    #[serde(default)]
    partial_json: String,
}

impl Default for AnthropicBlock {
    fn default() -> Self {
        AnthropicBlock {
            kind:  String::new(),
            text:  String::new(),
            input: JsonValue::Null,
        }
    }
}

fn scan_stream_chunk_anthropic(chunk_json: &[u8], vocab: &Vocabulary) -> BTreeSet<String> {
    let Ok(ev): Result<AnthropicStreamEvent, _> = serde_json::from_slice(chunk_json) else {
        return BTreeSet::new();
    };
    let mut hits = BTreeSet::new();
    match ev.kind.as_str() {
        // Incremental text delta
        "content_block_delta" => {
            if ev.delta.kind == "text_delta" && !ev.delta.text.is_empty() {
                hits.extend(vocab.scan(&ev.delta.text));
            }
            // Streaming tool_use arguments come as JSON fragments. Scan
            // each fragment directly; the cumulative scan over all
            // fragments of one tool_use call catches shard names appearing
            // across arbitrary fragment boundaries only approximately (see
            // Limitation note below), but typical shard names are short
            // enough to fit in a single fragment in practice.
            if ev.delta.kind == "input_json_delta" && !ev.delta.partial_json.is_empty() {
                hits.extend(vocab.scan(&ev.delta.partial_json));
            }
        }
        // Block open — scan any pre-filled text (rare).
        "content_block_start" => {
            if ev.content_block.kind == "text" && !ev.content_block.text.is_empty() {
                hits.extend(vocab.scan(&ev.content_block.text));
            }
        }
        _ => {}
    }
    hits
}

// Note on Anthropic streaming tool_use: shard-name references that straddle
// a fragment boundary (e.g. "models_" in one fragment, "state" in the next)
// will not match on either scan call. Shard-name vocabulary in the paper's
// experimental workloads uses names that fit comfortably inside single
// fragments, and the harness de-fragments before reporting; for non-streaming
// responses this concern does not apply. Full-fidelity streaming tool_use
// extraction is future work.

// ───────────────────────────────────────────────────────────────────────────
// Google Gemini request format
// ───────────────────────────────────────────────────────────────────────────
//
// Example request body:
//   {
//     "contents": [
//       {"role": "user",  "parts": [{"text": "Please update orm_query."}]},
//       {"role": "model", "parts": [
//           {"text": "I'll edit it."},
//           {"functionCall": {"name": "report",
//                             "args": {"shards_used": ["orm_query"]}}}
//       ]}
//     ],
//     "systemInstruction": {
//       "parts": [{"text": "You may access models_state."}]
//     },
//     "tools": [{"functionDeclarations": [...]}],
//     "generationConfig": {...}
//   }
//
// Notes:
//   - Roles are "user" and "model" (not "assistant" as in OpenAI/Anthropic)
//   - `contents` instead of `messages`
//   - `parts` instead of `content`
//   - System prompt lives in `systemInstruction.parts[*].text`
//   - Function calls in request history (assistant turns) carry their
//     args as already-parsed JSON values (not stringified). We scan the
//     stringified form to catch shard names appearing as values.

#[derive(Deserialize, Debug)]
struct GoogleRequest {
    #[serde(default, rename = "systemInstruction")]
    system_instruction: Option<JsonValue>,
    #[serde(default)]
    contents: Vec<GoogleContent>,
}

#[derive(Deserialize, Debug)]
struct GoogleContent {
    #[serde(default)]
    parts: Vec<GooglePart>,
}

/// Google's `Part` is a sum type. We deserialize the fields lazily so
/// unknown variants (e.g. `inlineData`, `fileData`) don't error.
#[derive(Deserialize, Debug, Default)]
struct GooglePart {
    #[serde(default)]
    text: String,
    #[serde(default, rename = "functionCall")]
    function_call: Option<GoogleFunctionCall>,
    #[serde(default, rename = "functionResponse")]
    function_response: Option<GoogleFunctionResponse>,
}

#[derive(Deserialize, Debug)]
struct GoogleFunctionCall {
    #[serde(default)]
    name: String,
    #[serde(default)]
    args: JsonValue,
}

#[derive(Deserialize, Debug)]
struct GoogleFunctionResponse {
    #[serde(default)]
    response: JsonValue,
}

fn scan_request_google(raw_body: &[u8], vocab: &Vocabulary) -> BTreeSet<String> {
    let Ok(req): Result<GoogleRequest, _> = serde_json::from_slice(raw_body) else {
        return BTreeSet::new();
    };
    let mut hits = BTreeSet::new();

    // Scan systemInstruction. Google's docs say it's `{parts: [{text: ...}]}`,
    // but be lenient: accept a string too.
    if let Some(sys) = &req.system_instruction {
        match sys {
            JsonValue::String(s) => hits.extend(vocab.scan(s)),
            JsonValue::Object(_) => {
                // {parts: [{text: ...}]}
                if let Some(parts) = sys.get("parts").and_then(|v| v.as_array()) {
                    for p in parts {
                        if let Some(t) = p.get("text").and_then(|v| v.as_str()) {
                            hits.extend(vocab.scan(t));
                        }
                    }
                }
            }
            _ => {}
        }
    }

    // Scan all contents — every role's parts.
    for content in &req.contents {
        for part in &content.parts {
            if !part.text.is_empty() {
                hits.extend(vocab.scan(&part.text));
            }
            if let Some(fc) = &part.function_call {
                if !fc.args.is_null() {
                    hits.extend(vocab.scan(&fc.args.to_string()));
                }
                if !fc.name.is_empty() {
                    hits.extend(vocab.scan(&fc.name));
                }
            }
            if let Some(fr) = &part.function_response {
                if !fr.response.is_null() {
                    hits.extend(vocab.scan(&fr.response.to_string()));
                }
            }
        }
    }
    hits
}

// ───────────────────────────────────────────────────────────────────────────
// Google Gemini response format
// ───────────────────────────────────────────────────────────────────────────
//
// Example response body:
//   {
//     "candidates": [
//       {
//         "content": {
//           "role": "model",
//           "parts": [
//             {"text": "I'll update the models_state."},
//             {"functionCall": {"name": "report_change",
//                               "args": {"shards_used": ["models_state"]}}}
//           ]
//         },
//         "finishReason": "STOP",
//         "index": 0,
//         "safetyRatings": [...]
//       }
//     ],
//     "promptFeedback": {...},
//     "usageMetadata": {...}
//   }
//
// We scan the `text` of every part, AND we scan the JSON-stringified
// `functionCall.args` of every part. Same approach as Anthropic tool_use.

#[derive(Deserialize, Debug)]
struct GoogleResponse {
    #[serde(default)]
    candidates: Vec<GoogleCandidate>,
}

#[derive(Deserialize, Debug)]
struct GoogleCandidate {
    #[serde(default)]
    content: Option<GoogleContent>,
}

fn scan_response_google(raw_body: &[u8], vocab: &Vocabulary) -> BTreeSet<String> {
    let Ok(resp): Result<GoogleResponse, _> = serde_json::from_slice(raw_body) else {
        return BTreeSet::new();
    };
    let mut hits = BTreeSet::new();
    for cand in &resp.candidates {
        if let Some(content) = &cand.content {
            for part in &content.parts {
                if !part.text.is_empty() {
                    hits.extend(vocab.scan(&part.text));
                }
                if let Some(fc) = &part.function_call {
                    if !fc.args.is_null() {
                        hits.extend(vocab.scan(&fc.args.to_string()));
                    }
                    if !fc.name.is_empty() {
                        hits.extend(vocab.scan(&fc.name));
                    }
                }
            }
        }
    }
    hits
}

/// Google streaming uses :streamGenerateContent. Each SSE chunk carries a
/// full GenerateContentResponse with one or more candidates and parts —
/// the same shape as the non-streaming response, just emitted
/// incrementally. We scan each chunk identically to the non-streaming
/// scanner.
fn scan_stream_chunk_google(chunk_json: &[u8], vocab: &Vocabulary) -> BTreeSet<String> {
    scan_response_google(chunk_json, vocab)
}

// ───────────────────────────────────────────────────────────────────────────
// Tests
// ───────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ProxyConfig;

    fn vocab(items: &[&str]) -> Vocabulary {
        let cfg = ProxyConfig {
            listen_port:            9000,
            upstream_url:           "https://api.openai.com".into(),
            upstream_url_anthropic: "https://api.anthropic.com".into(),
            upstream_url_google:    "https://generativelanguage.googleapis.com".into(),
            sbus_url:               "http://localhost:7000".into(),
            vocab_raw:              items.join(","),
            agent_header:           "X-SBus-Agent-Id".into(),
            session_header:         "X-SBus-Session-Id".into(),
            debug_passthrough:      false,
        };
        Vocabulary::from_config(&cfg).unwrap()
    }

    #[test]
    fn api_format_from_path() {
        assert_eq!(ApiFormat::from_path("/v1/chat/completions"), ApiFormat::OpenAi);
        assert_eq!(ApiFormat::from_path("/v1/messages"),         ApiFormat::Anthropic);
        assert_eq!(ApiFormat::from_path("/v1/messages?stream=1"), ApiFormat::Anthropic);
        // Google Gemini paths
        assert_eq!(
            ApiFormat::from_path("/v1beta/models/gemini-2.5-flash:generateContent"),
            ApiFormat::Google
        );
        assert_eq!(
            ApiFormat::from_path("/v1beta/models/gemini-2.5-flash:streamGenerateContent"),
            ApiFormat::Google
        );
        assert_eq!(
            ApiFormat::from_path("/v1/models/gemini-2.5-flash:generateContent"),
            ApiFormat::Google
        );
        // Unknown path falls back to OpenAI.
        assert_eq!(ApiFormat::from_path("/anything/else"),       ApiFormat::OpenAi);
    }

    #[test]
    fn upstream_path_dispatch() {
        assert_eq!(ApiFormat::OpenAi.upstream_path(),    Some("/v1/chat/completions"));
        assert_eq!(ApiFormat::Anthropic.upstream_path(), Some("/v1/messages"));
        // Google returns None — caller forwards the incoming path verbatim.
        assert_eq!(ApiFormat::Google.upstream_path(),    None);
    }

    #[test]
    fn scan_request_openai_simple() {
        let body = br#"{
            "model": "gpt-4o-mini",
            "messages": [
                {"role": "system", "content": "You have access to models_state."},
                {"role": "user",   "content": "Please update orm_query."}
            ]
        }"#;
        let v = vocab(&["models_state", "orm_query", "test_fixture"]);
        let hits = scan_request(body, &v, ApiFormat::OpenAi);
        assert_eq!(hits.len(), 2);
        assert!(hits.contains("models_state"));
        assert!(hits.contains("orm_query"));
    }

    #[test]
    fn scan_request_openai_multimodal_content_parts() {
        let body = br#"{
            "messages": [
                {"role": "user", "content": [
                    {"type": "text", "text": "Check orm_query behavior"},
                    {"type": "image_url", "image_url": {"url": "..."}}
                ]}
            ]
        }"#;
        let v = vocab(&["orm_query"]);
        let hits = scan_request(body, &v, ApiFormat::OpenAi);
        assert_eq!(hits.len(), 1);
    }

    #[test]
    fn scan_request_malformed_body_returns_empty() {
        let body = b"not JSON at all";
        let v = vocab(&["anything"]);
        assert!(scan_request(body, &v, ApiFormat::OpenAi).is_empty());
        assert!(scan_request(body, &v, ApiFormat::Anthropic).is_empty());
    }

    #[test]
    fn scan_response_openai_non_streaming() {
        let body = br#"{
            "choices": [{
                "message": {
                    "role": "assistant",
                    "content": "I'll update the orm_query as requested."
                }
            }]
        }"#;
        let v = vocab(&["orm_query"]);
        let hits = scan_response(body, &v, ApiFormat::OpenAi);
        assert_eq!(hits.len(), 1);
        assert!(hits.contains("orm_query"));
    }

    #[test]
    fn scan_stream_chunk_openai_delta() {
        let chunk = br#"{
            "choices": [{
                "delta": {"content": "referencing test_fixture here"}
            }]
        }"#;
        let v = vocab(&["test_fixture"]);
        let hits = scan_stream_chunk(chunk, &v, ApiFormat::OpenAi);
        assert_eq!(hits.len(), 1);
    }

    #[test]
    fn empty_messages_array_empty_hits() {
        let body = br#"{"messages": []}"#;
        let v = vocab(&["x"]);
        assert!(scan_request(body, &v, ApiFormat::OpenAi).is_empty());
        assert!(scan_request(body, &v, ApiFormat::Anthropic).is_empty());
    }

    // ─── Anthropic-format tests ──────────────────────────────────────────

    #[test]
    fn scan_request_anthropic_system_string() {
        let body = br#"{
            "model": "claude-haiku-4-5-20251001",
            "system": "You may access models_state and orm_query.",
            "messages": [
                {"role": "user", "content": "hi"}
            ]
        }"#;
        let v = vocab(&["models_state", "orm_query"]);
        let hits = scan_request(body, &v, ApiFormat::Anthropic);
        assert_eq!(hits.len(), 2);
    }

    #[test]
    fn scan_request_anthropic_system_array() {
        // Prompt-caching style — system is an array of blocks.
        let body = br#"{
            "system": [
                {"type": "text", "text": "Access: models_state"}
            ],
            "messages": [{"role": "user", "content": "ping"}]
        }"#;
        let v = vocab(&["models_state"]);
        let hits = scan_request(body, &v, ApiFormat::Anthropic);
        assert_eq!(hits.len(), 1);
    }

    #[test]
    fn scan_request_anthropic_content_array() {
        // Anthropic assistant turn with content as array of blocks.
        let body = br#"{
            "messages": [
                {"role": "user", "content": "please update"},
                {"role": "assistant", "content": [
                    {"type": "text", "text": "Using orm_query."},
                    {"type": "tool_use", "id": "t1", "name": "fetch",
                     "input": {"key": "test_fixture"}}
                ]}
            ]
        }"#;
        let v = vocab(&["orm_query", "test_fixture"]);
        // Scan the *request* only — tool_use blocks in the request message
        // history (assistant prior turns) only contribute via their `text`
        // blocks; `input` fields are not in the scan path for requests.
        let hits = scan_request(body, &v, ApiFormat::Anthropic);
        assert!(hits.contains("orm_query"));
    }

    #[test]
    fn scan_response_anthropic_text_only() {
        let body = br#"{
            "id": "msg_1",
            "type": "message",
            "role": "assistant",
            "content": [
                {"type": "text", "text": "I updated models_state and orm_query."}
            ],
            "stop_reason": "end_turn"
        }"#;
        let v = vocab(&["models_state", "orm_query"]);
        let hits = scan_response(body, &v, ApiFormat::Anthropic);
        assert_eq!(hits.len(), 2);
    }

    #[test]
    fn scan_response_anthropic_tool_use_input() {
        // This is the Haiku PROXY-PH2 case: shard names appear as values in
        // the tool_use.input JSON, not in any text block.
        let body = br#"{
            "id": "msg_1",
            "type": "message",
            "role": "assistant",
            "content": [
                {"type": "text", "text": "Calling the tool."},
                {"type": "tool_use", "id": "t1", "name": "report_change",
                 "input": {"change": "edit", "shards_used": ["models_state", "orm_query"]}}
            ],
            "stop_reason": "tool_use"
        }"#;
        let v = vocab(&["models_state", "orm_query", "test_fixture"]);
        let hits = scan_response(body, &v, ApiFormat::Anthropic);
        assert!(hits.contains("models_state"));
        assert!(hits.contains("orm_query"));
        assert!(!hits.contains("test_fixture"));  // not in input
    }

    #[test]
    fn scan_stream_chunk_anthropic_text_delta() {
        let chunk = br#"{
            "type": "content_block_delta",
            "index": 0,
            "delta": {"type": "text_delta", "text": "I will edit models_state."}
        }"#;
        let v = vocab(&["models_state"]);
        let hits = scan_stream_chunk(chunk, &v, ApiFormat::Anthropic);
        assert_eq!(hits.len(), 1);
    }

    #[test]
    fn scan_stream_chunk_anthropic_input_json_delta() {
        // Partial tool_use arguments streamed as JSON fragments.
        let chunk = br#"{
            "type": "content_block_delta",
            "index": 1,
            "delta": {"type": "input_json_delta",
                      "partial_json": "{\"shards_used\": [\"models_state\""}
        }"#;
        let v = vocab(&["models_state"]);
        let hits = scan_stream_chunk(chunk, &v, ApiFormat::Anthropic);
        assert!(hits.contains("models_state"));
    }

    #[test]
    fn scan_stream_chunk_anthropic_ignores_unrelated_events() {
        // Non-delta events (message_start, ping, etc.) should produce
        // no hits.
        let v = vocab(&["models_state"]);
        let ms = br#"{"type": "message_start", "message": {"id": "m1"}}"#;
        assert!(scan_stream_chunk(ms, &v, ApiFormat::Anthropic).is_empty());
        let ping = br#"{"type": "ping"}"#;
        assert!(scan_stream_chunk(ping, &v, ApiFormat::Anthropic).is_empty());
    }

    // ─── Google Gemini-format tests ──────────────────────────────────────

    #[test]
    fn scan_request_google_system_instruction_string() {
        // Lenient form: systemInstruction as a plain string. (Google's
        // public API uses the object form, but we accept both.)
        let body = br#"{
            "systemInstruction": "You may access models_state.",
            "contents": [{"role": "user", "parts": [{"text": "ping"}]}]
        }"#;
        let v = vocab(&["models_state"]);
        let hits = scan_request(body, &v, ApiFormat::Google);
        assert_eq!(hits.len(), 1);
        assert!(hits.contains("models_state"));
    }

    #[test]
    fn scan_request_google_system_instruction_object() {
        // Standard form: {parts: [{text: ...}]}.
        let body = br#"{
            "systemInstruction": {"parts": [{"text": "Use models_state and orm_query."}]},
            "contents": [{"role": "user", "parts": [{"text": "ok"}]}]
        }"#;
        let v = vocab(&["models_state", "orm_query"]);
        let hits = scan_request(body, &v, ApiFormat::Google);
        assert_eq!(hits.len(), 2);
    }

    #[test]
    fn scan_request_google_user_and_model_turns() {
        let body = br#"{
            "contents": [
                {"role": "user",  "parts": [{"text": "Please update orm_query."}]},
                {"role": "model", "parts": [
                    {"text": "I will edit models_state."},
                    {"functionCall": {"name": "report",
                                      "args": {"shards_used": ["test_fixture"]}}}
                ]}
            ]
        }"#;
        let v = vocab(&["orm_query", "models_state", "test_fixture"]);
        let hits = scan_request(body, &v, ApiFormat::Google);
        assert!(hits.contains("orm_query"));
        assert!(hits.contains("models_state"));
        assert!(hits.contains("test_fixture"));
    }

    #[test]
    fn scan_response_google_text_only() {
        let body = br#"{
            "candidates": [{
                "content": {
                    "role": "model",
                    "parts": [{"text": "Updated models_state and orm_query successfully."}]
                },
                "finishReason": "STOP"
            }]
        }"#;
        let v = vocab(&["models_state", "orm_query"]);
        let hits = scan_response(body, &v, ApiFormat::Google);
        assert_eq!(hits.len(), 2);
    }

    #[test]
    fn scan_response_google_function_call() {
        // The PROXY-PH2 case for Gemini: shard names live in
        // functionCall.args, not in any text part.
        let body = br#"{
            "candidates": [{
                "content": {
                    "role": "model",
                    "parts": [
                        {"text": "Calling tool."},
                        {"functionCall": {
                            "name": "report_change",
                            "args": {"change": "edit",
                                     "shards_used": ["models_state", "orm_query"]}
                        }}
                    ]
                },
                "finishReason": "STOP"
            }]
        }"#;
        let v = vocab(&["models_state", "orm_query", "test_fixture"]);
        let hits = scan_response(body, &v, ApiFormat::Google);
        assert!(hits.contains("models_state"));
        assert!(hits.contains("orm_query"));
        assert!(!hits.contains("test_fixture"));
    }

    #[test]
    fn scan_response_google_empty_candidates() {
        // Safety filter / no candidates / malformed → empty hits, never error.
        let body = br#"{"candidates": [], "promptFeedback": {"blockReason": "SAFETY"}}"#;
        let v = vocab(&["x"]);
        assert!(scan_response(body, &v, ApiFormat::Google).is_empty());
    }

    #[test]
    fn scan_stream_chunk_google_partial_response() {
        // Each Google streaming chunk is a full GenerateContentResponse,
        // so the scanner reuses the non-streaming logic.
        let chunk = br#"{
            "candidates": [{
                "content": {
                    "role": "model",
                    "parts": [{"text": "Partial: models_state was..."}]
                }
            }]
        }"#;
        let v = vocab(&["models_state"]);
        let hits = scan_stream_chunk(chunk, &v, ApiFormat::Google);
        assert!(hits.contains("models_state"));
    }
}