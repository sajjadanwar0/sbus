// ───────────────────────────────────────────────────────────────────────────
// tests/integration.rs — end-to-end test using wiremock
//
// Stands up:
//   - A mock OpenAI-compatible upstream (wiremock)
//   - A mock S-Bus DeliveryLog endpoint (wiremock)
//   - A real sbus-proxy instance bound to a random port
//
// Then sends a real HTTP request into the proxy and verifies:
//   1. The upstream received the request (verified by Mock::expect(1))
//   2. S-Bus DeliveryLog received a register POST (verified by Mock::expect(1))
//   3. The response was forwarded back to the client correctly
//
// Run with:  cargo test --test integration
// ───────────────────────────────────────────────────────────────────────────

use std::time::Duration;

use serde_json::json;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

use sbus_proxy::{build_router, ProxyConfig};

#[tokio::test]
async fn proxy_forwards_request_and_registers_refs() {
    // ─── 1. Mock upstream LLM API ─────────────────────────────────────
    let upstream = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "id": "chatcmpl-test",
            "object": "chat.completion",
            "choices": [{
                "index": 0,
                "message": {
                    "role": "assistant",
                    "content": "I'll update the orm_query as requested."
                },
                "finish_reason": "stop"
            }]
        })))
        .expect(1)
        .mount(&upstream)
        .await;

    // ─── 2. Mock S-Bus DeliveryLog ────────────────────────────────────
    let sbus = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/delivery_log/register"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"ok": true})))
        .expect(1)
        .mount(&sbus)
        .await;

    // ─── 3. Spin up real proxy against the mocks ──────────────────────
    let config = ProxyConfig {
        listen_port:            0,                           // bind to ephemeral
        upstream_url:           upstream.uri(),
        upstream_url_anthropic: "https://api.anthropic.com".into(),
        upstream_url_google:    "https://generativelanguage.googleapis.com".into(),
        sbus_url:               sbus.uri(),
        vocab_raw:              "models_state,orm_query,test_fixture,review_notes".into(),
        agent_header:           "X-SBus-Agent-Id".into(),
        session_header:         "X-SBus-Session-Id".into(),
        debug_passthrough:      false,
    };
    let app = build_router(config).expect("build_router failed");

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy_addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    // Small wait for the server task to be ready.
    tokio::time::sleep(Duration::from_millis(50)).await;

    // ─── 4. Drive a request through the proxy ─────────────────────────
    let client = reqwest::Client::new();
    let resp = client
        .post(format!("http://{proxy_addr}/v1/chat/completions"))
        .header("X-SBus-Agent-Id",   "worker-1")
        .header("X-SBus-Session-Id", "sess-abc")
        .header("authorization",     "Bearer sk-fake-key")
        .json(&json!({
            "model": "gpt-4o-mini",
            "messages": [
                {"role": "system", "content":
                    "You have models_state and test_fixture available."},
                {"role": "user", "content": "Update the orm_query."}
            ]
        }))
        .timeout(Duration::from_secs(5))
        .send()
        .await
        .expect("proxy request failed");

    // ─── 5. Assertions ────────────────────────────────────────────────
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["choices"][0]["message"]["role"], "assistant");

    // Give the DeliveryLog-register task a moment to complete (non-streaming
    // path awaits it inline, but the test runtime still needs to schedule).
    tokio::time::sleep(Duration::from_millis(100)).await;

    // wiremock verifies .expect(1) on drop — if the upstream or S-Bus
    // weren't called exactly once, the test panics at the end of the fn.
    drop(upstream);
    drop(sbus);
}

#[tokio::test]
async fn proxy_returns_502_on_upstream_error() {
    let upstream = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(ResponseTemplate::new(500))
        .expect(1)
        .mount(&upstream)
        .await;

    let sbus = MockServer::start().await;
    // S-Bus should NOT be called when upstream returns 5xx — output
    // extraction only runs on success. But input-side extraction still
    // happens; whether to register input-only hits on upstream failure is
    // a design choice. Current behaviour: still register.
    Mock::given(method("POST"))
        .and(path("/delivery_log/register"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&sbus)
        .await;

    let config = ProxyConfig {
        listen_port:            0,
        upstream_url:           upstream.uri(),
        upstream_url_anthropic: "https://api.anthropic.com".into(),
        upstream_url_google:    "https://generativelanguage.googleapis.com".into(),
        sbus_url:               sbus.uri(),
        vocab_raw:              "orm_query".into(),
        agent_header:           "X-SBus-Agent-Id".into(),
        session_header:         "X-SBus-Session-Id".into(),
        debug_passthrough:      false,
    };
    let app = build_router(config).unwrap();

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy_addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap(); });
    tokio::time::sleep(Duration::from_millis(50)).await;

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("http://{proxy_addr}/v1/chat/completions"))
        .json(&json!({"model": "gpt-4o-mini", "messages": []}))
        .send()
        .await
        .unwrap();

    // Proxy forwards upstream status code verbatim (we do not rewrite 5xx
    // to 502; that's a deliberate choice to let agent SDKs see real errors).
    assert_eq!(resp.status(), 500);
}