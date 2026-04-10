// src/api/handlers.rs — S-Bus v29 (Rust edition 2024)
//
// HMAC-SHA256 is implemented directly using sha2 (RFC 2104).
// This avoids the hmac crate version / trait-scope issues entirely.
// sha2 is already in Cargo.toml and compiles cleanly.
//
// DEPLOYMENT:
//   export SBUS_HMAC_KEY=$(openssl rand -hex 32)
//
//   # Python agent — sign agent_id before every commit:
//   import hmac, hashlib, os
//   def make_token(agent_id: str) -> str:
//       key = os.environ["SBUS_HMAC_KEY"].encode()
//       return hmac.new(key, agent_id.encode(), hashlib.sha256).hexdigest()
//   headers = {"X-Agent-Token": make_token("agent-1")}

use axum::{
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    Json,
};
use serde::Deserialize;
use serde_json::json;

use crate::bus::{
    engine::SBus,
    types::{CommitRequest, CreateShardRequest, RollbackRequest, SyncError},
};

// ── HMAC-SHA256 (RFC 2104) implemented with sha2 only ────────────────────────
//
// We implement HMAC directly rather than depending on the `hmac` crate,
// which has unstable trait-import requirements across editions (the
// `KeyInit::new_from_slice` path breaks in Rust 2024 without exact imports).
//
// HMAC(key, msg) = H( (key XOR opad) || H( (key XOR ipad) || msg ) )
//   where H = SHA-256, ipad = 0x36 * 64, opad = 0x5c * 64

fn hmac_sha256(key: &[u8], message: &[u8]) -> [u8; 32] {
    use sha2::{Digest, Sha256};

    const BLOCK: usize = 64; // SHA-256 block size in bytes

    // Step 1: normalize key to exactly BLOCK bytes.
    let mut k = [0u8; BLOCK];
    if key.len() > BLOCK {
        // Key longer than block → hash it first.
        let hashed = Sha256::digest(key);
        k[..32].copy_from_slice(&hashed);
    } else {
        k[..key.len()].copy_from_slice(key);
        // remaining bytes are already 0 (zero-padding).
    }

    // Step 2: derive inner and outer padded keys.
    let mut ipad = [0u8; BLOCK];
    let mut opad = [0u8; BLOCK];
    for i in 0..BLOCK {
        ipad[i] = k[i] ^ 0x36;
        opad[i] = k[i] ^ 0x5c;
    }

    // Step 3: inner hash = H(ipad || message).
    let inner = {
        let mut h = Sha256::new();
        h.update(ipad);
        h.update(message);
        h.finalize()
    };

    // Step 4: outer hash = H(opad || inner).
    let outer = {
        let mut h = Sha256::new();
        h.update(opad);
        h.update(inner);
        h.finalize()
    };

    outer.into()
}

// ── HMAC helpers ──────────────────────────────────────────────────────────────

fn hmac_key() -> Option<Vec<u8>> {
    std::env::var("SBUS_HMAC_KEY").ok().map(|s| s.into_bytes())
}

/// Verify X-Agent-Token header for write operations (commit, rollback).
///
/// - SBUS_HMAC_KEY not set → always Ok (dev / single-tenant mode).
/// - SBUS_HMAC_KEY set     → header must equal hex(HMAC-SHA256(key, agent_id)).
/// - GET /shard reads do NOT require HMAC.
fn verify_agent_hmac(headers: &HeaderMap, agent_id: &str) -> Result<(), SyncError> {
    let Some(key) = hmac_key() else {
        return Ok(()); // HMAC disabled — backwards-compatible
    };

    let token = headers
        .get("x-agent-token")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");

    let expected = hex::encode(hmac_sha256(&key, agent_id.as_bytes()));

    // Constant-time comparison — prevents timing-oracle attacks.
    if !constant_time_eq(token.as_bytes(), expected.as_bytes()) {
        return Err(SyncError::Unauthorized {
            agent_id: agent_id.to_owned(),
        });
    }
    Ok(())
}

/// XOR-fold comparison — O(n) regardless of where the first mismatch occurs.
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() { return false; }
    a.iter().zip(b.iter()).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

// ── Response helpers ──────────────────────────────────────────────────────────

fn err_resp(e: SyncError) -> impl IntoResponse {
    let status = StatusCode::from_u16(e.status_code())
        .unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    (status, Json(json!({"error": e.error_code(), "detail": e.to_string()}))).into_response()
}

macro_rules! blocking {
    ($bus:expr, $op:expr) => {
        match tokio::task::spawn_blocking(move || $op).await {
            Ok(Ok(r))  => (StatusCode::OK, Json(json!(r))).into_response(),
            Ok(Err(e)) => err_resp(e).into_response(),
            Err(_)     => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
        }
    };
}

// ── Shard CRUD ────────────────────────────────────────────────────────────────

pub async fn create_shard(
    State(bus): State<SBus>,
    Json(req):  Json<CreateShardRequest>,
) -> impl IntoResponse {
    blocking!(bus, bus.create_shard(req))
}

/// ?agent_id=X  → records delivery in DeliveryLog (WSS cross-shard tracking).
/// (omitted)    → read only, no delivery recorded (backwards-compatible).
#[derive(Deserialize)]
pub struct ReadShardParams {
    #[serde(default)]
    pub agent_id: String,
}

pub async fn read_shard(
    State(bus):    State<SBus>,
    Path(key):     Path<String>,
    Query(params): Query<ReadShardParams>,
) -> impl IntoResponse {
    let agent_id = params.agent_id.clone();
    blocking!(bus, bus.read_shard(&key, &agent_id))
}

pub async fn list_shards(State(bus): State<SBus>) -> impl IntoResponse {
    let shards = bus.list_shards();
    (StatusCode::OK, Json(json!({"shards": shards}))).into_response()
}

// ── ACP commit handlers ───────────────────────────────────────────────────────

/// POST /commit — legacy path (no DeliveryLog cross-shard check).
pub async fn commit(
    State(bus): State<SBus>,
    headers:    HeaderMap,
    Json(req):  Json<CommitRequest>,
) -> impl IntoResponse {
    if let Err(e) = verify_agent_hmac(&headers, &req.agent_id) {
        return err_resp(e).into_response();
    }
    blocking!(bus, bus.commit_delta(req))
}

/// POST /commit/v2 — WSS path.
/// DeliveryLog + optional explicit read_set (ARSI for FP = 0).
pub async fn commit_v2(
    State(bus): State<SBus>,
    headers:    HeaderMap,
    Json(req):  Json<CommitRequest>,
) -> impl IntoResponse {
    if let Err(e) = verify_agent_hmac(&headers, &req.agent_id) {
        return err_resp(e).into_response();
    }
    blocking!(bus, bus.commit_delta_v2(req))
}

/// POST /commit/v2_naive — deadlock demo. Deadlocks at N ≥ 8.
pub async fn commit_v2_naive(
    State(bus): State<SBus>,
    headers:    HeaderMap,
    Json(req):  Json<CommitRequest>,
) -> impl IntoResponse {
    if let Err(e) = verify_agent_hmac(&headers, &req.agent_id) {
        return err_resp(e).into_response();
    }
    blocking!(bus, bus.commit_delta_v2_naive(req))
}

// ── Rollback ──────────────────────────────────────────────────────────────────

pub async fn rollback(
    State(bus): State<SBus>,
    headers:    HeaderMap,
    Json(req):  Json<RollbackRequest>,
) -> impl IntoResponse {
    if let Err(e) = verify_agent_hmac(&headers, &req.agent_id) {
        return err_resp(e).into_response();
    }
    blocking!(bus, bus.rollback(req))
}

// ── Observability ─────────────────────────────────────────────────────────────

pub async fn stats(State(bus): State<SBus>) -> impl IntoResponse {
    (StatusCode::OK, Json(bus.stats())).into_response()
}

pub async fn metrics(State(bus): State<SBus>) -> impl IntoResponse {
    (StatusCode::OK, bus.prometheus_metrics()).into_response()
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hmac_sha256_known_vector() {
        // RFC 4231 Test Case 1
        // key  = 0x0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b (20 bytes)
        // data = "Hi There"
        // expected = b0344c61d8db38535ca8afceaf0bf12b881dc200c9833da726e9376c2e32cff7
        let key  = [0x0bu8; 20];
        let data = b"Hi There";
        let result = hmac_sha256(&key, data);
        let hex   = hex::encode(result);
        assert_eq!(
            hex,
            "b0344c61d8db38535ca8afceaf0bf12b881dc200c9833da726e9376c2e32cff7"
        );
    }

    #[test]
    fn constant_time_eq_works() {
        assert!(constant_time_eq(b"abc", b"abc"));
        assert!(!constant_time_eq(b"abc", b"abd"));
        assert!(!constant_time_eq(b"abc", b"abcd"));
        assert!(!constant_time_eq(b"", b"a"));
        assert!(constant_time_eq(b"", b""));
    }
}