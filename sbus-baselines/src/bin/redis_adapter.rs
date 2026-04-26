#![allow(deprecated)]

use std::{collections::HashMap, net::SocketAddr, sync::Arc};

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{delete, get, post},
    Json, Router,
};
use chrono::Utc;
use redis::{AsyncCommands, Client};
use serde::Deserialize;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use tokio::sync::RwLock;
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

fn k_ver(k: &str)     -> String { format!("shard:{k}:ver") }
fn k_content(k: &str) -> String { format!("shard:{k}:content") }
fn k_goal(k: &str)    -> String { format!("shard:{k}:goal") }
fn k_created(k: &str) -> String { format!("shard:{k}:created_at") }
fn k_updated(k: &str) -> String { format!("shard:{k}:updated_at") }
fn k_log(k: &str)     -> String { format!("shard_log:{k}") }
const SHARDS_INDEX: &str = "shards_index";

fn now_iso() -> String {
    Utc::now().to_rfc3339()
}

fn sha(s: &str) -> String {
    let mut h = Sha256::new();
    h.update(s.as_bytes());
    hex::encode(&h.finalize()[..8])
}

#[derive(Deserialize)]
struct CreateShardReq {
    key: String,
    #[serde(default)]
    content: String,
    #[serde(default)]
    goal_tag: String,
}

#[derive(Deserialize)]
struct ReadSetEntry {
    key: String,
    version_at_read: i64,
}

#[derive(Deserialize)]
struct CommitReq {
    key: String,
    expected_version: i64,
    delta: String,
    agent_id: String,
    #[serde(default)]
    read_set: Option<Vec<ReadSetEntry>>,
}

#[derive(Deserialize)]
struct SessionReq {
    agent_id: String,
}

#[derive(Deserialize)]
struct AgentIdQuery {
    #[serde(default)]
    agent_id: String,
}

struct AppState {
    client: Client,
    delivery_log: RwLock<HashMap<String, HashMap<String, i64>>>,
    commit_count: std::sync::atomic::AtomicU64,
    conflict_count: std::sync::atomic::AtomicU64,
    serialization_retries: std::sync::atomic::AtomicU64,
    max_retries: u32,
}

impl AppState {
    fn new(client: Client, max_retries: u32) -> Self {
        Self {
            client,
            delivery_log: RwLock::new(HashMap::new()),
            commit_count: std::sync::atomic::AtomicU64::new(0),
            conflict_count: std::sync::atomic::AtomicU64::new(0),
            serialization_retries: std::sync::atomic::AtomicU64::new(0),
            max_retries,
        }
    }
}

fn json_err(code: StatusCode, body: Value) -> (StatusCode, Json<Value>) {
    (code, Json(body))
}

async fn create_shard(
    State(state): State<Arc<AppState>>,
    Json(req): Json<CreateShardReq>,
) -> impl IntoResponse {
    let mut conn = match state.client.get_async_connection().await {
        Ok(c) => c,
        Err(e) => return json_err(StatusCode::INTERNAL_SERVER_ERROR,
                                  json!({"error": "redis", "detail": e.to_string()})),
    };

    let ver_key = k_ver(&req.key);
    loop {
        if let Err(e) = redis::cmd("WATCH").arg(&ver_key).query_async::<()>(&mut conn).await {
            return json_err(StatusCode::INTERNAL_SERVER_ERROR,
                            json!({"error": "redis", "detail": e.to_string()}));
        }
        let exists: bool = match redis::cmd("EXISTS").arg(&ver_key).query_async(&mut conn).await {
            Ok(v) => v,
            Err(e) => {
                let _: Result<(), _> = redis::cmd("UNWATCH").query_async(&mut conn).await;
                return json_err(StatusCode::INTERNAL_SERVER_ERROR,
                                json!({"error": "redis", "detail": e.to_string()}));
            }
        };
        if exists {
            let _: Result<(), _> = redis::cmd("UNWATCH").query_async(&mut conn).await;
            return json_err(StatusCode::CONFLICT, json!({"error": "ShardAlreadyExists"}));
        }

        let now = now_iso();
        let result: redis::RedisResult<Option<Vec<redis::Value>>> = redis::pipe()
            .atomic()
            .cmd("SET").arg(&ver_key).arg(0i64).ignore()
            .cmd("SET").arg(k_content(&req.key)).arg(&req.content).ignore()
            .cmd("SET").arg(k_goal(&req.key)).arg(&req.goal_tag).ignore()
            .cmd("SET").arg(k_created(&req.key)).arg(&now).ignore()
            .cmd("SET").arg(k_updated(&req.key)).arg(&now).ignore()
            .cmd("SADD").arg(SHARDS_INDEX).arg(&req.key).ignore()
            .query_async(&mut conn)
            .await;

        match result {
            Ok(Some(_)) => {
                return (StatusCode::OK, Json(json!({
                    "key":      req.key,
                    "version":  0,
                    "content":  req.content,
                    "created":  true,
                })));
            }
            Ok(None) => {
                continue;
            }
            Err(e) => {
                return json_err(StatusCode::INTERNAL_SERVER_ERROR,
                                json!({"error": "redis", "detail": e.to_string()}));
            }
        }
    }
}

async fn admin_create_shard(
    State(state): State<Arc<AppState>>,
    Json(req): Json<CreateShardReq>,
) -> impl IntoResponse {
    let mut conn = match state.client.get_multiplexed_async_connection().await {
        Ok(c) => c,
        Err(e) => return json_err(StatusCode::INTERNAL_SERVER_ERROR,
                                  json!({"error": "redis", "detail": e.to_string()})),
    };
    let now = now_iso();
    let res: redis::RedisResult<()> = redis::pipe()
        .atomic()
        .cmd("SET").arg(k_ver(&req.key)).arg(0i64).ignore()
        .cmd("SET").arg(k_content(&req.key)).arg(&req.content).ignore()
        .cmd("SET").arg(k_goal(&req.key)).arg(&req.goal_tag).ignore()
        .cmd("SET").arg(k_created(&req.key)).arg(&now).ignore()
        .cmd("SET").arg(k_updated(&req.key)).arg(&now).ignore()
        .cmd("SADD").arg(SHARDS_INDEX).arg(&req.key).ignore()
        .query_async(&mut conn)
        .await;

    match res {
        Ok(_) => (StatusCode::CREATED, Json(json!({
            "key":      req.key,
            "version":  0,
            "content":  req.content,
            "goal_tag": req.goal_tag,
            "status":   "created",
        }))),
        Err(e) => json_err(StatusCode::INTERNAL_SERVER_ERROR,
                           json!({"error": "redis", "detail": e.to_string()})),
    }
}

async fn read_shard(
    State(state): State<Arc<AppState>>,
    Path(key): Path<String>,
    Query(q): Query<AgentIdQuery>,
) -> impl IntoResponse {
    let mut conn = match state.client.get_multiplexed_async_connection().await {
        Ok(c) => c,
        Err(e) => return json_err(StatusCode::INTERNAL_SERVER_ERROR,
                                  json!({"error": "redis", "detail": e.to_string()})),
    };

    let ver: Option<i64> = match conn.get::<_, Option<i64>>(k_ver(&key)).await {
        Ok(v) => v,
        Err(e) => return json_err(StatusCode::INTERNAL_SERVER_ERROR,
                                  json!({"error": "redis", "detail": e.to_string()})),
    };
    let version = match ver {
        Some(v) => v,
        None => return json_err(StatusCode::NOT_FOUND,
                                json!({"error": "ShardNotFound", "key": key})),
    };

    let content:    String = conn.get(k_content(&key)).await.unwrap_or_default();
    let goal_tag:   String = conn.get(k_goal(&key)).await.unwrap_or_default();
    let created_at: Option<String> = conn.get(k_created(&key)).await.ok();
    let updated_at: Option<String> = conn.get(k_updated(&key)).await.ok();

    if !q.agent_id.is_empty() {
        let mut dl = state.delivery_log.write().await;
        dl.entry(q.agent_id.clone())
            .or_insert_with(HashMap::new)
            .insert(key.clone(), version);
    }

    (StatusCode::OK, Json(json!({
        "key":        key,
        "version":    version,
        "content":    content,
        "goal_tag":   goal_tag,
        "created_at": created_at,
        "updated_at": updated_at,
    })))
}

async fn list_shards(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let mut conn = match state.client.get_multiplexed_async_connection().await {
        Ok(c) => c,
        Err(e) => return json_err(StatusCode::INTERNAL_SERVER_ERROR,
                                  json!({"error": "redis", "detail": e.to_string()})),
    };
    let mut keys: Vec<String> = conn.smembers(SHARDS_INDEX).await.unwrap_or_default();
    keys.sort();
    (StatusCode::OK, Json(serde_json::to_value(keys).unwrap()))
}

async fn create_session(
    State(state): State<Arc<AppState>>,
    Json(req): Json<SessionReq>,
) -> impl IntoResponse {
    let mut dl = state.delivery_log.write().await;
    dl.entry(req.agent_id.clone()).or_insert_with(HashMap::new);
    (StatusCode::OK, Json(json!({
        "agent_id": req.agent_id,
        "status":   "created",
    })))
}

async fn admin_reset(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let mut conn = match state.client.get_multiplexed_async_connection().await {
        Ok(c) => c,
        Err(e) => return json_err(StatusCode::INTERNAL_SERVER_ERROR,
                                  json!({"error": "redis", "detail": e.to_string()})),
    };

    let keys: Vec<String> = conn.smembers(SHARDS_INDEX).await.unwrap_or_default();
    let n_cleared = keys.len();

    if !keys.is_empty() {
        let mut all_keys: Vec<String> = Vec::with_capacity(keys.len() * 6);
        for k in &keys {
            all_keys.push(k_ver(k));
            all_keys.push(k_content(k));
            all_keys.push(k_goal(k));
            all_keys.push(k_created(k));
            all_keys.push(k_updated(k));
            all_keys.push(k_log(k));
        }
        let _: redis::RedisResult<i64> =
            redis::cmd("DEL").arg(&all_keys).query_async(&mut conn).await;
    }
    let _: redis::RedisResult<i64> =
        redis::cmd("DEL").arg(SHARDS_INDEX).query_async(&mut conn).await;

    state.delivery_log.write().await.clear();
    state.commit_count.store(0, std::sync::atomic::Ordering::Relaxed);
    state.conflict_count.store(0, std::sync::atomic::Ordering::Relaxed);
    state.serialization_retries.store(0, std::sync::atomic::Ordering::Relaxed);

    (StatusCode::OK, Json(json!({"cleared": n_cleared})))
}

async fn health(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    match state.client.get_multiplexed_async_connection().await {
        Ok(mut c) => match redis::cmd("PING").query_async::<String>(&mut c).await {
            Ok(_) => (StatusCode::OK, Json(json!({"status": "ok", "backend": "redis-watch-rust"}))),
            Err(e) => json_err(StatusCode::SERVICE_UNAVAILABLE,
                               json!({"status": "unhealthy", "detail": e.to_string()})),
        },
        Err(e) => json_err(StatusCode::SERVICE_UNAVAILABLE,
                           json!({"status": "unhealthy", "detail": e.to_string()})),
    }
}

async fn delete_shard(
    State(state): State<Arc<AppState>>,
    Path(key): Path<String>,
) -> impl IntoResponse {
    let mut conn = match state.client.get_multiplexed_async_connection().await {
        Ok(c) => c,
        Err(e) => return json_err(StatusCode::INTERNAL_SERVER_ERROR,
                                  json!({"error": "redis", "detail": e.to_string()})),
    };
    let all_keys = [
        k_ver(&key), k_content(&key), k_goal(&key),
        k_created(&key), k_updated(&key), k_log(&key),
    ];
    let _: redis::RedisResult<i64> =
        redis::cmd("DEL").arg(&all_keys[..]).query_async(&mut conn).await;
    let _: redis::RedisResult<i64> =
        redis::cmd("SREM").arg(SHARDS_INDEX).arg(&key).query_async(&mut conn).await;
    (StatusCode::OK, Json(json!({"deleted": key})))
}

async fn stats(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let n_shards: i64 = match state.client.get_multiplexed_async_connection().await {
        Ok(mut c) => c.scard(SHARDS_INDEX).await.unwrap_or(0),
        Err(_) => 0,
    };

    let commits = state.commit_count.load(std::sync::atomic::Ordering::Relaxed);
    let conflicts = state.conflict_count.load(std::sync::atomic::Ordering::Relaxed);
    let serialization_retries =
        state.serialization_retries.load(std::sync::atomic::Ordering::Relaxed);
    let dl = state.delivery_log.read().await;
    let agents = dl.len();
    let entries: usize = dl.values().map(|m| m.len()).sum();

    (StatusCode::OK, Json(json!({
        "system":                "redis_watch_multi_rust",
        "commits":               commits,
        "conflicts":             conflicts,
        "serialization_retries": serialization_retries,
        "shards":                n_shards,
        "delivery_log": {
            "agents":  agents,
            "entries": entries,
        },
    })))
}

async fn commit_v2(
    State(state): State<Arc<AppState>>,
    Json(req): Json<CommitReq>,
) -> impl IntoResponse {
    let mut conn = match state.client.get_async_connection().await {
        Ok(c) => c,
        Err(e) => return json_err(StatusCode::INTERNAL_SERVER_ERROR,
                                  json!({"error": "redis", "detail": e.to_string()})),
    };

    let dl_snapshot: Vec<(String, i64)> = {
        let dl = state.delivery_log.read().await;
        dl.get(&req.agent_id)
            .map(|m| m.iter().map(|(k, v)| (k.clone(), *v)).collect())
            .unwrap_or_default()
    };

    let mut eff: HashMap<String, i64> = HashMap::new();
    if let Some(rs) = &req.read_set {
        for e in rs {
            if e.key != req.key {
                eff.insert(e.key.clone(), e.version_at_read);
            }
        }
    }
    for (k, v) in dl_snapshot.into_iter() {
        if k != req.key {
            eff.entry(k).or_insert(v);
        }
    }

    let ver_primary = k_ver(&req.key);
    let mut watch_keys: Vec<String> = Vec::with_capacity(1 + eff.len());
    watch_keys.push(ver_primary.clone());
    for k in eff.keys() {
        watch_keys.push(k_ver(k));
    }

    for attempt in 0..state.max_retries {
        if let Err(e) = redis::cmd("WATCH").arg(&watch_keys).query_async::<()>(&mut conn).await {
            return json_err(StatusCode::INTERNAL_SERVER_ERROR,
                            json!({"error": "redis", "detail": e.to_string()}));
        }

        let cur_ver: Option<i64> = match conn.get(&ver_primary).await {
            Ok(v) => v,
            Err(e) => {
                let _: Result<(), _> = redis::cmd("UNWATCH").query_async(&mut conn).await;
                return json_err(StatusCode::INTERNAL_SERVER_ERROR,
                                json!({"error": "redis", "detail": e.to_string()}));
            }
        };
        let cur_ver = match cur_ver {
            Some(v) => v,
            None => {
                let _: Result<(), _> = redis::cmd("UNWATCH").query_async(&mut conn).await;
                return json_err(StatusCode::NOT_FOUND,
                                json!({"error": "ShardNotFound", "key": req.key}));
            }
        };
        if cur_ver != req.expected_version {
            let _: Result<(), _> = redis::cmd("UNWATCH").query_async(&mut conn).await;
            state.conflict_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            return json_err(StatusCode::CONFLICT, json!({
                "error":    "VersionMismatch",
                "key":      req.key,
                "expected": req.expected_version,
                "found":    cur_ver,
            }));
        }

        let mut cross_failed: Option<(String, i64, i64)> = None;
        let mut cross_missing: Option<String> = None;
        for (k, expected_v) in eff.iter() {
            let k_cur: Option<i64> = match conn.get(k_ver(k)).await {
                Ok(v) => v,
                Err(e) => {
                    let _: Result<(), _> = redis::cmd("UNWATCH").query_async(&mut conn).await;
                    return json_err(StatusCode::INTERNAL_SERVER_ERROR,
                                    json!({"error": "redis", "detail": e.to_string()}));
                }
            };
            match k_cur {
                None => { cross_missing = Some(k.clone()); break; }
                Some(v) if v != *expected_v => {
                    cross_failed = Some((k.clone(), *expected_v, v));
                    break;
                }
                Some(_) => {}
            }
        }
        if let Some(missing_key) = cross_missing {
            let _: Result<(), _> = redis::cmd("UNWATCH").query_async(&mut conn).await;
            state.conflict_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            return json_err(StatusCode::CONFLICT, json!({
                "error":   "CrossShardStale",
                "key":     missing_key,
                "message": "shard_not_found_at_commit",
            }));
        }
        if let Some((fk, fv, cv)) = cross_failed {
            let _: Result<(), _> = redis::cmd("UNWATCH").query_async(&mut conn).await;
            state.conflict_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            return json_err(StatusCode::CONFLICT, json!({
                "error":             "CrossShardStale",
                "key":               fk,
                "version_at_read":   fv,
                "current_version":   cv,
            }));
        }

        let new_ver = cur_ver + 1;
        let now = now_iso();
        let log_line = format!("{new_ver}|{}|{}", req.agent_id, sha(&req.delta));

        let pipe_res: redis::RedisResult<Option<Vec<redis::Value>>> = redis::pipe()
            .atomic()
            .cmd("SET").arg(&ver_primary).arg(new_ver).ignore()
            .cmd("SET").arg(k_content(&req.key)).arg(&req.delta).ignore()
            .cmd("SET").arg(k_updated(&req.key)).arg(&now).ignore()
            .cmd("RPUSH").arg(k_log(&req.key)).arg(&log_line).ignore()
            .query_async(&mut conn)
            .await;

        match pipe_res {
            Ok(Some(_)) => {
                state.commit_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                return (StatusCode::OK, Json(json!({
                    "new_version": new_ver,
                    "shard_id":    sha(&req.delta),
                })));
            }
            Ok(None) => {
                state.serialization_retries.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                if attempt + 1 < state.max_retries {
                    tokio::time::sleep(std::time::Duration::from_millis(
                        10 * (attempt as u64 + 1),
                    )).await;
                    continue;
                } else {
                    break;
                }
            }
            Err(e) if matches!(e.kind(), redis::ErrorKind::ExecAbortError) => {
                state.serialization_retries.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                let _: Result<(), _> = redis::cmd("UNWATCH").query_async(&mut conn).await;
                if attempt + 1 < state.max_retries {
                    tokio::time::sleep(std::time::Duration::from_millis(
                        10 * (attempt as u64 + 1),
                    )).await;
                    continue;
                } else {
                    break;
                }
            }
            Err(e) => {
                let _: Result<(), _> = redis::cmd("UNWATCH").query_async(&mut conn).await;
                return json_err(StatusCode::INTERNAL_SERVER_ERROR,
                                json!({"error": "redis", "detail": e.to_string()}));
            }
        }
    }

    state.conflict_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    json_err(StatusCode::CONFLICT, json!({
        "error":   "VersionMismatch",
        "key":     req.key,
        "message": format!("WATCH/EXEC aborted (retries={})", state.max_retries),
    }))
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,sbus_baselines=debug".parse().unwrap()),
        )
        .init();

    let redis_url = std::env::var("REDIS_URL")
        .unwrap_or_else(|_| "redis://localhost:6379".to_owned());
    let port: u16 = std::env::var("REDIS_PORT")
        .ok().and_then(|s| s.parse().ok()).unwrap_or(7002);
    let max_retries: u32 = std::env::var("MAX_RETRIES")
        .ok().and_then(|s| s.parse().ok()).unwrap_or(1);

    let client = match Client::open(redis_url.as_str()) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Failed to parse REDIS_URL {redis_url}: {e}");
            std::process::exit(2);
        }
    };

    {
        let mut conn = match client.get_async_connection().await {
            Ok(c) => c,
            Err(e) => {
                eprintln!("Redis connection failed: {e}");
                eprintln!("  Check REDIS_URL={redis_url} and that redis-server is running.");
                std::process::exit(2);
            }
        };
        let pong: String = redis::cmd("PING").query_async(&mut conn).await.unwrap_or_default();
        if pong != "PONG" {
            eprintln!("Redis PING returned unexpected: {pong}");
            std::process::exit(2);
        }
    }
    info!("Redis reachable at {redis_url}");

    let state = Arc::new(AppState::new(client, max_retries));

    let app = Router::new()
        .route("/shard",          post(create_shard))
        .route("/shard/{key}",    get(read_shard))
        .route("/shard/{key}",    delete(delete_shard))
        .route("/shards",         get(list_shards))
        .route("/commit/v2",      post(commit_v2))
        .route("/session",        post(create_session))
        .route("/stats",          get(stats))
        .route("/admin/shard",    post(admin_create_shard))
        .route("/admin/reset",    post(admin_reset))
        .route("/admin/health",   get(health))
        .route("/health",         get(health))
        .with_state(state);

    let addr: SocketAddr = ([0, 0, 0, 0], port).into();
    info!("Rust Redis adapter on {addr}");
    warn!(
        "Internal retry budget: {max_retries}. {}",
        if max_retries == 1 {
            "No internal retry — WATCH aborts surface as 409."
        } else {
            "WITH internal retry (ablation only; not fair-baseline behaviour)."
        }
    );

    let listener = tokio::net::TcpListener::bind(&addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}