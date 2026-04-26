use std::{collections::HashMap, net::SocketAddr, sync::Arc};

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{delete, get, post},
    Json, Router,
};
use deadpool_postgres::{Config as PoolConfig, ManagerConfig, Pool, RecyclingMethod, Runtime};
use serde::{Deserialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use tokio::sync::RwLock;
use tokio_postgres::{error::SqlState, NoTls};
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

const SCHEMA_SHARDS: &str = "\
CREATE TABLE IF NOT EXISTS shards (
    key         TEXT PRIMARY KEY,
    version     BIGINT NOT NULL DEFAULT 0,
    content     TEXT NOT NULL DEFAULT '',
    goal_tag    TEXT NOT NULL DEFAULT '',
    created_at  TIMESTAMPTZ DEFAULT NOW(),
    updated_at  TIMESTAMPTZ DEFAULT NOW()
)";

const SCHEMA_SHARD_LOG: &str = "\
CREATE TABLE IF NOT EXISTS shard_log (
    id           BIGSERIAL PRIMARY KEY,
    key          TEXT NOT NULL,
    version      BIGINT NOT NULL,
    agent_id     TEXT NOT NULL,
    delta        TEXT NOT NULL,
    committed_at TIMESTAMPTZ DEFAULT NOW()
)";

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
    pool: Pool,
    delivery_log: RwLock<HashMap<String, HashMap<String, i64>>>,
    commit_count: std::sync::atomic::AtomicU64,
    conflict_count: std::sync::atomic::AtomicU64,
}

impl AppState {
    fn new(pool: Pool) -> Self {
        Self {
            pool,
            delivery_log: RwLock::new(HashMap::new()),
            commit_count: std::sync::atomic::AtomicU64::new(0),
            conflict_count: std::sync::atomic::AtomicU64::new(0),
        }
    }
}

fn sha(s: &str) -> String {
    let mut h = Sha256::new();
    h.update(s.as_bytes());
    hex::encode(h.finalize())
}

fn json_err(code: StatusCode, body: Value) -> (StatusCode, Json<Value>) {
    (code, Json(body))
}


async fn create_shard(
    State(state): State<Arc<AppState>>,
    Json(req): Json<CreateShardReq>,
) -> impl IntoResponse {
    let client = match state.pool.get().await {
        Ok(c) => c,
        Err(e) => {
            return json_err(StatusCode::INTERNAL_SERVER_ERROR,
                            json!({"error": "pool", "detail": e.to_string()}));
        }
    };
    let existing = client
        .query_opt("SELECT key FROM shards WHERE key=$1", &[&req.key])
        .await;
    match existing {
        Ok(Some(_)) => {
            return json_err(StatusCode::CONFLICT,
                            json!({"error": "ShardAlreadyExists"}));
        }
        Ok(None) => {}
        Err(e) => {
            return json_err(StatusCode::INTERNAL_SERVER_ERROR,
                            json!({"error": "db", "detail": e.to_string()}));
        }
    }
    if let Err(e) = client
        .execute(
            "INSERT INTO shards(key, content, goal_tag) VALUES($1, $2, $3)",
            &[&req.key, &req.content, &req.goal_tag],
        )
        .await
    {
        return json_err(StatusCode::INTERNAL_SERVER_ERROR,
                        json!({"error": "db", "detail": e.to_string()}));
    }
    (StatusCode::OK, Json(json!({
        "key":      req.key,
        "version":  0,
        "content":  req.content,
        "goal_tag": req.goal_tag,
    })))
}

async fn admin_create_shard(
    State(state): State<Arc<AppState>>,
    Json(req): Json<CreateShardReq>,
) -> impl IntoResponse {
    let client = match state.pool.get().await {
        Ok(c) => c,
        Err(e) => {
            return json_err(StatusCode::INTERNAL_SERVER_ERROR,
                            json!({"error": "pool", "detail": e.to_string()}));
        }
    };
    let sql = "\
        INSERT INTO shards(key, content, goal_tag, version) \
        VALUES($1, $2, $3, 0) \
        ON CONFLICT (key) DO UPDATE SET \
            content    = EXCLUDED.content, \
            goal_tag   = EXCLUDED.goal_tag, \
            version    = 0, \
            updated_at = NOW()";
    if let Err(e) = client
        .execute(sql, &[&req.key, &req.content, &req.goal_tag])
        .await
    {
        return json_err(StatusCode::INTERNAL_SERVER_ERROR,
                        json!({"error": "db", "detail": e.to_string()}));
    }
    (StatusCode::CREATED, Json(json!({
        "key":      req.key,
        "version":  0,
        "content":  req.content,
        "goal_tag": req.goal_tag,
        "status":   "created",
    })))
}

async fn read_shard(
    State(state): State<Arc<AppState>>,
    Path(key): Path<String>,
    Query(q): Query<AgentIdQuery>,
) -> impl IntoResponse {
    let client = match state.pool.get().await {
        Ok(c) => c,
        Err(e) => {
            return json_err(StatusCode::INTERNAL_SERVER_ERROR,
                            json!({"error": "pool", "detail": e.to_string()}));
        }
    };
    let row = client
        .query_opt(
            "SELECT key, version, content, goal_tag, \
                    created_at, updated_at \
             FROM shards WHERE key=$1",
            &[&key],
        )
        .await;
    let row = match row {
        Ok(Some(r)) => r,
        Ok(None) => {
            return json_err(StatusCode::NOT_FOUND,
                            json!({"error": "ShardNotFound", "key": key}));
        }
        Err(e) => {
            return json_err(StatusCode::INTERNAL_SERVER_ERROR,
                            json!({"error": "db", "detail": e.to_string()}));
        }
    };
    let version: i64 = row.get("version");
    let content: String = row.get("content");
    let goal_tag: String = row.get("goal_tag");
    let created_at: chrono::DateTime<chrono::Utc> = row.get("created_at");
    let updated_at: chrono::DateTime<chrono::Utc> = row.get("updated_at");

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
        "created_at": created_at.to_rfc3339(),
        "updated_at": updated_at.to_rfc3339(),
    })))
}

async fn list_shards(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let client = match state.pool.get().await {
        Ok(c) => c,
        Err(e) => {
            return json_err(StatusCode::INTERNAL_SERVER_ERROR,
                            json!({"error": "pool", "detail": e.to_string()}));
        }
    };
    let rows = client
        .query("SELECT key FROM shards ORDER BY key", &[])
        .await;
    match rows {
        Ok(rs) => {
            let keys: Vec<String> = rs.iter().map(|r| r.get::<_, String>(0)).collect();
            (StatusCode::OK, Json(serde_json::to_value(keys).unwrap()))
        }
        Err(e) => json_err(StatusCode::INTERNAL_SERVER_ERROR,
                           json!({"error": "db", "detail": e.to_string()})),
    }
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
    let client = match state.pool.get().await {
        Ok(c) => c,
        Err(e) => {
            return json_err(StatusCode::INTERNAL_SERVER_ERROR,
                            json!({"error": "pool", "detail": e.to_string()}));
        }
    };
    let n_before: i64 = match client
        .query_one("SELECT COUNT(*)::BIGINT FROM shards", &[])
        .await
    {
        Ok(row) => row.get(0),
        Err(e) => {
            return json_err(StatusCode::INTERNAL_SERVER_ERROR,
                            json!({"error": "db", "detail": e.to_string()}));
        }
    };
    if let Err(e) = client
        .execute("TRUNCATE TABLE shards, shard_log RESTART IDENTITY", &[])
        .await
    {
        return json_err(StatusCode::INTERNAL_SERVER_ERROR,
                        json!({"error": "db", "detail": e.to_string()}));
    }
    state.delivery_log.write().await.clear();
    state.commit_count.store(0, std::sync::atomic::Ordering::Relaxed);
    state.conflict_count.store(0, std::sync::atomic::Ordering::Relaxed);
    (StatusCode::OK, Json(json!({"cleared": n_before})))
}

async fn health(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    match state.pool.get().await {
        Ok(client) => match client.execute("SELECT 1", &[]).await {
            Ok(_) => (StatusCode::OK, Json(json!({"status": "ok", "backend": "pg-ser-rust"}))),
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
    let client = match state.pool.get().await {
        Ok(c) => c,
        Err(e) => {
            return json_err(StatusCode::INTERNAL_SERVER_ERROR,
                            json!({"error": "pool", "detail": e.to_string()}));
        }
    };
    if let Err(e) = client.execute("DELETE FROM shards WHERE key=$1", &[&key]).await {
        return json_err(StatusCode::INTERNAL_SERVER_ERROR,
                        json!({"error": "db", "detail": e.to_string()}));
    }
    (StatusCode::OK, Json(json!({"deleted": key})))
}

async fn stats(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let client = match state.pool.get().await {
        Ok(c) => c,
        Err(e) => {
            return json_err(StatusCode::INTERNAL_SERVER_ERROR,
                            json!({"error": "pool", "detail": e.to_string()}));
        }
    };

    let n_shards: i64 = client
        .query_one("SELECT COUNT(*)::BIGINT FROM shards", &[])
        .await
        .map(|row| row.get(0))
        .unwrap_or(0);

    let commits = state.commit_count.load(std::sync::atomic::Ordering::Relaxed);
    let conflicts = state.conflict_count.load(std::sync::atomic::Ordering::Relaxed);
    let total_attempts = commits + conflicts;
    let scr = if total_attempts > 0 {
        conflicts as f64 / total_attempts as f64
    } else {
        0.0
    };

    let dl = state.delivery_log.read().await;
    let tracked_agents = dl.len();
    let total_deliveries: usize = dl.values().map(|m| m.len()).sum();

    (StatusCode::OK, Json(json!({
        "system":           "postgresql_serializable_rust",
        "total_shards":     n_shards,
        "total_commits":    commits,
        "total_conflicts":  conflicts,
        "total_attempts":   total_attempts,
        "scr":              scr,
        "wal_enabled":      true,
        "wal_path":         "postgresql_wal",
        "delivery_log": {
            "tracked_agents":    tracked_agents,
            "total_deliveries":  total_deliveries,
        },
    })))
}

async fn commit_v2(
    State(state): State<Arc<AppState>>,
    Json(req): Json<CommitReq>,
) -> impl IntoResponse {
    let mut client = match state.pool.get().await {
        Ok(c) => c,
        Err(e) => {
            return json_err(StatusCode::INTERNAL_SERVER_ERROR,
                            json!({"error": "pool", "detail": e.to_string()}));
        }
    };

    let dl_snapshot: Vec<(String, i64)> = {
        let dl = state.delivery_log.read().await;
        dl.get(&req.agent_id)
            .map(|m| m.iter().map(|(k, v)| (k.clone(), *v)).collect())
            .unwrap_or_default()
    };

    let tx = match client.transaction().await {
        Ok(t) => t,
        Err(e) => {
            return json_err(StatusCode::INTERNAL_SERVER_ERROR,
                            json!({"error": "tx", "detail": e.to_string()}));
        }
    };
    if let Err(e) = tx
        .execute("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE", &[])
        .await
    {
        let _ = tx.rollback().await;
        return json_err(StatusCode::INTERNAL_SERVER_ERROR,
                        json!({"error": "tx_isolation", "detail": e.to_string()}));
    }

    let primary_row = match tx
        .query_opt(
            "SELECT version, content FROM shards WHERE key=$1 FOR UPDATE",
            &[&req.key],
        )
        .await
    {
        Ok(r) => r,
        Err(e) => {
            let _ = tx.rollback().await;
            return pg_error_to_response(e, &state, &req.key);
        }
    };
    let primary = match primary_row {
        Some(r) => r,
        None => {
            let _ = tx.rollback().await;
            return json_err(StatusCode::NOT_FOUND,
                            json!({"error": "ShardNotFound", "key": req.key}));
        }
    };
    let cur_ver: i64 = primary.get("version");

    if cur_ver != req.expected_version {
        let _ = tx.rollback().await;
        state.conflict_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        return json_err(StatusCode::CONFLICT, json!({
            "error":    "VersionMismatch",
            "key":      req.key,
            "expected": req.expected_version,
            "found":    cur_ver,
        }));
    }

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

    for (k, expected_v) in eff.into_iter() {
        let rs_row = match tx
            .query_opt(
                "SELECT version FROM shards WHERE key=$1 FOR SHARE",
                &[&k],
            )
            .await
        {
            Ok(r) => r,
            Err(e) => {
                let _ = tx.rollback().await;
                return pg_error_to_response(e, &state, &req.key);
            }
        };

        if let Some(rs_r) = rs_row {
            let found_v: i64 = rs_r.get("version");
            if found_v != expected_v {
                let _ = tx.rollback().await;
                state.conflict_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                return json_err(StatusCode::CONFLICT, json!({
                    "error": "CrossShardStale",
                    "key":   k,
                }));
            }
        }
    }

    let new_ver = cur_ver + 1;
    if let Err(e) = tx
        .execute(
            "UPDATE shards SET version=$1, content=$2, updated_at=NOW() \
             WHERE key=$3",
            &[&new_ver, &req.delta, &req.key],
        )
        .await
    {
        let _ = tx.rollback().await;
        return pg_error_to_response(e, &state, &req.key);
    }
    if let Err(e) = tx
        .execute(
            "INSERT INTO shard_log(key, version, agent_id, delta) \
             VALUES($1, $2, $3, $4)",
            &[&req.key, &new_ver, &req.agent_id, &req.delta],
        )
        .await
    {
        let _ = tx.rollback().await;
        return pg_error_to_response(e, &state, &req.key);
    }

    if let Err(e) = tx.commit().await {
        return pg_error_to_response(e, &state, &req.key);
    }

    state.commit_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    (StatusCode::OK, Json(json!({
        "new_version": new_ver,
        "shard_id":    sha(&req.delta),
    })))
}

fn pg_error_to_response(
    e: tokio_postgres::Error,
    state: &AppState,
    key: &str,
) -> (StatusCode, Json<Value>) {
    if let Some(code) = e.code() {
        if code == &SqlState::T_R_SERIALIZATION_FAILURE {
            state
                .conflict_count
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            return json_err(StatusCode::CONFLICT, json!({
                "error":   "VersionMismatch",
                "key":     key,
                "message": "serialization_failure (no internal retry)",
            }));
        }
    }
    json_err(StatusCode::INTERNAL_SERVER_ERROR,
             json!({"error": "db", "detail": e.to_string()}))
}

fn pool_config_from_env() -> Result<PoolConfig, String> {
    let dsn = std::env::var("PG_DSN").unwrap_or_else(|_| {
        "host=localhost dbname=sbus_baseline user=sbus_user password=sbus_pass"
            .to_owned()
    });
    let pool_size: usize = std::env::var("PG_POOL_SIZE")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(128);

    let mut cfg = PoolConfig::new();
    cfg.manager = Some(ManagerConfig {
        recycling_method: RecyclingMethod::Fast,
    });
    cfg.pool = Some(deadpool_postgres::PoolConfig::new(pool_size));

    if dsn.starts_with("postgresql://") || dsn.starts_with("postgres://") {
        cfg.url = Some(dsn);
        return Ok(cfg);
    }

    for tok in dsn.split_whitespace() {
        let (k, v) = match tok.split_once('=') {
            Some(kv) => kv,
            None => return Err(format!("bad DSN token: {tok}")),
        };
        match k {
            "host"     => cfg.host     = Some(v.to_owned()),
            "port"     => cfg.port     = v.parse().ok(),
            "dbname"   => cfg.dbname   = Some(v.to_owned()),
            "user"     => cfg.user     = Some(v.to_owned()),
            "password" => cfg.password = Some(v.to_owned()),
            _          => {}
        }
    }
    Ok(cfg)
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,sbus_baselines=debug".parse().unwrap()),
        )
        .init();

    let port: u16 = std::env::var("PG_PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(7001);

    let pool_cfg = match pool_config_from_env() {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Bad PG_DSN: {e}");
            std::process::exit(2);
        }
    };
    let pool = match pool_cfg.create_pool(Some(Runtime::Tokio1), NoTls) {
        Ok(p) => p,
        Err(e) => {
            eprintln!("Failed to create PG pool: {e}");
            std::process::exit(2);
        }
    };

    {
        let client = match pool.get().await {
            Ok(c) => c,
            Err(e) => {
                eprintln!("Initial PG connection failed: {e}");
                eprintln!("  Check PG_DSN and that PostgreSQL is running.");
                std::process::exit(2);
            }
        };
        if let Err(e) = client.batch_execute(SCHEMA_SHARDS).await {
            eprintln!("CREATE TABLE shards failed: {e}");
            std::process::exit(2);
        }
        if let Err(e) = client.batch_execute(SCHEMA_SHARD_LOG).await {
            eprintln!("CREATE TABLE shard_log failed: {e}");
            std::process::exit(2);
        }
    }
    info!("PG schema ready");

    let state = Arc::new(AppState::new(pool));

    let app = Router::new()
        .route("/shard",          post(create_shard))
        .route("/shard/{key}",    get(read_shard))
        .route("/shard/{key}",    delete(delete_shard))
        .route("/shards",         get(list_shards))
        .route("/commit/v2",      post(commit_v2))
        .route("/session",        post(create_session))
        .route("/stats",          get(stats))
        // parity with Rust S-Bus
        .route("/admin/shard",    post(admin_create_shard))
        .route("/admin/reset",    post(admin_reset))
        .route("/admin/health",   get(health))
        .route("/health",         get(health))
        .with_state(state);

    let addr: SocketAddr = ([0, 0, 0, 0], port).into();
    info!("Rust PG adapter on {addr}");
    warn!("Internal retry: DISABLED (SerializationFailure -> 409). \
           This is the fair-baseline behaviour. Set this expectation.");

    let listener = tokio::net::TcpListener::bind(&addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}