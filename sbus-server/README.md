# sbus-server

The S-Bus middleware itself: HTTP server, ACP commit engine, DeliveryLog,
and openraft-replicated cluster.

This crate is part of the [S-Bus workspace](../). For full project context,
see the [top-level README](../README.md).

---

## What this crate does

A single binary `sbus-server` that exposes an HTTP API for multi-agent
state coordination over natural-language shards. The four pieces of the
system are:

1. **`bus/registry.rs`** — `ShardRegistry` (per-shard versioned mutable
   state) and `DeliveryLog` (per-agent log of HTTP `GET` operations with
   TTL). The DeliveryLog is the central technical mechanism: it
   reconstructs each agent's read-set automatically from observed
   traffic.

2. **`bus/engine.rs`** — `SBus` struct with the `commit_delta()` Atomic
   Commit Protocol. Implements Algorithm 1 from the paper: build
   effective read-set, validate cross-shard versions against the
   DeliveryLog, validate primary shard version, atomic write under a
   write lock. Includes the Workload-B view-divergence counters that
   power the runtime ORI toggle for ablation experiments.

3. **`api/handlers.rs`** — Axum HTTP handlers. The complete endpoint
   list is documented in the source; key endpoints are listed below.

4. **`raft/`** — openraft 0.8.4 integration with sled-backed
   persistence. Used for the 3-node distributed deployment that backs
   the paper's DR-9 experiment.

---

## Building

```bash
# from the workspace root
cargo build --release -p sbus-server
```

Or:

```bash
# from this directory
cargo build --release
```

Requires Rust edition 2024 (stable 1.85+).

---

## Running

### Single-node (development / most experiments)

```bash
SBUS_ADMIN_ENABLED=1 cargo run --release
```

The server listens on `http://localhost:7000`. Admin endpoints
(`/admin/*`) require `SBUS_ADMIN_ENABLED=1`.

### 3-node Raft cluster

```bash
./start_raft_cluster.sh    # nodes on 7001, 7002, 7003
./stop_raft_cluster.sh
```

### Smoke test

```bash
./test_proxy_cross_shard_stale.sh
```

This runs an end-to-end check that cross-shard stale reads are rejected
with HTTP 409, matching paper §VII-C, Exp. SR.

---

## HTTP API

### Core

| Method | Endpoint | Purpose |
|---|---|---|
| `POST` | `/shard` | Create a new shard |
| `GET`  | `/shard/{key}?agent_id=X` | Read shard (records `(key, version)` in agent X's DeliveryLog) |
| `GET`  | `/shards` | List all shard keys |
| `POST` | `/commit` | Commit (legacy v1 — minimal validation) |
| `POST` | `/commit/v2` | Commit (current ACP — full DeliveryLog validation) |
| `POST` | `/rollback` | Roll back a shard to a prior version |
| `POST` | `/session` | Create / reset agent session |
| `POST` | `/delivery_log/register` | Proxy-side DL registration (skip-if-exists semantics) |
| `GET`  | `/stats` | Aggregate counters |
| `GET`  | `/metrics` | Prometheus-format metrics |

### Admin (require `SBUS_ADMIN_ENABLED=1` for some endpoints)

| Method | Endpoint | Purpose |
|---|---|---|
| `GET`  | `/admin/health` | Server liveness + summary stats |
| `POST` | `/admin/reset` | Clear all shards, sessions, counters |
| `GET`  | `/admin/config` | Current `ori_enabled` flag + view-divergence counters |
| `POST` | `/admin/config` | Toggle `ori_enabled` at runtime (Workload-B) |
| `GET`  | `/admin/delivery-log` | Dump the full DeliveryLog (debug) |
| `POST` | `/admin/inject-stale` | Inject a stale entry (for adversarial tests) |
| `POST` | `/admin/shard` | Create a shard with explicit content (test setup) |
| `POST` | `/admin/commit` | Apply a commit bypassing client retry (test setup) |
| `POST` | `/admin/add-node` | Cluster membership change |

### Cluster / Raft

| Method | Endpoint | Purpose |
|---|---|---|
| `GET`  | `/cluster/status` | Cluster membership + leader |
| `POST` | `/raft/append-entries` | openraft RPC |
| `POST` | `/raft/vote` | openraft RPC |
| `POST` | `/raft/install-snapshot` | openraft RPC |
| `POST` | `/raft/init` | Bootstrap a new cluster |
| `POST` | `/raft/add-learner` | Promote a learner to voter |
| `POST` | `/raft/change-membership` | Reconfigure cluster |
| `GET`  | `/raft/metrics` | Raft-internal state |
| `GET`  | `/raft/leader` | Current leader URL |

---

## Workload-B runtime ORI toggle

The paper's Workload-B experiment compares cross-shard view-divergence
under ORI-ON vs. ORI-OFF on a non-code workload (data-pipeline
architecture planning across 8 domains). The toggle is exposed via
`/admin/config`:

```bash
# Disable ORI (ori_off baseline)
curl -X POST http://localhost:7000/admin/config \
  -H 'content-type: application/json' \
  -d '{"ori_enabled": false}'

# Read state and counters
curl http://localhost:7000/admin/config
# → {"ori_enabled":          false,
#    "view_checked_commits": 638,
#    "view_divergent_commits": 590}

# Re-enable ORI
curl -X POST http://localhost:7000/admin/config \
  -d '{"ori_enabled": true}'
```

Under ORI-OFF, the cross-shard view-divergence counter increments on
every stale sibling read but the commit succeeds — measuring how many
stale commits ORI would have prevented. Under ORI-ON, divergent commits
are rejected with HTTP 409 `CROSSSHARDSTALE`.

The Python harness flips this flag once at the start of each trial via
`run_workload_b.py` in `sbus-experiments`.

---

## Configuration

| Variable | Default | Purpose |
|---|---|---|
| `SBUS_PORT` | `7000` | HTTP listen port |
| `SBUS_ADMIN_ENABLED` | unset | Set to `1` to enable `/admin/*` endpoints |
| `SBUS_RAFT_NODE_ID` | unset → single-node mode | Raft node ID |
| `SBUS_RAFT_PEERS` | unset | `id1=url1,id2=url2,...` peer list |
| `SBUS_DATA_DIR` | `./data/node{ID}` | sled storage directory |
| `SBUS_WAL_PATH` | unset → no WAL | Optional plaintext WAL path |
| `SBUS_SESSION_TTL` | `3600` | DeliveryLog session TTL (seconds) |
| `SBUS_LOG` | unset | Set to `1` to disable DeliveryLog entirely (ablation) |
| `RUST_LOG` | `info,sbus=debug` | tracing log level |

ACP-specific configuration is read from environment by `AcpConfig::from_env()`
in `bus/types.rs`.

---

## Testing

```bash
cargo test --release           # unit tests
cargo clippy --release -- -D warnings   # zero-warnings policy
```

End-to-end staleness rejection (paper §VII-C):

```bash
./test_proxy_cross_shard_stale.sh
```

---

## Lines of code

- `main.rs` — 161 lines (entry point, router, Raft bootstrap)
- `api/handlers.rs` — ~636 lines (29 handlers across core / admin / proxy / cluster)
- `api/raft_handlers.rs` — 161 lines
- `api/cluster_handlers.rs` — 21 lines
- `bus/engine.rs` — ~572 lines (SBus, ACP, WAL, view-divergence counters)
- `bus/registry.rs` — 354 lines (ShardRegistry + DeliveryLog)
- `bus/types.rs` — 214 lines
- `cluster/mod.rs` — 116 lines
- `raft/store.rs` — 418 lines (sled-backed log + state machine)
- `raft/network.rs` — 120 lines
- `raft/mod.rs` — 96 lines
- `metrics/collector.rs` — 219 lines

Total: ~3,100 lines of safe Rust, zero `unsafe`.
