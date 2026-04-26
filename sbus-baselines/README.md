# sbus-baselines

Rust-native concurrency-control adapters used as matched-mechanism
baselines in the S-Bus paper's PG-Comparison and PG-Contention
experiments.

This crate is part of the [S-Bus workspace](../). For full project
context, see the [top-level README](../README.md).

---

## What's in here

Two binaries that each expose the same JSON HTTP API as `sbus-server`,
backed by a different concurrency-control engine:

- **`pg-adapter`** (`src/bin/pg_adapter.rs`, ~651 lines) — PostgreSQL
  17 with `SET TRANSACTION ISOLATION LEVEL SERIALIZABLE` set on every
  connection from the deadpool. Cross-shard read-set validation
  happens inside a single SERIALIZABLE transaction; on conflict the
  database raises `40001` (`serialization_failure`) and the adapter
  translates it to HTTP 409 `CROSSSHARDSTALE`.

- **`redis-adapter`** (`src/bin/redis_adapter.rs`, ~596 lines) — Redis
  7 using `WATCH <key1> <key2> ... <keyN>; MULTI; ...; EXEC`. All
  read-set keys are `WATCH`ed before the transaction block; if any of
  them changes, `EXEC` returns `nil` and we retry.

Total: ~1,247 lines of safe Rust. Both adapters expose identical
`/shard`, `/commit`, `/stats`, etc. endpoints to `sbus-server` so the
Python experiment harness can target any of the three backends
interchangeably (paper §VII-M).

---

## Why these exist

Reviewers across multiple paper rounds have flagged the absence of a
database-CC baseline as the largest evaluation gap. The objection
"a 50-line Redis WATCH adapter, or PostgreSQL SERIALIZABLE, would give
you the same guarantee" is legitimate. The paper's Exp. PG-Comparison
and Exp. PG-Contention answer this by running the same multi-agent
workload through three independent CC engines and showing safety
parity:

- 0 Type-I corruptions across 200,880 commit attempts (PG-Comparison
  full sweep at N ∈ {4, 8, 16, 32, 64})
- 0 Type-I corruptions across 472,750 attempts under shared-shard
  contention with 427,308 active HTTP-409 conflicts (PG-Contention)
- SCR agreement across the three backends within 1 pp at N ≥ 8

The architectural value of S-Bus over transactional-DB baselines is
operational simplicity and the LLM-native contract, **not**
structural-safety differentiation — a narrower and sharper claim than
prior drafts of the paper advanced.

This crate exists to make that claim verifiable.

---

## Building

```bash
# from the workspace root
cargo build --release -p sbus-baselines

# binaries land in target/release/
ls target/release/pg-adapter target/release/redis-adapter
```

Or:

```bash
# from this directory
cargo build --release
```

Requires Rust edition 2024 (stable 1.85+). The `pg-adapter` binary
needs `tokio-postgres` + `deadpool-postgres`; the `redis-adapter`
binary needs `redis 0.27` with `tokio-comp` features. All declared
in `Cargo.toml`.

---

## Running

### `pg-adapter`

```bash
PG_DSN="host=localhost dbname=sbus_baseline user=sbus_user password=..." \
  ./target/release/pg-adapter
# → http://localhost:7001
```

The adapter creates two tables on startup if they don't exist:

```sql
CREATE TABLE IF NOT EXISTS shards (
    key         TEXT PRIMARY KEY,
    version     BIGINT NOT NULL DEFAULT 0,
    content     TEXT NOT NULL DEFAULT '',
    goal_tag    TEXT NOT NULL DEFAULT '',
    created_at  TIMESTAMPTZ DEFAULT NOW(),
    updated_at  TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS shard_log (
    id           BIGSERIAL PRIMARY KEY,
    key          TEXT NOT NULL,
    version      BIGINT NOT NULL,
    agent_id     TEXT NOT NULL,
    delta        TEXT NOT NULL,
    committed_at TIMESTAMPTZ DEFAULT NOW()
);
```

### `redis-adapter`

```bash
REDIS_URL=redis://127.0.0.1:6379 \
  ./target/release/redis-adapter
# → http://localhost:7002
```

Redis state lives under per-key `shard:{key}` hashes. The adapter
scaffolds these on first commit; no schema migration is needed.

---

## Configuration

### `pg-adapter`

| Variable | Required | Purpose |
|---|---|---|
| `PG_DSN` | yes | Postgres connection string |
| `PG_ADAPTER_PORT` | no (default `7001`) | HTTP listen port |
| `PG_POOL_SIZE` | no (default `16`) | deadpool max connections |

### `redis-adapter`

| Variable | Default | Purpose |
|---|---|---|
| `REDIS_URL` | `redis://127.0.0.1:6379` | Redis connection |
| `REDIS_ADAPTER_PORT` | `7002` | HTTP listen port |

---

## Reproducing PG-Comparison / PG-Contention

The experiment harness lives in
[`sbus-experiments`](https://github.com/sajjadanwar0/sbus-experiments).
The relevant scripts:

- `pg_bench_full.py` — main PG-Comparison sweep (200,880 attempts at
  N ∈ {4, 8, 16, 32, 64})
- `pg_comparison.py` — Rust-Native variant against these adapters
- `pg_bench_contention.py` — PG-Contention shared-shard sweep
- `exp_pg_contention.py` — analysis pipeline for PG-Contention

Typical setup (all three backends running):

```bash
# Terminal 1: S-Bus on 7000
cargo run --release -p sbus-server &

# Terminal 2: PG adapter on 7001
PG_DSN="host=localhost dbname=sbus user=sbus password=..." \
  cargo run --release -p sbus-baselines --bin pg-adapter &

# Terminal 3: Redis adapter on 7002
REDIS_URL=redis://127.0.0.1:6379 \
  cargo run --release -p sbus-baselines --bin redis-adapter &

# Terminal 4: run the harness
cd ../sbus-experiments
python3 pg_bench_full.py --backends sbus,pg,redis --agents 4 8 16 32 64
```

---

## Architectural notes

### `pg-adapter`: SERIALIZABLE under the hood

Each commit acquires a connection from the deadpool, opens a
SERIALIZABLE transaction, performs the cross-shard read-set check via
`SELECT version FROM shards WHERE key = ANY($1)`, then performs the
primary-shard `UPDATE ... WHERE version = $expected`. The single
SERIALIZABLE transaction covers both. On conflict, Postgres raises
`SerializationFailure` (`40001`); we translate to HTTP 409.

### `redis-adapter`: WATCH/MULTI/EXEC

Each commit issues `WATCH` on every key in the read-set (including the
primary shard's key), then `MULTI`, then the version-check `HGET` and
the conditional `HSET`/`HINCRBY`, then `EXEC`. If any watched key
changed in the interval, `EXEC` returns `nil` and we re-raise as
HTTP 409. We do **not** retry inside the adapter — retry is the
client's responsibility, matching `sbus-server`'s contract.

### What's deliberately *not* implemented

These adapters target safety parity, not feature parity. Specifically:

- No DeliveryLog. The cross-shard read-set is taken from the
  client-provided `read_set` field on the commit request only — there
  is no server-side automatic read-set reconstruction. This is the
  contract: a client targeting `pg-adapter` or `redis-adapter` must
  supply its read-set explicitly. (This is why the paper's
  architectural value claim is "operational simplicity" — the
  DeliveryLog is what makes S-Bus's API zero-coordination-code from
  the client's perspective.)
- No Raft / cluster mode. Postgres handles its own replication;
  Redis handles its own (or doesn't). These adapters are single-
  process processes in front of those backends.
- No proxy integration. `/delivery_log/register` returns 501
  Not Implemented.

If you point a stock S-Bus client at these adapters, ARSI mode (the
explicit-read-set mode) works; the default zero-coordination mode does
not.

---

## License

MIT.
