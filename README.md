# sbus

**Rust implementation of the S-Bus middleware.**

This repository contains the Rust workspace measured in:

> *S-Bus: Automatic Read-Set Reconstruction for Multi-Agent LLM State
> Coordination.* Sajjad Khan, 2026. _arXiv ID forthcoming._

S-Bus is an HTTP middleware that applies optimistic concurrency control
to multi-agent LLM state. The server-side **DeliveryLog** records
every HTTP `GET` request per agent session, automatically reconstructing
each agent's read-set at commit time and rejecting commits whose
cross-shard reads have been superseded — without any agent SDK changes
under HTTP/1.1.

**Companion repositories:**

- [`sbus-experiments`](https://github.com/sajjadanwar0/sbus-experiments) —
  Python experimental harness producing every empirical measurement in
  the paper.
- [`sbus-formals`](https://github.com/sajjadanwar0/sbus-formals) —
  TLA+, TLAPS, and Dafny mechanised proofs.

---

## Workspace structure

The repository is a Cargo workspace with three crates:

| Crate | Role | Default port |
|---|---|---|
| `sbus-server` | The S-Bus middleware itself: ACP, DeliveryLog, registry, optional Raft | 7000 |
| `sbus-baselines` | PostgreSQL and Redis HTTP adapters exposing the same `/shard` + `/commit` API for safety-parity comparisons | 7001 (PG), 7002 (Redis) |
| `sbus-proxy` | Transparent LLM-API proxy that intercepts completions and populates the DeliveryLog with shard references appearing in agent context (used in Exp. PROXY-PH2) | 9000 |

```
sbus/
├── Cargo.toml                   workspace manifest
├── Cargo.lock
├── README.md                    this file
├── LICENSE                      MIT
│
├── sbus-server/                 the measured system (~2,953 LOC)
│   ├── src/
│   │   ├── main.rs              Axum server entry
│   │   ├── api/                 HTTP route handlers (cluster + Raft + main)
│   │   ├── bus/                 ACP, DeliveryLog, registry, WAL
│   │   ├── cluster/             cluster coordination
│   │   ├── raft/                openraft integration (P1 session replication)
│   │   └── metrics/             Prometheus exposition + ORI counters
│   ├── benches/                 Criterion micro-benchmarks
│   └── Cargo.toml
│
├── sbus-baselines/              PG and Redis HTTP adapters (~1,247 LOC)
│   ├── src/
│   │   └── bin/
│   │       ├── pg_adapter.rs    PostgreSQL SERIALIZABLE adapter
│   │       └── redis_adapter.rs Redis WATCH/MULTI adapter
│   └── Cargo.toml
│
└── sbus-proxy/                  transparent LLM-API proxy (~2,194 LOC)
    ├── src/
    │   ├── main.rs              hyper-based proxy server entry
    │   ├── lib.rs
    │   ├── proxy.rs             multi-vendor request routing
    │   ├── extractor.rs         response-content shard-keyword extraction
    │   ├── delivery_log.rs      upstream DeliveryLog forwarding
    │   ├── vocabulary.rs        shard-vocabulary loading + matching
    │   └── config.rs
    ├── tests/integration.rs     end-to-end proxy tests
    └── Cargo.toml
```

---

## Quickstart

Requires Rust 1.75+ (2024 edition).

```bash
git clone https://github.com/sajjadanwar0/sbus
cd sbus

# Build the whole workspace
cargo build --release

# Run the server (Terminal 1)
SBUS_ADMIN_ENABLED=1 cargo run --release -p sbus-server
# → http://localhost:7000

# Sanity check (Terminal 2)
curl -X POST http://localhost:7000/shard \
    -H 'Content-Type: application/json' \
    -d '{"key":"models_state","content":"v1"}'

curl http://localhost:7000/shard/models_state

curl -X POST http://localhost:7000/commit \
    -H 'Content-Type: application/json' \
    -d '{"key":"models_state","expected_version":1,"delta":"v2","agent_id":"a1"}'
```

A 200 response on the third call means the ACP is working. For the
end-to-end validation harness, see
[`sbus-experiments`](https://github.com/sajjadanwar0/sbus-experiments).

---

## sbus-server

The HTTP middleware measured in the paper. ~950 lines of safe Rust for
the single-node ACP core; ~1,679 lines including Raft coordination and
sled persistence. Zero `unsafe` blocks.

### Key endpoints

| Method | Path | Description |
|---|---|---|
| `POST` | `/shard` | Create a new shard with initial content |
| `GET` | `/shard/:key` | Read current shard content; logged in DeliveryLog |
| `GET` | `/shards` | List all shard keys |
| `POST` | `/commit` | Atomic Commit Protocol — single-shard |
| `POST` | `/commit/v2` | ACP with explicit cross-shard `read_set` (ARSI mode) |
| `POST` | `/rollback` | Roll back a shard to a prior version |
| `GET` | `/stats` | Server statistics + ORI counters (`view_checked_commits`, `view_divergent_commits`) |
| `GET` | `/metrics` | Prometheus exposition |
| `POST` | `/admin/config` | Runtime ORI on/off toggle (gated by `SBUS_ADMIN_ENABLED=1`) |
| `POST` | `/admin/reset` | Wipe registry (used by experiment harness) |

### Configuration

```bash
SBUS_ADMIN_ENABLED=1            # enable /admin/* endpoints (default: off)
SBUS_LISTEN=0.0.0.0:7000        # bind address (default: 127.0.0.1:7000)
SBUS_WAL_PATH=/var/sbus/wal     # write-ahead log path (default: ./sbus.wal)
SBUS_RETRY_BUDGET=5             # ACP retry budget per agent step
SBUS_LEASE_TTL_SECS=30          # ownership-token timeout
```

### Distributed deployment (3-node Raft)

The `--features raft` flag enables openraft 0.8.4 with sled-backed
persistent storage. P1 session replication (Limitation 11 in the paper)
is on by default when Raft is enabled.

```bash
cargo run --release -p sbus-server --features raft -- \
    --node-id 0 --peers 127.0.0.1:7100,127.0.0.1:7101,127.0.0.1:7102
```

Election timeout is randomised in `[500, 1000]` ms; heartbeat interval
is 250 ms. See the paper's §IX for the full distributed-deployment
discussion and Exp. DR-9 for empirical validation.

### Benchmarks

```bash
cargo bench -p sbus-server
```

Criterion micro-benchmarks for the ACP commit path. The paper reports
ACP overhead of approximately 5 µs per commit at N=4 (≈ 0.06% of a
typical LLM inference call); your numbers will vary by hardware.

---

## sbus-baselines

PostgreSQL and Redis HTTP adapters that expose the same `/shard` and
`/commit` API as `sbus-server`, used in the paper's Exp. PG-Comparison
and Exp. PG-Contention sweeps. Both adapters achieve safety parity with
S-Bus on the contention workload (zero Type-I corruptions across
427,308 active conflicts; see paper Table XIV).

### PostgreSQL adapter

Requires PostgreSQL 14+ with the `sbus_baseline` database created.
Each agent session maps to a database connection with
`default_transaction_isolation = serializable` pinned at connection
time.

```bash
PG_DSN="host=localhost dbname=sbus_baseline user=sbus_user password=..." \
    cargo run --release -p sbus-baselines --bin pg-adapter
# → http://localhost:7001
```

### Redis adapter

Requires Redis 7+. Cross-shard read-set validation is implemented by
`WATCH`ing every key in the read-set before the `MULTI ... EXEC` block;
if any version differs inside the transaction, `EXEC` returns null and
the adapter retries.

```bash
REDIS_URL=redis://127.0.0.1:6379 \
    cargo run --release -p sbus-baselines --bin redis-adapter
# → http://localhost:7002
```

---

## sbus-proxy

Transparent LLM-API proxy used in the paper's Exp. PROXY-PH2 to
decompose Rhidden coverage into HTTP-this-step, DL-accumulation, and
proxy-marginal components. The proxy intercepts `/v1/chat/completions`
(OpenAI), `/v1/messages` (Anthropic), and `/v1beta/models/...`
(Google), scans response content for shard-vocabulary keywords, and
populates the upstream `sbus-server`'s DeliveryLog with detected
references at the agent's read-time version (skip-if-exists semantics).

```bash
SBUS_PROXY_VOCAB="models_state,query_compiler,test_fixture,review_notes" \
SBUS_PROXY_UPSTREAM=http://localhost:7000 \
    cargo run --release -p sbus-proxy
# → http://localhost:9000
```

Then point your agent's API client at `http://localhost:9000` instead
of the vendor's API endpoint. The proxy is stateless apart from the
DeliveryLog forwarding; vendor responses are passed through unmodified.

The paper reports the proxy's contribution as
**+0.0018 paired marginal at V=4** (Table XV), with monotonically
negative throughput contribution at larger vocabulary sizes.
Keyword-scan is safety-preserving but coverage-marginal at the proxy
layer; semantic-extraction-at-proxy is open work (Limitation 16).

---

## Reproducibility

The server's structural counters (`view_checked_commits`,
`view_divergent_commits`, `commit_attempts`, `commit_conflicts`) are
deterministic given the ACP retry logic and version checks. They do
not depend on LLM stochasticity. The paper's safety claims rest on
these counters.

The end-to-end experiments live in
[`sbus-experiments`](https://github.com/sajjadanwar0/sbus-experiments)
and require this repository's binaries running. See that repo's
`README.md` for the full reproduction recipe.

---

## What this repo establishes (and does not)

**Establishes (by the artifacts here):**

- A working Rust implementation of the S-Bus ACP, DeliveryLog, and
  cross-shard validation logic.
- HTTP adapters for PostgreSQL SERIALIZABLE and Redis WATCH/MULTI
  exposing the same API for safety-parity comparisons.
- A transparent LLM-API proxy for Rhidden coverage measurement.
- Compile-time absence of `unsafe` blocks across the workspace.

**Does not establish:**

- **Formal correspondence between this code and the paper's TLA+/Dafny
  specifications.** Refinement to the implementation is empirical
  (884K-attempt zero-corruption evidence in the paper), not mechanised.
  Standard practice short of IronFleet. See
  [`sbus-formals`](https://github.com/sajjadanwar0/sbus-formals) for
  the abstract-algorithm proofs.
- **Production hardening.** This is a research artifact. There is no
  HMAC request signing (Limitation 5 in the paper, future work),
  no TLS termination (use a reverse proxy), no multi-tenant isolation,
  and no rate limiting beyond what `axum` provides by default.

---

## Citation

```bibtex
@techreport{khan2026sbus,
  author      = {Khan, Sajjad},
  title       = {S-Bus: Automatic Read-Set Reconstruction for Multi-Agent
                 LLM State Coordination},
  institution = {Independent},
  year        = {2026},
  note        = {waiting for arXiv endorsment...}
}
```

---

## License

MIT. See `LICENSE` at the repo root.