# S-Bus

**A Rust workspace implementing automatic read-set reconstruction for
multi-agent LLM state coordination.**

This monorepo contains the Rust components of the S-Bus system. It is
organised as a Cargo workspace with three member crates:

- [`sbus-server/`](sbus-server/) — the S-Bus middleware itself: HTTP
  server, ACP commit engine, DeliveryLog, openraft-replicated cluster.
- [`sbus-baselines/`](sbus-baselines/) — Rust-native PostgreSQL
  (`SET TRANSACTION ISOLATION LEVEL SERIALIZABLE`) and Redis
  (`WATCH/MULTI/EXEC`) adapters. Used for the matched-mechanism CC
  baselines in the paper's PG-Comparison and PG-Contention experiments.
- [`sbus-proxy/`](sbus-proxy/) — transparent LLM-API proxy. Path-routes
  requests to OpenAI / Anthropic / Google upstreams, scans
  responses for shard references, and registers them with S-Bus's
  DeliveryLog via a skip-if-exists protocol. Used in PROXY-PH2.

Companion repositories:

- [`sbus-experiments`](https://github.com/sajjadanwar0/sbus-experiments) —
  Python experiment harness (PH-2, PH-3, ORI-Isolation, PG-Comparison,
  PROXY-PH2, Workload-B, DR-9, and others)
- [`sbus-formals`](https://github.com/sajjadanwar0/sbus-formals) —
  TLA+, TLAPS, and Dafny mechanised verification (687 TLAPS obligations,
  19 Dafny lemmas, 211M TLC states)

---

## The paper

**S-Bus: Automatic Read-Set Reconstruction for Multi-Agent LLM State
Coordination.** Sajjad Khan, 2026. [arXiv link — TBA]

Concurrent LLM agents sharing mutable natural-language state produce
**Structural Race Conditions**: write–write and cross-shard stale-read
conflicts that silently corrupt agent output. Existing multi-agent
frameworks (LangGraph, CrewAI, AutoGen) provide no write-ownership
semantics over shared state.

S-Bus's central technical mechanism is the **DeliveryLog**: a server-side
per-agent log of HTTP `GET` operations that automatically reconstructs
each agent's read-set at commit time without agent SDK changes (under
HTTP/1.1). The DeliveryLog turns ordinary HTTP traffic into a verifiable
read-set, enabling optimistic concurrency control over multi-agent
shared state.

The consistency property the DeliveryLog provides is **Observable-Read
Isolation (ORI)**: a partial causal consistency over the HTTP-observable
projection of an agent's read set.

S-Bus targets the **dedicated-shard topology** (each agent owns a
distinct write key, reads from shared reference shards) — see Box 2 in
the paper for deployment guidance. Single-shard collaborative writing
is out of scope.

---

## Headline empirical results

| Finding | Value | Source |
|---|---|---|
| Type-I corruptions under active contention | **0 / 427,308** | PG-Contention, three backends |
| Type-I corruptions, full sweep | 0 / 884,110 | PG-Comparison combined |
| Workload-B view-divergence (ORI-OFF) | 590 / 639 commits | server-side counters |
| Workload-B view-divergence (ORI-ON) | **0 / 638 commits** | server-side counters |
| TLAPS obligations proved | 687 / 687 (1 axiom) | `sbus-formals` |
| TLC distinct states (single node, N=4) | 211,696,712 | `sbus-formals` |
| Dafny inductive lemmas verified | 19 / 19 | `sbus-formals` |

---

## Repository layout

```
.
├── Cargo.toml                  # workspace root
├── README.md                   # this file
│
├── sbus-server/                # the measured system
│   ├── Cargo.toml
│   ├── README.md
│   └── src/
│       ├── main.rs             # entry point, HTTP router, Raft bootstrap
│       ├── api/                # HTTP handlers
│       │   ├── handlers.rs       (~636 lines: GET/shard, POST/commit, /admin/*)
│       │   ├── cluster_handlers.rs
│       │   └── raft_handlers.rs
│       ├── bus/                # core ACP
│       │   ├── engine.rs         (~572 lines: SBus, commit_delta, WAL)
│       │   ├── registry.rs       (ShardRegistry + DeliveryLog)
│       │   └── types.rs
│       ├── cluster/mod.rs
│       ├── raft/                 # openraft 0.8.4 + sled persistence
│       └── metrics/collector.rs  # Prometheus-style metrics
│
├── sbus-baselines/             # PG-SER and REDIS-WATCH adapters
│   ├── Cargo.toml
│   └── src/bin/
│       ├── pg_adapter.rs         (~651 lines: SET TRANSACTION ISOLATION LEVEL SERIALIZABLE)
│       └── redis_adapter.rs      (~596 lines: WATCH/MULTI/EXEC)
│
└── sbus-proxy/                 # transparent LLM-API proxy
    ├── Cargo.toml
    ├── src/
    │   ├── main.rs
    │   ├── lib.rs
    │   ├── config.rs             # 12-factor env-driven config (3 upstreams)
    │   ├── proxy.rs              (~453 lines: request routing, SSE streaming)
    │   ├── extractor.rs          (~1030 lines: keyword-scan reference detection)
    │   ├── vocabulary.rs         # domain vocabulary
    │   └── delivery_log.rs       # client for POST /delivery_log/register
    └── tests/integration.rs      # against mock servers
```

Total Rust: ~6,300 lines across three crates, zero `unsafe`.

---

## Building

The workspace builds all three crates with one command:

```bash
cargo build --release
```

Or build individual crates:

```bash
cargo build --release -p sbus-server
cargo build --release -p sbus-baselines    # produces pg-adapter, redis-adapter
cargo build --release -p sbus-proxy
```

Requires Rust edition 2024 (stable 1.85+).

---

## Running

### Single-node S-Bus (most experiments)

```bash
SBUS_ADMIN_ENABLED=1 ./target/release/sbus-server
# → http://localhost:7000
```

Admin endpoints (e.g. `/admin/reset`, `/admin/config`,
`/admin/delivery-log`) require `SBUS_ADMIN_ENABLED=1`.

### LLM-API proxy (PROXY-PH2 experiments)

```bash
SBUS_PROXY_VOCAB="models_state,query_compiler,test_fixture,review_notes" \
OPENAI_API_KEY=sk-... \
ANTHROPIC_API_KEY=sk-ant-... \
GOOGLE_API_KEY=... \
  ./target/release/sbus-proxy
# → http://localhost:9000
```

The proxy path-routes to:

- `/v1/chat/completions` → OpenAI (`api.openai.com`)
- `/v1/messages` → Anthropic (`api.anthropic.com`)
- `/v1beta/models/.../generateContent` → Google (`generativelanguage.googleapis.com`)

Override upstreams with `SBUS_PROXY_UPSTREAM_URL`,
`SBUS_PROXY_UPSTREAM_URL_ANTHROPIC`, `SBUS_PROXY_UPSTREAM_URL_GOOGLE`.

### CC baselines (PG-Comparison, PG-Contention)

```bash
# Postgres SERIALIZABLE adapter
PG_DSN="host=localhost dbname=sbus user=sbus_user password=..." \
  ./target/release/pg-adapter
# → http://localhost:7001

# Redis WATCH/MULTI adapter
REDIS_URL=redis://127.0.0.1:6379 \
  ./target/release/redis-adapter
# → http://localhost:7002
```

Both adapters expose the same JSON API as `sbus-server`, so the
experiment harness in `sbus-experiments` can target any of the three
backends interchangeably.

### 3-node Raft cluster

```bash
cd sbus-server
./start_raft_cluster.sh    # starts nodes on 7001 / 7002 / 7003
./stop_raft_cluster.sh     # tear down
```

---

## Workload-B configuration

The `sbus-server` binary supports runtime ORI toggling via
`POST /admin/config`. This is what powers the paper's Workload-B
experiment (cross-shard view-divergence under ORI-ON vs. ORI-OFF):

```bash
# Disable ORI (ori_off baseline)
curl -X POST http://localhost:7000/admin/config \
  -H 'content-type: application/json' \
  -d '{"ori_enabled": false}'

# Read current state and view-divergence counters
curl http://localhost:7000/admin/config
# → {"ori_enabled": false,
#    "view_checked_commits": 638,
#    "view_divergent_commits": 590}

# Re-enable ORI
curl -X POST http://localhost:7000/admin/config \
  -H 'content-type: application/json' \
  -d '{"ori_enabled": true}'
```

Under ORI-OFF, the cross-shard view-divergence counter still increments
on stale sibling reads, but the commit succeeds (the failure mode ORI is
designed to prevent). Under ORI-ON, divergent commits are rejected with
HTTP 409 `CROSSSHARDSTALE`.

This is the mechanism behind the paper's Table X: Workload-B records
**0 / 638 divergent commits under ORI-ON** vs. **590 / 639 under
ORI-OFF** (χ² = 1094.98, p < 10⁻²⁴⁰) across 8 architectural domains.

---

## Reproducing paper results

The Python harness lives in
[`sbus-experiments`](https://github.com/sajjadanwar0/sbus-experiments).
Each script is standalone and supports `--help`.

Quickest sanity check (≈2 min, no API keys needed):

```bash
# Terminal 1 — start S-Bus
cargo run --release -p sbus-server

# Terminal 2 — verify cross-shard staleness rejection
cd ../sbus-experiments
python3 cross_shard_validation.py --n-trials 50
```

Standard reproduction (≈15 min, ≈$2 API spend):

```bash
# SCR dose-response (Table XVIII, exact analytic match)
python3 exp_sjv5_parallel.py --tasks-limit 1
```

Full reproductions (PG-Comparison, PROXY-PH2 cross-backbone, etc.) are
documented in the experiments-repo README.

---

## Configuration reference

### `sbus-server`

| Variable | Default | Purpose |
|---|---|---|
| `SBUS_PORT` | `7000` | HTTP listen port |
| `SBUS_ADMIN_ENABLED` | unset | Set to `1` to enable `/admin/*` |
| `SBUS_RAFT_NODE_ID` | unset (single-node) | Raft node ID for cluster mode |
| `SBUS_RAFT_PEERS` | unset | `id1=url1,id2=url2,...` peer list |
| `SBUS_DATA_DIR` | `./data/node{ID}` | sled storage directory |
| `SBUS_WAL_PATH` | unset | Optional plaintext WAL path |
| `SBUS_SESSION_TTL` | `3600` | DeliveryLog session TTL (seconds) |
| `SBUS_LOG` | unset | Set to `1` to disable DeliveryLog (ablation) |
| `RUST_LOG` | `info,sbus=debug` | tracing log level |

### `sbus-proxy`

| Variable | Default | Purpose |
|---|---|---|
| `SBUS_PROXY_PORT` | `9000` | HTTP listen port |
| `SBUS_URL` | `http://localhost:7000` | Where to register DeliveryLog entries |
| `SBUS_PROXY_VOCAB` | required | Comma-separated shard names |
| `SBUS_PROXY_UPSTREAM_URL` | `https://api.openai.com` | OpenAI-format upstream |
| `SBUS_PROXY_UPSTREAM_URL_ANTHROPIC` | `https://api.anthropic.com` | Anthropic |
| `SBUS_PROXY_UPSTREAM_URL_GOOGLE` | `https://generativelanguage.googleapis.com` | Google Gemini |
| `SBUS_PROXY_AGENT_HEADER` | `X-SBus-Agent-Id` | Header carrying agent_id |
| `SBUS_PROXY_DEBUG_PASSTHROUGH` | `false` | Bypass extraction (for benchmarking overhead) |

### `sbus-baselines/pg-adapter`

| Variable | Required | Purpose |
|---|---|---|
| `PG_DSN` | yes | Postgres connection string |
| `PG_ADAPTER_PORT` | no (default `7001`) | HTTP listen port |

### `sbus-baselines/redis-adapter`

| Variable | Default | Purpose |
|---|---|---|
| `REDIS_URL` | `redis://127.0.0.1:6379` | Redis connection |
| `REDIS_ADAPTER_PORT` | `7002` | HTTP listen port |

---

## Scope and honest limitations

From the paper §VIII:

1. **Topology restriction.** ORI is semantically neutral in
   dedicated-shard topology, harmful in single-shard topology
   (Exp. SHARED-STATE). Box 2 provides deployment guidance; there is
   no automatic topology detection.
2. **HTTP/2 breakage.** DeliveryLog completeness relies on
   FIFO-per-TCP-connection ordering. HTTP/2 multiplexing can violate
   this. Mitigation: pin to HTTP/1.1 via reverse proxy, or use ARSI
   mode.
3. **`R_hidden` coverage.** Structural guarantees apply to the
   HTTP-observable projection of the read set (~26.1% of single-step
   references on the principal workload). Session-scoped DeliveryLog
   accumulation extends observable coverage of self-reported
   references to ~99.8% over a full session, but the denominator is
   itself contaminated by self-report over-claim of 29–37% (paper
   §VII-I); deflated upper bound on genuine-causal-read coverage is
   ≤ 70%.
4. **Concurrent-failover window.** Exp. DR-9 validates P1 session
   replication under sequential GET-then-kill (30/30 ORI invariants
   held). Leader failure within the ~5ms fire-and-forget replication
   window leaves an unreplicated DeliveryLog entry — equivalent to
   pre-P1 behaviour for that one agent session.
5. **No refinement proof.** Dafny verifies the abstract algorithm;
   the structural correspondence to the Rust implementation is by
   eye. Full Rust refinement via Verus or Creusot is future work.
6. **One retained TLAPS axiom.** `FunTypingReconstruction`, a
   primitive fact about TLA+ function-space typing not present in
   the standard library. See `sbus-formals/historical/` for ongoing
   work to discharge it.

The paper's §VIII discusses each of these in detail, and §VIII-A
treats them as threats to validity (internal, external, construct,
statistical) with mitigations cross-referenced.

---

## Citation

```bibtex
@techreport{khan2026sbus,
  author      = {Khan, Sajjad},
  title       = {S-Bus: Automatic Read-Set Reconstruction for Multi-Agent
                 LLM State Coordination},
  institution = {Independent},
  year        = {2026},
  note        = {arXiv preprint},
  url         = {https://arxiv.org/abs/...}
}
```

---

## License

MIT. See `LICENSE` files in each crate.

