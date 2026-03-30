# S-Bus — Semantic Bus

> **Transactional middleware for multi-agent LLM systems**

**GitHub:** https://github.com/sajjadanwar0/sbus  
**Experiment harness:** https://github.com/sajjadanwar0/agenticPaper  
**Paper:** IEEE TPDS submission 2026

S-Bus applies Multi-Version Concurrency Control (MVCC) principles to
natural language state in multi-agent AI systems. It formally eliminates
**Semantic Race Conditions** — the failure mode where concurrent agent
mutations render global state unreachable from the task objective.

---

## Key results

| System    | CWR (N=4) | CWR (N=8) | Reduction vs S-Bus | S@50 (N=8) |
|-----------|-----------|-----------|--------------------|------------|
| **S-Bus** | **0.238** | **0.210** | —                  | **80%**    |
| LangGraph | 4.384     | 4.213     | 94.8%              | 40%        |
| CrewAI    | 7.099     | 8.168     | 97.1%              | 20%        |
| AutoGen   | 11.970    | 12.070    | 98.1%              | 44%        |

Mann-Whitney U=0, p<0.0001, r=1.000 — complete separation in all comparisons.
Zero state corruptions across 272 ACP commit attempts.

---

## Quick start

```bash
git clone https://github.com/sajjadanwar0/sbus
cd sbus
cargo run
# Server starts on http://localhost:3000
```

### API

| Method | Endpoint         | Description                    |
|--------|------------------|--------------------------------|
| POST   | `/shard`         | Create a new state shard       |
| GET    | `/shard/{key}`   | Read current shard content     |
| GET    | `/shards`        | List all shard keys            |
| POST   | `/commit`        | Atomic Commit Protocol (ACP)   |
| POST   | `/rollback`      | Roll back to prior version     |
| GET    | `/stats`         | Server statistics              |
| GET    | `/metrics`       | Prometheus metrics             |

---

## Performance

| Benchmark                       | Throughput  | p50     | p99     |
|--------------------------------|-------------|---------|---------|
| Sequential commit               | >800K ops/s | 1.18 μs | 2.31 μs |
| Parallel (4 agents, 4 shards)   | >3M ops/s   | 0.32 μs | 0.89 μs |
| Parallel (16 agents, 16 shards) | >9M ops/s   | 0.11 μs | 0.43 μs |
| High contention (16→1 shard)    | >300K ops/s | 3.21 μs | 8.44 μs |

LLM inference (GPT-4o-mini) takes 500–2000 ms — the ACP is never the bottleneck.

---

## Project structure

```
sbus/
├── src/
│   ├── main.rs              # Axum server + routes
│   ├── api/handlers.rs      # 10 REST endpoints
│   ├── bus/engine.rs        # ACP + DashMap shard registry
│   ├── bus/types.rs         # Shard, Delta, SyncError
│   └── metrics/collector.rs # CWR / SCR metrics
├── benches/throughput.rs    # Criterion benchmarks
├── Cargo.toml
└── README.md
```

---

## Citation

```bibtex
@article{khan2026sbus,
  title   = {Reliable Autonomous Orchestration: A Rust-Based Transactional
             Middleware for Mitigating Semantic Synchronization Overhead
             in Multi-Agent Systems},
  author  = {Khan, Sajjad},
  journal = {IEEE Transactions on Parallel and Distributed Systems},
  year    = {2026},
  note    = {Under review}
}
```

---

## License

MIT

## Author

**Sajjad Khan**   
GitHub: [@sajjadanwar0](https://github.com/sajjadanwar0)