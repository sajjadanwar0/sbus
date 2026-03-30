# S-Bus: Semantic Bus for Multi-Agent LLM Systems

> Companion code for the IEEE TPDS paper  
> *"Reliable Autonomous Orchestration: A Rust-Based Transactional Middleware  
>  for Mitigating Semantic Synchronization Overhead in Multi-Agent Systems"*

## Repository Structure

```
sbus/
├── src/
│   ├── main.rs              # Axum HTTP server entry point
│   ├── bus/
│   │   ├── engine.rs        # S-Bus engine + Atomic Commit Protocol
│   │   └── types.rs         # Shard, Delta, SyncError types
│   ├── api/
│   │   └── handlers.rs      # REST API handlers
│   └── metrics/
│       └── collector.rs     # CWR / S@50 / SCR metric collection
├── benches/
│   └── throughput.rs        # Criterion microbenchmarks
├── datasets/
│   └── long_horizon_tasks.json  # LHP benchmark (15 tasks)
├── harness/
│   ├── run_experiment.py    # Python experiment harness
│   └── analyse.R            # Statistical analysis + figures
└── paper/
    └── paper.tex            # Full IEEE TPDS LaTeX paper
```

## Quick Start

### 1. Start the S-Bus server

```bash
cargo run --release
# S-Bus listening on http://localhost:3000
```

### 2. Run a quick smoke test

```bash
# Create a shard
curl -X POST http://localhost:3000/shard \
  -H 'Content-Type: application/json' \
  -d '{"key":"goal_1","content":"initial state","goal_tag":"test"}'

# Read it
curl http://localhost:3000/shard/goal_1

# Commit a delta
curl -X POST http://localhost:3000/commit \
  -H 'Content-Type: application/json' \
  -d '{"key":"goal_1","expected_ver":0,"content":"updated","rationale":"step 1","agent_id":"agent-0"}'

# Bus statistics
curl http://localhost:3000/stats
```

### 3. Run microbenchmarks

```bash
cargo bench
# Results in target/criterion/
```

### 4. Run experiments (requires OpenAI API key)

```bash
pip install openai httpx tiktoken pandas scipy
export OPENAI_API_KEY=sk-...

# Run S-Bus system on all tasks with 4 agents
python harness/run_experiment.py --system sbus --agents 4

# Run all systems (takes several hours + API credits)
python harness/run_experiment.py --all --agents 2 4 8 16 --analyse
```

### 5. Generate paper figures

```bash
Rscript harness/analyse.R results/results.csv figures/
# Produces fig1–fig4 + table1_main.tex
```

## API Reference

| Method | Path | Description |
|--------|------|-------------|
| POST | `/shard` | Create shard |
| GET  | `/shard/:key` | Non-blocking read |
| POST | `/commit` | Atomic Commit Protocol |
| POST | `/rollback` | Version rollback |
| GET  | `/stats` | Bus statistics (JSON) |
| GET  | `/metrics` | Prometheus exposition |
| GET  | `/results/csv` | Export run metrics |
| GET  | `/results/agg` | Aggregated stats (JSON) |

## Key Metrics

- **CWR** (Coordination-to-Work Ratio): `coord_tokens / work_tokens` — lower is better  
- **S@50** (Success at 50 steps): fraction of tasks solved within 50 agent steps  
- **SCR** (Semantic Conflict Rate): `rejected_commits / total_attempts` — lower is better  

## License

MIT
# sbus
