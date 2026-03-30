// benches/throughput.rs
// Criterion benchmarks measuring:
//   1. Commit throughput (commits/sec) at 1, 4, 8, 16 concurrent agents
//   2. Read latency (µs) — non-blocking path
//   3. Conflict rate under high contention (all agents targeting same shard)
//   4. Rollback latency at various delta log depths

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use std::sync::Arc;
use tokio::runtime::Runtime;

// We need to reach into the library. In a real crate this would be `use sbus::...`
// For now we inline the minimal types needed.
use std::collections::VecDeque;
use dashmap::DashMap;
use std::sync::atomic::{AtomicU64, Ordering};

// ── minimal inline types so bench compiles standalone ─────────────────────────

#[derive(Clone, Debug)]
struct Shard {
    version:       u64,
    content:       String,
    owner:         Option<String>,
    delta_log:     VecDeque<String>,
    conflict_count: u64,
    attempt_count:  u64,
}

impl Shard {
    fn new(content: String) -> Self {
        Self {
            version: 0,
            content,
            owner: None,
            delta_log: VecDeque::new(),
            conflict_count: 0,
            attempt_count: 0,
        }
    }
}

#[derive(Clone)]
struct Bus {
    registry:         Arc<DashMap<String, Shard>>,
    commit_counter:   Arc<AtomicU64>,
    conflict_counter: Arc<AtomicU64>,
}

impl Bus {
    fn new() -> Self {
        Self {
            registry:         Arc::new(DashMap::new()),
            commit_counter:   Arc::new(AtomicU64::new(0)),
            conflict_counter: Arc::new(AtomicU64::new(0)),
        }
    }

    fn create(&self, key: &str, content: &str) {
        self.registry.insert(key.to_owned(), Shard::new(content.to_owned()));
    }

    // Returns Ok(new_ver) or Err("version"|"conflict")
    fn commit(&self, key: &str, expected: u64, new_content: &str, _agent: &str)
        -> Result<u64, &'static str>
    {
        let mut e = self.registry.get_mut(key).ok_or("not_found")?;
        let s = e.value_mut();
        s.attempt_count += 1;

        if s.version != expected {
            s.conflict_count += 1;
            self.conflict_counter.fetch_add(1, Ordering::Relaxed);
            return Err("version");
        }
        if s.owner.is_some() {
            s.conflict_count += 1;
            self.conflict_counter.fetch_add(1, Ordering::Relaxed);
            return Err("conflict");
        }
        s.owner = Some("agent".into());
        s.content = new_content.to_owned();
        s.version += 1;
        let v = s.version;
        s.delta_log.push_back(new_content.to_owned());
        if s.delta_log.len() > 1000 { s.delta_log.pop_front(); }
        s.owner = None;
        self.commit_counter.fetch_add(1, Ordering::Relaxed);
        Ok(v)
    }

    fn read(&self, key: &str) -> Option<(u64, String)> {
        self.registry.get(key).map(|s| (s.version, s.content.clone()))
    }

    fn scr(&self) -> f64 {
        let commits   = self.commit_counter.load(Ordering::Relaxed);
        let conflicts = self.conflict_counter.load(Ordering::Relaxed);
        let total = commits + conflicts;
        if total == 0 { 0.0 } else { conflicts as f64 / total as f64 }
    }
}

// ── bench 1: sequential commit throughput ────────────────────────────────────

fn bench_sequential_commits(c: &mut Criterion) {
    let bus = Bus::new();
    bus.create("shard-0", "initial");

    let mut ver = 0u64;
    c.bench_function("sequential_commit", |b| {
        b.iter(|| {
            match bus.commit("shard-0", ver, "updated content", "agent-0") {
                Ok(v) => ver = v,
                Err(_) => {} // retry on conflict; shouldn't happen sequentially
            }
        })
    });
}

// ── bench 2: concurrent commits — N agents, N distinct shards ────────────────

fn bench_parallel_distinct(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("parallel_distinct_shards");

    for agents in [2usize, 4, 8, 16] {
        group.throughput(Throughput::Elements(agents as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(agents),
            &agents,
            |b, &n| {
                b.to_async(&rt).iter(|| async move {
                    let bus = Arc::new(Bus::new());
                    for i in 0..n {
                        bus.create(&format!("shard-{i}"), "init");
                    }
                    let handles: Vec<_> = (0..n).map(|i| {
                        let bus = bus.clone();
                        tokio::spawn(async move {
                            let key = format!("shard-{i}");
                            let (ver, _) = bus.read(&key).unwrap();
                            let _ = bus.commit(&key, ver, "new", &format!("agent-{i}"));
                        })
                    }).collect();
                    for h in handles { let _ = h.await; }
                });
            },
        );
    }
    group.finish();
}

// ── bench 3: high contention — N agents, 1 shared shard ──────────────────────

fn bench_contention(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("high_contention");

    for agents in [2usize, 4, 8, 16] {
        group.bench_with_input(
            BenchmarkId::from_parameter(agents),
            &agents,
            |b, &n| {
                b.to_async(&rt).iter(|| async move {
                    let bus = Arc::new(Bus::new());
                    bus.create("shared", "init");

                    let handles: Vec<_> = (0..n).map(|i| {
                        let bus = bus.clone();
                        tokio::spawn(async move {
                            // Each agent reads, then tries to commit
                            let (ver, _) = bus.read("shared").unwrap_or((0, "".into()));
                            let _ = bus.commit("shared", ver, "new", &format!("agent-{i}"));
                        })
                    }).collect();
                    for h in handles { let _ = h.await; }

                    bus.scr() // return SCR so compiler doesn't optimise away
                });
            },
        );
    }
    group.finish();
}

// ── bench 4: read latency ─────────────────────────────────────────────────────

fn bench_read_latency(c: &mut Criterion) {
    let bus = Bus::new();
    let content = "A".repeat(1024); // 1KB shard
    bus.create("read-target", &content);

    c.bench_function("read_shard_1kb", |b| {
        b.iter(|| {
            let _ = bus.read("read-target");
        })
    });
}

// ── bench 5: rollback at depth 100 / 500 / 1000 ──────────────────────────────

fn bench_rollback(c: &mut Criterion) {
    let mut group = c.benchmark_group("rollback_depth");

    for depth in [100usize, 500, 1000] {
        group.bench_with_input(
            BenchmarkId::from_parameter(depth),
            &depth,
            |b, &d| {
                b.iter_batched(
                    || {
                        // Setup: create a shard with `d` commits
                        let bus = Bus::new();
                        bus.create("rollback-target", "v0");
                        for i in 1..=d {
                            let (ver, _) = bus.read("rollback-target").unwrap();
                            let _ = bus.commit(
                                "rollback-target", ver,
                                &format!("v{i}"), "agent"
                            );
                        }
                        bus
                    },
                    |bus| {
                        // Rollback to v0 — requires traversing the full log
                        if let Some(mut e) = bus.registry.get_mut("rollback-target") {
                            e.delta_log.retain(|_| false);
                            e.version = 0;
                        }
                    },
                    criterion::BatchSize::SmallInput,
                );
            },
        );
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_sequential_commits,
    bench_parallel_distinct,
    bench_contention,
    bench_read_latency,
    bench_rollback,
);
criterion_main!(benches);
