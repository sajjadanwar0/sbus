// src/metrics/collector.rs
// Metric collection for CWR, S@50, and SCR.
// These are the three primary metrics defined in the paper.

use serde::{Deserialize, Serialize};
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};
use parking_lot::Mutex;
use chrono::{DateTime, Utc};

/// Per-run experiment metrics
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct RunMetrics {
    pub run_id:           String,
    pub system:           String,   // "sbus" | "crewai" | "autogen" | "langgraph"
    pub agent_count:      usize,
    pub task_id:          String,

    // CWR numerator / denominator
    pub coord_tokens:     u64,      // tokens spent on coordination
    pub work_tokens:      u64,      // tokens spent on actual task work

    // S@50
    pub steps_taken:      u64,
    pub success:          bool,     // solved within step budget?

    // SCR
    pub commit_attempts:  u64,
    pub commit_conflicts: u64,

    // Timing
    pub wall_ms:          u64,
    pub started_at:       DateTime<Utc>,
    pub finished_at:      Option<DateTime<Utc>>,
}

impl RunMetrics {
    /// Coordination-to-Work Ratio
    pub fn cwr(&self) -> f64 {
        if self.work_tokens == 0 { return f64::INFINITY; }
        self.coord_tokens as f64 / self.work_tokens as f64
    }

    /// Semantic Conflict Rate
    pub fn scr(&self) -> f64 {
        if self.commit_attempts == 0 { return 0.0; }
        self.commit_conflicts as f64 / self.commit_attempts as f64
    }
}

/// Thread-safe collector that aggregates metrics across parallel runs.
#[derive(Clone)]
pub struct MetricsCollector {
    runs: Arc<Mutex<Vec<RunMetrics>>>,
    // Prometheus-style counters for the HTTP /metrics endpoint
    total_coord_tokens: Arc<AtomicU64>,
    total_work_tokens:  Arc<AtomicU64>,
    total_commits:      Arc<AtomicU64>,
    total_conflicts:    Arc<AtomicU64>,
}

impl MetricsCollector {
    pub fn new() -> Self {
        Self {
            runs:                Arc::new(Mutex::new(Vec::new())),
            total_coord_tokens:  Arc::new(AtomicU64::new(0)),
            total_work_tokens:   Arc::new(AtomicU64::new(0)),
            total_commits:       Arc::new(AtomicU64::new(0)),
            total_conflicts:     Arc::new(AtomicU64::new(0)),
        }
    }

    pub fn record(&self, mut run: RunMetrics) {
        if run.finished_at.is_none() {
            run.finished_at = Some(Utc::now());
        }
        self.total_coord_tokens.fetch_add(run.coord_tokens, Ordering::Relaxed);
        self.total_work_tokens.fetch_add(run.work_tokens,  Ordering::Relaxed);
        self.total_commits.fetch_add(run.commit_attempts,  Ordering::Relaxed);
        self.total_conflicts.fetch_add(run.commit_conflicts, Ordering::Relaxed);
        self.runs.lock().push(run);
    }


    /// Aggregate statistics grouped by (system, agent_count)
    pub fn aggregate(&self) -> Vec<AggregateStats> {
        let runs = self.runs.lock();
        let mut groups: std::collections::HashMap<(String, usize), Vec<&RunMetrics>> =
            std::collections::HashMap::new();

        for r in runs.iter() {
            groups
                .entry((r.system.clone(), r.agent_count))
                .or_default()
                .push(r);
        }

        let mut out: Vec<AggregateStats> = groups
            .into_iter()
            .map(|((system, agent_count), group)| {
                let n        = group.len() as f64;
                let cwr_vals: Vec<f64> = group.iter().map(|r| r.cwr()).collect();
                let scr_vals: Vec<f64> = group.iter().map(|r| r.scr()).collect();
                let success_rate = group.iter().filter(|r| r.success).count() as f64 / n;
                let mean_wall    = group.iter().map(|r| r.wall_ms).sum::<u64>() as f64 / n;

                AggregateStats {
                    system,
                    agent_count,
                    n_tasks: group.len(),
                    mean_cwr:      mean(&cwr_vals),
                    std_cwr:       std_dev(&cwr_vals),
                    ci95_cwr:      ci95(&cwr_vals),
                    mean_scr:      mean(&scr_vals),
                    success_at_50: success_rate,
                    mean_wall_ms:  mean_wall,
                }
            })
            .collect();

        out.sort_by(|a, b| a.system.cmp(&b.system)
            .then(a.agent_count.cmp(&b.agent_count)));
        out
    }

    /// Export all runs as CSV (for R/Python statistical analysis)
    pub fn export_csv(&self) -> String {
        let mut out = String::from(
            "run_id,system,agent_count,task_id,coord_tokens,work_tokens,cwr,steps_taken,success,commit_attempts,commit_conflicts,scr,wall_ms\n"
        );
        for r in self.runs.lock().iter() {
            out.push_str(&format!(
                "{},{},{},{},{},{},{:.4},{},{},{},{},{:.4},{}\n",
                r.run_id, r.system, r.agent_count, r.task_id,
                r.coord_tokens, r.work_tokens, r.cwr(),
                r.steps_taken, r.success as u8,
                r.commit_attempts, r.commit_conflicts, r.scr(),
                r.wall_ms,
            ));
        }
        out
    }

    /// Prometheus-format metrics for /metrics endpoint
    pub fn prometheus_text(&self) -> String {
        let tc = self.total_coord_tokens.load(Ordering::Relaxed);
        let tw = self.total_work_tokens.load(Ordering::Relaxed);
        let commits   = self.total_commits.load(Ordering::Relaxed);
        let conflicts = self.total_conflicts.load(Ordering::Relaxed);
        let global_cwr = if tw > 0 { tc as f64 / tw as f64 } else { 0.0 };
        let global_scr = if commits > 0 { conflicts as f64 / commits as f64 } else { 0.0 };

        format!(
            "# HELP sbus_coord_tokens_total Total coordination tokens\n\
             # TYPE sbus_coord_tokens_total counter\n\
             sbus_coord_tokens_total {tc}\n\
             # HELP sbus_work_tokens_total Total work tokens\n\
             # TYPE sbus_work_tokens_total counter\n\
             sbus_work_tokens_total {tw}\n\
             # HELP sbus_global_cwr Global Coordination-to-Work Ratio\n\
             # TYPE sbus_global_cwr gauge\n\
             sbus_global_cwr {global_cwr:.4}\n\
             # HELP sbus_commit_conflicts_total Total ACP conflicts\n\
             # TYPE sbus_commit_conflicts_total counter\n\
             sbus_commit_conflicts_total {conflicts}\n\
             # HELP sbus_global_scr Global Semantic Conflict Rate\n\
             # TYPE sbus_global_scr gauge\n\
             sbus_global_scr {global_scr:.4}\n"
        )
    }
}

impl Default for MetricsCollector {
    fn default() -> Self { Self::new() }
}

/// Aggregated statistics for a (system, agent_count) group
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AggregateStats {
    pub system:        String,
    pub agent_count:   usize,
    pub n_tasks:       usize,
    pub mean_cwr:      f64,
    pub std_cwr:       f64,
    pub ci95_cwr:      f64,       // half-width of 95% CI
    pub mean_scr:      f64,
    pub success_at_50: f64,
    pub mean_wall_ms:  f64,
}

// ── helpers ──────────────────────────────────────────────────────────────────

fn mean(v: &[f64]) -> f64 {
    if v.is_empty() { return 0.0; }
    v.iter().sum::<f64>() / v.len() as f64
}

fn std_dev(v: &[f64]) -> f64 {
    if v.len() < 2 { return 0.0; }
    let m = mean(v);
    let var = v.iter().map(|x| (x - m).powi(2)).sum::<f64>() / (v.len() - 1) as f64;
    var.sqrt()
}

fn ci95(v: &[f64]) -> f64 {
    // 1.96 * SEM for n >= 30; use t=2.262 for n=10
    if v.is_empty() { return 0.0; }
    let t = if v.len() >= 30 { 1.96 } else { 2.262 };
    t * std_dev(v) / (v.len() as f64).sqrt()
}
