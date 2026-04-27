use std::{
    collections::HashMap,
    sync::{Arc, Mutex, RwLock},
    time::{Duration, Instant},
};

use crate::bus::types::Shard;

pub struct ShardRegistry {
    inner: RwLock<HashMap<String, Arc<Mutex<Shard>>>>,
}

impl ShardRegistry {
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(HashMap::new()),
        }
    }

    pub fn insert_if_absent(&self, key: String, shard: Shard) -> bool {
        let mut m = self.inner.write().unwrap();
        if m.contains_key(&key) {
            return false;
        }
        m.insert(key, Arc::new(Mutex::new(shard)));
        true
    }

    fn get_arc(&self, key: &str) -> Option<Arc<Mutex<Shard>>> {
        self.inner.read().unwrap().get(key).cloned()
    }

    pub fn with_read<R>(&self, key: &str, f: impl FnOnce(&Shard) -> R) -> Option<R> {
        let arc = self.get_arc(key)?;
        let guard = arc.lock().unwrap();
        Some(f(&guard))
    }

    pub fn with_write<R>(&self, key: &str, f: impl FnOnce(&mut Shard) -> R) -> Option<R> {
        let arc = self.get_arc(key)?;
        let mut guard = arc.lock().unwrap();
        Some(f(&mut guard))
    }

    pub fn len(&self) -> usize {
        self.inner.read().unwrap().len()
    }

    pub fn keys(&self) -> Vec<String> {
        self.inner.read().unwrap().keys().cloned().collect()
    }

    pub fn clear(&self) {
        self.inner.write().unwrap().clear();
    }

    pub fn snapshot_all(&self) -> Vec<(String, Shard)> {
        self.inner
            .read()
            .unwrap()
            .iter()
            .map(|(k, a)| (k.clone(), a.lock().unwrap().clone()))
            .collect()
    }
}

impl Default for ShardRegistry {
    fn default() -> Self {
        Self::new()
    }
}

const DEFAULT_TTL: u64 = 3600;

#[derive(Clone, Debug)]
pub struct DeliveryEntry {
    pub version: u64,
    pub delivered_at: Instant,
}

#[derive(Debug, Clone)]
pub struct SessionExpiredError {
    pub agent_id: String,
}

impl std::fmt::Display for SessionExpiredError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "DeliveryLog session expired for agent '{}'",
            self.agent_id
        )
    }
}

impl std::error::Error for SessionExpiredError {}

struct AgentSession {
    entries: HashMap<String, DeliveryEntry>,
    expired: bool,
}

pub struct DeliveryLog {
    inner: Mutex<HashMap<String, AgentSession>>,
    ttl: Duration,
    disabled: bool,
}

impl DeliveryLog {
    pub fn new() -> Self {
        let secs = std::env::var("SBUS_SESSION_TTL")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(DEFAULT_TTL);
        let disabled = std::env::var("SBUS_LOG").is_ok_and(|v| v == "1");
        Self {
            inner: Mutex::new(HashMap::new()),
            ttl: Duration::from_secs(secs),
            disabled,
        }
    }

    pub fn record(&self, agent_id: &str, key: &str, version: u64) {
        if self.disabled {
            return;
        }
        let mut m = self.inner.lock().unwrap();
        let s = m
            .entry(agent_id.to_owned())
            .or_insert_with(|| AgentSession {
                entries: HashMap::new(),
                expired: false,
            });
        if s.expired {
            s.entries.clear();
            s.expired = false;
        }
        s.entries.insert(
            key.to_owned(),
            DeliveryEntry {
                version,
                delivered_at: Instant::now(),
            },
        );
    }

    pub fn has_entry(&self, agent_id: &str, key: &str) -> bool {
        if self.disabled {
            return false;
        }
        let m = self.inner.lock().unwrap();
        let Some(s) = m.get(agent_id) else { return false; };
        if s.expired {
            return false;
        }
        let Some(e) = s.entries.get(key) else { return false; };
        Instant::now().duration_since(e.delivered_at) < self.ttl
    }

    pub fn touch(&self, agent_id: &str) {
        if self.disabled {
            return;
        }
        let mut m = self.inner.lock().unwrap();
        let s = m
            .entry(agent_id.to_owned())
            .or_insert_with(|| AgentSession {
                entries: HashMap::new(),
                expired: false,
            });
        if s.expired {
            s.entries.clear();
            s.expired = false;
        }
    }

    pub fn reset_session(&self, agent_id: &str) {
        if self.disabled {
            return;
        }
        self.inner.lock().unwrap().insert(
            agent_id.to_owned(),
            AgentSession {
                entries: HashMap::new(),
                expired: false,
            },
        );
    }

    pub fn build_effective_read_set(
        &self,
        agent_id: &str,
        primary_key: &str,
        explicit: &[(String, u64)],
    ) -> Result<Vec<(String, u64)>, SessionExpiredError> {
        let mut eff: HashMap<String, u64> = explicit
            .iter()
            .filter(|(k, _)| k != primary_key)
            .map(|(k, v)| (k.clone(), *v))
            .collect();

        if self.disabled {
            return Ok(eff.into_iter().collect());
        }

        let now = Instant::now();
        let m = self.inner.lock().unwrap();
        if let Some(s) = m.get(agent_id) {
            if s.expired {
                return Err(SessionExpiredError {
                    agent_id: agent_id.to_owned(),
                });
            }
            if !s.entries.is_empty()
                && s.entries
                    .values()
                    .all(|e| now.duration_since(e.delivered_at) >= self.ttl)
            {
                return Err(SessionExpiredError {
                    agent_id: agent_id.to_owned(),
                });
            }
            for (k, e) in &s.entries {
                if k == primary_key {
                    continue;
                }
                if now.duration_since(e.delivered_at) >= self.ttl {
                    continue;
                }
                eff.entry(k.clone()).or_insert(e.version);
            }
        }
        Ok(eff.into_iter().collect())
    }

    pub fn evict_stale(&self) {
        if self.disabled {
            return;
        }
        let now = Instant::now();
        let mut m = self.inner.lock().unwrap();
        for s in m.values_mut() {
            if s.expired {
                continue;
            }
            let before = s.entries.len();
            s.entries
                .retain(|_, e| now.duration_since(e.delivered_at) < self.ttl);
            if before > 0 && s.entries.is_empty() {
                s.expired = true;
            }
        }
    }

    pub fn clear(&self) {
        self.inner.lock().unwrap().clear();
    }

    pub fn agent_count(&self) -> usize {
        self.inner.lock().unwrap().len()
    }

    pub fn total_entries(&self) -> usize {
        self.inner
            .lock()
            .unwrap()
            .values()
            .map(|s| s.entries.len())
            .sum()
    }

    pub fn snapshot_all(&self) -> Vec<(String, Vec<(String, DeliveryEntry)>)> {
        self.inner
            .lock()
            .unwrap()
            .iter()
            .map(|(a, s)| {
                (
                    a.clone(),
                    s.entries
                        .iter()
                        .map(|(k, e)| (k.clone(), e.clone()))
                        .collect(),
                )
            })
            .collect()
    }
}

impl Default for DeliveryLog {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn has_entry_returns_false_for_missing_agent() {
        let dl = DeliveryLog::new();
        assert!(!dl.has_entry("nobody", "any_shard"));
    }

    #[test]
    fn has_entry_returns_true_after_record() {
        let dl = DeliveryLog::new();
        dl.record("alpha", "models_state", 3);
        assert!(dl.has_entry("alpha", "models_state"));
    }

    #[test]
    fn has_entry_returns_false_for_wrong_key() {
        let dl = DeliveryLog::new();
        dl.record("alpha", "models_state", 3);
        assert!(!dl.has_entry("alpha", "orm_query"));
    }

    #[test]
    fn has_entry_returns_false_for_wrong_agent() {
        let dl = DeliveryLog::new();
        dl.record("alpha", "models_state", 3);
        assert!(!dl.has_entry("beta", "models_state"));
    }

    #[test]
    fn has_entry_returns_false_after_reset_session() {
        let dl = DeliveryLog::new();
        dl.record("alpha", "models_state", 3);
        dl.reset_session("alpha");
        assert!(!dl.has_entry("alpha", "models_state"));
    }
}
