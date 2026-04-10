// src/bus/registry.rs — S-Bus v29 (FIX-TTL-v2)
//
// FIX-TTL-v2: Eager expiry check in build_effective_read_set.
//   Previously: build_effective_read_set relied on evict_stale() having already
//   marked sessions expired. Race condition: evict_stale runs every 5s; a commit
//   arriving between eviction cycles would see an un-expired session even if TTL
//   had elapsed. Result: HTTP 200 instead of HTTP 410 (verified experimentally).
//
//   Fix: build_effective_read_set now checks entry age directly against ttl_secs.
//   If ALL entries are stale (age > TTL), the session is considered expired and
//   Err(SessionExpiredError) is returned without waiting for the next eviction cycle.
//   evict_stale() still runs periodically for memory management.

use std::{
    collections::HashMap,
    sync::Mutex,
    time::{Duration, Instant},
};

// ── SafeMap ───────────────────────────────────────────────────────────────────

pub struct SafeMap<V: Clone> {
    inner: Mutex<HashMap<String, V>>,
}

impl<V: Clone> SafeMap<V> {
    pub fn new() -> Self { Self { inner: Mutex::new(HashMap::new()) } }

    pub fn insert_if_absent(&self, key: String, val: V) -> bool {
        let mut m = self.inner.lock().unwrap();
        if m.contains_key(&key) { return false; }
        m.insert(key, val); true
    }

    pub fn with_entry<R>(&self, key: &str, f: impl FnOnce(&mut V) -> R) -> Option<R> {
        self.inner.lock().unwrap().get_mut(key).map(f)
    }

    pub fn with_entry_read<R>(&self, key: &str, f: impl FnOnce(&V) -> R) -> Option<R> {
        self.inner.lock().unwrap().get(key).map(f)
    }

    pub fn with_map_write<R, E>(
        &self,
        f: impl FnOnce(&mut HashMap<String, V>) -> Result<R, E>,
    ) -> Result<R, E> {
        f(&mut self.inner.lock().unwrap())
    }

    pub fn len(&self) -> usize { self.inner.lock().unwrap().len() }
    pub fn keys(&self) -> Vec<String> { self.inner.lock().unwrap().keys().cloned().collect() }
    pub fn remove(&self, key: &str) { self.inner.lock().unwrap().remove(key); }

    pub fn retain(&self, mut f: impl FnMut(&str, &mut V) -> bool) {
        self.inner.lock().unwrap().retain(|k, v| f(k.as_str(), v));
    }

    pub fn snapshot(&self) -> Vec<(String, V)> {
        self.inner.lock().unwrap().iter().map(|(k,v)| (k.clone(), v.clone())).collect()
    }
}

impl<V: Clone> Default for SafeMap<V> { fn default() -> Self { Self::new() } }

// ── SimpleMap ─────────────────────────────────────────────────────────────────

pub struct SimpleMap<V: Clone> {
    inner: Mutex<HashMap<String, V>>,
}

impl<V: Clone> SimpleMap<V> {
    pub fn new() -> Self { Self { inner: Mutex::new(HashMap::new()) } }

    pub fn insert_if_absent(&self, key: String, val: V) -> Result<(), V> {
        let mut m = self.inner.lock().unwrap();
        if let Some(e) = m.get(&key) { return Err(e.clone()); }
        m.insert(key, val); Ok(())
    }

    pub fn remove(&self, key: &str) { self.inner.lock().unwrap().remove(key); }
    pub fn len(&self) -> usize { self.inner.lock().unwrap().len() }

    pub fn retain(&self, mut f: impl FnMut(&str, &mut V) -> bool) {
        self.inner.lock().unwrap().retain(|k, v| f(k.as_str(), v));
    }

    pub fn snapshot(&self) -> Vec<(String, V)> {
        self.inner.lock().unwrap().iter().map(|(k,v)| (k.clone(), v.clone())).collect()
    }
}

impl<V: Clone> Default for SimpleMap<V> { fn default() -> Self { Self::new() } }

// ── DeliveryLog ───────────────────────────────────────────────────────────────

pub const SESSION_TTL_SECS: u64 = 3600;

#[derive(Clone, Debug)]
pub struct DeliveryEntry {
    pub version:      u64,
    pub delivered_at: Instant,
}

#[derive(Debug, Clone)]
pub struct SessionExpiredError {
    pub agent_id: String,
}

impl std::fmt::Display for SessionExpiredError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f,
               "DeliveryLog session expired for agent '{}'. \
             Re-read all shards via GET /shard/:key?agent_id={} then retry.",
               self.agent_id, self.agent_id)
    }
}

struct AgentSession {
    entries: HashMap<String, DeliveryEntry>,
    expired: bool,  // set by evict_stale for memory management; also checked eagerly
}

pub struct DeliveryLog {
    inner:       Mutex<HashMap<String, AgentSession>>,
    ttl:         Duration,
    dl_disabled: bool,
}

impl DeliveryLog {
    pub fn new() -> Self {
        let secs = std::env::var("SBUS_SESSION_TTL")
            .ok().and_then(|s| s.parse().ok())
            .unwrap_or(SESSION_TTL_SECS);
        let dl_disabled = std::env::var("SBUS_LOG")
            .map(|v| v == "1" || v.to_ascii_lowercase() == "true")
            .unwrap_or(false);
        Self { inner: Mutex::new(HashMap::new()), ttl: Duration::from_secs(secs), dl_disabled }
    }

    pub fn record(&self, agent_id: &str, key: &str, version: u64) {
        if self.dl_disabled { return; }
        let mut m = self.inner.lock().unwrap();
        let session = m.entry(agent_id.to_owned()).or_insert_with(|| AgentSession {
            entries: HashMap::new(), expired: false,
        });
        if session.expired {
            session.entries.clear();
            session.expired = false;
            tracing::info!(agent_id, key, version,
                "DeliveryLog: new session started after expiry");
        }
        session.entries.insert(key.to_owned(), DeliveryEntry {
            version, delivered_at: Instant::now(),
        });
    }

    /// Build effective read-set. FIX-TTL-v2: eagerly checks entry age at commit
    /// time — does not wait for evict_stale() to run. Eliminates the race where
    /// a commit between eviction cycles would see an un-expired session.
    pub fn build_effective_read_set(
        &self,
        agent_id: &str,
        primary_key: &str,
        explicit_read_set: &[(String, u64)],
    ) -> Result<Vec<(String, u64)>, SessionExpiredError> {
        let mut effective: HashMap<String, u64> = explicit_read_set
            .iter()
            .filter(|(k, _)| k != primary_key)
            .map(|(k, v)| (k.clone(), *v))
            .collect();

        if self.dl_disabled {
            return Ok(effective.into_iter().collect());
        }

        let now = Instant::now();
        let m = self.inner.lock().unwrap();

        if let Some(session) = m.get(agent_id) {
            // Check 1: already marked expired by evict_stale
            if session.expired {
                return Err(SessionExpiredError { agent_id: agent_id.to_owned() });
            }

            // FIX-TTL-v2: Check 2: EAGER check — are ALL entries now stale?
            // This fires even if evict_stale hasn't run yet.
            let all_stale = !session.entries.is_empty()
                && session.entries.values().all(|e| now.duration_since(e.delivered_at) >= self.ttl);
            if all_stale {
                tracing::warn!(
                    agent_id,
                    ttl_secs = self.ttl.as_secs(),
                    "DeliveryLog: eager TTL check — all entries stale at commit time → HTTP 410"
                );
                return Err(SessionExpiredError { agent_id: agent_id.to_owned() });
            }

            // Merge fresh entries into effective read-set
            for (key, entry) in &session.entries {
                if key == primary_key { continue; }
                // Skip individually-stale entries (partial expiry)
                if now.duration_since(entry.delivered_at) >= self.ttl { continue; }
                effective.entry(key.clone()).or_insert(entry.version);
            }
        }

        Ok(effective.into_iter().collect())
    }

    /// Evict stale entries for memory management. Also marks sessions expired
    /// for the lazy path. FIX-TTL-v2: the eager check in build_effective_read_set
    /// handles commits that arrive before the next eviction cycle.
    pub fn evict_stale(&self) {
        if self.dl_disabled { return; }
        let now = Instant::now();
        let mut m = self.inner.lock().unwrap();
        for (agent_id, session) in m.iter_mut() {
            if session.expired { continue; }
            let before = session.entries.len();
            session.entries.retain(|_, e| now.duration_since(e.delivered_at) < self.ttl);
            if before > 0 && session.entries.is_empty() {
                session.expired = true;
                tracing::warn!(
                    agent_id = agent_id.as_str(),
                    ttl_secs = self.ttl.as_secs(),
                    "DeliveryLog session expired (eviction): next commit → HTTP 410"
                );
            }
        }
    }

    pub fn agent_count(&self) -> usize { self.inner.lock().unwrap().len() }
    pub fn total_entries(&self) -> usize {
        self.inner.lock().unwrap().values().map(|s| s.entries.len()).sum()
    }
}

impl Default for DeliveryLog { fn default() -> Self { Self::new() } }