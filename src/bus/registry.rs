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
    pub fn new() -> Self { Self { inner: RwLock::new(HashMap::new()) } }
    pub fn insert_if_absent(&self, key: String, shard: Shard) -> bool {
        let mut m = self.inner.write().unwrap();
        if m.contains_key(&key) { return false; }
        m.insert(key, Arc::new(Mutex::new(shard))); true
    }
    pub fn get_arc(&self, key: &str) -> Option<Arc<Mutex<Shard>>> {
        self.inner.read().unwrap().get(key).cloned()
    }
    pub fn with_read<R>(&self, key: &str, f: impl FnOnce(&Shard) -> R) -> Option<R> {
        Some(f(&*self.get_arc(key)?.lock().unwrap()))
    }
    pub fn with_write<R>(&self, key: &str, f: impl FnOnce(&mut Shard) -> R) -> Option<R> {
        Some(f(&mut *self.get_arc(key)?.lock().unwrap()))
    }
    pub fn len(&self) -> usize { self.inner.read().unwrap().len() }
    pub fn keys(&self) -> Vec<String> { self.inner.read().unwrap().keys().cloned().collect() }
    pub fn remove(&self, key: &str) { self.inner.write().unwrap().remove(key); }
    pub fn clear(&self) { self.inner.write().unwrap().clear(); }
    pub fn snapshot_all(&self) -> Vec<(String, Shard)> {
        self.inner.read().unwrap().iter()
            .map(|(k,a)| (k.clone(), a.lock().unwrap().clone())).collect()
    }
}
impl Default for ShardRegistry { fn default() -> Self { Self::new() } }

pub struct SafeMap<V: Clone> { inner: Mutex<HashMap<String, V>> }
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
    pub fn with_map_write<R,E>(&self, f: impl FnOnce(&mut HashMap<String,V>) -> Result<R,E>)
                               -> Result<R,E> { f(&mut self.inner.lock().unwrap()) }
    pub fn len(&self) -> usize { self.inner.lock().unwrap().len() }
    pub fn keys(&self) -> Vec<String> { self.inner.lock().unwrap().keys().cloned().collect() }
    pub fn remove(&self, key: &str) { self.inner.lock().unwrap().remove(key); }
    pub fn clear(&self) { self.inner.lock().unwrap().clear(); }
    pub fn retain(&self, mut f: impl FnMut(&str, &mut V) -> bool) {
        self.inner.lock().unwrap().retain(|k,v| f(k.as_str(),v));
    }
    pub fn snapshot(&self) -> Vec<(String,V)> {
        self.inner.lock().unwrap().iter().map(|(k,v)|(k.clone(),v.clone())).collect()
    }
}
impl<V: Clone> Default for SafeMap<V> { fn default() -> Self { Self::new() } }

pub struct SimpleMap<V: Clone> { inner: Mutex<HashMap<String, V>> }
impl<V: Clone> SimpleMap<V> {
    pub fn new() -> Self { Self { inner: Mutex::new(HashMap::new()) } }
    pub fn insert_if_absent(&self, key: String, val: V) -> Result<(), V> {
        let mut m = self.inner.lock().unwrap();
        if let Some(e) = m.get(&key) { return Err(e.clone()); }
        m.insert(key, val); Ok(())
    }
    pub fn remove(&self, key: &str) { self.inner.lock().unwrap().remove(key); }
    pub fn len(&self) -> usize { self.inner.lock().unwrap().len() }
    pub fn clear(&self) { self.inner.lock().unwrap().clear(); }
    pub fn retain(&self, mut f: impl FnMut(&str, &mut V) -> bool) {
        self.inner.lock().unwrap().retain(|k,v| f(k.as_str(),v));
    }
    pub fn snapshot(&self) -> Vec<(String,V)> {
        self.inner.lock().unwrap().iter().map(|(k,v)|(k.clone(),v.clone())).collect()
    }
}
impl<V: Clone> Default for SimpleMap<V> { fn default() -> Self { Self::new() } }

pub const DEFAULT_TTL: u64 = 3600;
#[derive(Clone, Debug)]
pub struct DeliveryEntry { pub version: u64, pub delivered_at: Instant }
#[derive(Debug, Clone)]
pub struct SessionExpiredError { pub agent_id: String }
impl std::fmt::Display for SessionExpiredError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "DeliveryLog session expired for agent '{}'", self.agent_id)
    }
}
struct AgentSession { entries: HashMap<String, DeliveryEntry>, expired: bool }
pub struct DeliveryLog {
    inner:    Mutex<HashMap<String, AgentSession>>,
    ttl:      Duration,
    disabled: bool,
}
impl DeliveryLog {
    pub fn new() -> Self {
        let secs = std::env::var("SBUS_SESSION_TTL")
            .ok().and_then(|s| s.parse().ok()).unwrap_or(DEFAULT_TTL);
        let disabled = std::env::var("SBUS_LOG").map(|v| v=="1").unwrap_or(false);
        Self { inner: Mutex::new(HashMap::new()), ttl: Duration::from_secs(secs), disabled }
    }
    pub fn record(&self, agent_id: &str, key: &str, version: u64) {
        if self.disabled { return; }
        let mut m = self.inner.lock().unwrap();
        let s = m.entry(agent_id.to_owned()).or_insert_with(||
            AgentSession { entries: HashMap::new(), expired: false });
        if s.expired { s.entries.clear(); s.expired = false; }
        s.entries.insert(key.to_owned(), DeliveryEntry { version, delivered_at: Instant::now() });
    }
    pub fn touch(&self, agent_id: &str) {
        if self.disabled { return; }
        let mut m = self.inner.lock().unwrap();
        let s = m.entry(agent_id.to_owned()).or_insert_with(||
            AgentSession { entries: HashMap::new(), expired: false });
        if s.expired { s.entries.clear(); s.expired = false; }
    }
    pub fn build_effective_read_set(
        &self, agent_id: &str, primary_key: &str, explicit: &[(String,u64)],
    ) -> Result<Vec<(String,u64)>, SessionExpiredError> {
        let mut eff: HashMap<String,u64> = explicit.iter()
            .filter(|(k,_)| k != primary_key).map(|(k,v)|(k.clone(),*v)).collect();
        if self.disabled { return Ok(eff.into_iter().collect()); }
        let now = Instant::now();
        let m = self.inner.lock().unwrap();
        if let Some(s) = m.get(agent_id) {
            if s.expired { return Err(SessionExpiredError { agent_id: agent_id.to_owned() }); }
            if !s.entries.is_empty() && s.entries.values()
                .all(|e| now.duration_since(e.delivered_at) >= self.ttl) {
                return Err(SessionExpiredError { agent_id: agent_id.to_owned() });
            }
            for (k, e) in &s.entries {
                if k == primary_key { continue; }
                if now.duration_since(e.delivered_at) >= self.ttl { continue; }
                eff.entry(k.clone()).or_insert(e.version);
            }
        }
        Ok(eff.into_iter().collect())
    }
    pub fn evict_stale(&self) {
        if self.disabled { return; }
        let now = Instant::now();
        let mut m = self.inner.lock().unwrap();
        for (_, s) in m.iter_mut() {
            if s.expired { continue; }
            let before = s.entries.len();
            s.entries.retain(|_, e| now.duration_since(e.delivered_at) < self.ttl);
            if before > 0 && s.entries.is_empty() { s.expired = true; }
        }
    }
    pub fn clear(&self) { self.inner.lock().unwrap().clear(); }
    pub fn agent_count(&self) -> usize { self.inner.lock().unwrap().len() }
    pub fn total_entries(&self) -> usize {
        self.inner.lock().unwrap().values().map(|s| s.entries.len()).sum()
    }
    pub fn snapshot_all(&self) -> Vec<(String, Vec<(String, DeliveryEntry)>)> {
        self.inner.lock().unwrap().iter().map(|(a, s)| {
            (a.clone(), s.entries.iter().map(|(k,e)|(k.clone(),e.clone())).collect())
        }).collect()
    }
}
impl Default for DeliveryLog { fn default() -> Self { Self::new() } }