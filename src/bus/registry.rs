use std::{collections::HashMap, sync::{Mutex, RwLock}};

pub struct SafeMap<V> {
    inner: RwLock<HashMap<String, V>>,
}
impl<V> SafeMap<V> {
    pub fn new() -> Self { Self { inner: RwLock::new(HashMap::new()) } }
    pub fn insert_if_absent(&self, key: String, value: V) -> bool {
        let mut m = self.inner.write().unwrap();
        if m.contains_key(&key) { return false; }
        m.insert(key, value); true
    }
    pub fn contains_key(&self, key: &str) -> bool { self.inner.read().unwrap().contains_key(key) }
    pub fn len(&self) -> usize { self.inner.read().unwrap().len() }
    pub fn keys(&self) -> Vec<String> { self.inner.read().unwrap().keys().cloned().collect() }
    pub fn with_entry_read<F, R>(&self, key: &str, f: F) -> Option<R>
    where F: FnOnce(&V) -> R { self.inner.read().unwrap().get(key).map(f) }
    pub fn with_entry<F, R>(&self, key: &str, f: F) -> Option<R>
    where F: FnOnce(&mut V) -> R { self.inner.write().unwrap().get_mut(key).map(f) }
    pub fn with_map_write<F, R>(&self, f: F) -> R
    where F: FnOnce(&mut HashMap<String, V>) -> R {
        let mut m = self.inner.write().unwrap(); f(&mut m)
    }
    pub fn iter_all<F>(&self, mut f: F) where F: FnMut(&str, &V) {
        for (k, v) in self.inner.read().unwrap().iter() { f(k, v); }
    }
    pub fn snapshot_cloned(&self) -> Vec<(String, V)> where V: Clone {
        self.inner.read().unwrap().iter().map(|(k, v)| (k.clone(), v.clone())).collect()
    }
}
impl<V> Default for SafeMap<V> { fn default() -> Self { Self::new() } }

pub struct SimpleMap<V: Clone> {
    inner: Mutex<HashMap<String, V>>,
}
impl<V: Clone> SimpleMap<V> {
    pub fn new() -> Self { Self { inner: Mutex::new(HashMap::new()) } }
    pub fn insert_if_absent(&self, key: String, value: V) -> Result<(), V> {
        let mut m = self.inner.lock().unwrap();
        match m.get(&key) { Some(v) => Err(v.clone()), None => { m.insert(key, value); Ok(()) } }
    }
    pub fn remove(&self, key: &str) -> Option<V> { self.inner.lock().unwrap().remove(key) }
    pub fn len(&self) -> usize { self.inner.lock().unwrap().len() }
    pub fn retain<F>(&self, mut f: F) where F: FnMut(&str, &V) -> bool {
        self.inner.lock().unwrap().retain(|k, v| f(k, v));
    }
    pub fn snapshot(&self) -> Vec<(String, V)> {
        self.inner.lock().unwrap().iter().map(|(k, v)| (k.clone(), v.clone())).collect()
    }
}
impl<V: Clone> Default for SimpleMap<V> { fn default() -> Self { Self::new() } }
