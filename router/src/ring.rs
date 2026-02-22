use std::{
    collections::{BTreeMap, HashSet},
    hash::{DefaultHasher, Hash, Hasher},
};

const VIRTUAL_NODES: usize = 150;

pub struct ConsistentHashRing {
    ring: BTreeMap<u64, String>,
    nodes: HashSet<String>,
}

impl ConsistentHashRing {
    pub fn new() -> Self {
        Self {
            ring: BTreeMap::new(),
            nodes: HashSet::new(),
        }
    }

    pub fn add_node(&mut self, addr: String) {
        if self.nodes.insert(addr.clone()) {
            for i in 0..VIRTUAL_NODES {
                let hash = self.hash(&format!("{}:{}", addr, i));
                self.ring.insert(hash, addr.clone());
            }
            self.record_metrics("added");
        }
    }

    pub fn remove_node(&mut self, addr: &str) {
        if self.nodes.remove(addr) {
            for i in 0..VIRTUAL_NODES {
                let hash = self.hash(&format!("{}:{}", addr, i));
                self.ring.remove(&hash);
            }
            self.record_metrics("removed");
        }
    }

    fn record_metrics(&self, action: &'static str) {
        let m = frontcache_metrics::get();
        m.ring_members.record(self.nodes.len() as u64, &[]);
        m.ring_member_changes
            .add(1, &[opentelemetry::KeyValue::new("action", action)]);
    }

    pub fn get_owner(&self, object: &str, block_offset: u64) -> Option<&String> {
        let key = format!("{}:{}", object, block_offset);
        let hash = self.hash(&key);

        self.ring
            .range(hash..)
            .next()
            .or_else(|| self.ring.iter().next())
            .map(|(_, node)| node)
    }

    fn hash(&self, key: &str) -> u64 {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        hasher.finish()
    }
}
