use std::collections::BTreeMap;
use std::collections::hash_map::RandomState;
use std::hash::BuildHasher;

const VIRTUAL_NODES: usize = 150;

pub struct ConsistentHashRing {
    ring: BTreeMap<u64, String>,
    nodes: Vec<String>,
}

impl ConsistentHashRing {
    pub fn new() -> Self {
        Self {
            ring: BTreeMap::new(),
            nodes: Vec::new(),
        }
    }

    pub fn add_node(&mut self, addr: String) {
        if self.nodes.contains(&addr) {
            return;
        }
        self.nodes.push(addr.clone());

        for i in 0..VIRTUAL_NODES {
            let key = format!("{}:{}", addr, i);
            let hash = self.hash(&key);
            self.ring.insert(hash, addr.clone());
        }
    }

    pub fn remove_node(&mut self, addr: &str) {
        self.nodes.retain(|n| n != addr);

        for i in 0..VIRTUAL_NODES {
            let key = format!("{}:{}", addr, i);
            let hash = self.hash(&key);
            self.ring.remove(&hash);
        }
    }

    pub fn get_owner(&self, object: &str, block_offset: u64) -> Option<&String> {
        if self.ring.is_empty() {
            return None;
        }

        let key = format!("{}:{}", object, block_offset);
        let hash = self.hash(&key);

        self.ring
            .range(hash..)
            .next()
            .or_else(|| self.ring.iter().next())
            .map(|(_, node)| node)
    }

    fn hash(&self, key: &str) -> u64 {
        RandomState::new().hash_one(key)
    }
}
