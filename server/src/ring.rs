use std::{
    collections::{BTreeMap, HashSet, hash_map::RandomState},
    hash::BuildHasher,
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
                let key = format!("{}:{}", addr, i);
                let hash = self.hash(&key);
                self.ring.insert(hash, addr.clone());
            }
        }
    }

    pub fn remove_node(&mut self, addr: &str) {
        if self.nodes.remove(addr) {
            for i in 0..VIRTUAL_NODES {
                let key = format!("{}:{}", addr, i);
                let hash = self.hash(&key);
                self.ring.remove(&hash);
            }
        }
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
        RandomState::new().hash_one(key)
    }
}
