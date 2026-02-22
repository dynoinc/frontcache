use std::sync::Arc;

use parking_lot::RwLock;
use rayon::prelude::*;
use xxhash_rust::xxh3::{xxh3_64, xxh3_64_with_seed};

const NUM_VPARTITIONS: usize = 1 << 18; // 262,144
const VP_MASK: u64 = NUM_VPARTITIONS as u64 - 1;

struct Snapshot {
    servers: Vec<String>,
    table: Vec<u16>,
}

/// Straw2-based router. All methods take `&self` — writes build a new snapshot
/// without holding any lock, then atomically swap it in. Readers are never blocked.
pub struct Straw2Router {
    snapshot: RwLock<Arc<Snapshot>>,
}

impl Straw2Router {
    pub fn new() -> Self {
        Self {
            snapshot: RwLock::new(Arc::new(Snapshot {
                servers: Vec::new(),
                table: Vec::new(),
            })),
        }
    }

    pub fn add_node(&self, addr: String) {
        let old = self.snapshot.read().clone();
        let mut servers = old.servers.clone();
        match servers.binary_search(&addr) {
            Ok(_) => return,
            Err(pos) => servers.insert(pos, addr),
        }
        let table = rebuild_table(&servers);
        let len = servers.len();
        *self.snapshot.write() = Arc::new(Snapshot { servers, table });
        record_metrics(len, "added");
    }

    pub fn remove_node(&self, addr: &str) {
        let old = self.snapshot.read().clone();
        let mut servers = old.servers.clone();
        match servers.binary_search_by(|s| s.as_str().cmp(addr)) {
            Ok(pos) => {
                servers.remove(pos);
            }
            Err(_) => return,
        }
        let table = rebuild_table(&servers);
        let len = servers.len();
        *self.snapshot.write() = Arc::new(Snapshot { servers, table });
        record_metrics(len, "removed");
    }

    pub fn set_servers(&self, mut servers: Vec<String>) {
        servers.sort();
        servers.dedup();
        let table = rebuild_table(&servers);
        let len = servers.len();
        *self.snapshot.write() = Arc::new(Snapshot { servers, table });
        record_metrics(len, "sync");
    }

    pub fn get_owner(&self, object: &str, block_offset: u64) -> Option<String> {
        let snap = self.snapshot.read().clone();
        if snap.servers.is_empty() {
            return None;
        }
        let key = format!("{}:{}", object, block_offset);
        let vp = (xxh3_64(key.as_bytes()) & VP_MASK) as usize;
        Some(snap.servers[snap.table[vp] as usize].clone())
    }
}

fn rebuild_table(servers: &[String]) -> Vec<u16> {
    if servers.is_empty() {
        return Vec::new();
    }
    (0..NUM_VPARTITIONS)
        .into_par_iter()
        .map(|vp| straw2_select(vp as u64, servers))
        .collect()
}

fn straw2_select(vp: u64, servers: &[String]) -> u16 {
    let mut best_idx: u16 = 0;
    let mut best_hash: u64 = 0;
    for (i, server) in servers.iter().enumerate() {
        let h = xxh3_64_with_seed(server.as_bytes(), vp);
        if h > best_hash || i == 0 {
            best_hash = h;
            best_idx = i as u16;
        }
    }
    best_idx
}

fn record_metrics(server_count: usize, action: &'static str) {
    let m = frontcache_metrics::get();
    m.ring_members.record(server_count as u64, &[]);
    m.ring_member_changes
        .add(1, &[opentelemetry::KeyValue::new("action", action)]);
}
