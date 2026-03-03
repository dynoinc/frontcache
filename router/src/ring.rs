use std::sync::Arc;

use parking_lot::RwLock;
use rayon::prelude::*;
use xxhash_rust::xxh3::{xxh3_64, xxh3_64_with_seed};

const NUM_VPARTITIONS: usize = 1 << 18;
const VP_MASK: u64 = NUM_VPARTITIONS as u64 - 1;

const REPLICAS: usize = 4;

struct Snapshot {
    servers: Vec<String>,
    table: Vec<[u16; REPLICAS]>,
}

/// Future extensions:
/// - Weighted servers (ln(h/MAX)/weight)
/// - Incremental rebuild on single-server changes
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
        let pos = match old.servers.binary_search(&addr) {
            Ok(_) => return,
            Err(pos) => pos,
        };
        let mut servers = old.servers.clone();
        servers.insert(pos, addr);
        let table = rebuild_table(&servers);
        let len = servers.len();
        *self.snapshot.write() = Arc::new(Snapshot { servers, table });
        record_metrics(len, "added");
    }

    pub fn remove_node(&self, addr: &str) {
        let old = self.snapshot.read().clone();
        let pos = match old.servers.binary_search_by(|s| s.as_str().cmp(addr)) {
            Ok(pos) => pos,
            Err(_) => return,
        };
        let mut servers = old.servers.clone();
        servers.remove(pos);
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

    pub fn get_owners(&self, object: &str, block_offset: u64) -> Option<Vec<String>> {
        let snap = self.snapshot.read().clone();
        if snap.servers.is_empty() {
            return None;
        }
        let key = format!("{}:{}", object, block_offset);
        let vp = (xxh3_64(key.as_bytes()) & VP_MASK) as usize;
        let addrs = snap.table[vp]
            .iter()
            .filter(|&&idx| idx != u16::MAX)
            .map(|&idx| snap.servers[idx as usize].clone())
            .collect::<Vec<_>>();
        Some(addrs)
    }
}

fn rebuild_table(servers: &[String]) -> Vec<[u16; REPLICAS]> {
    if servers.is_empty() {
        return Vec::new();
    }
    (0..NUM_VPARTITIONS)
        .into_par_iter()
        .map(|vp| straw2_select_top(vp as u64, servers))
        .collect()
}

fn straw2_select_top(vp: u64, servers: &[String]) -> [u16; REPLICAS] {
    let mut top = [(0u64, u16::MAX); REPLICAS];
    for (i, server) in servers.iter().enumerate() {
        let h = xxh3_64_with_seed(server.as_bytes(), vp);
        // Insert into sorted top-K (descending by hash)
        if let Some(pos) = top.iter().position(|&(th, _)| h > th) {
            top.copy_within(pos..REPLICAS - 1, pos + 1);
            top[pos] = (h, i as u16);
        }
    }
    let mut result = [u16::MAX; REPLICAS];
    for (i, &(_, idx)) in top.iter().enumerate() {
        result[i] = idx;
    }
    result
}

fn record_metrics(server_count: usize, action: &'static str) {
    let m = frontcache_metrics::get();
    m.ring_members.record(server_count as u64, &[]);
    m.ring_member_changes
        .add(1, &[opentelemetry::KeyValue::new("action", action)]);
}
