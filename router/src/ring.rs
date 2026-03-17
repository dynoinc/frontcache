use std::net::SocketAddr;
use std::sync::Arc;

use parking_lot::RwLock;
use rayon::prelude::*;
use std::hash::Hasher;
use xxhash_rust::xxh3::{Xxh3Default, xxh3_64_with_seed};

const NUM_VPARTITIONS: usize = 1 << 18;
const VP_MASK: u64 = NUM_VPARTITIONS as u64 - 1;

const REPLICAS: usize = 4;

#[derive(Clone)]
pub struct Node {
    pub pod_name: String,
    pub ip_addr: SocketAddr,
}

struct Snapshot {
    nodes: Vec<Node>,
    table: Vec<[u16; REPLICAS]>,
}

/// Future extensions:
/// - Weighted servers (ln(h/MAX)/weight)
/// - Incremental rebuild on single-server changes
pub struct Straw2Router {
    snapshot: RwLock<Arc<Snapshot>>,
}

impl Default for Straw2Router {
    fn default() -> Self {
        Self::new()
    }
}

impl Straw2Router {
    pub fn new() -> Self {
        Self {
            snapshot: RwLock::new(Arc::new(Snapshot {
                nodes: Vec::new(),
                table: Vec::new(),
            })),
        }
    }

    pub fn add_node(&self, node: Node) {
        let old = self.snapshot.read().clone();
        let pos = match old
            .nodes
            .binary_search_by(|n| n.pod_name.cmp(&node.pod_name))
        {
            Ok(_) => return,
            Err(pos) => pos,
        };
        let mut nodes = old.nodes.clone();
        nodes.insert(pos, node);
        self.commit(nodes, "added");
    }

    pub fn remove_node(&self, pod_name: &str) {
        let old = self.snapshot.read().clone();
        let pos = match old
            .nodes
            .binary_search_by(|n| n.pod_name.as_str().cmp(pod_name))
        {
            Ok(pos) => pos,
            Err(_) => return,
        };
        let mut nodes = old.nodes.clone();
        nodes.remove(pos);
        self.commit(nodes, "removed");
    }

    pub fn set_servers(&self, mut nodes: Vec<Node>) {
        nodes.sort_by(|a, b| a.pod_name.cmp(&b.pod_name));
        nodes.dedup_by(|a, b| a.pod_name == b.pod_name);
        self.commit(nodes, "sync");
    }

    fn commit(&self, nodes: Vec<Node>, action: &'static str) {
        let table = rebuild_table(&nodes);
        let len = nodes.len();
        *self.snapshot.write() = Arc::new(Snapshot { nodes, table });
        record_metrics(len, action);
    }

    pub fn get_owners(&self, object: &str, block_offset: u64) -> Option<Vec<SocketAddr>> {
        let snap = self.snapshot.read().clone();
        if snap.nodes.is_empty() {
            return None;
        }
        let mut h = Xxh3Default::new();
        h.write(object.as_bytes());
        h.write(b":");
        h.write(&block_offset.to_le_bytes());
        let vp = (h.finish() & VP_MASK) as usize;
        let addrs = snap.table[vp]
            .iter()
            .filter(|&&idx| idx != u16::MAX)
            .map(|&idx| snap.nodes[idx as usize].ip_addr)
            .collect::<Vec<_>>();
        Some(addrs)
    }
}

fn rebuild_table(nodes: &[Node]) -> Vec<[u16; REPLICAS]> {
    if nodes.is_empty() {
        return Vec::new();
    }
    (0..NUM_VPARTITIONS)
        .into_par_iter()
        .map(|vp| straw2_select_top(vp as u64, nodes))
        .collect()
}

fn straw2_select_top(vp: u64, nodes: &[Node]) -> [u16; REPLICAS] {
    let mut top = [(0u64, u16::MAX); REPLICAS];
    for (i, node) in nodes.iter().enumerate() {
        let h = xxh3_64_with_seed(node.pod_name.as_bytes(), vp);
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
