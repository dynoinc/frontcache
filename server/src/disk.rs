use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use tokio::task::JoinHandle;
use tokio::time::{Duration, sleep};
use tokio_util::sync::CancellationToken;

use crate::cache::{BLOCK_SIZE, Cache};

const PURGE_INTERVAL: Duration = Duration::from_secs(10);
const FLUSH_INTERVAL: Duration = Duration::from_secs(60);
const MAX_UTILIZATION_PERCENT: u64 = 95;

pub struct Disk {
    path: PathBuf,
    capacity: u64,
    used: AtomicU64,
}

impl Disk {
    pub fn new(path: PathBuf, capacity: u64) -> Arc<Self> {
        Arc::new(Self {
            path,
            capacity,
            used: AtomicU64::new(0),
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn capacity(&self) -> u64 {
        self.capacity
    }

    pub fn used(&self) -> u64 {
        self.used.load(Ordering::Relaxed)
    }

    pub fn available(&self) -> u64 {
        self.capacity.saturating_sub(self.used())
    }

    pub fn add_used(&self, bytes: u64) {
        self.used.fetch_add(bytes, Ordering::Relaxed);
    }

    pub fn sub_used(&self, bytes: u64) {
        self.used.fetch_sub(bytes, Ordering::Relaxed);
    }
}

pub fn select_disk(disks: &[Arc<Disk>]) -> &Arc<Disk> {
    disks.iter().max_by_key(|d| d.available()).unwrap()
}

pub fn start_purger(cache: Arc<Cache>, shutdown: CancellationToken) -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => break,
                _ = sleep(PURGE_INTERVAL) => {
                    for disk in cache.disks() {
                        let threshold = (disk.capacity() * MAX_UTILIZATION_PERCENT) / 100;
                        let used = disk.used();
                        if used <= threshold {
                            continue;
                        }
                        let excess = used - threshold;
                        let blocks_to_purge = excess.div_ceil(BLOCK_SIZE) as usize;

                        if let Err(e) = cache.purge_many_from(disk.path(), blocks_to_purge).await {
                            tracing::error!("Purge failed for {:?}: {}", disk.path(), e);
                        }
                    }

                    let m = frontcache_metrics::get();
                    m.blocks_total.record(cache.block_count() as u64, &[]);
                    let total_capacity: u64 = cache.disks().iter().map(|d| d.capacity()).sum();
                    let total_used: u64 = cache.disks().iter().map(|d| d.used()).sum();
                    m.disk_total_bytes.record(total_capacity as f64, &[]);
                    m.disk_available_bytes
                        .record((total_capacity - total_used) as f64, &[]);
                }
            }
        }
    })
}

pub fn start_flusher(cache: Arc<Cache>, shutdown: CancellationToken) -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => break,
                _ = sleep(FLUSH_INTERVAL) => { cache.flush_last_accessed(); }
            }
        }
    })
}
