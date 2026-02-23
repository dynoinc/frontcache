use nix::sys::statvfs::statvfs;
use tokio::time::{Duration, sleep};

use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::Result;
use parking_lot::RwLock;

use crate::cache::{BLOCK_SIZE, Cache};

const PURGE_INTERVAL: Duration = Duration::from_secs(10);
const FLUSH_INTERVAL: Duration = Duration::from_secs(60);
const MIN_FREE_PERCENT: u64 = 5;

#[derive(Copy, Clone)]
pub struct DiskStats {
    pub available: u64,
    pub total: u64,
}

pub struct Disk {
    path: PathBuf,
    stats: RwLock<DiskStats>,
}

impl Disk {
    pub fn new(path: PathBuf) -> Result<Arc<Self>> {
        let stats = get_disk_stats(&path)?;
        Ok(Arc::new(Self {
            path,
            stats: RwLock::new(stats),
        }))
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn available(&self) -> u64 {
        self.stats.read().available
    }

    pub async fn update_stats(&self) -> Result<DiskStats> {
        let stats = get_disk_stats(&self.path)?;
        *self.stats.write() = stats;
        Ok(stats)
    }
}

pub fn select_disk(disks: &[Arc<Disk>]) -> &Path {
    disks
        .iter()
        .max_by_key(|d| d.available())
        .map(|d| d.path())
        .unwrap()
}

pub fn get_disk_stats(path: &Path) -> Result<DiskStats> {
    let stat = statvfs(path)?;
    let block_size = stat.fragment_size() as u64;
    let total = block_size.saturating_mul(stat.blocks() as u64);
    let available = block_size.saturating_mul(stat.blocks_available() as u64);

    Ok(DiskStats { available, total })
}

pub fn start_purger(cache: Arc<Cache>) {
    tokio::spawn(async move {
        loop {
            sleep(PURGE_INTERVAL).await;

            let mut total_bytes: u64 = 0;
            let mut available_bytes: u64 = 0;
            for disk in cache.disks() {
                let stats = disk.update_stats().await;
                let blocks_to_purge = stats
                    .map(|s| {
                        total_bytes += s.total;
                        available_bytes += s.available;
                        let min_free = (s.total * MIN_FREE_PERCENT) / 100;
                        let space_to_free = min_free.saturating_sub(s.available);
                        space_to_free.div_ceil(BLOCK_SIZE) as usize
                    })
                    .unwrap_or(0);

                if let Err(e) = cache.purge_many_from(disk.path(), blocks_to_purge).await {
                    tracing::error!("Purge failed for {:?}: {}", disk.path(), e);
                }
            }
            let m = frontcache_metrics::get();
            m.disk_total_bytes.record(total_bytes as f64, &[]);
            m.disk_available_bytes.record(available_bytes as f64, &[]);
            m.blocks_total.record(cache.block_count() as u64, &[]);
        }
    });
}

pub fn start_flusher(cache: Arc<Cache>) {
    tokio::spawn(async move {
        loop {
            sleep(FLUSH_INTERVAL).await;
            cache.flush_last_accessed();
        }
    });
}
