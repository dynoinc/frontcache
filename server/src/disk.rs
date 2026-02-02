use nix::sys::statvfs::statvfs;
use opentelemetry::KeyValue;
use tokio::time::{Duration, sleep};

use crate::{
    cache::{BLOCK_SIZE, Cache},
    prelude::*,
};

const PURGE_INTERVAL: Duration = Duration::from_secs(10);
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

        let m = frontcache_metrics::get();
        m.disk_available_bytes.record(
            stats.available as f64,
            &[KeyValue::new("path", self.path.display().to_string())],
        );

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

            for disk in cache.disks() {
                let stats = disk.update_stats().await;
                let blocks_to_purge = stats
                    .map(|s| {
                        let min_free = (s.total * MIN_FREE_PERCENT) / 100;
                        let space_to_free = min_free.saturating_sub(s.available);
                        space_to_free.div_ceil(BLOCK_SIZE) as usize
                    })
                    .unwrap_or(0);

                if let Err(e) = cache.purge_many_from(disk.path(), blocks_to_purge).await {
                    tracing::error!("Purge failed for {:?}: {}", disk.path(), e);
                }
            }
        }
    });
}
