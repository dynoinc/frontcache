use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use opentelemetry::metrics::ObservableGauge;
use tokio::sync::Semaphore;
use tokio::task::JoinHandle;
use tokio::time::{Duration, sleep};
use tokio_util::sync::CancellationToken;

use crate::cache::Cache;

const FLUSH_INTERVAL: Duration = Duration::from_secs(60);
pub(crate) const MAX_UTILIZATION_PERCENT: u64 = 95;
pub(crate) const MIN_UTILIZATION_PERCENT: u64 = 90;

pub struct Disk {
    path: PathBuf,
    capacity: u64,
    used: AtomicU64,
    low_watermark: u64,
    high_watermark: u64,
    evict_lock: Arc<Semaphore>,
}

impl Disk {
    pub fn new(path: PathBuf, capacity: u64) -> Arc<Self> {
        Arc::new(Self {
            path,
            capacity,
            used: AtomicU64::new(0),
            low_watermark: capacity * MIN_UTILIZATION_PERCENT / 100,
            high_watermark: capacity * MAX_UTILIZATION_PERCENT / 100,
            evict_lock: Arc::new(Semaphore::new(1)),
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

    pub fn low_watermark(&self) -> u64 {
        self.low_watermark
    }

    pub fn high_watermark(&self) -> u64 {
        self.high_watermark
    }

    pub fn evict_lock(&self) -> Arc<Semaphore> {
        Arc::clone(&self.evict_lock)
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

pub struct DiskMetricHandles {
    _available: ObservableGauge<f64>,
    _total: ObservableGauge<f64>,
}

pub fn register_disk_metrics(cache: Arc<Cache>) -> DiskMetricHandles {
    let meter = frontcache_metrics::meter();

    let c = cache.clone();
    let _available = meter
        .f64_observable_gauge("disk_available_bytes")
        .with_description("Available disk space in bytes")
        .with_callback(move |observer| {
            let (used, total) = c.disks().iter().fold((0u64, 0u64), |acc, d| {
                (acc.0 + d.used(), acc.1 + d.capacity())
            });
            observer.observe(total.saturating_sub(used) as f64, &[]);
        })
        .build();

    let c = cache.clone();
    let _total = meter
        .f64_observable_gauge("disk_total_bytes")
        .with_description("Total disk space in bytes")
        .with_callback(move |observer| {
            let total: u64 = c.disks().iter().map(|d| d.capacity()).sum();
            observer.observe(total as f64, &[]);
        })
        .build();

    DiskMetricHandles { _available, _total }
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
