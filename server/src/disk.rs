use crate::cache::Cache;
use anyhow::Result;
use std::ffi::CString;
use std::os::unix::ffi::OsStrExt;
use std::path::Path;
use std::sync::Arc;
use tokio::time::{Duration, sleep};

pub struct DiskStats {
    pub used: u64,
    pub threshold: u64,
}

pub fn get_disk_stats(path: &Path) -> Result<DiskStats> {
    let c_path = CString::new(path.as_os_str().as_bytes())?;
    let mut stat: libc::statvfs = unsafe { std::mem::zeroed() };
    let rc = unsafe { libc::statvfs(c_path.as_ptr(), &mut stat) };
    if rc != 0 {
        return Err(anyhow::anyhow!(
            "statvfs failed: {}",
            std::io::Error::last_os_error()
        ));
    }

    let block_size = if stat.f_frsize > 0 {
        stat.f_frsize as u64
    } else {
        stat.f_bsize as u64
    };

    let total = block_size.saturating_mul(stat.f_blocks as u64);
    let available = block_size.saturating_mul(stat.f_bavail as u64);
    let used = total.saturating_sub(available);
    let threshold = (total * 95) / 100;

    Ok(DiskStats { used, threshold })
}

async fn purge_until_below_threshold(cache: &Cache) -> Result<()> {
    loop {
        let stats = get_disk_stats(cache.cache_dir())?;
        if stats.used < stats.threshold {
            return Ok(());
        }
        if !cache.purge_one().await? {
            return Ok(());
        }
    }
}

pub fn start_purger(cache: Arc<Cache>) {
    tokio::spawn(async move {
        loop {
            sleep(Duration::from_secs(10)).await;
            if let Err(e) = purge_until_below_threshold(&cache).await {
                tracing::error!("Purger failed: {}", e);
            }
        }
    });
}
