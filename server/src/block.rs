use std::{
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

use bytes::Bytes;
use memmap2::Mmap;
use tokio::io::AsyncWriteExt;

use std::path::PathBuf;

use anyhow::Result;

pub struct Block {
    path: PathBuf,
    mmap: Mmap,
    version: String,
    last_accessed: AtomicU64,
}

impl Block {
    pub async fn new(path: PathBuf, data: Bytes, version: String) -> Result<Self> {
        let mut file = tokio::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&path)
            .await?;

        file.set_len(data.len() as u64).await?;
        file.write_all(&data).await?;
        file.sync_all().await?;

        let std_file = file.into_std().await;
        let mmap = unsafe { memmap2::MmapOptions::new().map(&std_file)? };

        Ok(Self {
            path,
            mmap,
            version,
            last_accessed: AtomicU64::new(Self::now()),
        })
    }

    pub fn from_disk(path: PathBuf, version: String, last_accessed: u64) -> Result<Self> {
        let file = std::fs::OpenOptions::new().read(true).open(&path)?;
        let mmap = unsafe { memmap2::MmapOptions::new().map(&file)? };

        Ok(Self {
            path,
            mmap,
            version,
            last_accessed: AtomicU64::new(last_accessed),
        })
    }

    pub fn path(&self) -> &PathBuf {
        &self.path
    }

    pub fn data(&self) -> &[u8] {
        &self.mmap[..]
    }

    pub fn version(&self) -> &str {
        &self.version
    }

    pub fn record_access(&self) {
        self.last_accessed.store(Self::now(), Ordering::Relaxed);
    }

    pub fn last_accessed(&self) -> u64 {
        self.last_accessed.load(Ordering::Relaxed)
    }

    fn now() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
    }
}
