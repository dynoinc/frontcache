use std::{
    io::Write,
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

use bytes::Bytes;
use memmap2::Mmap;
use short_uuid::ShortUuid;

use std::path::Path;

use anyhow::Result;

pub struct PendingBlock {
    tmp: tempfile::NamedTempFile,
    path: PathBuf,
    version: String,
    size: u64,
}

impl PendingBlock {
    pub async fn prepare(cache_dir: &Path, data: Bytes, version: String) -> Result<Self> {
        let id = ShortUuid::generate().to_string();
        let prefix = &id[..2];
        let filename = format!("{}.blk", id);

        let final_dir = cache_dir.join(prefix);
        tokio::fs::create_dir_all(&final_dir).await?;

        let tmp = tempfile::NamedTempFile::new_in(cache_dir.join("tmp"))?;
        let size = data.len() as u64;
        let path = final_dir.join(&filename);

        let tmp = tokio::task::spawn_blocking(move || -> Result<tempfile::NamedTempFile> {
            let mut file = tmp;
            file.as_file().set_len(size)?;
            file.write_all(&data)?;
            file.as_file().sync_all()?;
            Ok(file)
        })
        .await??;

        Ok(Self {
            tmp,
            path,
            version,
            size,
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn size(&self) -> u64 {
        self.size
    }

    pub fn persist(self) -> Result<Block> {
        self.tmp.persist(&self.path)?;
        let file = std::fs::OpenOptions::new().read(true).open(&self.path)?;
        let mmap = unsafe { memmap2::MmapOptions::new().map(&file)? };

        Ok(Block {
            path: self.path,
            mmap,
            version: self.version,
            size: self.size,
            last_accessed: AtomicU64::new(Block::now()),
        })
    }
}

pub struct Block {
    path: PathBuf,
    mmap: Mmap,
    version: String,
    size: u64,
    last_accessed: AtomicU64,
}

impl Block {
    pub fn from_disk(
        path: PathBuf,
        version: String,
        size: u64,
        last_accessed: u64,
    ) -> Result<Self> {
        let file = std::fs::OpenOptions::new().read(true).open(&path)?;
        let mmap = unsafe { memmap2::MmapOptions::new().map(&file)? };

        Ok(Self {
            path,
            mmap,
            version,
            size,
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

    pub fn size(&self) -> u64 {
        self.size
    }

    pub fn record_access(&self) {
        self.last_accessed.store(Self::now(), Ordering::Relaxed);
    }

    pub fn last_accessed(&self) -> u64 {
        self.last_accessed.load(Ordering::Relaxed)
    }

    pub fn now() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
    }
}
