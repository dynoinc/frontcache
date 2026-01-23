use crate::index::Index;
use bytes::Bytes;
use memmap2::Mmap;
use std::fs::File;
use std::io::Result;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::io::AsyncWriteExt;

pub struct Block {
    path: PathBuf,
    _file: File,
    mmap: Mmap,
    index_key: String,
    index: Arc<Index>,
}

impl Block {
    pub async fn new(
        path: PathBuf,
        data: Bytes,
        index_key: String,
        index: Arc<Index>,
    ) -> Result<Self> {
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
            _file: std_file,
            mmap,
            index_key,
            index,
        })
    }

    pub fn from_disk(path: PathBuf, index_key: String, index: Arc<Index>) -> Result<Self> {
        let file = std::fs::OpenOptions::new().read(true).open(&path)?;
        let mmap = unsafe { memmap2::MmapOptions::new().map(&file)? };

        Ok(Self {
            path,
            _file: file,
            mmap,
            index_key,
            index,
        })
    }

    pub fn path(&self) -> &PathBuf {
        &self.path
    }

    pub fn data(&self) -> &[u8] {
        &self.mmap[..]
    }
}

impl Drop for Block {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
        let _ = self.index.delete(&self.index_key);
    }
}
