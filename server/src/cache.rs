use std::time::Instant;

use anyhow::bail;
use dashmap::{DashMap, mapref::entry::Entry};
use futures_util::future::{FutureExt, Shared};
use opentelemetry::KeyValue;
use short_uuid::ShortUuid;
use thiserror::Error;
use tokio::sync::oneshot;

use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::Result;

use crate::{
    block::Block,
    disk::{Disk, select_disk},
    index::{BlockEntry, BlockKey, BlockState, Index, IndexError},
    store::{Store, StoreError},
};

pub const BLOCK_SIZE: u64 = 16 * 1024 * 1024;

#[derive(Error, Debug, Clone)]
pub enum CacheError {
    #[error("Failed to create block file")]
    CreateFile(#[source] Arc<anyhow::Error>),
    #[error("Failed to update index")]
    IndexUpdate(#[source] Arc<IndexError>),
    #[error("Failed to read from store")]
    StoreRead(#[source] Arc<StoreError>),
}

type DownloadResult = Arc<Result<Arc<Block>, CacheError>>;

enum CacheState {
    Writing {
        result: Shared<oneshot::Receiver<DownloadResult>>,
    },
    Ready {
        block: Arc<Block>,
    },
}

pub struct Cache {
    states: DashMap<BlockKey, CacheState>,
    index: Arc<Index>,
    store: Arc<Store>,
    disks: Vec<Arc<Disk>>,
}

impl Cache {
    pub fn new(index: Arc<Index>, store: Arc<Store>, disks: Vec<Arc<Disk>>) -> Self {
        Self {
            states: DashMap::new(),
            index,
            store,
            disks,
        }
    }

    pub fn disks(&self) -> &[Arc<Disk>] {
        &self.disks
    }

    pub async fn init_from_disk(&self) -> Result<()> {
        let mut cleanup_keys = Vec::new();

        for (block_key, entry) in self.index.list_all()? {
            match entry.state {
                BlockState::Writing | BlockState::Purging => {
                    tracing::warn!("Found incomplete operation, cleaning up: {:?}", block_key);
                    let block_path = PathBuf::from(&entry.path);
                    let _ = tokio::fs::remove_file(&block_path).await;
                    cleanup_keys.push(block_key);
                }
                BlockState::Downloaded => {
                    let block_path = PathBuf::from(&entry.path);
                    if !block_path.exists() {
                        bail!("Block {:?} in index is missing from disk", block_key);
                    }
                    let block = Block::from_disk(block_path, entry.version.clone())?;
                    self.states.insert(
                        block_key,
                        CacheState::Ready {
                            block: Arc::new(block),
                        },
                    );
                }
            }
        }

        if !cleanup_keys.is_empty() {
            self.index.delete_many(&cleanup_keys)?;
        }

        tracing::info!("Loaded {} blocks from disk", self.states.len());
        Ok(())
    }

    pub async fn get(&self, object: &str, offset: u64) -> Result<Arc<Block>, CacheError> {
        let start = Instant::now();
        let block_offset = (offset / BLOCK_SIZE) * BLOCK_SIZE;
        let key = (object.to_string(), block_offset);

        match self.states.entry(key.clone()) {
            Entry::Occupied(entry) => match entry.get() {
                CacheState::Ready { block } => {
                    block.record_access();
                    let m = frontcache_metrics::get();
                    m.cache_get_duration.record(
                        start.elapsed().as_secs_f64(),
                        &[KeyValue::new("result", "hit")],
                    );
                    Ok(block.clone())
                }
                CacheState::Writing { result } => {
                    let result_future = result.clone();
                    drop(entry);

                    let outcome = match result_future.await {
                        Ok(arc_result) => arc_result.as_ref().clone(),
                        Err(_) => {
                            self.states.remove(&key);
                            Err(CacheError::CreateFile(Arc::new(anyhow::anyhow!(
                                "download task dropped for {key:?}"
                            ))))
                        }
                    };

                    let m = frontcache_metrics::get();
                    m.cache_get_duration.record(
                        start.elapsed().as_secs_f64(),
                        &[KeyValue::new("result", "wait")],
                    );

                    outcome
                }
            },
            Entry::Vacant(entry) => {
                let (tx, rx) = oneshot::channel();
                let result = rx.shared();
                entry.insert(CacheState::Writing {
                    result: result.clone(),
                });

                let short_id = ShortUuid::generate();
                let block_filename = format!("{}.blk", short_id);
                let download_result = self.download_block(&key, &block_filename).await;
                let shared_result = Arc::new(download_result);

                match shared_result.as_ref() {
                    Ok(block) => {
                        self.states.insert(
                            key,
                            CacheState::Ready {
                                block: block.clone(),
                            },
                        );
                    }
                    Err(_) => {
                        self.states.remove(&key);
                    }
                }

                let _ = tx.send(shared_result.clone());

                let m = frontcache_metrics::get();
                m.cache_get_duration.record(
                    start.elapsed().as_secs_f64(),
                    &[KeyValue::new("result", "miss")],
                );

                shared_result.as_ref().clone()
            }
        }
    }

    async fn download_block(
        &self,
        key: &BlockKey,
        block_filename: &str,
    ) -> Result<Arc<Block>, CacheError> {
        tracing::info!("Downloading block {key:?}");

        let cache_dir = select_disk(&self.disks);
        let block_path = cache_dir.join(block_filename);

        let (object_key, offset) = key;
        let read_result = match self.store.read_range(object_key, *offset, BLOCK_SIZE).await {
            Ok(r) => r,
            Err(e) => {
                return Err(CacheError::StoreRead(Arc::new(e)));
            }
        };

        self.index
            .upsert([(
                key.clone(),
                BlockEntry {
                    path: block_path.to_string_lossy().to_string(),
                    state: BlockState::Writing,
                    version: read_result.version.clone(),
                },
            )])
            .map_err(|e| CacheError::IndexUpdate(Arc::new(e)))?;

        let block = match Block::new(
            block_path.clone(),
            read_result.data,
            read_result.version.clone(),
        )
        .await
        {
            Ok(block) => block,
            Err(e) => {
                let _ = tokio::fs::remove_file(&block_path).await;
                let _ = self.index.delete(key);
                return Err(CacheError::CreateFile(Arc::new(e)));
            }
        };

        if let Err(e) = self.index.upsert([(
            key.clone(),
            BlockEntry {
                path: block_path.to_string_lossy().to_string(),
                state: BlockState::Downloaded,
                version: read_result.version,
            },
        )]) {
            let _ = tokio::fs::remove_file(block.path()).await;
            let _ = self.index.delete(key);
            return Err(CacheError::IndexUpdate(Arc::new(e)));
        }

        Ok(Arc::new(block))
    }

    pub async fn purge_many_from(&self, cache_dir: &Path, count: usize) -> Result<()> {
        let mut heap = std::collections::BinaryHeap::with_capacity(count + 1);

        for entry in self.states.iter() {
            if let CacheState::Ready { block } = entry.value()
                && block.path().starts_with(cache_dir)
            {
                let ts = block.last_accessed();
                let item = (
                    ts,
                    entry.key().clone(),
                    block.path().to_path_buf(),
                    block.version().to_string(),
                );

                if heap.len() < count {
                    heap.push(item);
                } else if let Some(mut top) = heap.peek_mut()
                    && ts < top.0
                {
                    *top = item;
                }
            }
        }

        let victims: Vec<_> = heap.into_iter().collect();
        if victims.is_empty() {
            return Ok(());
        }

        self.index
            .upsert(victims.iter().map(|(_, key, path, version)| {
                (
                    key.clone(),
                    BlockEntry {
                        path: path.to_string_lossy().to_string(),
                        state: BlockState::Purging,
                        version: version.clone(),
                    },
                )
            }))?;

        let keys: Vec<BlockKey> = victims.iter().map(|(_, key, _, _)| key.clone()).collect();

        for key in &keys {
            self.states.remove(key);
        }

        for (_, _, path, _) in &victims {
            if let Err(e) = tokio::fs::remove_file(path).await {
                tracing::error!("Failed to remove block file {}: {}", path.display(), e);
            }
        }

        self.index.delete_many(&keys)?;

        Ok(())
    }
}
