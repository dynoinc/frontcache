use std::time::Instant;

use anyhow::bail;
use dashmap::{DashMap, mapref::entry::Entry};
use futures_util::future::{FutureExt, Shared};
use opentelemetry::KeyValue;
use thiserror::Error;
use tokio::sync::oneshot;
use uuid::Uuid;

use crate::{
    block::Block,
    disk::{Disk, select_disk},
    index::{BlockEntry, BlockKey, BlockState, Index, IndexError},
    prelude::*,
    store::{Store, StoreError},
};

pub const BLOCK_SIZE: u64 = 16 * 1024 * 1024;

#[derive(Error, Debug, Clone)]
pub enum DownloadError {
    #[error("Failed to create block file")]
    CreateFile(#[source] Arc<anyhow::Error>),
    #[error("Failed to update index")]
    IndexUpdate(#[source] Arc<IndexError>),
    #[error("Failed to read from store")]
    StoreRead(#[source] Arc<StoreError>),
}

type DownloadResult = Arc<Result<Arc<Block>, DownloadError>>;

enum CacheState {
    Downloading {
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
        for (block_key, entry) in self.index.list_all()? {
            match entry.state {
                BlockState::Downloading | BlockState::Purging => {
                    tracing::warn!("Found incomplete operation, cleaning up: {:?}", block_key);
                    let block_path = PathBuf::from(&entry.path);
                    let _ = tokio::fs::remove_file(&block_path).await;
                    self.index.delete(&block_key)?;
                }
                BlockState::Downloaded => {
                    let block_path = PathBuf::from(&entry.path);
                    if !block_path.exists() {
                        bail!("Block {:?} in index is missing from disk", block_key);
                    }
                    let block = Block::from_disk(
                        block_path.clone(),
                        block_key.clone(),
                        self.index.clone(),
                    )?;
                    self.states.insert(
                        block_key,
                        CacheState::Ready {
                            block: Arc::new(block),
                        },
                    );
                }
            }
        }

        tracing::info!("Loaded {} blocks from disk", self.states.len());
        Ok(())
    }

    pub async fn get(&self, object: &str, offset: u64) -> Result<Arc<Block>> {
        let start = Instant::now();
        let block_offset = (offset / BLOCK_SIZE) * BLOCK_SIZE;
        let key = (object.to_string(), block_offset);

        match self.states.entry(key.clone()) {
            Entry::Occupied(entry) => match entry.get() {
                CacheState::Ready { block, .. } => {
                    block.record_access();
                    let m = frontcache_metrics::get();
                    m.cache_get_duration.record(
                        start.elapsed().as_secs_f64(),
                        &[KeyValue::new("result", "hit")],
                    );
                    Ok(block.clone())
                }
                CacheState::Downloading { result } => {
                    let result_future = result.clone();
                    drop(entry);

                    let outcome = match result_future.await {
                        Ok(arc_result) => match arc_result.as_ref() {
                            Ok(block) => Ok(block.clone()),
                            Err(e) => Err(e.clone().into()),
                        },
                        Err(_) => Err(anyhow::anyhow!("Download task dropped")),
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
                entry.insert(CacheState::Downloading {
                    result: result.clone(),
                });

                let uuid = Uuid::new_v4().simple().to_string();
                let block_filename = format!("{}.blk", &uuid[..16]);
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

                match shared_result.as_ref() {
                    Ok(block) => Ok(block.clone()),
                    Err(e) => Err(e.clone().into()),
                }
            }
        }
    }

    async fn download_block(
        &self,
        key: &BlockKey,
        block_filename: &str,
    ) -> Result<Arc<Block>, DownloadError> {
        tracing::info!("Downloading block {key:?}");

        let cache_dir = select_disk(&self.disks);
        let block_path = cache_dir.join(block_filename);

        self.index
            .upsert([(
                key.clone(),
                BlockEntry {
                    path: block_path.to_string_lossy().to_string(),
                    state: BlockState::Downloading,
                },
            )])
            .map_err(|e| DownloadError::IndexUpdate(Arc::new(e)))?;

        let (object_key, offset) = key;
        let data = match self.store.read_range(object_key, *offset, BLOCK_SIZE).await {
            Ok(data) => data,
            Err(e) => {
                let _ = self.index.delete(key);
                return Err(DownloadError::StoreRead(Arc::new(e)));
            }
        };
        let block =
            match Block::new(block_path.clone(), data, key.clone(), self.index.clone()).await {
                Ok(block) => block,
                Err(e) => {
                    let _ = tokio::fs::remove_file(&block_path).await;
                    let _ = self.index.delete(key);
                    return Err(DownloadError::CreateFile(Arc::new(e)));
                }
            };

        if let Err(e) = self.index.upsert([(
            key.clone(),
            BlockEntry {
                path: block_path.to_string_lossy().to_string(),
                state: BlockState::Downloaded,
            },
        )]) {
            let _ = tokio::fs::remove_file(block.path()).await;
            let _ = self.index.delete(key);
            return Err(DownloadError::IndexUpdate(Arc::new(e)));
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

                if heap.len() < count {
                    heap.push((ts, entry.key().clone()));
                } else if let Some(mut top) = heap.peek_mut()
                    && ts < top.0
                {
                    *top = (ts, entry.key().clone());
                }
            }
        }

        let victims: Vec<_> = heap.into_iter().collect();
        if victims.is_empty() {
            return Ok(());
        }

        self.index.upsert(victims.iter().map(|(_, key)| {
            (
                key.clone(),
                BlockEntry {
                    path: cache_dir.join("purged").to_string_lossy().to_string(),
                    state: BlockState::Purging,
                },
            )
        }))?;

        for (_, key) in victims {
            if let Some((_, CacheState::Ready { block })) = self.states.remove(&key) {
                block.delete_on_drop();
            }
        }

        Ok(())
    }
}
