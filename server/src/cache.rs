use std::time::Instant;

use dashmap::{DashMap, DashSet, mapref::entry::Entry};
use futures_util::future::{FutureExt, Shared};
use opentelemetry::KeyValue;
use thiserror::Error;
use tokio::sync::oneshot;

use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::Result;

use crate::{
    block::{Block, PendingBlock},
    disk::{Disk, select_disk},
    index::{BlockEntry, BlockKey, Index, IndexError},
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
    dirty: DashSet<BlockKey>,
    index: Arc<Index>,
    store: Arc<Store>,
    disks: Vec<Arc<Disk>>,
}

impl Cache {
    pub fn new(index: Arc<Index>, store: Arc<Store>, disks: Vec<Arc<Disk>>) -> Self {
        Self {
            states: DashMap::new(),
            dirty: DashSet::new(),
            index,
            store,
            disks,
        }
    }

    pub fn disks(&self) -> &[Arc<Disk>] {
        &self.disks
    }

    pub fn block_count(&self) -> usize {
        self.states.len()
    }

    pub async fn init_from_disk(&self) -> Result<()> {
        let mut cleanup_keys = Vec::new();

        for (block_key, record) in self.index.list_all()? {
            let block_path = PathBuf::from(&record.entry.path);
            if !block_path.exists() {
                tracing::warn!(
                    "Block {:?} in index is missing from disk, cleaning up",
                    block_key
                );
                cleanup_keys.push(block_key);
                continue;
            }
            let last_accessed = record.last_accessed.unwrap_or_else(Block::now);
            let block = Block::from_disk(
                block_path.clone(),
                record.entry.version.clone(),
                record.entry.size,
                last_accessed,
            )?;

            for disk in &self.disks {
                if block_path.starts_with(disk.path()) {
                    disk.add_used(record.entry.size);
                    break;
                }
            }

            self.states.insert(
                block_key,
                CacheState::Ready {
                    block: Arc::new(block),
                },
            );
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
                    self.dirty.insert(key);
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
                        Err(_) => panic!("download task dropped for {key:?}"),
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

                let download_result = self.download_block(&key).await;
                let shared_result = Arc::new(download_result);

                match shared_result.as_ref() {
                    Ok(block) => {
                        self.states.insert(
                            key,
                            CacheState::Ready {
                                block: block.clone(),
                            },
                        );
                        let m = frontcache_metrics::get();
                        m.block_changes.add(1, &[KeyValue::new("action", "added")]);
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

    async fn download_block(&self, key: &BlockKey) -> Result<Arc<Block>, CacheError> {
        tracing::info!("Downloading block {key:?}");

        let disk = select_disk(&self.disks);

        let (object_key, offset) = key;
        let read_result = match self.store.read_range(object_key, *offset, BLOCK_SIZE).await {
            Ok(r) => r,
            Err(e) => {
                return Err(CacheError::StoreRead(Arc::new(e)));
            }
        };

        let pending =
            match PendingBlock::prepare(disk.path(), read_result.data, read_result.version.clone())
                .await
            {
                Ok(p) => p,
                Err(e) => {
                    return Err(CacheError::CreateFile(Arc::new(e)));
                }
            };

        if let Err(e) = self.index.upsert([(
            key.clone(),
            BlockEntry {
                path: pending.path().to_string_lossy().to_string(),
                version: read_result.version,
                size: pending.size(),
            },
        )]) {
            return Err(CacheError::IndexUpdate(Arc::new(e)));
        }

        let block = match pending.persist() {
            Ok(b) => b,
            Err(e) => {
                let _ = self.index.delete_many(std::slice::from_ref(key));
                return Err(CacheError::CreateFile(Arc::new(e)));
            }
        };

        disk.add_used(block.size());
        self.dirty.insert(key.clone());
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
                    block.size(),
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

        let keys: Vec<BlockKey> = victims.iter().map(|(_, key, _, _)| key.clone()).collect();

        for key in &keys {
            self.states.remove(key);
            self.dirty.remove(key);
        }

        for (_, _, path, _) in &victims {
            if let Err(e) = tokio::fs::remove_file(path).await {
                tracing::error!("Failed to remove block file {:?}: {}", path, e);
            }
        }

        self.index.delete_many(&keys)?;

        for disk in &self.disks {
            let freed: u64 = victims
                .iter()
                .filter(|(_, _, path, _)| path.starts_with(disk.path()))
                .map(|(_, _, _, size)| size)
                .sum();
            if freed > 0 {
                disk.sub_used(freed);
            }
        }

        let m = frontcache_metrics::get();
        m.block_changes
            .add(keys.len() as u64, &[KeyValue::new("action", "removed")]);

        Ok(())
    }

    pub fn flush_last_accessed(&self) {
        let mut dirty_keys = Vec::new();
        self.dirty.retain(|key| {
            dirty_keys.push(key.clone());
            false
        });

        if dirty_keys.is_empty() {
            return;
        }

        let entries: Vec<_> = dirty_keys
            .into_iter()
            .filter_map(|key| {
                let entry = self.states.get(&key)?;
                if let CacheState::Ready { block } = entry.value() {
                    Some((key, block.last_accessed()))
                } else {
                    None
                }
            })
            .collect();

        if let Err(e) = self.index.flush_last_accessed(entries) {
            tracing::error!("Failed to flush last_accessed: {}", e);
        }
    }
}
