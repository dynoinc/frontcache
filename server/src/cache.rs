use std::collections::HashSet;
use std::time::Instant;

use dashmap::{DashMap, DashSet, mapref::entry::Entry};
use futures_util::future::{FutureExt, Shared};
use opentelemetry::KeyValue;
use rayon::prelude::*;
use thiserror::Error;
use tokio::sync::oneshot;

use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::Result;
use bytes::Bytes;

use crate::{
    block::{Block, BlockReader, PendingBlock},
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
    #[error("Failed to read block from disk")]
    ReadBlock(#[source] Arc<anyhow::Error>),
}

pub enum CacheHit {
    Disk {
        reader: BlockReader,
        version: String,
        size: u64,
    },
    Fresh {
        data: Bytes,
        version: String,
    },
}

impl CacheHit {
    pub fn version(&self) -> &str {
        match self {
            CacheHit::Disk { version, .. } => version,
            CacheHit::Fresh { version, .. } => version,
        }
    }

    pub fn size(&self) -> u64 {
        match self {
            CacheHit::Disk { size, .. } => *size,
            CacheHit::Fresh { data, .. } => data.len() as u64,
        }
    }
}

#[derive(Clone)]
struct FreshHit {
    data: Bytes,
    version: String,
}

type DownloadResult = Arc<Result<FreshHit, CacheError>>;

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

enum Action {
    Read(Arc<Block>),
    Wait(Shared<oneshot::Receiver<DownloadResult>>),
    Download(oneshot::Sender<DownloadResult>),
}

fn walk_blk_files(dir: &Path) -> Vec<PathBuf> {
    let mut files = Vec::new();
    let entries = match std::fs::read_dir(dir) {
        Ok(e) => e,
        Err(e) => {
            tracing::error!("Failed to read cache directory {:?}: {}", dir, e);
            return files;
        }
    };
    for entry in entries {
        let entry = match entry {
            Ok(e) => e,
            Err(e) => {
                tracing::warn!("Failed to read entry in {:?}: {}", dir, e);
                continue;
            }
        };
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }
        match std::fs::read_dir(&path) {
            Ok(subdir) => {
                for sub_entry in subdir {
                    let sub_entry = match sub_entry {
                        Ok(e) => e,
                        Err(e) => {
                            tracing::warn!("Failed to read entry in {:?}: {}", path, e);
                            continue;
                        }
                    };
                    let sub_path = sub_entry.path();
                    if sub_path.extension().is_some_and(|ext| ext == "blk") {
                        files.push(sub_path);
                    }
                }
            }
            Err(e) => {
                tracing::warn!("Failed to read subdir {:?}: {}", path, e);
            }
        }
    }
    files
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

    pub fn init_from_disk(&self) -> Result<()> {
        let disk_paths: Vec<&Path> = self.disks.iter().map(|d| d.path()).collect();
        let existing: HashSet<PathBuf> = disk_paths
            .par_iter()
            .flat_map_iter(|path| walk_blk_files(path))
            .collect();

        let mut cleanup_keys = Vec::new();
        let mut indexed_paths = HashSet::new();

        for (block_key, record) in self.index.list_all()? {
            let block_path = PathBuf::from(&record.entry.path);
            if !existing.contains(&block_path) {
                cleanup_keys.push(block_key);
                continue;
            }

            indexed_paths.insert(block_path.clone());

            let last_accessed = record.last_accessed.unwrap_or_else(Block::now);
            let block = Block::new(
                block_path.clone(),
                record.entry.version.clone(),
                record.entry.size,
                last_accessed,
            );

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
            tracing::warn!("Cleaning up {} stale index entries", cleanup_keys.len());
            self.index.delete_many(&cleanup_keys)?;
        }

        let orphans: Vec<_> = existing.difference(&indexed_paths).collect();
        if !orphans.is_empty() {
            tracing::warn!("Deleting {} orphan block files not in index", orphans.len());
            for path in &orphans {
                if let Err(e) = std::fs::remove_file(path) {
                    tracing::error!("Failed to remove orphan {:?}: {}", path, e);
                }
            }
        }

        tracing::info!("Indexed {} blocks from disk", self.states.len());
        Ok(())
    }

    pub async fn get(&self, object: &str, offset: u64) -> Result<CacheHit, CacheError> {
        let start = Instant::now();
        let block_offset = (offset / BLOCK_SIZE) * BLOCK_SIZE;
        let key = (object.to_string(), block_offset);

        let action = match self.states.entry(key.clone()) {
            Entry::Occupied(entry) => match entry.get() {
                CacheState::Ready { block } => {
                    let block = block.clone();
                    block.record_access();
                    self.dirty.insert(key.clone());
                    Action::Read(block)
                }
                CacheState::Writing { result } => Action::Wait(result.clone()),
            },
            Entry::Vacant(entry) => {
                let (tx, rx) = oneshot::channel();
                entry.insert(CacheState::Writing {
                    result: rx.shared(),
                });
                Action::Download(tx)
            }
        };

        match action {
            Action::Read(block) => match block.open_reader() {
                Ok(reader) => {
                    let m = frontcache_metrics::get();
                    m.cache_get_duration.record(
                        start.elapsed().as_secs_f64(),
                        &[KeyValue::new("result", "hit")],
                    );
                    Ok(CacheHit::Disk {
                        reader,
                        version: block.version().to_owned(),
                        size: block.size(),
                    })
                }
                Err(e)
                    if e.downcast_ref::<std::io::Error>()
                        .is_some_and(|io| io.kind() == std::io::ErrorKind::NotFound) =>
                {
                    tracing::warn!("Block file missing for {:?}, re-downloading", key);
                    let stale_path = block.path().clone();
                    let evicted = match self.states.entry(key.clone()) {
                        Entry::Occupied(e)
                            if matches!(
                                e.get(),
                                CacheState::Ready { block: b } if *b.path() == stale_path
                            ) =>
                        {
                            Some(e.remove_entry().1)
                        }
                        _ => None,
                    };
                    if let Some(CacheState::Ready { block: stale }) = evicted {
                        self.evict_from_disk(&key, &stale);
                    }
                    Box::pin(self.get(object, offset)).await
                }
                Err(e) => Err(CacheError::ReadBlock(Arc::new(e))),
            },
            Action::Wait(future) => {
                let outcome = match future.await {
                    Ok(arc_result) => arc_result,
                    Err(_) => panic!("download task dropped for {key:?}"),
                };

                let m = frontcache_metrics::get();
                m.cache_get_duration.record(
                    start.elapsed().as_secs_f64(),
                    &[KeyValue::new("result", "wait")],
                );

                match outcome.as_ref() {
                    Ok(fresh) => Ok(CacheHit::Fresh {
                        data: fresh.data.clone(),
                        version: fresh.version.clone(),
                    }),
                    Err(e) => Err(e.clone()),
                }
            }
            Action::Download(tx) => {
                let result = self.download_block(&key).await;

                let shared = Arc::new(match &result {
                    Ok(fresh) => Ok(fresh.clone()),
                    Err(e) => {
                        self.states.remove(&key);
                        Err(e.clone())
                    }
                });

                if result.is_ok() {
                    let m = frontcache_metrics::get();
                    m.block_changes.add(1, &[KeyValue::new("action", "added")]);
                }

                let _ = tx.send(shared);

                let m = frontcache_metrics::get();
                m.cache_get_duration.record(
                    start.elapsed().as_secs_f64(),
                    &[KeyValue::new("result", "miss")],
                );

                match result {
                    Ok(fresh) => Ok(CacheHit::Fresh {
                        data: fresh.data,
                        version: fresh.version,
                    }),
                    Err(e) => Err(e),
                }
            }
        }
    }

    fn evict_from_disk(&self, key: &BlockKey, block: &Block) {
        for disk in &self.disks {
            if block.path().starts_with(disk.path()) {
                disk.sub_used(block.size());
                break;
            }
        }
        if let Err(e) = self.index.delete_many(std::slice::from_ref(key)) {
            tracing::error!("Failed to delete index entry for {:?}: {}", key, e);
        }
    }

    async fn download_block(&self, key: &BlockKey) -> Result<FreshHit, CacheError> {
        tracing::info!("Downloading block {key:?}");

        let disk = select_disk(&self.disks);

        let (object_key, offset) = key;
        let read_result = match self.store.read_range(object_key, *offset, BLOCK_SIZE).await {
            Ok(r) => r,
            Err(e) => {
                return Err(CacheError::StoreRead(Arc::new(e)));
            }
        };

        let data = read_result.data.clone();

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
                version: read_result.version.clone(),
                size: pending.size(),
            },
        )]) {
            return Err(CacheError::IndexUpdate(Arc::new(e)));
        }

        let block = match pending.persist() {
            Ok(b) => b,
            Err(e) => {
                if let Err(idx_err) = self.index.delete_many(std::slice::from_ref(key)) {
                    tracing::error!("Failed to rollback index entry for {:?}: {}", key, idx_err);
                }
                return Err(CacheError::CreateFile(Arc::new(e)));
            }
        };

        disk.add_used(block.size());
        self.dirty.insert(key.clone());
        self.states.insert(
            key.clone(),
            CacheState::Ready {
                block: Arc::new(block),
            },
        );

        Ok(FreshHit {
            data,
            version: read_result.version,
        })
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
