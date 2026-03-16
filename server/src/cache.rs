use std::collections::{HashMap, HashSet};
use std::sync::Mutex;
use std::time::Instant;

use dashmap::{DashMap, mapref::entry::Entry};
use futures_util::future::{FutureExt, Shared};
use opentelemetry::KeyValue;
use rayon::prelude::*;
use thiserror::Error;
use tokio::sync::oneshot;

use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::Result;
use bytes::Bytes;

use frontcache_store::{Store, StoreError};

use crate::{
    block::{Block, BlockReader, PendingBlock},
    disk::{Disk, select_disk},
    index::{BlockEntry, BlockKey, Index, IndexError},
    limiter::FetchLimiter,
};

#[derive(Error, Debug, Clone)]
pub enum CacheError {
    #[error("{key}:{offset}: failed to create block file")]
    CreateFile {
        key: String,
        offset: u64,
        #[source]
        source: Arc<anyhow::Error>,
    },
    #[error("{key}:{offset}: failed to update index")]
    IndexUpdate {
        key: String,
        offset: u64,
        #[source]
        source: Arc<IndexError>,
    },
    #[error("{key}:{offset}: failed to read from store")]
    StoreRead {
        key: String,
        offset: u64,
        #[source]
        source: Arc<StoreError>,
    },
    #[error("{key}:{offset}: failed to read block from disk")]
    ReadBlock {
        key: String,
        offset: u64,
        #[source]
        source: Arc<anyhow::Error>,
    },
    #[error("{key}:{offset}: download task aborted")]
    DownloadAborted { key: String, offset: u64 },
    #[error("{key}:{offset}: request throttled by backend fetch limiter")]
    Throttled { key: String, offset: u64 },
}

pub enum CacheHit {
    Disk {
        reader: BlockReader,
        block_size: u64,
        object_size: u64,
        e_tag: String,
    },
    Fresh {
        data: Bytes,
        object_size: u64,
        e_tag: String,
    },
}

impl CacheHit {
    pub fn block_size(&self) -> u64 {
        match self {
            CacheHit::Disk { block_size, .. } => *block_size,
            CacheHit::Fresh { data, .. } => data.len() as u64,
        }
    }

    pub fn object_size(&self) -> u64 {
        match self {
            CacheHit::Disk { object_size, .. } => *object_size,
            CacheHit::Fresh { object_size, .. } => *object_size,
        }
    }

    pub fn e_tag(&self) -> &str {
        match self {
            CacheHit::Disk { e_tag, .. } => e_tag,
            CacheHit::Fresh { e_tag, .. } => e_tag,
        }
    }
}

#[derive(Clone)]
struct FreshHit {
    data: Bytes,
    object_size: u64,
    e_tag: String,
}

type DownloadResult = Arc<Result<FreshHit, CacheError>>;

struct Slot {
    block: Option<Arc<Block>>,
    inflight: Option<Shared<oneshot::Receiver<DownloadResult>>>,
}

type ObjKey = (String, u64);

pub struct Cache {
    states: DashMap<ObjKey, Slot>,
    dirty: Mutex<HashMap<BlockKey, u64>>,
    index: Arc<Index>,
    store: Arc<Store>,
    disks: Vec<Arc<Disk>>,
    limiter: Arc<FetchLimiter>,
    block_size: u64,
}

enum Action {
    Read(Arc<Block>),
    Wait(Shared<oneshot::Receiver<DownloadResult>>),
    Download {
        tx: oneshot::Sender<DownloadResult>,
        rx: Shared<oneshot::Receiver<DownloadResult>>,
    },
}

fn walk_blk_files(dir: &Path) -> std::io::Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        if !entry.path().is_dir() {
            continue;
        }
        for sub in std::fs::read_dir(entry.path())? {
            let path = sub?.path();
            if path.extension().is_some_and(|ext| ext == "blk") {
                files.push(path);
            }
        }
    }
    Ok(files)
}

impl Cache {
    pub fn new(
        index: Arc<Index>,
        store: Arc<Store>,
        disks: Vec<Arc<Disk>>,
        limiter: Arc<FetchLimiter>,
        block_size: u64,
    ) -> Self {
        assert!(block_size > 0, "block_size must be > 0");
        Self {
            states: DashMap::new(),
            dirty: Mutex::new(HashMap::new()),
            index,
            store,
            disks,
            limiter,
            block_size,
        }
    }

    pub fn block_size(&self) -> u64 {
        self.block_size
    }

    pub fn disks(&self) -> &[Arc<Disk>] {
        &self.disks
    }

    fn mark_dirty(&self, key: BlockKey, ts: u64) {
        self.dirty.lock().unwrap().insert(key, ts);
    }

    pub fn slot_count(&self) -> usize {
        self.states.len()
    }

    pub fn block_count(&self) -> usize {
        self.states
            .iter()
            .filter(|entry| entry.value().block.is_some())
            .count()
    }

    pub fn init_from_disk(&self) -> Result<()> {
        let disk_paths: Vec<&Path> = self.disks.iter().map(|d| d.path()).collect();
        let existing: HashSet<PathBuf> = disk_paths
            .par_iter()
            .map(|path| walk_blk_files(path))
            .collect::<std::io::Result<Vec<_>>>()?
            .into_iter()
            .flatten()
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
            let block = Arc::new(Block::new(
                block_path.clone(),
                record.entry.e_tag.clone(),
                record.entry.block_size,
                record.entry.object_size,
                last_accessed,
            ));

            for disk in &self.disks {
                if block_path.starts_with(disk.path()) {
                    disk.add_used(record.entry.block_size);
                    break;
                }
            }

            self.states.entry(block_key).or_insert(Slot {
                block: Some(block),
                inflight: None,
            });
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
                    if let Ok(meta) = std::fs::metadata(path) {
                        for disk in &self.disks {
                            if path.starts_with(disk.path()) {
                                disk.add_used(meta.len());
                                break;
                            }
                        }
                    }
                }
            }
        }

        tracing::info!("Indexed {} blocks from disk", self.block_count());
        Ok(())
    }

    pub async fn get(self: &Arc<Self>, object: &str, offset: u64) -> Result<CacheHit, CacheError> {
        let start = Instant::now();
        let block_offset = (offset / self.block_size) * self.block_size;
        let obj_key: ObjKey = (object.to_string(), block_offset);

        let action = match self.states.entry(obj_key.clone()) {
            Entry::Occupied(mut entry) => {
                let slot = entry.get_mut();

                if let Some(block) = &slot.block {
                    block.record_access();
                    self.mark_dirty(obj_key.clone(), block.last_accessed());
                    Action::Read(block.clone())
                } else if let Some(shared) = &slot.inflight {
                    Action::Wait(shared.clone())
                } else {
                    let (tx, rx) = oneshot::channel();
                    let shared = rx.shared();
                    slot.inflight = Some(shared.clone());
                    Action::Download { tx, rx: shared }
                }
            }
            Entry::Vacant(entry) => {
                let (tx, rx) = oneshot::channel();
                let shared = rx.shared();
                entry.insert(Slot {
                    block: None,
                    inflight: Some(shared.clone()),
                });
                Action::Download { tx, rx: shared }
            }
        };

        match action {
            Action::Read(block) => match block.open_reader() {
                Ok(reader) => {
                    let m = frontcache_metrics::get();
                    m.cache_duration.record(
                        start.elapsed().as_secs_f64() * 1000.0,
                        &[
                            KeyValue::new("result", "hit"),
                            KeyValue::new("operation", "get"),
                        ],
                    );
                    Ok(CacheHit::Disk {
                        reader,
                        block_size: block.block_size(),
                        object_size: block.object_size(),
                        e_tag: block.e_tag().to_string(),
                    })
                }
                Err(e)
                    if e.downcast_ref::<std::io::Error>()
                        .is_some_and(|io| io.kind() == std::io::ErrorKind::NotFound) =>
                {
                    tracing::warn!(
                        "Block file missing for {:?} e_tag={}, re-downloading",
                        obj_key,
                        block.e_tag()
                    );
                    self.evict_block(&obj_key, &block);
                    Box::pin(self.get(object, offset)).await
                }
                Err(e) => Err(CacheError::ReadBlock {
                    key: object.to_string(),
                    offset,
                    source: Arc::new(e),
                }),
            },
            Action::Wait(future) => {
                let outcome = match future.await {
                    Ok(outcome) => outcome,
                    Err(_) => {
                        return Err(CacheError::DownloadAborted {
                            key: object.to_string(),
                            offset,
                        });
                    }
                };

                let m = frontcache_metrics::get();
                m.cache_duration.record(
                    start.elapsed().as_secs_f64() * 1000.0,
                    &[
                        KeyValue::new("result", "wait"),
                        KeyValue::new("operation", "get"),
                    ],
                );

                match outcome.as_ref() {
                    Ok(fresh) => Ok(CacheHit::Fresh {
                        data: fresh.data.clone(),
                        object_size: fresh.object_size,
                        e_tag: fresh.e_tag.clone(),
                    }),
                    Err(e) => Err(e.clone()),
                }
            }
            Action::Download { tx, rx } => {
                let cache = Arc::clone(self);
                let obj_key_owned = obj_key.clone();

                tokio::spawn(async move {
                    let result = cache.download_block(&obj_key_owned).await;

                    if let Some(mut slot) = cache.states.get_mut(&obj_key_owned) {
                        slot.inflight = None;
                    }
                    cache.states.remove_if(&obj_key_owned, |_, s| {
                        s.block.is_none() && s.inflight.is_none()
                    });

                    let shared = Arc::new(match &result {
                        Ok(fresh) => Ok(fresh.clone()),
                        Err(e) => Err(e.clone()),
                    });
                    let _ = tx.send(shared);
                });

                let outcome = rx.await.map_err(|_| CacheError::DownloadAborted {
                    key: object.to_string(),
                    offset,
                })?;

                let m = frontcache_metrics::get();
                let label = match outcome.as_ref() {
                    Ok(_) => "miss",
                    Err(CacheError::Throttled { .. }) => "throttled",
                    Err(_) => "error",
                };
                m.cache_duration.record(
                    start.elapsed().as_secs_f64() * 1000.0,
                    &[
                        KeyValue::new("result", label),
                        KeyValue::new("operation", "get"),
                    ],
                );

                match outcome.as_ref() {
                    Ok(fresh) => Ok(CacheHit::Fresh {
                        data: fresh.data.clone(),
                        object_size: fresh.object_size,
                        e_tag: fresh.e_tag.clone(),
                    }),
                    Err(e) => Err(e.clone()),
                }
            }
        }
    }

    fn evict_block(&self, obj_key: &ObjKey, block: &Block) {
        for disk in &self.disks {
            if block.path().starts_with(disk.path()) {
                disk.sub_used(block.block_size());
                break;
            }
        }
        if let Err(e) = self.index.delete_many(std::slice::from_ref(obj_key)) {
            tracing::error!("Failed to delete index entry for {:?}: {}", obj_key, e);
        }
        if let Some(mut slot) = self.states.get_mut(obj_key) {
            slot.block = None;
        }
        self.states
            .remove_if(obj_key, |_, s| s.block.is_none() && s.inflight.is_none());
    }

    async fn download_block(self: &Arc<Self>, obj_key: &ObjKey) -> Result<FreshHit, CacheError> {
        let (object, block_offset) = obj_key;
        let key = object.clone();
        let offset = *block_offset;
        tracing::info!("Downloading block {}:{}", key, offset);

        let _permit =
            self.limiter
                .acquire_concurrency()
                .await
                .map_err(|_| CacheError::Throttled {
                    key: key.clone(),
                    offset,
                })?;
        self.limiter
            .wait_for_request()
            .await
            .map_err(|_| CacheError::Throttled {
                key: key.clone(),
                offset,
            })?;

        let disk = select_disk(&self.disks);

        let read_result = self
            .store
            .read_range(object, offset, self.block_size)
            .await
            .map_err(|e| CacheError::StoreRead {
                key: key.clone(),
                offset,
                source: Arc::new(e),
            })?;

        let data = read_result.data.clone();
        let object_size = read_result.object_size;
        self.limiter
            .wait_for_bytes(data.len())
            .await
            .map_err(|_| CacheError::Throttled {
                key: key.clone(),
                offset,
            })?;
        let e_tag = read_result.e_tag.clone();

        // Remove old block if present
        let old_block = self.states.get(obj_key).and_then(|slot| slot.block.clone());
        if let Some(old) = &old_block {
            for d in &self.disks {
                if old.path().starts_with(d.path()) {
                    d.sub_used(old.block_size());
                    break;
                }
            }
            let _ = tokio::fs::remove_file(old.path()).await;
        }

        let pending =
            PendingBlock::prepare(disk.path(), read_result.data, e_tag.clone(), object_size)
                .await
                .map_err(|e| CacheError::CreateFile {
                    key: key.clone(),
                    offset,
                    source: Arc::new(e),
                })?;

        let block_key: BlockKey = (object.clone(), offset);
        self.index
            .upsert([(
                block_key.clone(),
                BlockEntry {
                    path: pending.path().to_string_lossy().to_string(),
                    e_tag: e_tag.clone(),
                    block_size: pending.block_size(),
                    object_size,
                },
            )])
            .map_err(|e| CacheError::IndexUpdate {
                key: key.clone(),
                offset,
                source: Arc::new(e),
            })?;

        let block = match pending.persist() {
            Ok(b) => b,
            Err(e) => {
                if let Err(idx_err) = self.index.delete_many(std::slice::from_ref(&block_key)) {
                    tracing::error!("Failed to rollback index entry: {}", idx_err);
                }
                return Err(CacheError::CreateFile {
                    key,
                    offset,
                    source: Arc::new(e),
                });
            }
        };

        disk.add_used(block.block_size());
        if disk.used() > disk.high_watermark()
            && let Ok(permit) = disk.evict_lock().try_acquire_owned()
        {
            let cache = Arc::clone(self);
            let disk = Arc::clone(disk);
            tokio::spawn(async move {
                let _permit = permit;
                let bytes_to_reclaim = disk.used().saturating_sub(disk.low_watermark());
                if let Err(e) = cache.purge_bytes_from(disk.path(), bytes_to_reclaim).await {
                    tracing::error!("Eviction failed for {:?}: {}", disk.path(), e);
                }
            });
        }

        self.mark_dirty(block_key, block.last_accessed());

        let block = Arc::new(block);
        let block_size = block.block_size();
        if let Some(mut slot) = self.states.get_mut(obj_key) {
            slot.block = Some(block);
        }

        let m = frontcache_metrics::get();
        m.disk_byte_changes
            .add(block_size, &[KeyValue::new("action", "downloaded")]);

        Ok(FreshHit {
            data,
            object_size,
            e_tag,
        })
    }

    pub async fn purge_bytes_from(&self, cache_dir: &Path, bytes_to_reclaim: u64) -> Result<()> {
        let start = std::time::Instant::now();
        let mut candidates: Vec<(u64, ObjKey, PathBuf, u64)> = Vec::new();
        for entry in self.states.iter() {
            let obj_key = entry.key();
            if let Some(block) = &entry.value().block
                && block.path().starts_with(cache_dir)
            {
                candidates.push((
                    block.last_accessed(),
                    obj_key.clone(),
                    block.path().to_path_buf(),
                    block.block_size(),
                ));
            }
        }
        candidates.sort_unstable_by_key(|c| c.0);

        let mut victims: Vec<(u64, ObjKey, PathBuf, u64)> = Vec::new();
        let mut total_bytes = 0u64;
        for c in candidates {
            if total_bytes >= bytes_to_reclaim {
                break;
            }
            total_bytes += c.3;
            victims.push(c);
        }

        if victims.is_empty() {
            return Ok(());
        }

        let mut deleted_keys = Vec::with_capacity(victims.len());
        let mut deleted_bytes = 0u64;
        for (_, key, path, size) in &victims {
            let gone = match tokio::fs::remove_file(path).await {
                Ok(()) => true,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => true,
                Err(e) => {
                    tracing::error!("Failed to remove block file {:?}: {}", path, e);
                    false
                }
            };

            if gone {
                if let Some(mut slot) = self.states.get_mut(key) {
                    slot.block = None;
                }
                self.states
                    .remove_if(key, |_, s| s.block.is_none() && s.inflight.is_none());
                for disk in &self.disks {
                    if path.starts_with(disk.path()) {
                        disk.sub_used(*size);
                        break;
                    }
                }
                deleted_keys.push(key.clone());
                deleted_bytes += size;
            }
        }

        if deleted_keys.is_empty() {
            return Ok(());
        }

        self.index.delete_many(&deleted_keys)?;

        let m = frontcache_metrics::get();
        m.disk_byte_changes
            .add(deleted_bytes, &[KeyValue::new("action", "purged")]);
        m.purge_duration
            .record(start.elapsed().as_secs_f64() * 1000.0, &[]);

        Ok(())
    }

    pub fn flush_last_accessed(&self) {
        let start = std::time::Instant::now();
        let entries = std::mem::take(&mut *self.dirty.lock().unwrap());

        if !entries.is_empty()
            && let Err(e) = self.index.flush_last_accessed(entries)
        {
            tracing::error!("Failed to flush last_accessed: {}", e);
        }

        frontcache_metrics::get()
            .flush_duration
            .record(start.elapsed().as_secs_f64() * 1000.0, &[]);
    }
}
