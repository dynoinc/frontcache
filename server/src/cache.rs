use std::collections::{BTreeMap, HashSet};
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
    #[error("Version mismatch: upstream={upstream}, requested={requested}")]
    VersionMismatch { upstream: String, requested: String },
}

pub enum CacheHit {
    Disk { reader: BlockReader, size: u64 },
    Fresh { data: Bytes },
}

impl CacheHit {
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
}

type DownloadResult = Arc<Result<FreshHit, CacheError>>;

struct Slot {
    versions: BTreeMap<String, Arc<Block>>,
    inflight: BTreeMap<Option<String>, Shared<oneshot::Receiver<DownloadResult>>>,
}

impl Slot {
    fn new() -> Self {
        Self {
            versions: BTreeMap::new(),
            inflight: BTreeMap::new(),
        }
    }
}

type ObjKey = (String, u64);

pub struct Cache {
    states: DashMap<ObjKey, Slot>,
    dirty: DashSet<BlockKey>,
    index: Arc<Index>,
    store: Arc<Store>,
    disks: Vec<Arc<Disk>>,
}

enum Action {
    Read(Arc<Block>),
    Wait(Shared<oneshot::Receiver<DownloadResult>>),
    Download {
        tx: oneshot::Sender<DownloadResult>,
        rx: Shared<oneshot::Receiver<DownloadResult>>,
        had_versions: bool,
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
        self.states
            .iter()
            .map(|entry| entry.value().versions.len())
            .sum()
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
                record.entry.version.clone(),
                record.entry.size,
                last_accessed,
            ));

            for disk in &self.disks {
                if block_path.starts_with(disk.path()) {
                    disk.add_used(record.entry.size);
                    break;
                }
            }

            let obj_key = (block_key.0, block_key.1);
            let version = block_key.2;
            self.states
                .entry(obj_key)
                .or_insert_with(Slot::new)
                .versions
                .insert(version, block);
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

        tracing::info!("Indexed {} blocks from disk", self.block_count());
        Ok(())
    }

    pub async fn get(
        self: &Arc<Self>,
        object: &str,
        offset: u64,
        version: Option<&str>,
    ) -> Result<CacheHit, CacheError> {
        let start = Instant::now();
        let block_offset = (offset / BLOCK_SIZE) * BLOCK_SIZE;
        let obj_key: ObjKey = (object.to_string(), block_offset);

        let action = match self.states.entry(obj_key.clone()) {
            Entry::Occupied(mut entry) => {
                let slot = entry.get_mut();

                let block = match version {
                    Some(v) => slot.versions.get(v).cloned(),
                    None => slot.versions.values().next().cloned(),
                };
                if let Some(block) = block {
                    block.record_access();
                    self.dirty
                        .insert((obj_key.0.clone(), obj_key.1, block.version().to_string()));
                    Action::Read(block)
                } else {
                    let inflight_key = version.map(|v| v.to_string());
                    if let Some(shared) = slot.inflight.get(&inflight_key) {
                        Action::Wait(shared.clone())
                    } else {
                        let (tx, rx) = oneshot::channel();
                        let had_versions = !slot.versions.is_empty();
                        let shared = rx.shared();
                        slot.inflight.insert(inflight_key, shared.clone());
                        Action::Download {
                            tx,
                            rx: shared,
                            had_versions,
                        }
                    }
                }
            }
            Entry::Vacant(entry) => {
                let (tx, rx) = oneshot::channel();
                let inflight_key = version.map(|v| v.to_string());
                let mut slot = Slot::new();
                let shared = rx.shared();
                slot.inflight.insert(inflight_key, shared.clone());
                entry.insert(slot);
                Action::Download {
                    tx,
                    rx: shared,
                    had_versions: false,
                }
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
                        size: block.size(),
                    })
                }
                Err(e)
                    if e.downcast_ref::<std::io::Error>()
                        .is_some_and(|io| io.kind() == std::io::ErrorKind::NotFound) =>
                {
                    tracing::warn!(
                        "Block file missing for {:?} version={}, re-downloading",
                        obj_key,
                        block.version()
                    );
                    self.evict_version(&obj_key, block.version(), &block);
                    Box::pin(self.get(object, offset, version)).await
                }
                Err(e) => Err(CacheError::ReadBlock(Arc::new(e))),
            },
            Action::Wait(future) => {
                let outcome = future.await.unwrap();

                let m = frontcache_metrics::get();
                m.cache_get_duration.record(
                    start.elapsed().as_secs_f64(),
                    &[KeyValue::new("result", "wait")],
                );

                match outcome.as_ref() {
                    Ok(fresh) => Ok(CacheHit::Fresh {
                        data: fresh.data.clone(),
                    }),
                    Err(e) => Err(e.clone()),
                }
            }
            Action::Download {
                tx,
                rx,
                had_versions,
            } => {
                let cache = Arc::clone(self);
                let obj_key_owned = obj_key.clone();
                let version_owned = version.map(|v| v.to_string());

                tokio::spawn(async move {
                    let result = cache
                        .download_block(&obj_key_owned, version_owned.as_deref(), had_versions)
                        .await;

                    if let Some(mut slot) = cache.states.get_mut(&obj_key_owned) {
                        slot.inflight.remove(&version_owned);
                    }

                    let shared = Arc::new(match &result {
                        Ok(fresh) => Ok(fresh.clone()),
                        Err(e) => Err(e.clone()),
                    });
                    let _ = tx.send(shared);
                });

                let outcome = rx.await.unwrap();

                let m = frontcache_metrics::get();
                let label = if outcome.is_ok() { "miss" } else { "error" };
                m.cache_get_duration.record(
                    start.elapsed().as_secs_f64(),
                    &[KeyValue::new("result", label)],
                );

                match outcome.as_ref() {
                    Ok(fresh) => Ok(CacheHit::Fresh {
                        data: fresh.data.clone(),
                    }),
                    Err(e) => Err(e.clone()),
                }
            }
        }
    }

    fn evict_version(&self, obj_key: &ObjKey, version: &str, block: &Block) {
        for disk in &self.disks {
            if block.path().starts_with(disk.path()) {
                disk.sub_used(block.size());
                break;
            }
        }
        let block_key: BlockKey = (obj_key.0.clone(), obj_key.1, version.to_string());
        if let Err(e) = self.index.delete_many(std::slice::from_ref(&block_key)) {
            tracing::error!("Failed to delete index entry for {:?}: {}", block_key, e);
        }
        if let Some(mut slot) = self.states.get_mut(obj_key) {
            slot.versions.remove(version);
        }
        self.dirty.remove(&block_key);
    }

    async fn download_block(
        self: &Arc<Self>,
        obj_key: &ObjKey,
        requested_version: Option<&str>,
        had_versions: bool,
    ) -> Result<FreshHit, CacheError> {
        let (object, block_offset) = obj_key;
        tracing::info!("Downloading block {}:{}", object, block_offset);

        // HEAD check: only when we have cached versions but not the requested one.
        // Cold misses skip HEAD — high chance the requested version is the current one.
        if had_versions && let Some(requested) = requested_version {
            let upstream_ver = self
                .store
                .head(object)
                .await
                .map_err(|e| CacheError::StoreRead(Arc::new(e)))?;
            if upstream_ver != requested {
                return Err(CacheError::VersionMismatch {
                    upstream: upstream_ver,
                    requested: requested.to_string(),
                });
            }
        }

        let disk = select_disk(&self.disks);

        let read_result = self
            .store
            .read_range(object, *block_offset, BLOCK_SIZE)
            .await
            .map_err(|e| CacheError::StoreRead(Arc::new(e)))?;

        let data = read_result.data.clone();
        let version = read_result.version.clone();

        let pending = PendingBlock::prepare(disk.path(), read_result.data, version.clone())
            .await
            .map_err(|e| CacheError::CreateFile(Arc::new(e)))?;

        let block_key: BlockKey = (object.clone(), *block_offset, version.clone());
        self.index
            .upsert([(
                block_key.clone(),
                BlockEntry {
                    path: pending.path().to_string_lossy().to_string(),
                    version: version.clone(),
                    size: pending.size(),
                },
            )])
            .map_err(|e| CacheError::IndexUpdate(Arc::new(e)))?;

        let block = match pending.persist() {
            Ok(b) => b,
            Err(e) => {
                if let Err(idx_err) = self.index.delete_many(&[block_key]) {
                    tracing::error!("Failed to rollback index entry: {}", idx_err);
                }
                return Err(CacheError::CreateFile(Arc::new(e)));
            }
        };

        disk.add_used(block.size());
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

        self.dirty.insert(block_key);

        let block = Arc::new(block);
        if let Some(mut slot) = self.states.get_mut(obj_key) {
            slot.versions.insert(version.clone(), block);
        }

        let m = frontcache_metrics::get();
        m.block_changes.add(1, &[KeyValue::new("action", "added")]);

        if let Some(requested) = requested_version
            && version != requested
        {
            return Err(CacheError::VersionMismatch {
                upstream: version,
                requested: requested.to_string(),
            });
        }

        Ok(FreshHit { data })
    }

    pub async fn purge_bytes_from(&self, cache_dir: &Path, bytes_to_reclaim: u64) -> Result<()> {
        let mut candidates: Vec<(u64, BlockKey, PathBuf, u64)> = Vec::new();
        for entry in self.states.iter() {
            let obj_key = entry.key();
            for (version, block) in &entry.value().versions {
                if block.path().starts_with(cache_dir) {
                    candidates.push((
                        block.last_accessed(),
                        (obj_key.0.clone(), obj_key.1, version.clone()),
                        block.path().to_path_buf(),
                        block.size(),
                    ));
                }
            }
        }
        candidates.sort_unstable_by_key(|c| c.0);

        let mut victims: Vec<(u64, BlockKey, PathBuf, u64)> = Vec::new();
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
                let obj_key: ObjKey = (key.0.clone(), key.1);
                if let Some(mut slot) = self.states.get_mut(&obj_key) {
                    slot.versions.remove(&key.2);
                }
                self.dirty.remove(key);
                for disk in &self.disks {
                    if path.starts_with(disk.path()) {
                        disk.sub_used(*size);
                        break;
                    }
                }
                deleted_keys.push(key.clone());
            }
        }

        if deleted_keys.is_empty() {
            return Ok(());
        }

        self.index.delete_many(&deleted_keys)?;

        let m = frontcache_metrics::get();
        m.block_changes.add(
            deleted_keys.len() as u64,
            &[KeyValue::new("action", "removed")],
        );

        Ok(())
    }

    pub fn flush_last_accessed(&self) {
        let now = Block::now();
        let mut entries = Vec::new();
        self.dirty.retain(|key| {
            entries.push((key.clone(), now));
            false
        });

        if !entries.is_empty()
            && let Err(e) = self.index.flush_last_accessed(entries)
        {
            tracing::error!("Failed to flush last_accessed: {}", e);
        }
    }
}
