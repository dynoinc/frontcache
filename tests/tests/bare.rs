use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use object_store::memory::InMemory;
use object_store::path::Path as ObjPath;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    ObjectStoreExt, PutMultipartOptions, PutOptions, PutPayload, PutResult,
};
use tempfile::TempDir;
use tokio::sync::{Barrier, mpsc};
use tokio_util::sync::CancellationToken;

use frontcache_server::{
    cache::Cache,
    disk::{Disk, start_flusher},
    index::Index,
    store::Store,
};

const BUCKET: &str = "test-bucket";

// ---------------------------------------------------------------------------
// GatedStore — ObjectStore wrapper with deterministic synchronization
// ---------------------------------------------------------------------------

struct GatedStore {
    inner: Arc<InMemory>,
    call_tx: mpsc::Sender<()>,
    gate: Barrier,
    get_count: AtomicUsize,
}

impl fmt::Debug for GatedStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "GatedStore")
    }
}

impl fmt::Display for GatedStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "GatedStore")
    }
}

#[async_trait]
impl ObjectStore for GatedStore {
    async fn put_opts(
        &self,
        location: &ObjPath,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &ObjPath,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &ObjPath,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        self.get_count.fetch_add(1, Ordering::SeqCst);
        let _ = self.call_tx.send(()).await;
        self.gate.wait().await;
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<ObjPath>>,
    ) -> BoxStream<'static, object_store::Result<ObjPath>> {
        self.inner.delete_stream(locations)
    }

    fn list(
        &self,
        prefix: Option<&ObjPath>,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&ObjPath>,
    ) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &ObjPath,
        to: &ObjPath,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

// ---------------------------------------------------------------------------
// BareCache — Cache layer without gRPC stack
// ---------------------------------------------------------------------------

struct BareCache {
    cache: Arc<Cache>,
    store: Arc<GatedStore>,
    call_rx: mpsc::Receiver<()>,
    _tmp: TempDir,
}

fn bare_cache() -> BareCache {
    frontcache_metrics::init();

    let inner = Arc::new(InMemory::new());
    let (call_tx, call_rx) = mpsc::channel(16);
    let gated = Arc::new(GatedStore {
        inner: inner.clone(),
        call_tx,
        gate: Barrier::new(2),
        get_count: AtomicUsize::new(0),
    });

    let tmp = TempDir::new().unwrap();
    let cache_dir = tmp.path().join("cache");
    std::fs::create_dir_all(cache_dir.join("tmp")).unwrap();

    let disk = Disk::new(cache_dir, 1024 * 1024 * 1024);
    let index = Arc::new(Index::open(tmp.path().join("index.db")).unwrap());
    let store = Arc::new(Store::new());
    store.backends.insert(
        ("inmem".to_string(), BUCKET.to_string()),
        gated.clone() as Arc<dyn ObjectStore>,
    );
    let cache = Arc::new(Cache::new(index, store, vec![disk]));
    cache.init_from_disk().unwrap();

    BareCache {
        cache,
        store: gated,
        call_rx,
        _tmp: tmp,
    }
}

fn object_key(path: &str) -> String {
    format!("inmem://{}/{}", BUCKET, path)
}

async fn seed(store: &InMemory, path: &str, data: &[u8]) -> Result<()> {
    store
        .put(&ObjPath::from(path), Bytes::copy_from_slice(data).into())
        .await?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn concurrent_reads_coalesce() -> Result<()> {
    let mut bc = bare_cache();
    seed(&bc.store.inner, "test/coalesce.dat", &vec![42u8; 1024]).await?;

    let key = object_key("test/coalesce.dat");
    let cache1 = bc.cache.clone();
    let cache2 = bc.cache.clone();
    let k1 = key.clone();
    let k2 = key.clone();

    let t1 = tokio::spawn(async move { cache1.get(&k1, 0, None).await });
    let t2 = tokio::spawn(async move { cache2.get(&k2, 0, None).await });

    // Wait for exactly one store call to enter
    bc.call_rx.recv().await.unwrap();
    // Release it
    bc.store.gate.wait().await;

    let r1 = t1.await?;
    let r2 = t2.await?;
    assert!(r1.is_ok());
    assert!(r2.is_ok());
    assert_eq!(bc.store.get_count.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn abort_get_download_survives() -> Result<()> {
    let mut bc = bare_cache();
    seed(&bc.store.inner, "test/abort.dat", &vec![7u8; 1024]).await?;

    let key = object_key("test/abort.dat");
    let cache1 = bc.cache.clone();
    let k1 = key.clone();

    // Start a get that will be aborted
    let task = tokio::spawn(async move { cache1.get(&k1, 0, None).await });

    // Download task is at the gate
    bc.call_rx.recv().await.unwrap();

    // Abort the caller — spawned download task continues
    task.abort();

    // Release the store call — download runs to completion
    bc.store.gate.wait().await;

    // Second get — succeeds via Wait (inflight) or Read (cached)
    let hit = bc.cache.get(&key, 0, None).await;
    assert!(hit.is_ok());
    assert_eq!(bc.store.get_count.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn download_error_reaches_waiters() -> Result<()> {
    let mut bc = bare_cache();
    // Don't seed — key is missing

    let key = object_key("test/missing.dat");
    let cache1 = bc.cache.clone();
    let cache2 = bc.cache.clone();
    let k1 = key.clone();
    let k2 = key.clone();

    let t1 = tokio::spawn(async move { cache1.get(&k1, 0, None).await });
    let t2 = tokio::spawn(async move { cache2.get(&k2, 0, None).await });

    // One store call enters (the other caller coalesces)
    bc.call_rx.recv().await.unwrap();
    // Release — store returns NotFound
    bc.store.gate.wait().await;

    let r1 = t1.await?;
    let r2 = t2.await?;
    assert!(r1.is_err());
    assert!(r2.is_err());
    assert_eq!(bc.store.get_count.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn shutdown_stops_background_tasks() -> Result<()> {
    frontcache_metrics::init();

    let tmp = TempDir::new()?;
    let cache_dir = tmp.path().join("cache");
    std::fs::create_dir_all(cache_dir.join("tmp"))?;

    let disk = Disk::new(cache_dir, 1024 * 1024 * 1024);
    let index = Arc::new(Index::open(tmp.path().join("index.db"))?);
    let store = Arc::new(Store::new());
    let cache = Arc::new(Cache::new(index, store, vec![disk]));
    cache.init_from_disk()?;

    let token = CancellationToken::new();
    let flusher = start_flusher(cache.clone(), token.clone());

    token.cancel();

    tokio::time::timeout(Duration::from_secs(1), flusher)
        .await
        .expect("flusher did not stop in time")
        .expect("flusher panicked");

    Ok(())
}
