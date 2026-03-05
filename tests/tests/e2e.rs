use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use bytes::Bytes;
use futures::TryStreamExt;
use object_store::{ObjectStore, ObjectStoreExt, memory::InMemory, path::Path as ObjPath};
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;

use frontcache_client::CacheClient;
use frontcache_proto::{
    cache_service_server::CacheServiceServer, router_service_server::RouterServiceServer,
};
use frontcache_router::{ring::Straw2Router, server::RouterServer};
use frontcache_server::{
    cache::Cache, disk::Disk, index::Index, limiter::FetchLimiter, server::CacheServer,
    store::Store,
};

const BUCKET: &str = "test-bucket";
const DATA_42_1K: &[u8] = &[42u8; 1024];

fn object_key(path: &str) -> String {
    format!("inmem://{}/{}", BUCKET, path)
}

struct TestCluster {
    client: CacheClient,
    mock: Arc<InMemory>,
    _tmp: TempDir,
}

async fn start_cluster() -> Result<TestCluster> {
    frontcache_metrics::init();

    let mock = Arc::new(InMemory::new());

    let tmp = TempDir::new()?;
    let cache_dir = tmp.path().join("cache");
    std::fs::create_dir_all(cache_dir.join("tmp"))?;

    let disk = Disk::new(cache_dir, 1024 * 1024 * 1024);
    let index = Arc::new(Index::open(tmp.path().join("index.db"))?);
    let store = Arc::new(Store::new());
    store.backends.insert(
        ("inmem".to_string(), BUCKET.to_string()),
        mock.clone() as Arc<dyn ObjectStore>,
    );
    let cache = Arc::new(Cache::new(
        index,
        store,
        vec![disk],
        Arc::new(FetchLimiter::from_env()),
    ));
    cache.init_from_disk()?;

    // Cache server
    let server_listener = TcpListener::bind("127.0.0.1:0").await?;
    let server_addr = server_listener.local_addr()?;
    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .layer(frontcache_metrics::layer())
            .add_service(CacheServiceServer::new(CacheServer::new(cache)))
            .serve_with_incoming(TcpListenerStream::new(server_listener))
            .await
            .unwrap();
    });

    // Router
    let ring = Arc::new(Straw2Router::new());
    ring.add_node(server_addr.to_string());
    let router_listener = TcpListener::bind("127.0.0.1:0").await?;
    let router_addr = router_listener.local_addr()?;
    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .layer(frontcache_metrics::layer())
            .add_service(RouterServiceServer::new(RouterServer::new(ring)))
            .serve_with_incoming(TcpListenerStream::new(router_listener))
            .await
            .unwrap();
    });

    // Connect client (retry until servers are listening)
    let addr = format!("http://{}", router_addr);
    let client = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match CacheClient::new(addr.clone()).await {
                Ok(c) => return c,
                Err(_) => tokio::time::sleep(Duration::from_millis(10)).await,
            }
        }
    })
    .await
    .expect("servers did not start in time");

    Ok(TestCluster {
        client,
        mock,
        _tmp: tmp,
    })
}

async fn read_all(
    client: &CacheClient,
    key: &str,
    range: impl std::ops::RangeBounds<u64>,
    version: Option<&str>,
) -> Result<Vec<u8>> {
    let mut stream = client.stream_range(key, range, version).await?;
    let mut buf = Vec::new();
    while let Some(chunk) = stream.try_next().await? {
        buf.extend_from_slice(&chunk);
    }
    Ok(buf)
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
async fn basic_read() -> Result<()> {
    let c = start_cluster().await?;

    seed(&c.mock, "test/file.dat", DATA_42_1K).await?;

    let got = read_all(&c.client, &object_key("test/file.dat"), 0..1024, None).await?;
    assert_eq!(got.as_slice(), DATA_42_1K);
    Ok(())
}

#[tokio::test]
async fn cache_hit() -> Result<()> {
    let c = start_cluster().await?;
    let path = "test/cached.dat";

    let data = vec![7u8; 2048];
    seed(&c.mock, path, &data).await?;

    // First read — cache miss, fetches from store
    let got = read_all(&c.client, &object_key(path), 0..2048, None).await?;
    assert_eq!(got, data);

    // Delete from mock store
    c.mock.delete(&ObjPath::from(path)).await?;

    // Second read — served from cache (store no longer has it)
    let got = read_all(&c.client, &object_key(path), 0..2048, None).await?;
    assert_eq!(got, data);
    Ok(())
}

#[tokio::test]
async fn range_read() -> Result<()> {
    let c = start_cluster().await?;
    let path = "test/range.dat";

    let data: Vec<u8> = (0..=255u8).cycle().take(65536).collect();
    seed(&c.mock, path, &data).await?;

    let got = read_all(&c.client, &object_key(path), 1000..6000, None).await?;
    assert_eq!(got, &data[1000..6000]);
    Ok(())
}

#[tokio::test]
async fn not_found() -> Result<()> {
    let c = start_cluster().await?;

    let result = read_all(&c.client, &object_key("test/missing.dat"), 0..1024, None).await;
    assert!(result.is_err());
    Ok(())
}

#[tokio::test]
async fn multi_block() -> Result<()> {
    let c = start_cluster().await?;
    let path = "test/big.dat";

    // 20 MiB > 16 MiB block size — spans two blocks
    let size = 20 * 1024 * 1024;
    let data: Vec<u8> = (0..=255u8).cycle().take(size).collect();
    seed(&c.mock, path, &data).await?;

    let got = read_all(&c.client, &object_key(path), 0..size as u64, None).await?;
    assert_eq!(got.len(), size);
    assert_eq!(got, data);
    Ok(())
}

#[tokio::test]
async fn version_mismatch() -> Result<()> {
    let c = start_cluster().await?;
    let path = "test/versioned.dat";

    seed(&c.mock, path, &[1u8; 1024]).await?;

    // Request a version that won't match the upstream etag
    let result = read_all(&c.client, &object_key(path), 0..1024, Some("wrong-version")).await;
    assert!(result.is_err());
    Ok(())
}

#[tokio::test]
async fn multiversion() -> Result<()> {
    let c = start_cluster().await?;
    let path = "test/multi.dat";
    let key = object_key(path);
    let obj_path = ObjPath::from(path);

    // Put version A
    let data_a = vec![0xAA; 1024];
    let put_a = c
        .mock
        .put(&obj_path, Bytes::from(data_a.clone()).into())
        .await?;
    let ver_a = put_a.e_tag.unwrap().trim_matches('"').to_string();

    // Read without version — caches block as ver_a
    let got = read_all(&c.client, &key, 0..1024, None).await?;
    assert_eq!(got, data_a);

    // Overwrite with version B
    let data_b = vec![0xBB; 1024];
    let put_b = c
        .mock
        .put(&obj_path, Bytes::from(data_b.clone()).into())
        .await?;
    let ver_b = put_b.e_tag.unwrap().trim_matches('"').to_string();
    assert_ne!(ver_a, ver_b);

    // Read ver_b — cache has ver_a but not ver_b, triggers download
    let got = read_all(&c.client, &key, 0..1024, Some(&ver_b)).await?;
    assert_eq!(got, data_b);

    // Both versions now cached — read ver_a from cache (store was overwritten)
    let got = read_all(&c.client, &key, 0..1024, Some(&ver_a)).await?;
    assert_eq!(got, data_a);

    // Read ver_b again from cache
    let got = read_all(&c.client, &key, 0..1024, Some(&ver_b)).await?;
    assert_eq!(got, data_b);

    Ok(())
}
