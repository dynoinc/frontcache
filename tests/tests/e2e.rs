use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use bytes::Bytes;
use object_store::{ObjectStore, ObjectStoreExt, memory::InMemory, path::Path as ObjPath};
use tempfile::TempDir;
use tokio::net::TcpListener;

use frontcache_router::{
    ring::{Node, Straw2Router},
    server::RouterServer,
};
use frontcache_server::{
    cache::Cache, disk::Disk, index::Index, limiter::FetchLimiter, server::CacheServer,
};
use frontcache_store::{BucketConfig, Store};

const BUCKET: &str = "test-bucket";
const DATA_42_1K: &[u8] = &[42u8; 1024];

fn object_key(path: &str) -> String {
    format!("/{}/{}", BUCKET, path)
}

struct TestCluster {
    client: reqwest::Client,
    base_url: String,
    mock: Arc<InMemory>,
    _tmp: TempDir,
}

async fn start_cluster_with_block_size(block_size: u64) -> Result<TestCluster> {
    frontcache_metrics::init();

    let mock = Arc::new(InMemory::new());

    let mut config = BucketConfig::default();
    config.buckets.insert(
        BUCKET.to_string(),
        frontcache_store::config::BucketEntry {
            provider: "inmem".to_string(),
        },
    );
    let config = Arc::new(config);

    let tmp = TempDir::new()?;
    let cache_dir = tmp.path().join("cache");
    std::fs::create_dir_all(cache_dir.join("tmp"))?;

    let disk = Disk::new(cache_dir, 1024 * 1024 * 1024);
    let index = Arc::new(Index::open(tmp.path().join("index.db"))?);
    let store = Arc::new(Store::new(config.clone()));
    store.backends.insert(
        ("inmem".to_string(), BUCKET.to_string()),
        mock.clone() as Arc<dyn ObjectStore>,
    );
    let cache = Arc::new(Cache::new(
        index,
        store.clone(),
        vec![disk],
        Arc::new(FetchLimiter::from_env()),
        block_size,
    ));
    cache.init_from_disk()?;

    // Start server
    let server_listener = TcpListener::bind("127.0.0.1:0").await?;
    let server_addr = server_listener.local_addr()?;
    let server_app = CacheServer::new(cache, 1024).into_router();
    tokio::spawn(async move {
        axum::serve(server_listener, server_app).await.unwrap();
    });

    // Start router
    let ring = Arc::new(Straw2Router::new());
    ring.add_node(Node {
        pod_name: "test-server".into(),
        ip_addr: server_addr,
    });
    let router_listener = TcpListener::bind("127.0.0.1:0").await?;
    let router_addr = router_listener.local_addr()?;
    let router_app = RouterServer::new(ring, block_size).into_router();
    tokio::spawn(async move {
        axum::serve(router_listener, router_app).await.unwrap();
    });

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()?;

    // Wait for servers to be ready
    let base_url = format!("http://{}", router_addr);
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match client.get(format!("{}/healthz", base_url)).send().await {
                Ok(r) if r.status().is_success() => return,
                _ => tokio::time::sleep(Duration::from_millis(10)).await,
            }
        }
    })
    .await
    .expect("servers did not start in time");

    Ok(TestCluster {
        client,
        base_url,
        mock,
        _tmp: tmp,
    })
}

async fn start_cluster() -> Result<TestCluster> {
    start_cluster_with_block_size(4096).await
}

async fn read_all(
    client: &reqwest::Client,
    base_url: &str,
    key: &str,
    start: u64,
    end: u64,
) -> Result<Vec<u8>> {
    let url = format!("{}{}", base_url, key);
    let req = client
        .get(&url)
        .header("Range", format!("bytes={}-{}", start, end - 1));
    let resp = req.send().await?;
    let status = resp.status();
    if status == reqwest::StatusCode::TEMPORARY_REDIRECT {
        // Follow redirect manually
        let location = resp
            .headers()
            .get("location")
            .unwrap()
            .to_str()?
            .to_string();
        let req = client
            .get(&location)
            .header("Range", format!("bytes={}-{}", start, end - 1));
        let resp = req.send().await?;
        if !resp.status().is_success() && resp.status() != reqwest::StatusCode::PARTIAL_CONTENT {
            anyhow::bail!(
                "HTTP {} from redirect: {}",
                resp.status(),
                resp.text().await?
            );
        }
        return Ok(resp.bytes().await?.to_vec());
    }
    if !status.is_success() && status != reqwest::StatusCode::PARTIAL_CONTENT {
        anyhow::bail!("HTTP {}: {}", status, resp.text().await?);
    }
    Ok(resp.bytes().await?.to_vec())
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

    let got = read_all(
        &c.client,
        &c.base_url,
        &object_key("test/file.dat"),
        0,
        1024,
    )
    .await?;
    assert_eq!(got.as_slice(), DATA_42_1K);
    Ok(())
}

#[tokio::test]
async fn cache_hit() -> Result<()> {
    let c = start_cluster().await?;
    let path = "test/cached.dat";

    let data = vec![7u8; 2048];
    seed(&c.mock, path, &data).await?;

    // First read — cache miss
    let got = read_all(&c.client, &c.base_url, &object_key(path), 0, 2048).await?;
    assert_eq!(got, data);

    // Delete from mock store
    c.mock.delete(&ObjPath::from(path)).await?;

    // Second read — served from cache
    let got = read_all(&c.client, &c.base_url, &object_key(path), 0, 2048).await?;
    assert_eq!(got, data);
    Ok(())
}

#[tokio::test]
async fn range_read_fuzz() -> Result<()> {
    let c = start_cluster().await?;
    let mut rng = fastrand::Rng::new();

    for i in 0..20 {
        let obj_size = rng.usize(1..65536);
        let data: Vec<u8> = (0..obj_size).map(|_| rng.u8(..)).collect();
        let path = format!("test/fuzz-{i}.dat");
        seed(&c.mock, &path, &data).await?;
        let key = object_key(&path);

        for _ in 0..10 {
            let start = rng.u64(..obj_size as u64);
            let last_block_end = ((obj_size as u64 - 1) / 4096 + 1) * 4096;
            let end = rng.u64(start + 1..=last_block_end);
            let expected_end = end.min(obj_size as u64) as usize;

            let got = read_all(&c.client, &c.base_url, &key, start, end).await?;
            assert_eq!(
                got,
                &data[start as usize..expected_end],
                "read mismatch at {start}..{end} (obj_size={obj_size})"
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn not_found() -> Result<()> {
    let c = start_cluster().await?;

    let url = format!("{}{}", c.base_url, object_key("test/missing.dat"));
    let resp = c
        .client
        .get(&url)
        .header("Range", "bytes=0-1023")
        .send()
        .await?;
    assert_eq!(resp.status(), reqwest::StatusCode::NOT_FOUND);
    Ok(())
}

#[tokio::test]
async fn multi_block() -> Result<()> {
    let c = start_cluster().await?;
    let path = "test/big.dat";

    // 20 KiB > 4 KiB block size — spans five blocks
    let size = 20 * 1024;
    let data: Vec<u8> = (0..=255u8).cycle().take(size).collect();
    seed(&c.mock, path, &data).await?;

    let got = read_all(&c.client, &c.base_url, &object_key(path), 0, size as u64).await?;
    assert_eq!(got.len(), size);
    assert_eq!(got, data);
    Ok(())
}

#[tokio::test]
async fn overwrite_serves_cached_until_evicted() -> Result<()> {
    let c = start_cluster().await?;
    let path = "test/overwrite.dat";
    let key = object_key(path);
    let obj_path = ObjPath::from(path);

    // Put version A and cache it
    let data_a = vec![0xAA; 1024];
    c.mock
        .put(&obj_path, Bytes::from(data_a.clone()).into())
        .await?;
    let got = read_all(&c.client, &c.base_url, &key, 0, 1024).await?;
    assert_eq!(got, data_a);

    // Overwrite with version B in backend
    let data_b = vec![0xBB; 1024];
    c.mock
        .put(&obj_path, Bytes::from(data_b.clone()).into())
        .await?;

    // Cache still serves version A (no invalidation)
    let got = read_all(&c.client, &c.base_url, &key, 0, 1024).await?;
    assert_eq!(got, data_a);

    Ok(())
}
