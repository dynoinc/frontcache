#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result, bail};
use clap::Parser;
use frontcache_server::{
    cache::Cache,
    disk::{Disk, register_disk_metrics, start_flusher},
    index::Index,
    limiter::FetchLimiter,
    server::CacheServer,
};
use frontcache_store::{BucketConfig, Store};
use tokio_util::sync::CancellationToken;

#[derive(Parser, Debug)]
#[command(name = "frontcache-server")]
#[command(about = "Distributed pull-through cache for object storage")]
struct Args {
    #[arg(long, default_value = "0.0.0.0:8080")]
    listen: SocketAddr,

    #[arg(
        long,
        value_delimiter = ',',
        default_value = "/tmp/frontcache:1GiB",
        help = "Cache directories as path:size (e.g. /data/cache:10GiB)"
    )]
    cache_dirs: Vec<String>,

    #[arg(long, default_value = "/var/lib/frontcache/index.db")]
    index_path: PathBuf,

    #[arg(long, default_value_t = 16 * 1024 * 1024)]
    block_size: u64,

    #[arg(long, default_value_t = 256 * 1024)]
    chunk_size: usize,

    #[arg(long, help = "Path to bucket config YAML file")]
    bucket_config: Option<PathBuf>,
}

fn parse_size(s: &str) -> Result<u64> {
    let s = s.trim();
    let (num, unit) = if let Some(n) = s.strip_suffix("TiB") {
        (n, 1024 * 1024 * 1024 * 1024)
    } else if let Some(n) = s.strip_suffix("GiB") {
        (n, 1024 * 1024 * 1024)
    } else if let Some(n) = s.strip_suffix("MiB") {
        (n, 1024 * 1024)
    } else if let Some(n) = s.strip_suffix("KiB") {
        (n, 1024)
    } else {
        bail!("Unknown size unit in '{}', use KiB/MiB/GiB/TiB", s);
    };
    let n: u64 = num.parse().context(format!("Invalid number in '{}'", s))?;
    Ok(n * unit)
}

fn parse_cache_dir(s: &str) -> Result<(PathBuf, u64)> {
    let (path, size) = s
        .rsplit_once(':')
        .context(format!("Expected path:size format, got '{}'", s))?;
    Ok((PathBuf::from(path), parse_size(size)?))
}

#[tokio::main]
async fn main() -> Result<()> {
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .expect("failed to install rustls crypto provider");
    tracing_subscriber::fmt::init();
    frontcache_metrics::init();

    let args = Args::parse();

    let bucket_config = Arc::new(BucketConfig::load(args.bucket_config.as_deref())?);

    let mut disks = Vec::new();
    for entry in &args.cache_dirs {
        let (path, capacity) = parse_cache_dir(entry)?;
        tokio::fs::create_dir_all(&path).await?;
        let tmp_dir = path.join("tmp");
        if tmp_dir.exists() {
            tokio::fs::remove_dir_all(&tmp_dir).await?;
        }
        tokio::fs::create_dir(&tmp_dir).await?;
        disks.push(Disk::new(path, capacity));
    }

    if let Some(parent) = args.index_path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    let index = Arc::new(Index::open(args.index_path)?);
    let store = Arc::new(Store::new(bucket_config));
    let limiter = Arc::new(FetchLimiter::from_env());
    let cache = Arc::new(Cache::new(
        index.clone(),
        store,
        disks,
        limiter,
        args.block_size,
    ));

    cache.init_from_disk()?;

    let _disk_metrics = register_disk_metrics(cache.clone());
    let shutdown = CancellationToken::new();
    let flusher = start_flusher(cache.clone(), shutdown.clone());

    tracing::info!("Starting frontcache server on {}", args.listen);
    CacheServer::new(cache.clone(), args.chunk_size)
        .serve(args.listen)
        .await?;

    tracing::info!("Shutting down");
    shutdown.cancel();
    let _ = flusher.await;
    for disk in cache.disks() {
        let _ = disk.evict_lock().acquire_owned().await;
    }
    cache.flush_last_accessed();
    frontcache_metrics::shutdown()?;
    Ok(())
}
