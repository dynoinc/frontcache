mod block;
mod cache;
mod disk;
mod index;
mod server;
mod store;

#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result, bail};
use cache::Cache;
use clap::Parser;
use disk::{Disk, start_flusher, start_purger};
use index::Index;
use server::CacheServer;
use store::Store;

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
}

fn parse_size(s: &str) -> Result<u64> {
    let s = s.trim();
    let (num, unit) = if let Some(n) = s.strip_suffix("GiB") {
        (n, 1024 * 1024 * 1024)
    } else if let Some(n) = s.strip_suffix("MiB") {
        (n, 1024 * 1024)
    } else if let Some(n) = s.strip_suffix("KiB") {
        (n, 1024)
    } else {
        bail!("Unknown size unit in '{}', use KiB/MiB/GiB", s);
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
    tracing_subscriber::fmt::init();
    frontcache_metrics::init();

    let args = Args::parse();

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
    let store = Arc::new(Store::new());
    let cache = Arc::new(Cache::new(index.clone(), store, disks));

    cache.init_from_disk().await?;
    start_purger(cache.clone());
    start_flusher(cache.clone());

    tracing::info!("Starting frontcache server on {}", args.listen);
    CacheServer::new(cache.clone()).serve(args.listen).await?;

    frontcache_metrics::shutdown()?;
    Ok(())
}
