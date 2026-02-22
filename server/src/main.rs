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

use anyhow::Result;
use cache::Cache;
use clap::Parser;
use disk::{Disk, start_purger};
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
        default_value = "/tmp/frontcache",
        help = "Cache directories (comma-separated for multiple)"
    )]
    cache_dirs: Vec<PathBuf>,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    frontcache_metrics::init();

    let args = Args::parse();

    for dir in &args.cache_dirs {
        std::fs::create_dir_all(dir)?;
    }

    let index = Arc::new(Index::open(args.cache_dirs[0].join("index.db"))?);
    let store = Arc::new(Store::new());
    let disks = args
        .cache_dirs
        .into_iter()
        .map(Disk::new)
        .collect::<Result<Vec<_>>>()?;
    let cache = Arc::new(Cache::new(index.clone(), store, disks));

    cache.init_from_disk().await?;
    start_purger(cache.clone());

    tracing::info!("Starting frontcache server on {}", args.listen);
    CacheServer::new(cache.clone()).serve(args.listen).await?;

    frontcache_metrics::shutdown()?;
    Ok(())
}
