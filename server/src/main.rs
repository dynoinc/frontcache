mod block;
mod cache;
mod disk;
mod index;
mod membership;
mod prelude;
mod ring;
mod server;
mod store;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

use std::net::SocketAddr;

use cache::Cache;
use clap::Parser;
use disk::{Disk, start_purger};
use index::Index;
use membership::K8sMembership;
use prelude::*;
use ring::ConsistentHashRing;
use server::CacheServer;
use store::Store;

#[derive(Parser, Debug)]
#[command(name = "frontcache-server")]
#[command(about = "Distributed pull-through cache for object storage")]
struct Args {
    #[arg(long, default_value = "0.0.0.0:8080")]
    listen: SocketAddr,

    #[arg(long, value_delimiter = ',', default_value = "/tmp/frontcache")]
    cache_dirs: Vec<PathBuf>,

    #[arg(long, default_value = "app=frontcache")]
    label: String,

    #[arg(long, help = "Root directory for file:// URLs (testing only)")]
    local_root: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let meter_provider = frontcache_metrics::init("frontcache-server");

    let args = Args::parse();

    for dir in &args.cache_dirs {
        std::fs::create_dir_all(dir)?;
    }

    let index = Arc::new(Index::open(args.cache_dirs[0].join("index.db"))?);
    let store = Arc::new(Store::new(args.local_root));
    let disks = args
        .cache_dirs
        .into_iter()
        .map(Disk::new)
        .collect::<Result<Vec<_>>>()?;
    let cache = Arc::new(Cache::new(index.clone(), store, disks));
    let ring = Arc::new(RwLock::new(ConsistentHashRing::new()));

    cache.init_from_disk().await?;
    start_purger(cache.clone());

    if std::path::Path::new("/var/run/secrets/kubernetes.io/serviceaccount/token").exists() {
        tracing::info!("Kubernetes environment detected, enabling pod discovery");
        let port = args.listen.port();
        let membership = K8sMembership::new(args.label.clone(), port).await?;

        let ring_clone = ring.clone();
        tokio::spawn(async move {
            if let Err(e) = membership.watch_pods(ring_clone).await {
                tracing::error!("Membership watcher failed: {}", e);
            }
        });
    } else {
        tracing::info!("Running in standalone mode");
        ring.write().add_node(args.listen.to_string());
    }

    tracing::info!("Starting frontcache server on {}", args.listen);
    CacheServer::new(cache.clone(), ring)
        .serve(args.listen)
        .await?;

    meter_provider.shutdown()?;
    Ok(())
}
