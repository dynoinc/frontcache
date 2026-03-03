#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::Result;
use clap::Parser;

use frontcache_router::{membership::K8sMembership, ring::Straw2Router, server::RouterServer};
use tokio_util::sync::CancellationToken;

#[derive(Parser, Debug)]
#[command(name = "frontcache-router")]
#[command(about = "Router for distributed pull-through cache")]
struct Args {
    #[arg(long, default_value = "0.0.0.0:8081")]
    listen: SocketAddr,

    #[arg(
        long,
        default_value = "",
        help = "Kubernetes pod label selector for server discovery; empty disables discovery"
    )]
    label: String,

    #[arg(long, default_value = "8080")]
    server_port: u16,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    frontcache_metrics::init();

    let args = Args::parse();

    let ring = Arc::new(Straw2Router::new());

    let shutdown = CancellationToken::new();
    let mut watcher_handle = None;

    if !args.label.trim().is_empty() {
        tracing::info!("Label selector configured, enabling pod discovery");
        let membership = K8sMembership::new(args.label.clone(), args.server_port).await?;

        let ring_clone = ring.clone();
        let cancel = shutdown.clone();
        watcher_handle = Some(tokio::spawn(async move {
            if let Err(e) = membership.watch_pods(ring_clone, cancel).await {
                tracing::error!("Membership watcher failed: {}", e);
            }
        }));
    } else {
        tracing::info!("No label selector configured, running in standalone mode");
        ring.add_node(format!("localhost:{}", args.server_port));
    }

    tracing::info!("Starting frontcache router on {}", args.listen);
    RouterServer::new(ring).serve(args.listen).await?;

    shutdown.cancel();
    if let Some(h) = watcher_handle {
        let _ = h.await;
    }
    frontcache_metrics::shutdown()?;
    Ok(())
}
