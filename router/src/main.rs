mod membership;
mod ring;
mod server;

#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::Result;
use clap::Parser;
use parking_lot::RwLock;

use membership::K8sMembership;
use ring::ConsistentHashRing;
use server::RouterServer;

#[derive(Parser, Debug)]
#[command(name = "frontcache-router")]
#[command(about = "Router for distributed pull-through cache")]
struct Args {
    #[arg(long, default_value = "0.0.0.0:8081")]
    listen: SocketAddr,

    #[arg(long, default_value = "app=frontcache")]
    label: String,

    #[arg(long, default_value = "8080")]
    server_port: u16,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let meter_provider = frontcache_metrics::init("frontcache_router");

    let args = Args::parse();

    let ring = Arc::new(RwLock::new(ConsistentHashRing::new()));

    if std::path::Path::new("/var/run/secrets/kubernetes.io/serviceaccount/token").exists() {
        tracing::info!("Kubernetes environment detected, enabling pod discovery");
        let membership = K8sMembership::new(args.label.clone(), args.server_port).await?;

        let ring_clone = ring.clone();
        tokio::spawn(async move {
            if let Err(e) = membership.watch_pods(ring_clone).await {
                tracing::error!("Membership watcher failed: {}", e);
            }
        });
    } else {
        tracing::info!("Running in standalone mode");
        ring.write()
            .add_node(format!("localhost:{}", args.server_port));
    }

    tracing::info!("Starting frontcache router on {}", args.listen);
    RouterServer::new(ring).serve(args.listen).await?;

    meter_provider.shutdown()?;
    Ok(())
}
