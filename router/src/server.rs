use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::Result;
use frontcache_proto::{
    LookupOwnerRequest, LookupOwnerResponse,
    router_service_server::{RouterService, RouterServiceServer},
};
use tonic::{Request, Response, Status, transport::Server};

use crate::ring::Straw2Router;

pub struct RouterServer {
    ring: Arc<Straw2Router>,
    block_size: u64,
}

impl RouterServer {
    pub fn new(ring: Arc<Straw2Router>, block_size: u64) -> Self {
        assert!(block_size > 0, "block_size must be > 0");
        Self { ring, block_size }
    }

    pub async fn serve(self, addr: SocketAddr) -> Result<()> {
        let svc = RouterServiceServer::new(self);
        Server::builder()
            .layer(frontcache_metrics::layer())
            .add_service(svc)
            .serve_with_shutdown(addr, shutdown_signal())
            .await?;
        Ok(())
    }
}

async fn shutdown_signal() {
    let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .expect("failed to install SIGTERM handler");

    tokio::select! {
        _ = tokio::signal::ctrl_c() => tracing::info!("Received Ctrl+C, starting graceful shutdown"),
        _ = sigterm.recv() => tracing::info!("Received SIGTERM, starting graceful shutdown"),
    }
}

#[tonic::async_trait]
impl RouterService for RouterServer {
    async fn lookup_owner(
        &self,
        request: Request<LookupOwnerRequest>,
    ) -> Result<Response<LookupOwnerResponse>, Status> {
        let req = request.get_ref();
        let block_offset = (req.offset / self.block_size) * self.block_size;

        let addrs = self
            .ring
            .get_owners(&req.key, block_offset)
            .ok_or_else(|| Status::unavailable("no nodes available"))?;

        Ok(Response::new(LookupOwnerResponse { addrs }))
    }
}
