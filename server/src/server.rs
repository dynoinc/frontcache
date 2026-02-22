use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;

use anyhow::Result;
use frontcache_proto::{
    ReadRangeRequest, ReadRangeResponse,
    cache_service_server::{CacheService, CacheServiceServer},
};
use futures_util::Stream;
use tonic::{Request, Response, Status, transport::Server};

use crate::{
    cache::{BLOCK_SIZE, Cache, CacheError},
    store::StoreError,
};

const CHUNK_SIZE: usize = 256 * 1024;

pub struct CacheServer {
    cache: Arc<Cache>,
}

impl CacheServer {
    pub fn new(cache: Arc<Cache>) -> Self {
        Self { cache }
    }

    pub async fn serve(self, addr: SocketAddr) -> Result<()> {
        let svc = CacheServiceServer::new(self);
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
impl CacheService for CacheServer {
    type ReadRangeStream = Pin<Box<dyn Stream<Item = Result<ReadRangeResponse, Status>> + Send>>;

    async fn read_range(
        &self,
        request: Request<ReadRangeRequest>,
    ) -> Result<Response<Self::ReadRangeStream>, Status> {
        let req = request.get_ref();
        let offset = req.offset;
        let length = req.length;

        let block = self.cache.get(&req.key, offset).await.map_err(|e| {
            let msg = format!("{:?}", e);
            match &e {
                CacheError::StoreRead(store_err) => match store_err.as_ref() {
                    StoreError::InvalidKey(_) | StoreError::UnsupportedProvider(_) => {
                        Status::invalid_argument(msg)
                    }
                    StoreError::NotFound(_) => Status::not_found(msg),
                    StoreError::Backend(_) => Status::internal(msg),
                },
                _ => Status::internal(msg),
            }
        })?;

        if block.version() != req.version {
            return Err(Status::failed_precondition(format!(
                "version mismatch: cached={}, requested={}",
                block.version(),
                req.version
            )));
        }

        let block_offset = (offset / BLOCK_SIZE) * BLOCK_SIZE;
        let offset_in_block = (offset - block_offset) as usize;
        let chunk_end = offset_in_block + length as usize;

        let stream = futures_util::stream::unfold(
            (block, offset_in_block),
            move |(block, chunk_start)| async move {
                if chunk_start >= chunk_end {
                    return None;
                }
                let chunk_size = CHUNK_SIZE.min(chunk_end - chunk_start);
                let chunk = bytes::Bytes::copy_from_slice(
                    &block.data()[chunk_start..chunk_start + chunk_size],
                );
                Some((
                    Ok(ReadRangeResponse { data: chunk }),
                    (block, chunk_start + chunk_size),
                ))
            },
        );

        Ok(Response::new(Box::pin(stream)))
    }
}
