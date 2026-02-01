use std::net::SocketAddr;

use frontcache_proto::{
    LookupOwnerRequest, LookupOwnerResponse, ReadRangeRequest, ReadRangeResponse,
    cache_service_server::{CacheService, CacheServiceServer},
};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, transport::Server};

use crate::{
    cache::{BLOCK_SIZE, Cache, CacheError},
    prelude::*,
    ring::ConsistentHashRing,
    store::StoreError,
};

const CHUNK_SIZE: usize = 256 * 1024;

pub struct CacheServer {
    cache: Arc<Cache>,
    ring: Arc<RwLock<ConsistentHashRing>>,
}

impl CacheServer {
    pub fn new(cache: Arc<Cache>, ring: Arc<RwLock<ConsistentHashRing>>) -> Self {
        Self { cache, ring }
    }

    async fn stream_data(
        cache: Arc<Cache>,
        tx: tokio::sync::mpsc::Sender<Result<ReadRangeResponse, Status>>,
        object: &str,
        offset: u64,
        length: u64,
        version: &str,
    ) -> Result<(), Status> {
        let block = cache.get(object, offset).await.map_err(|e| {
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

        if block.version() != version {
            return Err(Status::failed_precondition(format!(
                "version mismatch: cached={}, requested={}",
                block.version(),
                version
            )));
        }

        let block_offset = (offset / BLOCK_SIZE) * BLOCK_SIZE;
        let offset_in_block = (offset - block_offset) as usize;
        let to_read = length as usize;

        let block_data = block.data();
        let mut chunk_start = offset_in_block;
        let chunk_end = offset_in_block + to_read;

        while chunk_start < chunk_end {
            let chunk_size = CHUNK_SIZE.min(chunk_end - chunk_start);
            let chunk =
                bytes::Bytes::copy_from_slice(&block_data[chunk_start..chunk_start + chunk_size]);

            tx.send(Ok(ReadRangeResponse { data: chunk }))
                .await
                .map_err(|_| Status::internal("Failed to send chunk"))?;

            chunk_start += chunk_size;
        }

        Ok(())
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
    async fn lookup_owner(
        &self,
        request: Request<LookupOwnerRequest>,
    ) -> Result<Response<LookupOwnerResponse>, Status> {
        let req = request.get_ref();
        let block_offset = (req.offset / BLOCK_SIZE) * BLOCK_SIZE;

        let ring = self.ring.read();
        let owner = ring
            .get_owner(&req.key, block_offset)
            .ok_or_else(|| Status::unavailable("no nodes available"))?;

        Ok(Response::new(LookupOwnerResponse {
            addr: owner.clone(),
        }))
    }

    type ReadRangeStream = ReceiverStream<Result<ReadRangeResponse, Status>>;

    async fn read_range(
        &self,
        request: Request<ReadRangeRequest>,
    ) -> Result<Response<Self::ReadRangeStream>, Status> {
        let req = request.get_ref();
        let object = req.key.clone();
        let offset = req.offset;
        let length = req.length;
        let version = req.version.clone();

        let cache = self.cache.clone();

        let (tx, rx) = tokio::sync::mpsc::channel(4);

        tokio::spawn(async move {
            if let Err(e) =
                Self::stream_data(cache, tx.clone(), &object, offset, length, &version).await
            {
                let _ = tx.send(Err(e)).await;
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }
}
