use crate::cache::{BLOCK_SIZE, Cache};
use crate::ring::ConsistentHashRing;
use anyhow::{Result, anyhow};
use frontcache_proto::cache_service_server::{CacheService, CacheServiceServer};
use frontcache_proto::{LookupOwnerRequest, LookupOwnerResponse};
use frontcache_proto::{ReadRangeRequest, ReadRangeResponse};
use parking_lot::RwLock;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Server;
use tonic::{Request, Response, Status};

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
    ) -> Result<()> {
        let block = cache
            .get(object, offset)
            .await
            .map_err(|e| anyhow!("Failed to get block: {}", e))?;

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
                .map_err(|_| anyhow!("Failed to send chunk"))?;

            chunk_start += chunk_size;
        }

        Ok(())
    }

    pub async fn serve(self, addr: SocketAddr) -> Result<()> {
        let svc = CacheServiceServer::new(self);
        Server::builder().add_service(svc).serve(addr).await?;
        Ok(())
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

        let cache = self.cache.clone();

        let (tx, rx) = tokio::sync::mpsc::channel(4);

        tokio::spawn(async move {
            if let Err(e) = Self::stream_data(cache, tx.clone(), &object, offset, length).await {
                let _ = tx.send(Err(Status::internal(e.to_string()))).await;
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }
}
