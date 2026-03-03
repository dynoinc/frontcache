use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;

use anyhow::Result;
use frontcache_proto::{
    ReadRangeRequest, ReadRangeResponse,
    cache_service_server::{CacheService, CacheServiceServer},
};
use futures_util::Stream;
use opentelemetry::KeyValue;
use tonic::{Request, Response, Status, transport::Server};

use crate::{
    cache::{BLOCK_SIZE, Cache, CacheError, CacheHit},
    store::StoreError,
};

pub struct CacheServer {
    cache: Arc<Cache>,
    chunk_size: usize,
}

impl CacheServer {
    pub fn new(cache: Arc<Cache>) -> Self {
        Self {
            cache,
            chunk_size: crate::env_or("FRONTCACHE_STREAM_CHUNK_SIZE", 256 * 1024),
        }
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

        let hit = self
            .cache
            .get(&req.key, offset, req.version.as_deref())
            .await
            .map_err(|e| {
                let msg = format!("{:?}", e);
                match &e {
                    CacheError::StoreRead(store_err) => match store_err.as_ref() {
                        StoreError::InvalidKey(_) | StoreError::UnsupportedProvider(_) => {
                            Status::invalid_argument(msg)
                        }
                        StoreError::NotFound(_) => Status::not_found(msg),
                        StoreError::Backend(_) => Status::internal(msg),
                    },
                    CacheError::VersionMismatch { .. } => Status::failed_precondition(msg),
                    CacheError::Throttled => Status::resource_exhausted(msg),
                    _ => Status::internal(msg),
                }
            })?;

        let block_offset = (offset / BLOCK_SIZE) * BLOCK_SIZE;
        let offset_in_block = (offset - block_offset) as usize;
        let block_len = hit.size() as usize;

        if offset_in_block >= block_len {
            return Err(Status::out_of_range(format!(
                "offset {} beyond block size {}",
                offset_in_block, block_len
            )));
        }

        let chunk_end = block_len.min(offset_in_block + length as usize);
        frontcache_metrics::get().disk_byte_changes.add(
            (chunk_end - offset_in_block) as u64,
            &[KeyValue::new("action", "served")],
        );

        let chunk_size = self.chunk_size;
        let stream: Pin<Box<dyn Stream<Item = Result<ReadRangeResponse, Status>> + Send>> =
            match hit {
                CacheHit::Disk { reader, .. } => Box::pin(futures_util::stream::try_unfold(
                    (reader, offset_in_block),
                    move |(reader, pos)| async move {
                        if pos >= chunk_end {
                            return Ok(None);
                        }
                        let len = chunk_size.min(chunk_end - pos);
                        let chunk = reader
                            .read_chunk(pos as u64, len)
                            .await
                            .map_err(|e| Status::internal(format!("read_chunk: {e}")))?;
                        let chunk_len = chunk.len();
                        if chunk_len != len {
                            return Err(Status::data_loss(format!(
                                "short read at offset {pos}: expected {len} bytes, got {chunk_len}"
                            )));
                        }
                        Ok(Some((
                            ReadRangeResponse { data: chunk },
                            (reader, pos + chunk_len),
                        )))
                    },
                )),
                CacheHit::Fresh { data, .. } => {
                    Box::pin(futures_util::stream::unfold(offset_in_block, move |pos| {
                        let data = data.clone();
                        async move {
                            if pos >= chunk_end {
                                return None;
                            }
                            let len = chunk_size.min(chunk_end - pos);
                            let chunk = data.slice(pos..pos + len);
                            Some((Ok(ReadRangeResponse { data: chunk }), pos + len))
                        }
                    }))
                }
            };

        Ok(Response::new(stream))
    }
}
