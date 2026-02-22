use anyhow::Result;
use bytes::{Bytes, BytesMut};
use dashmap::DashMap;
use frontcache_proto::{
    LookupOwnerRequest, ReadRangeRequest,
    cache_service_client::CacheServiceClient,
    router_service_client::RouterServiceClient,
};
use futures::future::try_join_all;
use std::sync::Arc;
use tonic::transport::Channel;
use tower::ServiceBuilder;

const BLOCK_SIZE: u64 = 16 * 1024 * 1024;

type MetricsChannel = frontcache_metrics::RpcMetricsService<Channel>;

#[derive(Clone)]
pub struct CacheClient {
    router: RouterServiceClient<MetricsChannel>,
    connections: DashMap<String, CacheServiceClient<MetricsChannel>>,
    _provider: Arc<opentelemetry_sdk::metrics::SdkMeterProvider>,
}

impl CacheClient {
    pub async fn new(router_addr: String) -> Result<Self> {
        let provider = frontcache_metrics::init("frontcache_client");

        let channel = Channel::from_shared(format!("http://{}", router_addr))?
            .connect()
            .await?;
        let channel = ServiceBuilder::new()
            .layer(frontcache_metrics::layer())
            .service(channel);
        let router = RouterServiceClient::new(channel);

        Ok(Self {
            router,
            connections: DashMap::new(),
            _provider: Arc::new(provider),
        })
    }

    pub async fn read_range(
        &self,
        key: &str,
        offset: u64,
        length: u64,
        version: &str,
    ) -> Result<Bytes> {
        let end_offset = offset + length;
        let mut tasks = Vec::new();
        let mut current = offset;

        while current < end_offset {
            let block_offset = (current / BLOCK_SIZE) * BLOCK_SIZE;
            let read_end = end_offset.min(block_offset + BLOCK_SIZE);
            let (read_offset, read_len) = (current, read_end - current);
            current = read_end;

            let self_clone = self.clone();
            let key = key.to_string();
            let version = version.to_string();
            tasks.push(async move {
                let owner = self_clone.router.clone()
                    .lookup_owner(LookupOwnerRequest {
                        key: key.clone(),
                        offset: block_offset,
                    })
                    .await?
                    .into_inner()
                    .addr;

                let mut client = self_clone.get_or_create_cache_connection(&owner).await?;
                let mut stream = client
                    .read_range(ReadRangeRequest {
                        key,
                        offset: read_offset,
                        length: read_len,
                        version,
                    })
                    .await?
                    .into_inner();

                let mut chunks = Vec::new();
                while let Some(resp) = stream.message().await? {
                    chunks.push(resp.data);
                }

                Ok::<_, anyhow::Error>(chunks)
            });
        }

        let chunks: Vec<Bytes> = try_join_all(tasks).await?.into_iter().flatten().collect();
        match chunks.as_slice() {
            [single] => Ok(single.clone()),
            _ => {
                let mut buf = BytesMut::with_capacity(length as usize);
                for chunk in chunks {
                    buf.extend_from_slice(&chunk);
                }
                Ok(buf.freeze())
            }
        }
    }

    async fn get_or_create_cache_connection(
        &self,
        addr: &str,
    ) -> Result<CacheServiceClient<MetricsChannel>> {
        if let Some(client) = self.connections.get(addr) {
            return Ok(client.clone());
        }

        let channel = Channel::from_shared(format!("http://{}", addr))?
            .connect()
            .await?;

        let channel = ServiceBuilder::new()
            .layer(frontcache_metrics::layer())
            .service(channel);

        let client = CacheServiceClient::new(channel);
        self.connections.insert(addr.to_string(), client.clone());
        Ok(client)
    }
}
