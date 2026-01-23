use anyhow::Result;
use bytes::{Bytes, BytesMut};
use dashmap::DashMap;
use frontcache_proto::cache_service_client::CacheServiceClient;
use frontcache_proto::{LookupOwnerRequest, ReadRangeRequest};
use futures::future::try_join_all;
use tonic::transport::Channel;

const BLOCK_SIZE: u64 = 16 * 1024 * 1024;

#[derive(Clone)]
pub struct CacheClient {
    local_addr: String,
    connections: DashMap<String, CacheServiceClient<Channel>>,
}

impl CacheClient {
    pub async fn new(port: u16) -> Result<Self> {
        let local_addr = format!("localhost:{}", port);
        Ok(Self {
            local_addr,
            connections: DashMap::new(),
        })
    }

    pub async fn read_range(&self, key: &str, offset: u64, length: u64) -> Result<Bytes> {
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
            tasks.push(async move {
                let mut local = self_clone
                    .get_or_create_connection(&self_clone.local_addr)
                    .await?;
                let owner = local
                    .lookup_owner(LookupOwnerRequest {
                        key: key.clone(),
                        offset: block_offset,
                    })
                    .await?
                    .into_inner()
                    .addr;

                let mut client = self_clone.get_or_create_connection(&owner).await?;
                let mut stream = client
                    .read_range(ReadRangeRequest {
                        key,
                        offset: read_offset,
                        length: read_len,
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
                let mut result = BytesMut::with_capacity(length as usize);
                for chunk in chunks {
                    result.extend_from_slice(&chunk);
                }
                Ok(result.freeze())
            }
        }
    }

    async fn get_or_create_connection(&self, addr: &str) -> Result<CacheServiceClient<Channel>> {
        if let Some(client) = self.connections.get(addr) {
            return Ok(client.clone());
        }

        let client = CacheServiceClient::connect(format!("http://{}", addr)).await?;
        self.connections.insert(addr.to_string(), client.clone());
        Ok(client)
    }
}
