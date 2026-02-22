use anyhow::Result;
use bytes::{Bytes, BytesMut};
use dashmap::DashMap;
use frontcache_proto::{
    LookupOwnerRequest, ReadRangeRequest, cache_service_client::CacheServiceClient,
    router_service_client::RouterServiceClient,
};
use futures::future::try_join_all;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tokio_retry2::{Retry, RetryError, strategy::ExponentialBackoff};
use tonic::transport::Channel;
use tower::ServiceBuilder;

const BLOCK_SIZE: u64 = 16 * 1024 * 1024;

type MetricsChannel = frontcache_metrics::RpcMetricsService<Channel>;

struct ClientConfig {
    lookup_timeout: Duration,
    read_timeout: Duration,
    max_retries: usize,
}

fn env_or<T: FromStr>(name: &str, default: T) -> T {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

impl ClientConfig {
    fn from_env() -> Self {
        Self {
            lookup_timeout: Duration::from_millis(env_or("FRONTCACHE_LOOKUP_TIMEOUT_MS", 500)),
            read_timeout: Duration::from_millis(env_or("FRONTCACHE_READ_TIMEOUT_MS", 5000)),
            max_retries: env_or("FRONTCACHE_MAX_RETRIES", 3),
        }
    }
}

fn is_retryable(code: tonic::Code) -> bool {
    !matches!(
        code,
        tonic::Code::InvalidArgument
            | tonic::Code::NotFound
            | tonic::Code::AlreadyExists
            | tonic::Code::PermissionDenied
            | tonic::Code::Unauthenticated
            | tonic::Code::Unimplemented
            | tonic::Code::OutOfRange
            | tonic::Code::FailedPrecondition
    )
}

#[derive(Clone)]
pub struct CacheClient {
    router: RouterServiceClient<MetricsChannel>,
    connections: DashMap<String, CacheServiceClient<MetricsChannel>>,
    config: Arc<ClientConfig>,
}

impl CacheClient {
    pub async fn new(router_addr: String) -> Result<Self> {
        let config = ClientConfig::from_env();
        frontcache_metrics::init();

        let channel = Channel::from_shared(format!("http://{}", router_addr))?
            .timeout(config.lookup_timeout)
            .connect()
            .await?;
        let channel = ServiceBuilder::new()
            .layer(frontcache_metrics::layer())
            .service(channel);
        let router = RouterServiceClient::new(channel);

        Ok(Self {
            router,
            connections: DashMap::new(),
            config: Arc::new(config),
        })
    }

    pub async fn read_range(
        &self,
        key: &str,
        offset: u64,
        length: u64,
        version: Option<&str>,
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
            let version = version.map(|v| v.to_string());
            tasks.push(async move {
                let owner = self_clone.lookup_owner(&key, block_offset).await?;
                let client = self_clone.get_or_create_cache_connection(&owner).await?;

                let strategy = ExponentialBackoff::from_millis(100)
                    .factor(2)
                    .max_delay(Duration::from_secs(1))
                    .take(self_clone.config.max_retries);

                let chunks = Retry::spawn(strategy, || {
                    let mut client = client.clone();
                    let key = key.clone();
                    let version = version.clone();
                    async move {
                        let mut stream = client
                            .read_range(ReadRangeRequest {
                                key,
                                offset: read_offset,
                                length: read_len,
                                version,
                            })
                            .await
                            .map_err(|s| {
                                if is_retryable(s.code()) {
                                    RetryError::transient(anyhow::Error::from(s))
                                } else {
                                    RetryError::permanent(anyhow::Error::from(s))
                                }
                            })?
                            .into_inner();

                        let mut chunks = Vec::new();
                        while let Some(resp) = stream.message().await.map_err(|s| {
                            if is_retryable(s.code()) {
                                RetryError::transient(anyhow::Error::from(s))
                            } else {
                                RetryError::permanent(anyhow::Error::from(s))
                            }
                        })? {
                            chunks.push(resp.data);
                        }

                        Ok::<_, RetryError<anyhow::Error>>(chunks)
                    }
                })
                .await?;

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

    async fn lookup_owner(&self, key: &str, block_offset: u64) -> Result<String> {
        let strategy = ExponentialBackoff::from_millis(100)
            .factor(2)
            .max_delay(Duration::from_secs(1))
            .take(self.config.max_retries);

        let router = self.router.clone();
        let req = LookupOwnerRequest {
            key: key.to_string(),
            offset: block_offset,
        };

        let resp = Retry::spawn(strategy, || {
            let mut router = router.clone();
            let req = req.clone();
            async move {
                router.lookup_owner(req).await.map_err(|s| {
                    if is_retryable(s.code()) {
                        RetryError::transient(s)
                    } else {
                        RetryError::permanent(s)
                    }
                })
            }
        })
        .await?;

        Ok(resp.into_inner().addr)
    }

    async fn get_or_create_cache_connection(
        &self,
        addr: &str,
    ) -> Result<CacheServiceClient<MetricsChannel>> {
        if let Some(client) = self.connections.get(addr) {
            return Ok(client.clone());
        }

        let channel = Channel::from_shared(format!("http://{}", addr))?
            .timeout(self.config.read_timeout)
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
