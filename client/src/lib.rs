use anyhow::Result;
use bytes::Bytes;
use dashmap::DashMap;
use frontcache_proto::{
    LookupOwnerRequest, ReadRangeRequest, cache_service_client::CacheServiceClient,
    router_service_client::RouterServiceClient,
};
use futures::{Stream, StreamExt, TryStreamExt};
use std::pin::Pin;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tokio_retry2::{Retry, RetryError, strategy::ExponentialBackoff};
use tonic::transport::Channel;
use tower::ServiceBuilder;

const BLOCK_SIZE: u64 = 16 * 1024 * 1024;
const PREFETCH_BLOCKS: usize = 16;

type MetricsChannel = frontcache_metrics::RpcMetricsService<Channel>;
pub type ByteStream = Pin<Box<dyn Stream<Item = Result<Bytes>> + Send + 'static>>;

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

    pub fn stream_range(
        &self,
        key: &str,
        offset: u64,
        length: u64,
        version: Option<&str>,
    ) -> Result<ByteStream> {
        let end_offset = offset
            .checked_add(length)
            .ok_or_else(|| anyhow::anyhow!("offset + length overflows u64"))?;
        if length == 0 {
            return Ok(Box::pin(futures::stream::empty()));
        }

        let mut block_reads = Vec::new();
        let mut current = offset;

        while current < end_offset {
            let block_offset = (current / BLOCK_SIZE) * BLOCK_SIZE;
            let read_end = end_offset.min(block_offset + BLOCK_SIZE);
            let (read_offset, read_len) = (current, read_end - current);
            current = read_end;
            block_reads.push((block_offset, read_offset, read_len));
        }

        let client = self.clone();
        let key = key.to_string();
        let version = version.map(|v| v.to_string());

        let stream = futures::stream::iter(block_reads.into_iter().map(
            move |(block_offset, read_offset, read_len)| {
                let client = client.clone();
                let key = key.clone();
                let version = version.clone();
                async move {
                    client
                        .read_block_stream_with_retry(
                            key,
                            block_offset,
                            read_offset,
                            read_len,
                            version,
                        )
                        .await
                }
            },
        ))
        .buffered(PREFETCH_BLOCKS)
        .try_flatten();

        Ok(Box::pin(stream))
    }

    async fn read_block_stream_with_retry(
        &self,
        key: String,
        block_offset: u64,
        read_offset: u64,
        read_len: u64,
        version: Option<String>,
    ) -> Result<ByteStream> {
        let owner = self.lookup_owner(&key, block_offset).await?;
        let client = self.get_or_create_cache_connection(&owner).await?;

        let strategy = ExponentialBackoff::from_millis(100)
            .factor(2)
            .max_delay(Duration::from_secs(1))
            .take(self.config.max_retries);

        let response = Retry::spawn(strategy, || {
            let mut client = client.clone();
            let key = key.clone();
            let version = version.clone();
            async move {
                let stream = client
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
                Ok::<_, RetryError<anyhow::Error>>(stream)
            }
        })
        .await?;

        let chunk_stream = futures::stream::try_unfold(response, |mut stream| async move {
            match stream.message().await {
                Ok(Some(resp)) => Ok(Some((resp.data, stream))),
                Ok(None) => Ok(None),
                Err(status) => Err(anyhow::Error::from(status)),
            }
        })
        .try_filter(|bytes| futures::future::ready(!bytes.is_empty()));

        Ok(Box::pin(chunk_stream))
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

        let endpoint =
            Channel::from_shared(format!("http://{}", addr))?.timeout(self.config.read_timeout);

        let strategy = ExponentialBackoff::from_millis(100)
            .factor(2)
            .max_delay(Duration::from_secs(1))
            .take(self.config.max_retries);

        let channel = Retry::spawn(strategy, || {
            let endpoint = endpoint.clone();
            async move { endpoint.connect().await.map_err(RetryError::transient) }
        })
        .await?;

        let channel = ServiceBuilder::new()
            .layer(frontcache_metrics::layer())
            .service(channel);

        let client = CacheServiceClient::new(channel);
        self.connections.insert(addr.to_string(), client.clone());
        Ok(client)
    }
}
