use anyhow::Result;
use bytes::{Bytes, BytesMut};
use dashmap::DashMap;
use frontcache_proto::{
    LookupOwnerRequest, ReadRangeRequest, ReadRangeResponse,
    cache_service_client::CacheServiceClient, router_service_client::RouterServiceClient,
};
use futures::{Stream, StreamExt, TryStreamExt};
use std::ops::{Bound, RangeBounds};
use std::pin::Pin;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tokio_retry2::{Retry, RetryError, strategy::ExponentialBackoff};
use tonic::transport::Channel;
use tower::ServiceBuilder;

pub const BLOCK_SIZE: u64 = 16 * 1024 * 1024;

type MetricsChannel = frontcache_metrics::RpcMetricsService<Channel>;
pub type ByteStream = Pin<Box<dyn Stream<Item = Result<Bytes>> + Send + 'static>>;
type RawStream = tonic::Streaming<ReadRangeResponse>;

struct ClientConfig {
    lookup_timeout: Duration,
    read_timeout: Duration,
    max_retries: usize,
    prefetch_blocks: usize,
}

fn env_or<T: FromStr>(name: &str, default: T) -> T {
    match std::env::var(name) {
        Ok(v) => match v.parse() {
            Ok(parsed) => parsed,
            Err(_) => {
                tracing::warn!("Invalid value '{}' for {}, using default", v, name);
                default
            }
        },
        Err(_) => default,
    }
}

impl ClientConfig {
    fn from_env() -> Self {
        Self {
            lookup_timeout: Duration::from_millis(env_or("FRONTCACHE_LOOKUP_TIMEOUT_MS", 500)),
            read_timeout: Duration::from_millis(env_or("FRONTCACHE_READ_TIMEOUT_MS", 5000)),
            max_retries: env_or("FRONTCACHE_MAX_RETRIES", 3),
            prefetch_blocks: env_or("FRONTCACHE_PREFETCH_BLOCKS", 16),
        }
    }
}

fn retry_strategy(max_retries: usize) -> impl Iterator<Item = Duration> {
    ExponentialBackoff::from_millis(100)
        .factor(2)
        .max_delay(Duration::from_secs(1))
        .map(tokio_retry2::strategy::jitter)
        .take(max_retries)
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

fn resolve_range(range: &impl RangeBounds<u64>) -> (u64, Option<u64>) {
    let offset = match range.start_bound() {
        Bound::Included(&s) => s,
        Bound::Excluded(&s) => s + 1,
        Bound::Unbounded => 0,
    };
    let end = match range.end_bound() {
        Bound::Included(&e) => Some(e + 1),
        Bound::Excluded(&e) => Some(e),
        Bound::Unbounded => None,
    };
    (offset, end)
}

fn block_reads(offset: u64, end: u64) -> Vec<(u64, u64, u64)> {
    let mut reads = Vec::new();
    let mut current = offset;
    while current < end {
        let block_offset = (current / BLOCK_SIZE) * BLOCK_SIZE;
        let read_end = end.min(block_offset + BLOCK_SIZE);
        reads.push((block_offset, current, read_end - current));
        current = read_end;
    }
    reads
}

fn raw_to_byte_stream(stream: RawStream) -> ByteStream {
    Box::pin(
        futures::stream::try_unfold(stream, |mut s| async move {
            match s.message().await {
                Ok(Some(resp)) => Ok(Some((resp.data, s))),
                Ok(None) => Ok(None),
                Err(status) => Err(anyhow::Error::from(status)),
            }
        })
        .try_filter(|bytes| futures::future::ready(!bytes.is_empty())),
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

        let channel = Channel::from_shared(router_addr)?.connect().await?;
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

    pub async fn stream_range(
        &self,
        key: &str,
        range: impl RangeBounds<u64>,
        version: Option<&str>,
    ) -> Result<ByteStream> {
        let (offset, end) = resolve_range(&range);

        match end {
            Some(end) if end <= offset => Ok(Box::pin(futures::stream::empty())),
            Some(end) => Ok(self.make_block_stream(key, block_reads(offset, end), version)),
            None => {
                let block_offset = (offset / BLOCK_SIZE) * BLOCK_SIZE;
                let read_len = BLOCK_SIZE - (offset - block_offset);
                let key_s = key.to_string();
                let ver_s = version.map(|v| v.to_string());
                let (object_size, first) = self
                    .read_block_with_size(key_s, block_offset, offset, read_len, ver_s)
                    .await?;

                let next = block_offset + BLOCK_SIZE;
                if next >= object_size {
                    return Ok(first);
                }
                let rest = self.make_block_stream(key, block_reads(next, object_size), version);
                Ok(Box::pin(first.chain(rest)))
            }
        }
    }

    pub async fn read_range(
        &self,
        key: &str,
        range: impl RangeBounds<u64>,
        version: Option<&str>,
    ) -> Result<Bytes> {
        let (offset, end) = resolve_range(&range);

        let (end, first_block) = match end {
            Some(end) => (end, None),
            None => {
                let block_offset = (offset / BLOCK_SIZE) * BLOCK_SIZE;
                let read_len = BLOCK_SIZE - (offset - block_offset);
                let key_s = key.to_string();
                let ver_s = version.map(|v| v.to_string());
                let (object_size, stream) = self
                    .read_block_with_size(key_s, block_offset, offset, read_len, ver_s)
                    .await?;
                let data = drain_stream(stream).await?;
                (object_size, Some(data))
            }
        };

        if end <= offset {
            return Ok(Bytes::new());
        }
        let length = (end - offset) as usize;

        let reads_start = if first_block.is_some() {
            let block_offset = (offset / BLOCK_SIZE) * BLOCK_SIZE;
            (block_offset + BLOCK_SIZE).min(end)
        } else {
            offset
        };

        let client = self.clone();
        let key_s = key.to_string();
        let ver_s = version.map(|v| v.to_string());
        let prefetch = self.config.prefetch_blocks;

        let results: Vec<(usize, Bytes)> = futures::stream::iter(block_reads(reads_start, end))
            .map(move |(block_offset, read_offset, read_len)| {
                let client = client.clone();
                let key = key_s.clone();
                let version = ver_s.clone();
                let buf_pos = (read_offset - offset) as usize;
                async move {
                    let raw = client
                        .read_block_raw(key, block_offset, read_offset, read_len, version)
                        .await?;
                    let data = drain_stream(raw_to_byte_stream(raw)).await?;
                    Ok::<_, anyhow::Error>((buf_pos, data))
                }
            })
            .buffer_unordered(prefetch)
            .try_collect()
            .await?;

        let mut buf = BytesMut::with_capacity(length);
        // Safety: all positions will be written by the block copies below
        unsafe { buf.set_len(length) };
        if let Some(data) = &first_block {
            buf[..data.len()].copy_from_slice(data);
        }
        for (pos, data) in results {
            buf[pos..pos + data.len()].copy_from_slice(&data);
        }
        Ok(buf.freeze())
    }

    fn make_block_stream(
        &self,
        key: &str,
        reads: Vec<(u64, u64, u64)>,
        version: Option<&str>,
    ) -> ByteStream {
        let client = self.clone();
        let key = key.to_string();
        let version = version.map(|v| v.to_string());
        let prefetch = self.config.prefetch_blocks;

        Box::pin(
            futures::stream::iter(reads)
                .map(move |(block_offset, read_offset, read_len)| {
                    let client = client.clone();
                    let key = key.clone();
                    let version = version.clone();
                    async move {
                        let raw = client
                            .read_block_raw(key, block_offset, read_offset, read_len, version)
                            .await?;
                        Ok::<_, anyhow::Error>(raw_to_byte_stream(raw))
                    }
                })
                .buffered(prefetch)
                .try_flatten(),
        )
    }

    async fn read_block_with_size(
        &self,
        key: String,
        block_offset: u64,
        read_offset: u64,
        read_len: u64,
        version: Option<String>,
    ) -> Result<(u64, ByteStream)> {
        let mut raw = self
            .read_block_raw(key, block_offset, read_offset, read_len, version)
            .await?;
        let first = raw
            .message()
            .await?
            .ok_or_else(|| anyhow::anyhow!("empty response stream"))?;
        let object_size = first.object_size;
        let rest = raw_to_byte_stream(raw);
        let stream: ByteStream = if first.data.is_empty() {
            rest
        } else {
            Box::pin(futures::stream::once(async { Ok(first.data) }).chain(rest))
        };
        Ok((object_size, stream))
    }

    async fn read_block_raw(
        &self,
        key: String,
        block_offset: u64,
        read_offset: u64,
        read_len: u64,
        version: Option<String>,
    ) -> Result<RawStream> {
        let strategy = retry_strategy(self.config.max_retries);

        let client = self.clone();
        let read_timeout = self.config.read_timeout;
        Retry::spawn(strategy, || {
            let client = client.clone();
            let key = key.clone();
            let version = version.clone();
            async move {
                let addrs = client
                    .lookup_owners(&key, block_offset)
                    .await
                    .map_err(RetryError::transient)?;

                let mut last_err = None;
                for addr in &addrs {
                    let mut cache_client = match client.get_or_create_cache_connection(addr).await {
                        Ok(c) => c,
                        Err(e) => {
                            last_err = Some(RetryError::transient(e));
                            continue;
                        }
                    };
                    let mut req = tonic::Request::new(ReadRangeRequest {
                        key: key.clone(),
                        offset: read_offset,
                        length: read_len,
                        version: version.clone(),
                    });
                    req.set_timeout(read_timeout);
                    match cache_client.read_range(req).await {
                        Ok(resp) => return Ok(resp.into_inner()),
                        Err(s) if !is_retryable(s.code()) => {
                            return Err(RetryError::permanent(anyhow::Error::from(s)));
                        }
                        Err(s) => {
                            last_err = Some(RetryError::transient(anyhow::Error::from(s)));
                        }
                    }
                }
                Err(last_err.unwrap_or_else(|| {
                    RetryError::transient(anyhow::anyhow!("no owners returned"))
                }))
            }
        })
        .await
    }

    async fn lookup_owners(&self, key: &str, block_offset: u64) -> Result<Vec<String>> {
        let strategy = retry_strategy(self.config.max_retries);

        let router = self.router.clone();
        let resp = Retry::spawn(strategy, || {
            let mut router = router.clone();
            let mut req = tonic::Request::new(LookupOwnerRequest {
                key: key.to_string(),
                offset: block_offset,
            });
            req.set_timeout(self.config.lookup_timeout);
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

        Ok(resp.into_inner().addrs)
    }

    async fn get_or_create_cache_connection(
        &self,
        addr: &str,
    ) -> Result<CacheServiceClient<MetricsChannel>> {
        if let Some(client) = self.connections.get(addr) {
            return Ok(client.clone());
        }

        let endpoint = Channel::from_shared(format!("http://{}", addr))?;

        let strategy = retry_strategy(self.config.max_retries);

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

async fn drain_stream(stream: ByteStream) -> Result<Bytes> {
    let data: BytesMut = stream
        .try_fold(BytesMut::new(), |mut acc, chunk| async move {
            acc.extend_from_slice(&chunk);
            Ok(acc)
        })
        .await?;
    Ok(data.freeze())
}
