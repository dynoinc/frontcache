# FrontCache [![Check](https://github.com/dynoinc/frontcache/actions/workflows/check.yml/badge.svg)](https://github.com/dynoinc/frontcache/actions/workflows/check.yml)

Distributed pull-through cache for object storage (S3/GCS).

## Features

- Block-based caching with configurable block size (default 16MB)
- Straw2 hashing for distributed block ownership
- Kubernetes auto-discovery
- Aligned direct I/O reads
- OpenTelemetry metrics for observability
- Backend fetch rate limiting (concurrency, RPS, bandwidth)

## Architecture

FrontCache runs as two binaries:

- **frontcache-router** — stateless routing layer. Accepts `LookupOwner` RPCs and returns the server pod that owns a given block using straw2 hashing over 262,144 virtual partitions.
- **frontcache-server** — data plane. Accepts `ReadRange` RPCs, fetches from object storage on cache miss, and serves from local disk on cache hit.

Clients talk to the router to discover which server owns a block, then read directly from that server.

## Configuration

### Router Flags (`frontcache-router`)

| Flag | Default | Description |
|------|---------|-------------|
| `--listen` | `0.0.0.0:8081` | Address to listen on |
| `--label` | `` | Label selector for Kubernetes pod discovery |
| `--server-port` | `8080` | Port that server pods listen on |
| `--block-size` | `16777216` | Block size in bytes |

### Server Flags (`frontcache-server`)

| Flag | Default | Description |
|------|---------|-------------|
| `--listen` | `0.0.0.0:8080` | Address to listen on |
| `--cache-dirs` | `/tmp/frontcache:1GiB` | Cache directories with sizes (`path:size`, comma-separated for multiple) |
| `--index-path` | `/var/lib/frontcache/index.db` | Path to the block index database |
| `--block-size` | `16777216` | Block size in bytes |
| `--chunk-size` | `262144` | gRPC stream chunk size in bytes |

### Server Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `FRONTCACHE_STORE_MAX_INFLIGHT_FETCHES` | `0` (unlimited) | Max concurrent backend fetches |
| `FRONTCACHE_STORE_MAX_REQUESTS_PER_SEC` | `0` (unlimited) | Backend request rate limit |
| `FRONTCACHE_STORE_REQUESTS_BURST` | `1` | Request rate burst size |
| `FRONTCACHE_STORE_MAX_BYTES_PER_SEC` | `0` (unlimited) | Backend bandwidth limit |
| `FRONTCACHE_STORE_BYTES_BURST` | `16777216` | Bandwidth burst size |
| `FRONTCACHE_STORE_LIMIT_WAIT_TIMEOUT_MS` | `0` (unlimited) | Timeout waiting for rate limiter |
| `FRONTCACHE_MIN_UTILIZATION_PERCENT` | `90` | Purger low watermark |
| `FRONTCACHE_MAX_UTILIZATION_PERCENT` | `95` | Purger high watermark |
| `FRONTCACHE_FLUSH_INTERVAL_SECS` | `60` | LRU timestamp flush interval |

Metrics are exported via [OpenTelemetry](https://opentelemetry.io/docs/languages/sdk-configuration/general/) using standard `OTEL_*` environment variables. If `OTEL_EXPORTER_OTLP_ENDPOINT` is unset, a no-op provider is used.

### Cache Purger

After each block download, the server checks disk usage. When a cache directory exceeds the high watermark (default 95%), a background task evicts least-recently-used blocks until usage drops to the low watermark (default 90%). LRU timestamps are persisted to a separate redb table and flushed every 60 seconds, so eviction order survives restarts.

## Kubernetes

The router auto-discovers server pods by watching pods with a matching label selector (`--label`). The ServiceAccount needs RBAC permissions:

```yaml
rules:
  - apiGroups: [""]
    resources: ["pods"]
    verbs: ["get", "list", "watch"]
```

## Rust Client

```rust
use frontcache_client::{CacheClient, CacheClientBuilder};
use futures::StreamExt;

#[tokio::main]
async fn main() -> Result<()> {
    // Quick start with defaults
    let client = CacheClient::new("http://router:8081").await?;

    // Or configure via builder
    let client = CacheClientBuilder::new("http://router:8081")
        .block_size(16 * 1024 * 1024)
        .build()
        .await?;

    // Streaming read (ordered, lazy)
    let mut stream = client.stream_range("s3://bucket/file", 0..1024 * 1024, Some("v1")).await?;
    while let Some(chunk) = stream.next().await {
        let bytes = chunk?;
        // process bytes...
    }

    // Buffered read (concurrent, out-of-order assembly)
    let data = client.read_range("s3://bucket/file", 0..1024 * 1024, Some("v1")).await?;

    Ok(())
}
```

## License

MIT
