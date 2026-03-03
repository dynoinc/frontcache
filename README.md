# FrontCache [![Check](https://github.com/dynoinc/frontcache/actions/workflows/check.yml/badge.svg)](https://github.com/dynoinc/frontcache/actions/workflows/check.yml)

Distributed pull-through cache for object storage (S3/GCS).

## Features

- Block-based caching with 16MB fixed blocks
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

### Server Flags (`frontcache-server`)

| Flag | Default | Description |
|------|---------|-------------|
| `--listen` | `0.0.0.0:8080` | Address to listen on |
| `--cache-dirs` | `/tmp/frontcache:1GiB` | Cache directories with sizes (`path:size`, comma-separated for multiple) |
| `--index-path` | `/var/lib/frontcache/index.db` | Path to the block index database |

### Environment Variables

Metrics are exported via [OpenTelemetry](https://opentelemetry.io/docs/languages/sdk-configuration/general/) using standard `OTEL_*` environment variables. If `OTEL_EXPORTER_OTLP_ENDPOINT` is unset, a no-op provider is used.

### Cache Purger

The server runs a background purger that monitors disk usage every 10 seconds. When a cache directory exceeds 95% capacity, the purger evicts least-recently-used blocks until the target fill rate is restored. LRU timestamps are persisted to a separate redb table and flushed every 60 seconds, so eviction order survives restarts.

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
use frontcache_client::CacheClient;
use futures::StreamExt;

#[tokio::main]
async fn main() -> Result<()> {
    let client = CacheClient::new("http://router:8081".to_string()).await?;

    let mut stream = client.stream_range("s3://bucket/file", 0, 1024 * 1024, Some("v1"))?;
    while let Some(chunk) = stream.next().await {
        let bytes = chunk?;
        // process bytes...
    }
    Ok(())
}
```

## License

MIT
