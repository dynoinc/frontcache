# FrontCache [![Check](https://github.com/dynoinc/frontcache/actions/workflows/check.yml/badge.svg)](https://github.com/dynoinc/frontcache/actions/workflows/check.yml)

Distributed pull-through cache for object storage (S3/GCS) written in < 2000 lines of code.

## Features

- Block-based caching with 16MB fixed blocks
- Straw2 hashing for distributed block ownership
- Kubernetes auto-discovery
- Memory-mapped zero-copy reads
- OpenTelemetry metrics for observability

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
| `--label` | `app=frontcache` | Label selector for Kubernetes pod discovery |
| `--server-port` | `8080` | Port that server pods listen on |

### Server Flags (`frontcache-server`)

| Flag | Default | Description |
|------|---------|-------------|
| `--listen` | `0.0.0.0:8080` | Address to listen on |
| `--cache-dirs` | `/tmp/frontcache` | Cache directories (comma-separated for multiple) |

### Environment Variables

Metrics are exported via [OpenTelemetry](https://opentelemetry.io/docs/languages/sdk-configuration/general/) using standard `OTEL_*` environment variables. If `OTEL_EXPORTER_OTLP_ENDPOINT` is unset, a no-op provider is used.

### Cache Purger

The server runs a background purger that monitors disk usage every 10 seconds. When a cache directory exceeds 95% capacity, the purger evicts least-recently-used blocks until the target fill rate is restored. LRU state is not persisted to disk.

## Kubernetes

The router auto-discovers server pods by watching pods with a matching label selector (`--label`). The ServiceAccount needs RBAC permissions:

```yaml
rules:
  - apiGroups: [""]
    resources: ["pods"]
    verbs: ["get", "list", "watch"]
```

## Client Libraries

### Rust Client

```rust
use frontcache_client::CacheClient;

#[tokio::main]
async fn main() -> Result<()> {
    let client = CacheClient::new("router:8081".to_string()).await?;

    let data = client.read_range("s3://bucket/file", 0, 1024 * 1024, Some("v1")).await?;
    println!("Read {} bytes", data.len());
    Ok(())
}
```

### Python Client

```python
import asyncio
import frontcache

async def main():
    client = await frontcache.connect("router:8081")

    data = await client.read_range("s3://bucket/file", 0, 1024 * 1024, "v1")  # or omit version for immutable objects
    print(f"Read {len(data)} bytes")

asyncio.run(main())
```

## License

MIT
