# FrontCache [![Check](https://github.com/dynoinc/frontcache/actions/workflows/check.yml/badge.svg)](https://github.com/dynoinc/frontcache/actions/workflows/check.yml)

Distributed pull-through cache for object storage (S3/GCS) written in < 2000 lines of code.

## Features

- Block-based caching with 16MB fixed blocks
- Consistent hashing for distributed block ownership
- Kubernetes auto-discovery
- Memory-mapped zero-copy reads
- OpenTelemetry metrics for observability

## Configuration

### Command Line Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--listen` | `0.0.0.0:8080` | Address to listen on |
| `--cache-dirs` | `/tmp/frontcache` | Cache directories (comma-separated for multiple) |
| `--label` | `app=frontcache` | Label selector for Kubernetes pod discovery |

### Environment Variables

| Variable | Description |
|----------|-------------|
| `OTEL_EXPORTER_OTLP_ENDPOINT` | OTLP endpoint for metrics (e.g., `http://otel-collector:4317`). If unset, metrics are disabled. |
| `OTEL_SERVICE_NAME` | Service name for metrics. Defaults to `unknown_service` if not set. |

### Cache Purger

The server runs a background purger that monitors disk usage every 10 seconds. When a cache directory exceeds 95% capacity, the purger evicts least-recently-used blocks until the target fill rate is restored. LRU state is not persisted to disk.

## Kubernetes

The server auto-discovers peers by watching pods with a matching label selector (`--label`). The ServiceAccount needs RBAC permissions:

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
    let client = CacheClient::new(8080).await?;

    let data = client.read_range("s3://bucket/file", 0, 1024 * 1024, "v1").await?;
    println!("Read {} bytes", data.len());
    Ok(())
}
```

### Python Client

```python
import asyncio
import frontcache

async def main():
    client = await frontcache.connect(8080)

    data = await client.read_range("s3://bucket/file", 0, 1024 * 1024, "v1")
    print(f"Read {len(data)} bytes")

asyncio.run(main())
```

## License

MIT
