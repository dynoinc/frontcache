# FrontCache

Distributed pull-through cache for object storage (S3/GCS) written in < 2000 lines of code.

## Features

- Block-based caching with 16MB fixed blocks
- Consistent hashing for distributed block ownership
- Kubernetes auto-discovery
- Memory-mapped zero-copy reads
- OpenTelemetry metrics for observability

Run `just check` to build, test, and verify the project.

## Usage

### Server

```bash
# Works both standalone and in Kubernetes
./target/debug/frontcache-server \
  --listen 0.0.0.0:8080 \
  --cache-dir /data/cache \
  --label app=frontcache \
  --local-root /data  # Optional: enable file:// (testing only)
```

### Rust Client

```rust
use frontcache_client::CacheClient;

#[tokio::main]
async fn main() -> Result<()> {
    let client = CacheClient::new(8080).await?;

    let data = client.read_range("s3://bucket/file", 0, 1024 * 1024).await?;
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

    data = await client.read_range("file://datasets/model.bin", 0, 1024 * 1024)
    print(f"Read {len(data)} bytes")

asyncio.run(main())
```

## Observability

FrontCache includes OpenTelemetry metrics for monitoring cache performance, upstream store operations, and RPC calls.

Set `OTEL_EXPORTER_OTLP_ENDPOINT` to export metrics (e.g., `http://localhost:4317`). If unset, a no-op provider is used.

### Metrics

**Client & Server Metrics:**
- `rpc_duration_seconds` - RPC request duration (labels: rpc.method, rpc.grpc.status)

**Server Metrics:**
- `store_read_duration_seconds` - Upstream store read duration
- `store_read_bytes` - Bytes read from upstream store
- `cache_get_duration_seconds` - Cache operation duration
- `disk_available_bytes` - Available disk space

## License

MIT
