# FrontCache [![Check](https://github.com/dynoinc/frontcache/actions/workflows/check.yml/badge.svg)](https://github.com/dynoinc/frontcache/actions/workflows/check.yml)

Distributed pull-through cache for object storage (S3/GCS).

## Features

- Block-based caching with configurable block size (default 16MB)
- Straw2 consistent hashing for distributed block ownership across 262,144 virtual partitions
- Multi-block reads stitched by router with prefetch
- Kubernetes auto-discovery of server pods
- Aligned direct I/O reads (O_DIRECT on Linux)
- LRU eviction with configurable watermarks
- Backend fetch rate limiting (concurrency, RPS, bandwidth)
- OpenTelemetry metrics

## Architecture

FrontCache runs as two binaries:

- **frontcache-router** — HTTP gateway. Accepts `GET /<bucket>/<key>` requests with optional `Range` headers. Splits reads into block-aligned fetches and routes each block to the owning server via straw2 hashing. Streams the response back with prefetch buffering.
- **frontcache-server** — data plane. Serves cached blocks via HTTP. Fetches from object storage on cache miss, serves from local disk on cache hit.

```
Client → Router → Server (cache hit → disk)
                        ↘ (cache miss → S3/GCS → disk → response)
```

## Configuration

### Bucket Config

A YAML file maps bucket names to storage providers. The server accepts `--bucket-config <path>`. If omitted, all buckets default to S3.

```yaml
default_provider: s3
buckets:
  my-gcs-bucket:
    provider: gs
```

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
| `--cache-dirs` | `/tmp/frontcache:1GiB` | Cache directories with sizes (`path:size`, comma-separated) |
| `--index-path` | `/var/lib/frontcache/index.db` | Path to the block index database |
| `--block-size` | `16777216` | Block size in bytes |
| `--chunk-size` | `262144` | HTTP response chunk size in bytes |
| `--bucket-config` | (none) | Path to bucket config YAML |

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

## Usage

```bash
# Read a full object
curl http://router:8081/my-bucket/path/to/file.bin -o file.bin

# Read a range
curl -H "Range: bytes=0-4095" http://router:8081/my-bucket/path/to/file.bin
```

## Kubernetes

The router auto-discovers server pods by watching pods with a matching label selector (`--label`). The ServiceAccount needs RBAC permissions:

```yaml
rules:
  - apiGroups: [""]
    resources: ["pods"]
    verbs: ["get", "list", "watch"]
```

## License

MIT
