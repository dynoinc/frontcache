# FrontCache [![Check](https://github.com/dynoinc/frontcache/actions/workflows/check.yml/badge.svg)](https://github.com/dynoinc/frontcache/actions/workflows/check.yml)

Distributed pull-through cache for object storage (S3/GCS) with an S3-compatible HTTP API.

## Features

- S3-compatible HTTP API (GetObject, HeadObject, PutObject, DeleteObject, ListObjectsV2, multipart upload)
- Block-based caching with configurable block size (default 16MB)
- Straw2 hashing for distributed block ownership
- Single-block reads redirected (307) to owning server; multi-block reads stitched by router
- Kubernetes auto-discovery
- Aligned direct I/O reads
- OpenTelemetry metrics for observability
- Backend fetch rate limiting (concurrency, RPS, bandwidth)

## Architecture

FrontCache runs as two binaries:

- **frontcache-router** — S3-compatible gateway. Accepts standard HTTP requests (`GET`, `HEAD`, `PUT`, `DELETE`, `POST`) on `/<bucket>/<key>`. Cached reads (GET/HEAD) are routed to the server owning the block via straw2 hashing over 262,144 virtual partitions. Writes and listings pass through directly to the backend.
- **frontcache-server** — data plane. Serves cached blocks via HTTP. Fetches from object storage on cache miss, serves from local disk on cache hit.

Any S3-compatible client works out of the box — curl, boto3, AWS CLI, Spark, DuckDB, etc.

### Supported Operations

| Operation | HTTP | Router Behavior |
|---|---|---|
| GetObject | `GET /bucket/key` | Cached — 307 redirect (single block) or stitch (multi-block) |
| GetObject + Range | `GET /bucket/key` + `Range` header | Cached — same as above |
| HeadObject | `HEAD /bucket/key` | Cached — routes to server, returns headers |
| ListObjectsV2 | `GET /bucket?list-type=2` | Pass-through to backend |
| PutObject | `PUT /bucket/key` | Pass-through to backend |
| DeleteObject | `DELETE /bucket/key` | Pass-through to backend |
| CreateMultipartUpload | `POST /bucket/key?uploads` | Pass-through to backend |
| UploadPart | `PUT /bucket/key?partNumber=N&uploadId=X` | Pass-through to backend |
| CompleteMultipartUpload | `POST /bucket/key?uploadId=X` | Pass-through to backend |

## Configuration

### Bucket Config

A YAML file maps bucket names to storage providers. Both router and server accept `--bucket-config <path>`. If omitted, all buckets default to S3.

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
| `--bucket-config` | (none) | Path to bucket config YAML |

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
# Read a range
curl -H "Range: bytes=0-4095" http://router:8081/my-bucket/path/to/file.bin

# Head request
curl -I http://router:8081/my-bucket/path/to/file.bin

# Upload a file
curl -X PUT -T myfile.bin http://router:8081/my-bucket/path/to/file.bin

# List objects
curl "http://router:8081/my-bucket?list-type=2&prefix=path/"

# Delete
curl -X DELETE http://router:8081/my-bucket/path/to/file.bin
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
