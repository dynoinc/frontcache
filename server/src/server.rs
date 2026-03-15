use std::net::SocketAddr;
use std::sync::Arc;

use axum::{
    Router,
    body::Body,
    extract::{Request, State},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
    routing::get,
};
use opentelemetry::KeyValue;

use crate::cache::{Cache, CacheError, CacheHit};
use frontcache_store::StoreError;

#[derive(Clone)]
struct AppState {
    cache: Arc<Cache>,
    chunk_size: usize,
}

pub struct CacheServer {
    cache: Arc<Cache>,
    chunk_size: usize,
}

impl CacheServer {
    pub fn new(cache: Arc<Cache>, chunk_size: usize) -> Self {
        assert!(chunk_size > 0, "chunk_size must be > 0");
        Self { cache, chunk_size }
    }

    pub fn into_router(self) -> Router {
        let state = AppState {
            cache: self.cache,
            chunk_size: self.chunk_size,
        };
        Router::new()
            .route("/healthz", get(healthz))
            .route("/{*key}", get(get_object).head(head_object))
            .layer(frontcache_metrics::layer())
            .with_state(state)
    }

    pub async fn serve(self, addr: SocketAddr) -> anyhow::Result<()> {
        let app = self.into_router();
        let listener = tokio::net::TcpListener::bind(addr).await?;
        tracing::info!("Server listening on {}", addr);
        axum::serve(listener, app)
            .with_graceful_shutdown(shutdown_signal())
            .await?;
        Ok(())
    }
}

async fn shutdown_signal() {
    let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .expect("failed to install SIGTERM handler");

    tokio::select! {
        _ = tokio::signal::ctrl_c() => tracing::info!("Received Ctrl+C, starting graceful shutdown"),
        _ = sigterm.recv() => tracing::info!("Received SIGTERM, starting graceful shutdown"),
    }
}

async fn healthz() -> StatusCode {
    StatusCode::OK
}

/// Parse HTTP Range header: `bytes=N-M` or `bytes=N-`
/// Returns (offset, Option<end_exclusive>)
fn parse_range(headers: &HeaderMap) -> Option<(u64, Option<u64>)> {
    let range = headers.get(header::RANGE)?.to_str().ok()?;
    let range = range.strip_prefix("bytes=")?;
    let (start, end) = range.split_once('-')?;
    let start: u64 = start.parse().ok()?;
    let end = if end.is_empty() {
        None
    } else {
        Some(end.parse::<u64>().ok()? + 1) // HTTP range end is inclusive, convert to exclusive
    };
    Some((start, end))
}

fn cache_error_to_response(e: CacheError) -> Response {
    let msg = format!("{:?}", e);
    let status = match &e {
        CacheError::StoreRead { source, .. } => match source.as_ref() {
            StoreError::InvalidKey(_)
            | StoreError::InvalidRange { .. }
            | StoreError::UnsupportedProvider(_) => StatusCode::BAD_REQUEST,
            StoreError::NotFound(_) => StatusCode::NOT_FOUND,
            StoreError::Backend(_) => StatusCode::BAD_GATEWAY,
        },
        CacheError::Throttled { .. } => StatusCode::TOO_MANY_REQUESTS,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    };
    (status, msg).into_response()
}

async fn get_object(State(state): State<AppState>, req: Request) -> Response {
    let key = format!("/{}", req.uri().path().trim_start_matches('/'));
    let headers = req.headers().clone();

    let (offset, end) = parse_range(&headers).unwrap_or_default();

    let hit = match state.cache.get(&key, offset).await {
        Ok(hit) => hit,
        Err(e) => return cache_error_to_response(e),
    };

    let bs = state.cache.block_size();
    let block_offset = (offset / bs) * bs;
    let offset_in_block = (offset - block_offset) as usize;
    let block_len = hit.block_size() as usize;
    let object_size = hit.object_size();
    let etag = hit.version().to_string();

    if offset_in_block >= block_len {
        return StatusCode::RANGE_NOT_SATISFIABLE.into_response();
    }

    let req_end = match end {
        Some(e) => {
            let e_in_block = (e - block_offset) as usize;
            block_len.min(e_in_block)
        }
        None => block_len,
    };
    let chunk_end = req_end;
    let serve_bytes = chunk_end - offset_in_block;

    frontcache_metrics::get()
        .disk_byte_changes
        .add(serve_bytes as u64, &[KeyValue::new("action", "served")]);

    let chunk_size = state.chunk_size;
    let body: Body = match hit {
        CacheHit::Disk { reader, .. } => Body::from_stream(futures_util::stream::try_unfold(
            (reader, offset_in_block),
            move |(reader, pos)| async move {
                if pos >= chunk_end {
                    return Ok::<_, std::io::Error>(None);
                }
                let len = chunk_size.min(chunk_end - pos);
                let chunk = reader
                    .read_chunk(pos as u64, len)
                    .await
                    .map_err(std::io::Error::other)?;
                let chunk_len = chunk.len();
                Ok(Some((chunk, (reader, pos + chunk_len))))
            },
        )),
        CacheHit::Fresh { data, .. } => Body::from(data.slice(offset_in_block..chunk_end)),
    };

    let range_start = offset;
    let range_end = block_offset + chunk_end as u64 - 1; // inclusive

    Response::builder()
        .status(StatusCode::PARTIAL_CONTENT)
        .header(
            header::CONTENT_RANGE,
            format!("bytes {}-{}/{}", range_start, range_end, object_size),
        )
        .header(header::CONTENT_LENGTH, serve_bytes.to_string())
        .header(header::ACCEPT_RANGES, "bytes")
        .header(header::ETAG, format!("\"{}\"", etag))
        .header(frontcache_metrics::OP_HEADER, "GetObject")
        .body(body)
        .unwrap()
}

async fn head_object(State(state): State<AppState>, req: Request) -> Response {
    let key = format!("/{}", req.uri().path().trim_start_matches('/'));

    let hit = match state.cache.get(&key, 0).await {
        Ok(hit) => hit,
        Err(e) => return cache_error_to_response(e),
    };

    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_LENGTH, hit.object_size().to_string())
        .header(header::ETAG, format!("\"{}\"", hit.version()))
        .header(header::ACCEPT_RANGES, "bytes")
        .header(frontcache_metrics::OP_HEADER, "HeadObject")
        .body(Body::empty())
        .unwrap()
}
