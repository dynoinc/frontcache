use std::net::SocketAddr;
use std::sync::Arc;

use axum::{
    Router,
    body::Body,
    extract::{Request, State},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
    routing::get,
    serve::ListenerExt,
};
use futures_util::future::Either;
use futures_util::{StreamExt, stream};
use http_body_util::BodyExt;
use hyper_util::client::legacy::Client as HyperClient;
use hyper_util::rt::TokioExecutor;

use crate::ring::Straw2Router;

const PREFETCH_BLOCKS: usize = 16;

type HttpClient = HyperClient<hyper_util::client::legacy::connect::HttpConnector, Body>;

#[derive(Clone)]
struct AppState {
    ring: Arc<Straw2Router>,
    block_size: u64,
    http_client: HttpClient,
}

pub struct RouterServer {
    ring: Arc<Straw2Router>,
    block_size: u64,
}

impl RouterServer {
    pub fn new(ring: Arc<Straw2Router>, block_size: u64) -> Self {
        assert!(block_size > 0, "block_size must be > 0");
        Self { ring, block_size }
    }

    pub fn into_router(self) -> Router {
        let state = AppState {
            ring: self.ring,
            block_size: self.block_size,
            http_client: {
                let mut connector = hyper_util::client::legacy::connect::HttpConnector::new();
                connector.set_nodelay(true);
                HyperClient::builder(TokioExecutor::new()).build(connector)
            },
        };
        Router::new()
            .route("/healthz", get(healthz))
            .route("/{*key}", get(get_object))
            .layer(frontcache_metrics::layer())
            .with_state(state)
    }

    pub async fn serve(self, addr: SocketAddr) -> anyhow::Result<()> {
        let app = self.into_router();
        let listener = tokio::net::TcpListener::bind(addr).await?;
        tracing::info!("Router listening on {}", addr);
        axum::serve(
            listener.tap_io(|tcp| {
                if let Err(e) = tcp.set_nodelay(true) {
                    tracing::warn!("failed to set TCP_NODELAY: {e}");
                }
            }),
            app,
        )
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

fn extract_key(req: &Request) -> String {
    format!("/{}", req.uri().path().trim_start_matches('/'))
}

fn parse_range(headers: &HeaderMap) -> Option<(u64, Option<u64>)> {
    let range = headers.get(header::RANGE)?.to_str().ok()?;
    let range = range.strip_prefix("bytes=")?;
    let (start, end) = range.split_once('-')?;
    let start: u64 = start.parse().ok()?;
    let end = if end.is_empty() {
        None
    } else {
        Some(end.parse::<u64>().ok()? + 1)
    };
    Some((start, end))
}

/// Compute block-aligned reads: Vec<(block_offset, read_offset, read_len)>
fn block_reads(offset: u64, end: u64, bs: u64) -> Vec<(u64, u64, u64)> {
    let mut reads = Vec::new();
    let mut current = offset;
    while current < end {
        let block_offset = (current / bs) * bs;
        let read_end = end.min(block_offset + bs);
        reads.push((block_offset, current, read_end - current));
        current = read_end;
    }
    reads
}

async fn get_object(State(state): State<AppState>, req: Request) -> Response {
    let key = extract_key(&req);
    let headers = req.headers().clone();
    let range = parse_range(&headers);
    let (offset, requested_end) = range.unwrap_or_default();

    let bs = state.block_size;
    let first_block_offset = (offset / bs) * bs;
    let first_read_len =
        (first_block_offset + bs - offset).min(requested_end.unwrap_or(u64::MAX) - offset);
    let first = (first_block_offset, offset, first_read_len);

    // Preflight: fetch the first block. The server response tells us
    // object_size and etag via Content-Range / ETag headers, avoiding
    // a separate HEAD round-trip.
    let (object_size, etag, first_stream) = match fetch_block_with_meta(
        state.http_client.clone(),
        state.ring.clone(),
        key.clone(),
        first,
    )
    .await
    {
        Ok(v) => v,
        Err(_) => return StatusCode::SERVICE_UNAVAILABLE.into_response(),
    };

    if offset >= object_size {
        return StatusCode::RANGE_NOT_SATISFIABLE.into_response();
    }

    let end = requested_end.map_or(object_size, |e| e.min(object_size));
    if end <= offset {
        return (
            StatusCode::OK,
            [(header::CONTENT_LENGTH, "0")],
            Body::empty(),
        )
            .into_response();
    }

    let content_len = end - offset;
    let remaining_reads: Vec<_> = block_reads(offset, end, bs).into_iter().skip(1).collect();

    let client = state.http_client.clone();
    let ring = state.ring.clone();
    let key_clone = key.clone();

    let rest_stream =
        stream::iter(remaining_reads)
            .map(move |(block_offset, read_offset, read_len)| {
                let client = client.clone();
                let ring = ring.clone();
                let key = key_clone.clone();
                async move {
                    fetch_block(client, ring, key, (block_offset, read_offset, read_len)).await
                }
            })
            .buffered(PREFETCH_BLOCKS)
            .map(|result| match result {
                Ok(chunk_stream) => Either::Left(chunk_stream),
                Err(e) => Either::Right(stream::once(async move { Err(std::io::Error::other(e)) })),
            })
            .flatten();

    let body_stream = first_stream.chain(rest_stream);

    let status = if range.is_some() {
        StatusCode::PARTIAL_CONTENT
    } else {
        StatusCode::OK
    };

    let mut builder = Response::builder()
        .status(status)
        .header(header::ACCEPT_RANGES, "bytes")
        .header(header::CONTENT_LENGTH, content_len.to_string())
        .header(header::CONTENT_TYPE, "application/octet-stream")
        .header(header::ETAG, format!("\"{}\"", etag))
        .header(frontcache_metrics::OP_HEADER, "GetObject");

    if range.is_some() {
        builder = builder.header(
            header::CONTENT_RANGE,
            format!("bytes {}-{}/{}", offset, end - 1, object_size),
        );
    }

    builder.body(Body::from_stream(body_stream)).unwrap()
}

async fn try_fetch(
    client: &HttpClient,
    addr: SocketAddr,
    key: &str,
    range: &str,
) -> Result<http::Response<hyper::body::Incoming>, anyhow::Error> {
    let uri = format!("http://{}{}", addr, key);
    let req = http::Request::builder()
        .uri(uri)
        .header("Range", range)
        .body(Body::empty())
        .unwrap();
    let resp = client.request(req).await?;
    if resp.status().is_success() || resp.status() == StatusCode::PARTIAL_CONTENT {
        Ok(resp)
    } else {
        anyhow::bail!("server {} returned {}", addr, resp.status())
    }
}

async fn fetch_raw_block(
    client: &HttpClient,
    ring: &Straw2Router,
    key: &str,
    (block_offset, read_offset, read_len): (u64, u64, u64),
) -> Result<http::Response<hyper::body::Incoming>, anyhow::Error> {
    let addrs = ring
        .get_owners(key, block_offset)
        .ok_or_else(|| anyhow::anyhow!("no nodes available"))?;

    let range = format!("bytes={}-{}", read_offset, read_offset + read_len - 1);
    let mut last_err = None;
    for &addr in &addrs {
        match try_fetch(client, addr, key, &range).await {
            Ok(resp) => return Ok(resp),
            Err(e) => last_err = Some(e),
        }
    }
    Err(last_err.unwrap_or_else(|| anyhow::anyhow!("no owners")))
}

fn into_byte_stream(
    resp: http::Response<hyper::body::Incoming>,
) -> impl futures_util::Stream<Item = Result<bytes::Bytes, std::io::Error>> {
    resp.into_body()
        .into_data_stream()
        .map(|r| r.map_err(std::io::Error::other))
}

/// Parse the total size from a Content-Range header: "bytes 0-4095/52428800"
fn parse_content_range_total(headers: &http::HeaderMap) -> Option<u64> {
    let val = headers.get(header::CONTENT_RANGE)?.to_str().ok()?;
    val.rsplit_once('/')?.1.parse().ok()
}

/// Fetch a block and extract object_size + etag from the server's Content-Range / ETag headers.
async fn fetch_block_with_meta(
    client: HttpClient,
    ring: Arc<Straw2Router>,
    key: String,
    block: (u64, u64, u64),
) -> Result<
    (
        u64,
        String,
        impl futures_util::Stream<Item = Result<bytes::Bytes, std::io::Error>>,
    ),
    anyhow::Error,
> {
    let resp = fetch_raw_block(&client, &ring, &key, block).await?;
    let object_size = parse_content_range_total(resp.headers())
        .ok_or_else(|| anyhow::anyhow!("server response missing Content-Range header"))?;
    let etag = resp
        .headers()
        .get(header::ETAG)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .trim_matches('"')
        .to_string();
    Ok((object_size, etag, into_byte_stream(resp)))
}

async fn fetch_block(
    client: HttpClient,
    ring: Arc<Straw2Router>,
    key: String,
    block: (u64, u64, u64),
) -> Result<impl futures_util::Stream<Item = Result<bytes::Bytes, std::io::Error>>, anyhow::Error> {
    let resp = fetch_raw_block(&client, &ring, &key, block).await?;
    Ok(into_byte_stream(resp))
}
