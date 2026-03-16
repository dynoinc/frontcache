use std::net::SocketAddr;
use std::sync::Arc;

use axum::{
    Router,
    body::Body,
    extract::{Query, Request, State},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
    routing::get,
};
use dashmap::DashMap;
use futures_util::future::Either;
use futures_util::{StreamExt, stream};
use object_store::MultipartUpload;
use serde::Deserialize;
use tokio::sync::Mutex;

use crate::ring::Straw2Router;
use frontcache_store::Store;

const PREFETCH_BLOCKS: usize = 16;

#[derive(Clone)]
struct AppState {
    ring: Arc<Straw2Router>,
    block_size: u64,
    server_port: u16,
    http_client: reqwest::Client,
    store: Arc<Store>,
    uploads: Arc<DashMap<String, Mutex<Box<dyn MultipartUpload>>>>,
}

pub struct RouterServer {
    ring: Arc<Straw2Router>,
    block_size: u64,
    server_port: u16,
    store: Arc<Store>,
}

impl RouterServer {
    pub fn new(
        ring: Arc<Straw2Router>,
        block_size: u64,
        server_port: u16,
        store: Arc<Store>,
    ) -> Self {
        assert!(block_size > 0, "block_size must be > 0");
        Self {
            ring,
            block_size,
            server_port,
            store,
        }
    }

    pub fn into_router(self) -> Router {
        let state = AppState {
            ring: self.ring,
            block_size: self.block_size,
            server_port: self.server_port,
            http_client: reqwest::Client::new(),
            store: self.store,
            uploads: Arc::new(DashMap::new()),
        };
        Router::new()
            .route("/healthz", get(healthz))
            .route(
                "/{*key}",
                get(get_or_list)
                    .head(head_object)
                    .put(put_handler)
                    .delete(delete_object)
                    .post(post_handler),
            )
            .layer(frontcache_metrics::layer())
            .with_state(state)
    }

    pub async fn serve(self, addr: SocketAddr) -> anyhow::Result<()> {
        let app = self.into_router();
        let listener = tokio::net::TcpListener::bind(addr).await?;
        tracing::info!("Router listening on {}", addr);
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

/// GET /{bucket} — could be ListObjectsV2 or GetObject for a bucket-named key
#[derive(Deserialize)]
struct ListParams {
    #[serde(rename = "list-type")]
    list_type: Option<String>,
    prefix: Option<String>,
    #[allow(dead_code)]
    delimiter: Option<String>,
}

async fn get_or_list(
    State(state): State<AppState>,
    Query(params): Query<ListParams>,
    req: Request,
) -> Response {
    let path = req.uri().path().trim_start_matches('/');
    if !path.contains('/') && params.list_type.as_deref() == Some("2") {
        return list_objects(state, path, params).await;
    }
    get_object(State(state), req).await
}

async fn get_object(State(state): State<AppState>, req: Request) -> Response {
    let key = format!("/{}", req.uri().path().trim_start_matches('/'));
    let headers = req.headers().clone();

    let (offset, end) = parse_range(&headers).unwrap_or_default();

    // For open-ended ranges, HEAD the first block's owner to learn object_size
    let end = match end {
        Some(e) => e,
        None => match head_to_server(&state, &key).await {
            Ok((object_size, _etag)) => {
                if offset >= object_size {
                    return StatusCode::RANGE_NOT_SATISFIABLE.into_response();
                }
                object_size
            }
            Err(resp) => return resp,
        },
    };

    if end <= offset {
        return (
            StatusCode::OK,
            [(header::CONTENT_LENGTH, "0")],
            Body::empty(),
        )
            .into_response();
    }

    let bs = state.block_size;
    let reads = block_reads(offset, end, bs);
    // Preflight: fetch the first block before committing to 206, so we can
    // return a proper HTTP error (e.g. 503) if all servers are unreachable.
    let mut reads_iter = reads.into_iter();
    let first = reads_iter.next().unwrap(); // reads is non-empty (end > offset)
    let first_stream = match fetch_block(
        state.http_client.clone(),
        state.ring.clone(),
        state.server_port,
        key.clone(),
        first,
    )
    .await
    {
        Ok(s) => s,
        Err(_) => return StatusCode::SERVICE_UNAVAILABLE.into_response(),
    };

    let client = state.http_client.clone();
    let ring = state.ring.clone();
    let server_port = state.server_port;
    let key_clone = key.clone();

    let rest_stream = stream::iter(reads_iter)
        .map(move |(block_offset, read_offset, read_len)| {
            let client = client.clone();
            let ring = ring.clone();
            let key = key_clone.clone();
            async move {
                fetch_block(
                    client,
                    ring,
                    server_port,
                    key,
                    (block_offset, read_offset, read_len),
                )
                .await
            }
        })
        .buffered(PREFETCH_BLOCKS)
        .map(|result| match result {
            Ok(chunk_stream) => {
                Either::Left(chunk_stream.map(|r| r.map_err(std::io::Error::other)))
            }
            Err(e) => Either::Right(stream::once(async move { Err(std::io::Error::other(e)) })),
        })
        .flatten();

    let body_stream = first_stream
        .map(|r| r.map_err(std::io::Error::other))
        .chain(rest_stream);

    Response::builder()
        .status(StatusCode::PARTIAL_CONTENT)
        .header(header::ACCEPT_RANGES, "bytes")
        .header(
            header::CONTENT_RANGE,
            format!("bytes {}-{}/*", offset, end - 1),
        )
        .header(frontcache_metrics::OP_HEADER, "GetObject")
        .body(Body::from_stream(body_stream))
        .unwrap()
}

async fn fetch_block(
    client: reqwest::Client,
    ring: Arc<Straw2Router>,
    server_port: u16,
    key: String,
    (block_offset, read_offset, read_len): (u64, u64, u64),
) -> Result<impl futures_util::Stream<Item = reqwest::Result<bytes::Bytes>>, anyhow::Error> {
    let addrs = ring
        .get_owners(&key, block_offset)
        .ok_or_else(|| anyhow::anyhow!("no nodes available"))?;

    let read_end = read_offset + read_len - 1; // inclusive for HTTP Range
    let mut last_err = None;
    for addr in &addrs {
        let ip = addr.split(':').next().unwrap_or(addr);
        let url = format!("http://{}:{}{}", ip, server_port, key);
        let req = client
            .get(&url)
            .header("Range", format!("bytes={}-{}", read_offset, read_end));
        match req.send().await {
            Ok(resp)
                if resp.status().is_success()
                    || resp.status() == reqwest::StatusCode::PARTIAL_CONTENT =>
            {
                return Ok(resp.bytes_stream());
            }
            Ok(resp) => {
                last_err = Some(anyhow::anyhow!(
                    "server {} returned {}",
                    addr,
                    resp.status()
                ));
            }
            Err(e) => {
                last_err = Some(e.into());
            }
        }
    }
    Err(last_err.unwrap_or_else(|| anyhow::anyhow!("no owners")))
}

/// HEAD the server owning block 0 to learn object_size and ETag.
async fn head_to_server(state: &AppState, key: &str) -> Result<(u64, String), Response> {
    let addrs = state
        .ring
        .get_owners(key, 0)
        .ok_or_else(|| StatusCode::SERVICE_UNAVAILABLE.into_response())?;

    for addr in &addrs {
        let ip = addr.split(':').next().unwrap_or(addr);
        let url = format!("http://{}:{}{}", ip, state.server_port, key);
        match state.http_client.head(&url).send().await {
            Ok(resp) if resp.status().is_success() => {
                let size = resp
                    .headers()
                    .get(header::CONTENT_LENGTH)
                    .and_then(|v| v.to_str().ok())
                    .and_then(|s| s.parse::<u64>().ok())
                    .unwrap_or(0);
                let etag = resp
                    .headers()
                    .get(header::ETAG)
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or("")
                    .trim_matches('"')
                    .to_string();
                return Ok((size, etag));
            }
            Ok(resp) => {
                let status = StatusCode::from_u16(resp.status().as_u16())
                    .unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
                let body = resp.text().await.unwrap_or_default();
                return Err((status, body).into_response());
            }
            Err(_) => continue,
        }
    }
    Err(StatusCode::SERVICE_UNAVAILABLE.into_response())
}

async fn head_object(State(state): State<AppState>, req: Request) -> Response {
    let key = format!("/{}", req.uri().path().trim_start_matches('/'));
    match head_to_server(&state, &key).await {
        Ok((size, etag)) => Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_LENGTH, size.to_string())
            .header(header::ETAG, format!("\"{}\"", etag))
            .header(header::ACCEPT_RANGES, "bytes")
            .header(frontcache_metrics::OP_HEADER, "HeadObject")
            .body(Body::empty())
            .unwrap(),
        Err(resp) => resp,
    }
}

async fn list_objects(state: AppState, bucket: &str, params: ListParams) -> Response {
    let prefix = params.prefix.as_deref();
    match state.store.list(bucket, prefix).await {
        Ok(result) => {
            let mut xml = String::from("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
            xml.push_str("<ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">");
            xml.push_str(&format!("<Name>{}</Name>", bucket));
            if let Some(p) = prefix {
                xml.push_str(&format!("<Prefix>{}</Prefix>", p));
            }
            xml.push_str("<IsTruncated>false</IsTruncated>");
            for obj in &result.objects {
                xml.push_str("<Contents>");
                xml.push_str(&format!("<Key>{}</Key>", obj.location));
                xml.push_str(&format!("<Size>{}</Size>", obj.size));
                xml.push_str(&format!(
                    "<LastModified>{}</LastModified>",
                    obj.last_modified.to_rfc3339()
                ));
                if let Some(ref etag) = obj.e_tag {
                    xml.push_str(&format!("<ETag>{}</ETag>", etag));
                }
                xml.push_str("</Contents>");
            }
            for prefix in &result.common_prefixes {
                xml.push_str(&format!(
                    "<CommonPrefixes><Prefix>{}</Prefix></CommonPrefixes>",
                    prefix
                ));
            }
            xml.push_str("</ListBucketResult>");

            Response::builder()
                .status(StatusCode::OK)
                .header(header::CONTENT_TYPE, "application/xml")
                .header(frontcache_metrics::OP_HEADER, "ListObjectsV2")
                .body(Body::from(xml))
                .unwrap()
        }
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, format!("{:?}", e)).into_response(),
    }
}

#[derive(Deserialize)]
struct PutParams {
    #[serde(rename = "partNumber")]
    part_number: Option<u32>,
    #[serde(rename = "uploadId")]
    upload_id: Option<String>,
}

async fn put_handler(
    State(state): State<AppState>,
    Query(params): Query<PutParams>,
    req: Request,
) -> Response {
    if let (Some(_part_number), Some(upload_id)) = (params.part_number, params.upload_id.as_ref()) {
        return upload_part(state, req, upload_id).await;
    }
    put_object(state, req).await
}

async fn put_object(state: AppState, req: Request) -> Response {
    let key = format!("/{}", req.uri().path().trim_start_matches('/'));
    let body = match axum::body::to_bytes(req.into_body(), 512 * 1024 * 1024).await {
        Ok(b) => b,
        Err(e) => return (StatusCode::BAD_REQUEST, format!("{}", e)).into_response(),
    };
    match state.store.put(&key, body).await {
        Ok(result) => {
            let mut builder = Response::builder()
                .status(StatusCode::OK)
                .header(frontcache_metrics::OP_HEADER, "PutObject");
            if let Some(etag) = result.e_tag {
                builder = builder.header(header::ETAG, etag);
            }
            builder.body(Body::empty()).unwrap()
        }
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, format!("{:?}", e)).into_response(),
    }
}

async fn delete_object(State(state): State<AppState>, req: Request) -> Response {
    let key = format!("/{}", req.uri().path().trim_start_matches('/'));
    match state.store.delete(&key).await {
        Ok(()) => Response::builder()
            .status(StatusCode::NO_CONTENT)
            .header(frontcache_metrics::OP_HEADER, "DeleteObject")
            .body(Body::empty())
            .unwrap(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, format!("{:?}", e)).into_response(),
    }
}

#[derive(Deserialize)]
struct PostParams {
    uploads: Option<String>,
    #[serde(rename = "uploadId")]
    upload_id: Option<String>,
}

async fn post_handler(
    State(state): State<AppState>,
    Query(params): Query<PostParams>,
    req: Request,
) -> Response {
    if params.uploads.is_some() {
        return create_multipart(state, req).await;
    }
    if let Some(ref upload_id) = params.upload_id {
        return complete_multipart(state, upload_id).await;
    }
    StatusCode::BAD_REQUEST.into_response()
}

async fn create_multipart(state: AppState, req: Request) -> Response {
    let key = format!("/{}", req.uri().path().trim_start_matches('/'));
    let bucket = key
        .strip_prefix('/')
        .and_then(|s| s.split('/').next())
        .unwrap_or("");
    let obj_key = key
        .strip_prefix('/')
        .and_then(|s| s.split_once('/'))
        .map(|(_, k)| k)
        .unwrap_or("");

    match state.store.create_multipart(&key).await {
        Ok(upload) => {
            let upload_id = uuid::Uuid::new_v4().to_string();
            state.uploads.insert(upload_id.clone(), Mutex::new(upload));

            let xml = format!(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
                 <InitiateMultipartUploadResult>\
                 <Bucket>{}</Bucket>\
                 <Key>{}</Key>\
                 <UploadId>{}</UploadId>\
                 </InitiateMultipartUploadResult>",
                bucket, obj_key, upload_id
            );

            Response::builder()
                .status(StatusCode::OK)
                .header(header::CONTENT_TYPE, "application/xml")
                .header(frontcache_metrics::OP_HEADER, "CreateMultipartUpload")
                .body(Body::from(xml))
                .unwrap()
        }
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, format!("{:?}", e)).into_response(),
    }
}

async fn upload_part(state: AppState, req: Request, upload_id: &str) -> Response {
    let body = match axum::body::to_bytes(req.into_body(), 512 * 1024 * 1024).await {
        Ok(b) => b,
        Err(e) => return (StatusCode::BAD_REQUEST, format!("{}", e)).into_response(),
    };

    let upload_entry = match state.uploads.get(upload_id) {
        Some(e) => e,
        None => return (StatusCode::NOT_FOUND, "upload not found").into_response(),
    };

    let mut upload = upload_entry.lock().await;
    let fut = upload.put_part(object_store::PutPayload::from(body));
    match fut.await {
        Ok(()) => Response::builder()
            .status(StatusCode::OK)
            .header(frontcache_metrics::OP_HEADER, "UploadPart")
            .body(Body::empty())
            .unwrap(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, format!("{:?}", e)).into_response(),
    }
}

async fn complete_multipart(state: AppState, upload_id: &str) -> Response {
    let (_, upload) = match state.uploads.remove(upload_id) {
        Some(e) => e,
        None => return (StatusCode::NOT_FOUND, "upload not found").into_response(),
    };

    let mut upload = upload.into_inner();
    match upload.complete().await {
        Ok(result) => {
            let etag = result.e_tag.unwrap_or_default();
            let xml = format!(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
                 <CompleteMultipartUploadResult>\
                 <ETag>{}</ETag>\
                 </CompleteMultipartUploadResult>",
                etag
            );

            Response::builder()
                .status(StatusCode::OK)
                .header(header::CONTENT_TYPE, "application/xml")
                .header(frontcache_metrics::OP_HEADER, "CompleteMultipartUpload")
                .body(Body::from(xml))
                .unwrap()
        }
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, format!("{:?}", e)).into_response(),
    }
}
