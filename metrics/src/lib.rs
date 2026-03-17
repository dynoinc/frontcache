use std::sync::OnceLock;
use std::task::{Context, Poll};
use std::time::Instant;

use opentelemetry::{
    KeyValue,
    metrics::{Counter, Gauge, Histogram, Meter, MeterProvider as _},
};
use opentelemetry_otlp::{MetricExporter, WithExportConfig};
use opentelemetry_sdk::metrics::{PeriodicReader, SdkMeterProvider};
use tower::{Layer, Service, ServiceExt};

static METER_PROVIDER: OnceLock<SdkMeterProvider> = OnceLock::new();
static METRICS: OnceLock<Metrics> = OnceLock::new();

/// Header set by handlers to identify the S3 operation for metrics.
pub const OP_HEADER: &str = "x-fc-op";

pub struct Metrics {
    http_duration: Histogram<f64>,

    pub store_duration: Histogram<f64>,
    pub store_read_bytes: Histogram<f64>,

    pub cache_duration: Histogram<f64>,
    pub purge_duration: Histogram<f64>,
    pub flush_duration: Histogram<f64>,

    pub ring_members: Gauge<u64>,
    pub ring_member_changes: Counter<u64>,

    pub disk_byte_changes: Counter<u64>,
}

pub fn init() {
    let provider = METER_PROVIDER.get_or_init(|| {
        if let Ok(endpoint) = std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT") {
            tracing::info!("Exporting metrics to: {}", endpoint);
            let exporter = MetricExporter::builder()
                .with_tonic()
                .with_endpoint(endpoint)
                .build()
                .expect("Failed to create OTLP exporter");

            let reader = PeriodicReader::builder(exporter).build();

            SdkMeterProvider::builder().with_reader(reader).build()
        } else {
            tracing::info!("No OTEL_EXPORTER_OTLP_ENDPOINT set, using no-op provider");
            SdkMeterProvider::builder().build()
        }
    });

    let meter = provider.meter("frontcache");

    let latency_buckets = vec![
        1.0, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1000.0, 2500.0, 5000.0, 10000.0,
    ];

    METRICS.get_or_init(|| Metrics {
        http_duration: meter
            .f64_histogram("http_duration_ms")
            .with_description("HTTP request duration in milliseconds")
            .with_boundaries(latency_buckets.clone())
            .build(),

        store_duration: meter
            .f64_histogram("store_duration_ms")
            .with_description("Upstream store operation duration in milliseconds")
            .with_boundaries(latency_buckets.clone())
            .build(),
        store_read_bytes: meter
            .f64_histogram("store_read_bytes")
            .with_description("Bytes read from upstream store")
            .with_boundaries(vec![
                1024.0,       // 1 KB
                4096.0,       // 4 KB
                16_384.0,     // 16 KB
                65_536.0,     // 64 KB
                262_144.0,    // 256 KB
                1_048_576.0,  // 1 MB
                4_194_304.0,  // 4 MB
                16_777_216.0, // 16 MB
                67_108_864.0, // 64 MB
            ])
            .build(),

        cache_duration: meter
            .f64_histogram("cache_duration_ms")
            .with_description("Cache operation duration in milliseconds")
            .with_boundaries(latency_buckets.clone())
            .build(),
        purge_duration: meter
            .f64_histogram("purge_duration_ms")
            .with_description("Cache eviction (purge) duration in milliseconds")
            .with_boundaries(latency_buckets.clone())
            .build(),
        flush_duration: meter
            .f64_histogram("flush_duration_ms")
            .with_description("LRU flush duration in milliseconds")
            .with_boundaries(latency_buckets)
            .build(),

        ring_members: meter
            .u64_gauge("ring_members")
            .with_description("Current number of members in the hash ring")
            .build(),
        ring_member_changes: meter
            .u64_counter("ring_member_changes")
            .with_description("Number of members added or removed from the hash ring")
            .build(),

        disk_byte_changes: meter
            .u64_counter("disk_byte_changes")
            .with_description("Bytes added to, evicted from, or served from cache")
            .build(),
    });
}

pub fn shutdown() -> opentelemetry_sdk::error::OTelSdkResult {
    if let Some(provider) = METER_PROVIDER.get() {
        provider.shutdown()?;
    }
    Ok(())
}

pub fn elapsed_ms(start: std::time::Instant) -> f64 {
    start.elapsed().as_secs_f64() * 1000.0
}

pub fn get() -> &'static Metrics {
    METRICS
        .get()
        .expect("Metrics not initialized. Call metrics::init() before using metrics")
}

pub fn meter() -> Meter {
    METER_PROVIDER
        .get()
        .expect("Metrics not initialized. Call metrics::init() before using metrics")
        .meter("frontcache")
}

#[derive(Clone)]
pub struct RpcMetricsLayer;

impl<S> Layer<S> for RpcMetricsLayer {
    type Service = RpcMetricsService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        RpcMetricsService { inner }
    }
}

#[derive(Clone)]
pub struct RpcMetricsService<S> {
    pub(crate) inner: S,
}

impl<S, ReqBody, ResBody> Service<http::Request<ReqBody>> for RpcMetricsService<S>
where
    S: Service<http::Request<ReqBody>, Response = http::Response<ResBody>> + Clone + Send + 'static,
    S::Error: std::fmt::Display,
    S::Future: Send + 'static,
    ReqBody: Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Self::Response, Self::Error>> + Send>,
    >;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: http::Request<ReqBody>) -> Self::Future {
        let start = Instant::now();
        let clone = self.inner.clone();
        let inner = std::mem::replace(&mut self.inner, clone);

        Box::pin(async move {
            let result = inner.oneshot(req).await;

            let (status, operation) = match &result {
                Ok(resp) => {
                    let status = resp.status().as_u16().to_string();
                    let operation = resp
                        .headers()
                        .get(OP_HEADER)
                        .and_then(|v| v.to_str().ok())
                        .unwrap_or("unknown")
                        .to_string();
                    (status, operation)
                }
                Err(_) => ("error".to_string(), "unknown".to_string()),
            };

            get().http_duration.record(
                elapsed_ms(start),
                &[
                    KeyValue::new("operation", operation),
                    KeyValue::new("http.status", status),
                ],
            );

            result
        })
    }
}

pub fn layer() -> RpcMetricsLayer {
    RpcMetricsLayer
}
