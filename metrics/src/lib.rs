use std::sync::OnceLock;
use std::task::{Context, Poll};
use std::time::Instant;

use opentelemetry::{
    KeyValue,
    metrics::{Counter, Gauge, Histogram, MeterProvider as _},
};
use opentelemetry_otlp::{MetricExporter, WithExportConfig};
use opentelemetry_sdk::{
    metrics::{PeriodicReader, SdkMeterProvider},
    runtime,
};
use tonic::Code;
use tower::{Layer, Service, ServiceExt};

static METRICS: OnceLock<Metrics> = OnceLock::new();

pub struct Metrics {
    rpc_duration: Histogram<f64>,
    pub store_read_duration: Histogram<f64>,
    pub store_read_bytes: Histogram<f64>,
    pub cache_get_duration: Histogram<f64>,
    pub disk_available_bytes: Gauge<f64>,
    pub ring_members: Gauge<u64>,
    pub ring_member_changes: Counter<u64>,
}

pub fn init(service_name: &'static str) -> SdkMeterProvider {
    let provider = if let Ok(endpoint) = std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT") {
        tracing::info!("Exporting metrics to: {}", endpoint);
        let exporter = MetricExporter::builder()
            .with_tonic()
            .with_endpoint(endpoint)
            .build()
            .expect("Failed to create OTLP exporter");

        let reader = PeriodicReader::builder(exporter, runtime::Tokio)
            .with_interval(std::time::Duration::from_secs(60))
            .build();

        SdkMeterProvider::builder().with_reader(reader).build()
    } else {
        tracing::info!("No OTEL_EXPORTER_OTLP_ENDPOINT set, using no-op provider");
        SdkMeterProvider::builder().build()
    };

    let meter = provider.meter(service_name);

    let latency_buckets = vec![
        0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
    ];

    let metrics = Metrics {
        rpc_duration: meter
            .f64_histogram("rpc_duration_seconds")
            .with_description("RPC request duration in seconds")
            .with_boundaries(latency_buckets.clone())
            .build(),
        store_read_duration: meter
            .f64_histogram("store_read_duration_seconds")
            .with_description("Upstream store read duration in seconds")
            .with_boundaries(latency_buckets.clone())
            .build(),
        store_read_bytes: meter
            .f64_histogram("store_read_bytes")
            .with_description("Bytes read from upstream store")
            .build(),
        cache_get_duration: meter
            .f64_histogram("cache_get_duration_seconds")
            .with_description("Cache get operation duration in seconds")
            .with_boundaries(latency_buckets)
            .build(),
        disk_available_bytes: meter
            .f64_gauge("disk_available_bytes")
            .with_description("Available disk space in bytes")
            .build(),
        ring_members: meter
            .u64_gauge("ring_members")
            .with_description("Current number of members in the hash ring")
            .build(),
        ring_member_changes: meter
            .u64_counter("ring_member_changes")
            .with_description("Number of members added or removed from the hash ring")
            .build(),
    };

    let _ = METRICS.set(metrics);
    provider
}

pub fn get() -> &'static Metrics {
    METRICS.get().expect("Metrics not initialized")
}

// Tower layer for automatic RPC metrics on both client and server
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
        let method = req
            .uri()
            .path()
            .split('/')
            .next_back()
            .unwrap_or("unknown")
            .to_string();
        let start = Instant::now();
        let inner = self.inner.clone();

        Box::pin(async move {
            let result = inner.oneshot(req).await;

            let status = match &result {
                Ok(resp) => resp
                    .headers()
                    .get("grpc-status")
                    .and_then(|v| v.to_str().ok())
                    .and_then(|s| s.parse::<i32>().ok())
                    .map(|code| format!("{:?}", Code::from_i32(code)))
                    .unwrap_or_else(|| "Ok".to_string()),
                Err(_) => "Unknown".to_string(),
            };

            get().rpc_duration.record(
                start.elapsed().as_secs_f64(),
                &[
                    KeyValue::new("rpc.method", method),
                    KeyValue::new("rpc.grpc.status", status),
                ],
            );

            result
        })
    }
}

pub fn layer() -> RpcMetricsLayer {
    RpcMetricsLayer
}
