use std::sync::OnceLock;
use std::task::{Context, Poll};
use std::time::Instant;

use opentelemetry::{
    KeyValue,
    metrics::{Histogram, MeterProvider as _},
};
use opentelemetry_otlp::{MetricExporter, WithExportConfig};
use opentelemetry_sdk::{
    metrics::{PeriodicReader, SdkMeterProvider},
    runtime,
};
use tonic::Code;
use tower::{Layer, Service};

static METRICS: OnceLock<Metrics> = OnceLock::new();

pub struct Metrics {
    rpc_duration: Histogram<f64>,
    pub store_read_duration: Histogram<f64>,
    pub store_read_bytes: Histogram<f64>,
    pub cache_get_duration: Histogram<f64>,
    pub disk_available_bytes: opentelemetry::metrics::Gauge<f64>,
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

    let metrics = Metrics {
        rpc_duration: meter
            .f64_histogram("rpc_duration_seconds")
            .with_description("RPC request duration in seconds")
            .build(),
        store_read_duration: meter
            .f64_histogram("store_read_duration_seconds")
            .with_description("Upstream store read duration in seconds")
            .build(),
        store_read_bytes: meter
            .f64_histogram("store_read_bytes")
            .with_description("Bytes read from upstream store")
            .build(),
        cache_get_duration: meter
            .f64_histogram("cache_get_duration_seconds")
            .with_description("Cache get operation duration in seconds")
            .build(),
        disk_available_bytes: meter
            .f64_gauge("disk_available_bytes")
            .with_description("Available disk space in bytes")
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
        let mut inner = self.inner.clone();

        Box::pin(async move {
            let result = inner.call(req).await;

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
