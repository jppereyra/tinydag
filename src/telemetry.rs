//! OpenTelemetry initialisation.
//!
//! Call `init()` once at the start of `main()`. It inspects the environment:
//!
//! - `OTEL_EXPORTER_OTLP_ENDPOINT` — if absent, a plain fmt subscriber is
//!   installed and no-op OTel providers are registered; no collector required.
//! - `OTEL_SERVICE_NAME` — service name attached to every span/metric.
//!   Defaults to `"tinydag"`.
//! - `RUST_LOG` — tracing filter; defaults to `"info"`.

use opentelemetry::{trace::TracerProvider as _, KeyValue};
use opentelemetry_sdk::{
    metrics::SdkMeterProvider,
    trace::SdkTracerProvider,
    Resource,
};
use tracing_subscriber::{layer::SubscriberExt as _, util::SubscriberInitExt as _, EnvFilter};

// ---------------------------------------------------------------------------
// Public handle
// ---------------------------------------------------------------------------

/// Holds the active OTel providers. Call [`shutdown`][Self::shutdown] before
/// the process exits to flush buffered spans and metrics.
pub struct Providers {
    pub tracer_provider: SdkTracerProvider,
    pub meter_provider:  SdkMeterProvider,
}

impl Providers {
    pub fn shutdown(self) {
        let _ = self.tracer_provider.shutdown();
        let _ = self.meter_provider.shutdown();
    }
}

// ---------------------------------------------------------------------------
// init()
// ---------------------------------------------------------------------------

/// Initialize tracing and OTel.
pub fn init() -> Providers {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info"));

    let endpoint = std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT").ok();

    if endpoint.is_none() {
        // No collector required — plain fmt subscriber + no-op OTel providers.
        let tracer_provider = SdkTracerProvider::builder().build();
        let meter_provider  = SdkMeterProvider::builder().build();

        opentelemetry::global::set_tracer_provider(tracer_provider.clone());
        opentelemetry::global::set_meter_provider(meter_provider.clone());

        tracing_subscriber::registry()
            .with(filter)
            .with(tracing_subscriber::fmt::layer())
            .init();

        return Providers { tracer_provider, meter_provider };
    }

    let service_name = std::env::var("OTEL_SERVICE_NAME")
        .unwrap_or_else(|_| "tinydag".to_string());

    let resource = Resource::builder()
        .with_attribute(KeyValue::new("service.name",    service_name))
        .with_attribute(KeyValue::new("service.version", env!("CARGO_PKG_VERSION")))
        .build();

    // --- Traces -----------------------------------------------------------
    let span_exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_tonic()
        .build()
        .expect("failed to build OTLP span exporter");

    let tracer_provider = SdkTracerProvider::builder()
        .with_batch_exporter(span_exporter)
        .with_resource(resource.clone())
        .build();

    // --- Metrics ----------------------------------------------------------
    let metric_exporter = opentelemetry_otlp::MetricExporter::builder()
        .with_tonic()
        .build()
        .expect("failed to build OTLP metric exporter");

    let reader = opentelemetry_sdk::metrics::PeriodicReader::builder(metric_exporter)
        .build();

    let meter_provider = SdkMeterProvider::builder()
        .with_reader(reader)
        .with_resource(resource)
        .build();

    // Register as globals so existing `opentelemetry::global::tracer/meter`
    // calls throughout the codebase route to these providers.
    opentelemetry::global::set_tracer_provider(tracer_provider.clone());
    opentelemetry::global::set_meter_provider(meter_provider.clone());

    // Wire tracing spans → OTel via tracing-opentelemetry.
    let tracer = tracer_provider.tracer("tinydag");
    let otel_layer = tracing_opentelemetry::layer().with_tracer(tracer);

    tracing_subscriber::registry()
        .with(filter)
        .with(tracing_subscriber::fmt::layer())
        .with(otel_layer)
        .init();

    Providers { tracer_provider, meter_provider }
}
