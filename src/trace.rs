use std::borrow::Cow;
use std::error::Error;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use minitrace::collector::Config as TracingConfig;
use minitrace_opentelemetry::OpenTelemetryReporter;
use opentelemetry::trace::SpanKind;
use opentelemetry::{InstrumentationLibrary, KeyValue};
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::Resource;

static ENABLED: AtomicBool = AtomicBool::new(false);

pub fn init(endpoint: String) -> Result<(), Box<dyn Error>> {
    if ENABLED.swap(true, Ordering::SeqCst) {
        return Err("Tracing already initialized".into());
    }

    let config = TracingConfig::default();
    let opentelemetry_exporter = opentelemetry_otlp::new_exporter()
        .tonic()
        .with_endpoint(endpoint)
        .with_protocol(opentelemetry_otlp::Protocol::Grpc)
        .with_timeout(Duration::from_secs(
            opentelemetry_otlp::OTEL_EXPORTER_OTLP_TIMEOUT_DEFAULT,
        ))
        .build_span_exporter()
        .expect("initialize oltp exporter");

    let resource = Resource::new([KeyValue::new("service.name", "camellia")]);
    let resource = Cow::Owned(resource);
    let instrumentation_lib =
        InstrumentationLibrary::new("minitrace", Some("7.1.0"), None::<&'static str>, None);

    let reporter = OpenTelemetryReporter::new(
        opentelemetry_exporter,
        SpanKind::Server,
        resource,
        instrumentation_lib,
    );

    minitrace::set_reporter(reporter, config);
    Ok(())
}

pub fn shutdown() {
    if !ENABLED.swap(false, Ordering::SeqCst) {
        return;
    }

    minitrace::flush();
}
