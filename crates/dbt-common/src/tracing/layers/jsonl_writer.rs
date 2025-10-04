use dbt_telemetry::{
    LogRecordInfo, SpanEndInfo, SpanStartInfo, TelemetryOutputFlags, TelemetryRecordRef,
};
use tracing::level_filters::LevelFilter;

use super::super::{
    background_writer::BackgroundWriter,
    data_provider::DataProvider,
    layer::{ConsumerLayer, TelemetryConsumer},
    shared_writer::SharedWriter,
    shutdown::TelemetryShutdownItem,
};

/// Build jsonl layer for arbitrary writer. This will writer directly to
/// the writer. If you want to write to slow IO sink, prefer `build_jsonl_layer_with_background_writer`
pub fn build_jsonl_layer<W: SharedWriter + 'static>(
    writer: W,
    max_log_verbosity: LevelFilter,
) -> ConsumerLayer {
    Box::new(TelemetryJsonlWriterLayer::new(writer).with_filter(max_log_verbosity))
}

/// Build jsonl layer with a background writer. This is preferred for writing to
/// slow IO sinks like files.
pub fn build_jsonl_layer_with_background_writer<W: std::io::Write + Send + 'static>(
    writer: W,
    max_log_verbosity: LevelFilter,
) -> (ConsumerLayer, TelemetryShutdownItem) {
    let (writer, handle) = BackgroundWriter::new(writer);

    (
        build_jsonl_layer(writer, max_log_verbosity),
        Box::new(handle),
    )
}

/// A tracing layer that reads telemetry data from extensions and writes it as JSON.
///
/// This layer reads TelemetryRecord data from span extensions and serializes
/// it to JSON using the provided writer.
pub struct TelemetryJsonlWriterLayer {
    writer: Box<dyn SharedWriter>,
}

impl TelemetryJsonlWriterLayer {
    pub fn new<W: SharedWriter + 'static>(writer: W) -> Self {
        Self {
            writer: Box::new(writer),
        }
    }
}

impl TelemetryConsumer for TelemetryJsonlWriterLayer {
    fn is_span_enabled(&self, span: &SpanStartInfo, _meta: &tracing::Metadata) -> bool {
        span.attributes
            .output_flags()
            .contains(TelemetryOutputFlags::EXPORT_JSONL)
    }

    fn is_log_enabled(&self, log_record: &LogRecordInfo, _meta: &tracing::Metadata) -> bool {
        log_record
            .attributes
            .output_flags()
            .contains(TelemetryOutputFlags::EXPORT_JSONL)
    }

    fn on_span_start(&self, span: &SpanStartInfo, _: &DataProvider<'_>) {
        if let Ok(json) = serde_json::to_string(&TelemetryRecordRef::SpanStart(span)) {
            // Currently we silently ignore write errors. We expect writers to be
            // smart enough to avoid trying to write after fatal errors and report
            // them during shutdown.
            let _ = self.writer.writeln(json.as_str());
        }
    }

    fn on_span_end(&self, span: &SpanEndInfo, _: &DataProvider<'_>) {
        if let Ok(json) = serde_json::to_string(&TelemetryRecordRef::SpanEnd(span)) {
            // Currently we silently ignore write errors. We expect writers to be
            // smart enough to avoid trying to write after fatal errors and report
            // them during shutdown.
            let _ = self.writer.writeln(json.as_str());
        }
    }

    fn on_log_record(&self, record: &LogRecordInfo, _: &DataProvider<'_>) {
        if let Ok(json) = serde_json::to_string(&TelemetryRecordRef::LogRecord(record)) {
            // Currently we silently ignore write errors. We expect writers to be
            // smart enough to avoid trying to write after fatal errors and report
            // them during shutdown.
            let _ = self.writer.writeln(json.as_str());
        }
    }
}
