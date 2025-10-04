use crate::{
    TelemetryOutputFlags,
    attributes::{ArrowSerializableTelemetryEvent, ProtoTelemetryEvent, TelemetryEventRecType},
    serialize::arrow::ArrowAttributes,
};

use prost::Name;
pub use proto_rust::v1::public::events::fusion::invocation::{Invocation, InvocationEvalArgs};
pub use proto_rust::v1::public::events::fusion::process::Process;

impl ProtoTelemetryEvent for Invocation {
    const RECORD_CATEGORY: TelemetryEventRecType = TelemetryEventRecType::Span;
    const OUTPUT_FLAGS: TelemetryOutputFlags = TelemetryOutputFlags::EXPORT_ALL;

    fn event_display_name(&self) -> String {
        format!(
            "{} invocation ({})",
            self.process_info
                .as_ref()
                .map(|p| p.package.as_ref())
                .unwrap_or("dbt-fusion"),
            self.invocation_id
        )
    }

    fn has_sensitive_data(&self) -> bool {
        true
    }

    fn clone_without_sensitive_data(&self) -> Option<Box<dyn crate::AnyTelemetryEvent>> {
        Some(Box::new(Invocation {
            raw_command: "<redacted>".to_string(),
            eval_args: self.eval_args.as_ref().map(|ea| InvocationEvalArgs {
                // Only retain the command, redact everything else
                command: ea.command.clone(),
                ..Default::default()
            }),
            ..self.clone()
        }))
    }
}

impl ArrowSerializableTelemetryEvent for Invocation {
    fn to_arrow_record(&self) -> ArrowAttributes<'_> {
        ArrowAttributes {
            json_payload: serde_json::to_string(self)
                .unwrap_or_else(|_| {
                    panic!(
                        "Failed to serialize event type \"{}\" to JSON",
                        Self::full_name()
                    )
                })
                .into(),
            ..Default::default()
        }
    }

    fn from_arrow_record(record: &ArrowAttributes) -> Result<Self, String> {
        serde_json::from_str(record.json_payload.as_ref().ok_or_else(|| {
            format!(
                "Missing json payload for event type \"{}\"",
                Self::full_name()
            )
        })?)
        .map_err(|e| {
            format!(
                "Failed to deserialize event type \"{}\" from JSON: {}",
                Self::full_name(),
                e
            )
        })
    }
}
