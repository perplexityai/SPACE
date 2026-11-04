//! Signal attribution during the migration away from native Datadog.

use std::sync::OnceLock;

use crate::{sink::MetricsSinkChoice, traces::TraceExporter};

static POLICY: OnceLock<AttributionPolicy> = OnceLock::new();

/// A process-wide contract, selected from resolved destinations, not features
/// or endpoint presence. Only observability initialization can install it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AttributionPolicy {
    /// Preserve production Datadog queries and wire fields exactly. Remove this
    /// compatibility path only when native Datadog consumers have migrated.
    Legacy,
    /// Separate the emitting process from logical roles and remote peers.
    Otel,
}

impl AttributionPolicy {
    pub(crate) fn from_destinations(metrics: MetricsSinkChoice, traces: TraceExporter) -> Self {
        match (metrics, traces) {
            // Mixed destinations must not change native Datadog logs or spans.
            (MetricsSinkChoice::Datadog, _) | (_, TraceExporter::Datadog) => Self::Legacy,
            (MetricsSinkChoice::Otlp, _) | (_, TraceExporter::Otlp) => Self::Otel,
            _ => Self::Legacy,
        }
    }

    /// Before shared initialization, preserve the historical Datadog-default
    /// contract for libraries using a caller-owned subscriber. Do not cache that
    /// fallback: an early log must not pin a later OTLP initialization to Legacy.
    pub fn current() -> Self {
        POLICY.get().copied().unwrap_or(Self::Legacy)
    }

    pub(crate) fn insert_log_service(
        self,
        values: &mut serde_json::Map<String, serde_json::Value>,
        producer: &str,
    ) {
        match self {
            Self::Legacy => crate::insert_string_if_missing(values, "service", producer),
            Self::Otel => {
                if let Some(role) = values.insert("service".to_string(), producer.into())
                    && role.as_str() != Some(producer)
                {
                    // An explicit role is more precise than the legacy field.
                    values.entry("service.role").or_insert(role);
                }
            }
        }
    }
}

pub(crate) fn init(metrics: MetricsSinkChoice, traces: TraceExporter) {
    POLICY.get_or_init(|| AttributionPolicy::from_destinations(metrics, traces));
}

/// Attribute a shared client diagnostic or span to its remote peer in OTLP
/// mode, preserving the original `service` field in Legacy mode.
///
/// Wrap a tracing invocation containing exactly one `peer.service = value,`
/// field. The remaining tokens (including field order, target, parent, level,
/// and span name) pass through unchanged. Selecting the field at the callsite
/// also preserves text logs, trace attributes, and field-sensitive filters;
/// rewriting only the JSON formatter would miss those consumers.
///
/// ```
/// pplx_space_observability::with_peer_service!(tracing::debug!(
///     peer.service = "example-peer",
///     "connecting"
/// ));
/// ```
#[macro_export]
macro_rules! with_peer_service {
    ($namespace:ident :: $emit:ident ! ($($fields:tt)*)) => {
        $crate::with_peer_service!(@fields ($namespace::$emit) [] $($fields)*)
    };
    (@fields ($emit:path) [$($before:tt)*] peer.service = $peer:expr, $($after:tt)*) => {
        match $crate::attribution::AttributionPolicy::current() {
            $crate::attribution::AttributionPolicy::Legacy => {
                $emit!($($before)* service = $peer, $($after)*)
            }
            $crate::attribution::AttributionPolicy::Otel => {
                $emit!($($before)* peer.service = $peer, $($after)*)
            }
        }
    };
    (@fields ($emit:path) [$($before:tt)*] $next:tt $($rest:tt)*) => {
        $crate::with_peer_service!(@fields ($emit) [$($before)* $next] $($rest)*)
    };
    (@fields ($emit:path) [$($before:tt)*]) => {
        compile_error!("with_peer_service! requires a peer.service = value, field")
    };
}
