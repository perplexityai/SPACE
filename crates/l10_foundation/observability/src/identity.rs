//! Shared identity precedence, with byte-preserving Datadog fallbacks.

use std::collections::BTreeMap;

use crate::logging::{SPACE_ENV, SPACE_SERVICE_NAME, SPACE_VERSION};

/// Resolve the telemetry service name, preserving raw Datadog fallbacks.
/// Launchers use this view so DD-only child naming retains its legacy suffix rules.
pub fn service_name(default: &str) -> String {
    Identity::from_env(default).service_name().to_string()
}

#[derive(Clone, Debug)]
struct Setting {
    preferred: Option<String>,
    datadog: Option<String>,
}

impl Setting {
    fn raw(&self) -> Option<&str> {
        // Explicit SPACE/OTel identity wins over the legacy Datadog value.
        self.preferred.as_deref().or(self.datadog.as_deref())
    }

    fn normalized(&self) -> Option<&str> {
        self.raw().map(str::trim).filter(|value| !value.is_empty())
    }
}

#[derive(Clone, Debug)]
pub(crate) struct Identity {
    default_service: String,
    service: Setting,
    environment: Setting,
    version: Setting,
    #[cfg(feature = "otlp-metrics")]
    attributes: BTreeMap<String, String>,
    #[cfg(feature = "datadog")]
    has_otel_resource_settings: bool,
}

impl Identity {
    pub(crate) fn from_env(default_service: &str) -> Self {
        Self::from_lookup(default_service, |name| std::env::var(name).ok())
    }

    pub(crate) fn from_lookup(
        default_service: &str,
        mut lookup: impl FnMut(&str) -> Option<String>,
    ) -> Self {
        let raw_attributes = lookup("OTEL_RESOURCE_ATTRIBUTES");
        // Match the SDK's comma-separated key=value grammar, including values
        // containing '='. Malformed entries are ignored without logging input.
        let mut attributes: BTreeMap<String, String> = raw_attributes
            .as_deref()
            .into_iter()
            .flat_map(|value| value.split_terminator(','))
            .filter_map(|entry| entry.split_once('='))
            .map(|(key, value)| (key.trim().to_string(), percent_decode(value.trim())))
            .collect();
        let otel_service = lookup("OTEL_SERVICE_NAME");
        let space_service = lookup(SPACE_SERVICE_NAME);
        let space_environment = lookup(SPACE_ENV);
        let space_version = lookup(SPACE_VERSION);
        #[cfg(feature = "datadog")]
        let has_otel_resource_settings = raw_attributes.is_some() || otel_service.is_some();
        let service = Setting {
            preferred: first_nonblank([
                space_service,
                otel_service,
                attributes.remove("service.name"),
            ]),
            datadog: lookup("DD_SERVICE"),
        };
        let environment = Setting {
            preferred: first_nonblank([
                space_environment,
                attributes.remove("deployment.environment.name"),
            ]),
            datadog: lookup("DD_ENV"),
        };
        let version = Setting {
            preferred: first_nonblank([space_version, attributes.remove("service.version")]),
            datadog: lookup("DD_VERSION"),
        };

        // Shared resource attributes must never impersonate another process,
        // node, or pod, even when a local field is unavailable. In Kubernetes,
        // HOSTNAME is the pod name and must not create a phantom host.
        for key in [
            "service.instance.id",
            "host.name",
            "k8s.node.name",
            "k8s.pod.name",
        ] {
            attributes.remove(key);
        }
        if let Some(node) =
            first_nonblank([lookup("SPACE_K8S_NODE_NAME"), lookup("KUBE_NODE_NAME")])
        {
            attributes.insert("host.name".into(), node.clone());
            attributes.insert("k8s.node.name".into(), node);
        } else if lookup("KUBERNETES_SERVICE_HOST").is_none()
            && let Some(host) = first_nonblank([lookup("HOSTNAME")])
        {
            attributes.insert("host.name".into(), host);
        }
        if let Some(pod) = first_nonblank([lookup("SPACE_K8S_POD_NAME")]) {
            attributes.insert("k8s.pod.name".into(), pod);
        }
        let host = first_nonblank([lookup("HOSTNAME"), lookup("SPACE_NODE_ID")])
            .unwrap_or_else(|| "unknown-host".into());
        attributes.insert(
            "service.instance.id".into(),
            format!("{host}:{}", std::process::id()),
        );
        Self {
            default_service: default_service.into(),
            service,
            environment,
            version,
            #[cfg(feature = "otlp-metrics")]
            attributes,
            #[cfg(feature = "datadog")]
            has_otel_resource_settings,
        }
    }

    pub(crate) fn service_name(&self) -> &str {
        self.service
            .raw()
            .filter(|value| !value.is_empty())
            .unwrap_or(&self.default_service)
    }

    pub(crate) fn log_service_name(&self) -> &str {
        self.service.normalized().unwrap_or(&self.default_service)
    }

    pub(crate) fn log_environment(&self) -> Option<&str> {
        self.environment.normalized()
    }

    pub(crate) fn log_version(&self) -> Option<&str> {
        self.version.normalized()
    }

    // Do not normalize the DD fallback: existing DogStatsD tags include empty
    // strings and whitespace, and omit a service tag when DD_SERVICE is absent.
    pub(crate) fn metric_identity(&self) -> [Option<&str>; 3] {
        [
            self.environment.raw(),
            self.service.raw(),
            self.version.raw(),
        ]
    }

    #[cfg(feature = "otel")]
    pub(crate) fn trace_environment(&self) -> Option<&str> {
        self.environment
            .raw()
            .filter(|value| !value.trim().is_empty())
    }

    #[cfg(feature = "otel")]
    pub(crate) fn trace_version(&self) -> Option<&str> {
        self.version.raw().filter(|value| !value.trim().is_empty())
    }

    #[cfg(feature = "datadog")]
    pub(crate) fn has_otel_resource_settings(&self) -> bool {
        self.has_otel_resource_settings
    }

    #[cfg(feature = "otlp-metrics")]
    pub(crate) fn metric_resource(&self, cluster_id: Option<&str>) -> opentelemetry_sdk::Resource {
        self.resource(
            self.environment.raw(),
            self.version.raw(),
            cluster_id,
            false,
        )
    }

    #[cfg(feature = "otel")]
    pub(crate) fn trace_resource(&self) -> opentelemetry_sdk::Resource {
        self.resource(self.trace_environment(), self.trace_version(), None, true)
    }

    #[cfg(feature = "otlp-metrics")]
    fn resource(
        &self,
        environment: Option<&str>,
        version: Option<&str>,
        cluster_id: Option<&str>,
        sdk_attributes: bool,
    ) -> opentelemetry_sdk::Resource {
        use opentelemetry::KeyValue;
        use opentelemetry_sdk::{Resource, resource::TelemetryResourceDetector};

        let mut attributes = self.attributes.clone();
        attributes.insert("service.name".into(), self.service_name().into());
        if let Some(value) = environment {
            attributes.insert("deployment.environment.name".into(), value.into());
        }
        if let Some(value) = version {
            attributes.insert("service.version".into(), value.into());
        }
        if let Some(value) = cluster_id {
            attributes.insert("space.cluster.id".into(), value.into());
        }
        // Only traces previously emitted SDK metadata. Never rerun the SDK's
        // env detectors after resolution: they could undo identity precedence.
        let mut builder = Resource::builder_empty();
        if sdk_attributes {
            builder = builder.with_detector(Box::new(TelemetryResourceDetector));
        }
        builder
            .with_attributes(
                attributes
                    .into_iter()
                    .map(|(key, value)| KeyValue::new(key, value)),
            )
            .build()
    }
}

/// Percent-decode a resource attribute value the way the OTel SDK does, so a
/// shared `OTEL_RESOURCE_ATTRIBUTES` means the same thing to every producer.
///
/// A malformed escape is kept verbatim rather than dropped or repaired: the
/// value can carry customer data, so it is never logged, and a service that
/// configured a literal `%` should still see it.
fn percent_decode(value: &str) -> String {
    if !value.contains('%') {
        return value.to_string();
    }
    let mut decoded = Vec::with_capacity(value.len());
    let mut rest = value.as_bytes();
    while let Some((&first, tail)) = rest.split_first() {
        if first == b'%'
            && let Some(escape) = tail.get(..2)
            && let Ok(text) = std::str::from_utf8(escape)
            && let Ok(byte) = u8::from_str_radix(text, 16)
        {
            decoded.push(byte);
            rest = tail.get(2..).unwrap_or_default();
            continue;
        }
        decoded.push(first);
        rest = tail;
    }
    // A decode that is not valid UTF-8 was not an escape this producer can
    // honor; keep the original so every producer still agrees on it.
    String::from_utf8(decoded).unwrap_or_else(|_| value.to_string())
}

fn first_nonblank(values: impl IntoIterator<Item = Option<String>>) -> Option<String> {
    values
        .into_iter()
        .flatten()
        .map(|value| value.trim().to_string())
        .find(|value| !value.is_empty())
}

#[cfg(test)]
mod tests;
