use std::net::UdpSocket;

use super::{Metric, MetricKind, MetricSink, MetricValue};

pub(super) struct DatadogSink {
    socket: UdpSocket,
    namespace: String,
    constant_tags: Vec<String>,
}

impl DatadogSink {
    pub(super) fn from_env(namespace: String, constant_tags: Vec<String>) -> Option<Self> {
        let host = std::env::var("DD_AGENT_HOST").unwrap_or_else(|_| "127.0.0.1".to_string());
        let port = std::env::var("DD_DOGSTATSD_PORT")
            .ok()
            .and_then(|raw| raw.parse::<u16>().ok())
            .unwrap_or(8125);

        // Init failures disable all custom metrics for the process.
        // Log enough to tell "config missing" apart from "agent unreachable"
        // without crashing services on transient agent restarts.
        let socket = match UdpSocket::bind("0.0.0.0:0") {
            Ok(socket) => socket,
            Err(err) => {
                tracing::warn!(
                    %err,
                    "dogstatsd: failed to bind UDP socket; SPACE custom metrics disabled"
                );
                return None;
            }
        };
        if let Err(err) = socket.connect((host.as_str(), port)) {
            tracing::warn!(
                %err,
                dd_agent_host = %host,
                dd_dogstatsd_port = port,
                "dogstatsd: failed to connect to agent; SPACE custom metrics disabled"
            );
            return None;
        }

        tracing::info!(
            dd_agent_host = %host,
            dd_dogstatsd_port = port,
            "dogstatsd: client initialized"
        );

        Some(Self {
            socket,
            namespace,
            constant_tags,
        })
    }

    #[cfg(test)]
    pub(super) fn connected_to(
        address: std::net::SocketAddr,
        namespace: String,
        constant_tags: Vec<String>,
    ) -> Self {
        let socket = UdpSocket::bind("127.0.0.1:0").expect("bind DogStatsD test client");
        socket
            .connect(address)
            .expect("connect DogStatsD test client");
        Self {
            socket,
            namespace,
            constant_tags,
        }
    }

    fn emit_dogstatsd(&self, name: &str, value: &str, kind: &str, tags: &[String]) {
        let metric_name = format!("{}.{}", self.namespace, name);
        let mut payload = format!("{metric_name}:{value}|{kind}");
        let mut all_tags = self.constant_tags.clone();
        all_tags.extend_from_slice(tags);
        if !all_tags.is_empty() {
            payload.push_str("|#");
            payload.push_str(&all_tags.join(","));
        }
        let _ = self.socket.send(payload.as_bytes());
    }
}

impl MetricSink for DatadogSink {
    fn label(&self) -> &'static str {
        "datadog"
    }

    fn emit(&self, metric: Metric<'_>) {
        self.emit_dogstatsd(
            metric.name,
            &dogstatsd_value(metric.value),
            dogstatsd_kind(metric.kind),
            metric.tags,
        );
    }
}

fn dogstatsd_kind(kind: MetricKind) -> &'static str {
    match kind {
        MetricKind::Counter => "c",
        MetricKind::Gauge => "g",
        MetricKind::Distribution => "d",
    }
}

fn dogstatsd_value(value: MetricValue) -> String {
    match value {
        MetricValue::Unsigned(value) => value.to_string(),
        MetricValue::Float(value) => format!("{value:.3}"),
    }
}

#[cfg(test)]
mod tests;
