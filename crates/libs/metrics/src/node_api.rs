use crate::emit::{counter, gauge, tag, timing_ms};

pub fn uds_per_sandbox_in_flight(value: usize) {
    gauge("node_api.uds_per_sandbox.in_flight", value as f64, &[]);
}

/// One rejection, tagged with the `scope` whose permits ran out: `node`,
/// `sandbox` (the shared per-sandbox budget) or `sandbox_reserved`.
pub fn uds_per_sandbox_rejected(scope: &str) {
    counter(
        "node_api.uds_per_sandbox.rejected",
        1,
        &[tag("scope", scope)],
    );
}

pub fn uds_per_sandbox_handler_ms(value_ms: f64) {
    timing_ms("node_api.uds_per_sandbox.handler_ms", value_ms, &[]);
}

pub fn service_port_ingress_in_flight(value: usize) {
    gauge("node_api.service_port_ingress.in_flight", value as f64, &[]);
}

pub fn service_port_ingress_rejected() {
    counter("node_api.service_port_ingress.rejected", 1, &[]);
}

pub fn service_port_ingress_handler_ms(value_ms: f64) {
    timing_ms("node_api.service_port_ingress.handler_ms", value_ms, &[]);
}
