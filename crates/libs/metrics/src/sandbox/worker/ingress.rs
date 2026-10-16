//! Ingress protection and admission, including service-port ingress rejections.

use super::*;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ServicePortIngressRejectionReason {
    Capacity,
    StartTimeout,
}

impl ServicePortIngressRejectionReason {
    pub(super) const fn as_str(self) -> &'static str {
        match self {
            Self::Capacity => "capacity",
            Self::StartTimeout => "start_timeout",
        }
    }
}

pub fn service_port_ingress_rejected(reason: ServicePortIngressRejectionReason) {
    counter(
        "sandbox.worker.service_port_ingress.rejected",
        1,
        &[tag("reason", reason.as_str())],
    );
}

/// Worker-wide current ingress ownership, sampled by the lifecycle scheduler.
pub fn ingress_protection(count: usize, oldest_age_seconds: f64) {
    gauge(
        "sandbox.worker.ingress.protected_streams",
        count as f64,
        &[],
    );
    gauge(
        "sandbox.worker.ingress.oldest_protected_stream_age_seconds",
        oldest_age_seconds,
        &[],
    );
}

pub fn ingress_admission_wait(start: Instant) {
    timing_ms(
        "sandbox.worker.ingress.admission_wait_ms",
        elapsed_ms(start),
        &[],
    );
}

pub fn ingress_lifecycle_deferred() {
    counter("sandbox.worker.ingress.lifecycle_deferred", 1, &[]);
}
