//! Cancellation-safe terminal accounting for reached inspection boundaries.

#[derive(Clone, Copy, Debug)]
pub(super) enum Outcome {
    Success,
    Error,
    Denied,
    Cancelled,
    Skipped,
}

impl Outcome {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::Error => "error",
            Self::Denied => "denied",
            Self::Cancelled => "cancelled",
            Self::Skipped => "skipped",
        }
    }
}

#[derive(Debug)]
enum Boundary {
    InspectionAdmission,
    HttpAuthorization {
        protocol: &'static str,
    },
    OriginTcp,
    Forwarding {
        protocol: &'static str,
        operation: &'static str,
    },
}

/// Records cancellation on drop so reached work cannot disappear from its denominator.
#[derive(Debug)]
pub(super) struct Attempt {
    boundary: Boundary,
    finished: bool,
}

impl Attempt {
    pub(super) fn inspection_admission() -> Self {
        Self {
            boundary: Boundary::InspectionAdmission,
            finished: false,
        }
    }

    pub(super) fn http_authorization(protocol: &'static str) -> Self {
        Self {
            boundary: Boundary::HttpAuthorization { protocol },
            finished: false,
        }
    }

    pub(super) fn origin_tcp() -> Self {
        Self {
            boundary: Boundary::OriginTcp,
            finished: false,
        }
    }

    pub(super) fn forwarding(protocol: &'static str, operation: &'static str) -> Self {
        Self {
            boundary: Boundary::Forwarding {
                protocol,
                operation,
            },
            finished: false,
        }
    }

    pub(super) fn finish(&mut self, outcome: Outcome, reason: &'static str) {
        if self.finished {
            return;
        }
        self.finished = true;
        self.record(outcome, reason);
        self.record_diagnostic(outcome, reason);
    }

    fn record(&self, outcome: Outcome, reason: &'static str) {
        match self.boundary {
            Boundary::InspectionAdmission => {
                pplx_space_metrics::gateway::inspection_admission(outcome.as_str(), reason);
            }
            Boundary::HttpAuthorization { protocol } => {
                pplx_space_metrics::gateway::http_authorization(protocol, outcome.as_str(), reason);
            }
            Boundary::OriginTcp => {
                pplx_space_metrics::gateway::origin_tcp_connect(outcome.as_str(), reason);
            }
            Boundary::Forwarding {
                protocol,
                operation,
            } => {
                pplx_space_metrics::gateway::forwarding(
                    protocol,
                    operation,
                    outcome.as_str(),
                    reason,
                );
            }
        }
    }

    fn record_diagnostic(&self, outcome: Outcome, reason: &'static str) {
        if matches!(outcome, Outcome::Success | Outcome::Skipped) {
            return;
        }
        match self.boundary {
            Boundary::InspectionAdmission => tracing::debug!(
                telemetry_type = "gateway_boundary_outcome",
                boundary = "inspection_admission",
                outcome = outcome.as_str(),
                reason,
                "inspection boundary ended"
            ),
            Boundary::HttpAuthorization { protocol } => tracing::debug!(
                telemetry_type = "gateway_boundary_outcome",
                boundary = "http_authorization",
                protocol,
                outcome = outcome.as_str(),
                reason,
                "inspection boundary ended"
            ),
            Boundary::OriginTcp => tracing::debug!(
                telemetry_type = "gateway_boundary_outcome",
                boundary = "origin_tcp",
                outcome = outcome.as_str(),
                reason,
                "inspection boundary ended"
            ),
            Boundary::Forwarding {
                protocol,
                operation,
            } => tracing::debug!(
                telemetry_type = "gateway_boundary_outcome",
                boundary = "forwarding",
                protocol,
                operation,
                outcome = outcome.as_str(),
                reason,
                "inspection boundary ended"
            ),
        }
    }
}

impl Drop for Attempt {
    fn drop(&mut self) {
        if !self.finished {
            self.record(Outcome::Cancelled, "work_dropped");
            self.record_diagnostic(Outcome::Cancelled, "work_dropped");
        }
    }
}

#[cfg(test)]
mod tests;
