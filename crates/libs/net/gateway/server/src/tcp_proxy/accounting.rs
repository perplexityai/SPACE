//! Cancellation-safe terminal accounting for reached inspection boundaries.

use pplx_space_metrics::gateway::{InspectionMode, InspectionPath, InspectionStage};

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
    InspectionAdmission {
        mode: InspectionMode,
        stage: InspectionStage,
        path: InspectionPath,
        policy_reason: &'static str,
    },
    InspectionSession {
        stage: InspectionStage,
    },
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
            boundary: Boundary::InspectionAdmission {
                mode: InspectionMode::Unknown,
                stage: InspectionStage::PolicyReadiness,
                path: InspectionPath::Unknown,
                policy_reason: "not_evaluated",
            },
            finished: false,
        }
    }

    pub(super) fn inspection_session() -> Self {
        Self {
            boundary: Boundary::InspectionSession {
                stage: InspectionStage::GuestHandshake,
            },
            finished: false,
        }
    }

    pub(super) fn admission_mode(&mut self, captured_policy: &crate::state::TrafficPolicy) {
        let mode = match captured_policy {
            crate::state::TrafficPolicy::Ready(policy) => match policy.tls_interception {
                pplx_space_net_common::TlsInterception::On => InspectionMode::On,
                pplx_space_net_common::TlsInterception::Off => InspectionMode::Off,
            },
            crate::state::TrafficPolicy::NotReady(
                crate::state::PolicyNotReady::InterceptionNotReady,
            ) => InspectionMode::On,
            crate::state::TrafficPolicy::NotReady(_) => InspectionMode::Unknown,
        };
        if let Boundary::InspectionAdmission { mode: current, .. } = &mut self.boundary {
            *current = mode;
        }
    }

    pub(super) fn admission_policy(&mut self, reason: &'static str) {
        if let Boundary::InspectionAdmission { policy_reason, .. } = &mut self.boundary {
            *policy_reason = reason;
        }
    }

    pub(super) fn admission_path(&mut self, selected_path: InspectionPath) {
        if let Boundary::InspectionAdmission { path, .. } = &mut self.boundary {
            *path = selected_path;
        }
    }

    pub(super) fn inspection_stage(&mut self, current_stage: InspectionStage) {
        match &mut self.boundary {
            Boundary::InspectionAdmission { stage, .. } | Boundary::InspectionSession { stage } => {
                *stage = current_stage
            }
            _ => unreachable!("only inspection boundaries have stages"),
        }
    }

    pub(super) fn stage(&self) -> InspectionStage {
        match self.boundary {
            Boundary::InspectionAdmission { stage, .. } | Boundary::InspectionSession { stage } => {
                stage
            }
            _ => unreachable!("only inspection boundaries have stages"),
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
            Boundary::InspectionAdmission {
                mode,
                stage,
                path,
                policy_reason,
            } => {
                pplx_space_metrics::gateway::inspection_admission(
                    outcome.as_str(),
                    reason,
                    mode,
                    stage,
                    path,
                    policy_reason,
                );
            }
            Boundary::InspectionSession { stage } => {
                pplx_space_metrics::gateway::inspection_session(outcome.as_str(), reason, stage);
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
            Boundary::InspectionAdmission {
                mode,
                stage,
                path,
                policy_reason,
            } => tracing::debug!(
                telemetry_type = "gateway_boundary_outcome",
                boundary = "inspection_admission",
                outcome = outcome.as_str(),
                reason,
                tls_interception = mode.as_str(),
                stage = stage.as_str(),
                path = path.as_str(),
                policy_reason,
                "inspection boundary ended"
            ),
            Boundary::InspectionSession { stage } => tracing::debug!(
                telemetry_type = "gateway_boundary_outcome",
                boundary = "inspection_session",
                outcome = outcome.as_str(),
                reason,
                stage = stage.as_str(),
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
