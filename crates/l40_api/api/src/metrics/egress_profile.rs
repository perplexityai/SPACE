//! Operator egress profile admission metrics.

use pplx_space_metrics_emit::{counter, metric_vocabulary, tag};

metric_vocabulary! {
    /// Result of operator egress profile selection or saved-selection resolution.
    EgressProfileOutcome {
        Ready => "ready",
        InvalidRequest => "invalid_request",
        Unconfigured => "unconfigured",
    }
}

pub fn egress_profile(profile: &str, outcome: EgressProfileOutcome) {
    counter(
        "space.sandbox.egress_profile_admission",
        1,
        &[
            tag(
                "outcome",
                if outcome == EgressProfileOutcome::Ready {
                    "ok"
                } else {
                    "failed"
                },
            ),
            tag("reason", outcome),
            tag("profile", profile),
        ],
    );
}
