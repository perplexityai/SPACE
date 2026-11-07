//! The ingress preparation metric the API server records.

use pplx_space_metrics_emit::{counter, tag};

/// Access preparation separates observing an existing restore owner from
/// entering the existing activation path. No per-sandbox metric labels.
pub fn ingress_preparation_path(observing: bool) {
    counter(
        "internal_api.ingress.preparation_path",
        1,
        &[tag(
            "path",
            if observing {
                "observe_owner"
            } else {
                "access_activation"
            },
        )],
    );
}
