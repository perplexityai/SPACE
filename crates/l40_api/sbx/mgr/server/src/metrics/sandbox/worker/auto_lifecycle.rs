//! The auto-lifecycle delete admission and its blocked deletes.

use super::*;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AutoLifecycleDeleteAdmissionDisposition {
    ReceiptClaimed,
    Receiptless,
    Blocked,
    Failed,
}

impl AutoLifecycleDeleteAdmissionDisposition {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ReceiptClaimed => "receipt_claimed",
            Self::Receiptless => "receiptless",
            Self::Blocked => "blocked",
            Self::Failed => "failed",
        }
    }
}

impl fmt::Display for AutoLifecycleDeleteAdmissionDisposition {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
#[error("unknown auto-lifecycle delete admission disposition {0:?}")]
pub struct AutoLifecycleDeleteAdmissionDispositionParseError(pub(super) String);

impl FromStr for AutoLifecycleDeleteAdmissionDisposition {
    type Err = AutoLifecycleDeleteAdmissionDispositionParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "receipt_claimed" => Ok(Self::ReceiptClaimed),
            "receiptless" => Ok(Self::Receiptless),
            "blocked" => Ok(Self::Blocked),
            "failed" => Ok(Self::Failed),
            other => Err(AutoLifecycleDeleteAdmissionDispositionParseError(
                other.to_string(),
            )),
        }
    }
}

pub fn auto_lifecycle_delete_admission(
    disposition: AutoLifecycleDeleteAdmissionDisposition,
    reason: AutoLifecycleDeleteAdmissionReason,
) {
    counter(
        "sandbox.worker.auto_lifecycle.delete_admission",
        1,
        &[tag("disposition", disposition), tag("reason", reason)],
    );
}

pub fn auto_lifecycle_delete_blocked(
    count: u64,
    oldest_age_secs: u64,
    reason: AutoLifecycleDeleteAdmissionReason,
) {
    gauge(
        "sandbox.worker.auto_lifecycle.delete_blocked.count",
        count as f64,
        &[tag("reason", reason)],
    );
    gauge(
        "sandbox.worker.auto_lifecycle.delete_blocked.oldest_age_secs",
        oldest_age_secs as f64,
        &[tag("reason", reason)],
    );
}
