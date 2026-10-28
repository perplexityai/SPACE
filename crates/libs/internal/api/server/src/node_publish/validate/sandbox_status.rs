use pplx_space_common_core::types::SandboxLifecycle;

use crate::metadata::optional_trimmed;

#[derive(Clone, Debug)]
enum ReportedStatus {
    Known {
        lifecycle: SandboxLifecycle,
        label: String,
    },
    Other(String),
}

/// Classify known states without rewriting their persisted spelling.
///
/// Publication has historically accepted nonempty unknown labels. They remain
/// explicit opaque evidence rather than acquiring a known lifecycle's authority.
#[derive(Clone, Debug)]
pub(crate) struct PublishedSandboxStatus(ReportedStatus);

impl PublishedSandboxStatus {
    pub(crate) fn from_proto(raw: Option<&str>) -> Option<Self> {
        let label = optional_trimmed(raw)?;
        let status = match label.to_ascii_lowercase().parse() {
            Ok(lifecycle) => ReportedStatus::Known { lifecycle, label },
            Err(_) => ReportedStatus::Other(label),
        };
        Some(Self(status))
    }

    pub(crate) fn as_str(&self) -> &str {
        match &self.0 {
            ReportedStatus::Known { label, .. } | ReportedStatus::Other(label) => label,
        }
    }

    pub(crate) fn lifecycle(&self) -> Option<SandboxLifecycle> {
        match self.0 {
            ReportedStatus::Known { lifecycle, .. } => Some(lifecycle),
            ReportedStatus::Other(_) => None,
        }
    }
}
