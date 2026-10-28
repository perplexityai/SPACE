use pplx_space_common_core::types::{PolicyAuthority, RuntimeKind};
use tonic::Status;

use super::fields::ExecutionProjection;
use crate::optional_trimmed;

/// A nonempty legacy runtime label, retaining its wire spelling for storage.
///
/// Without execution metadata this is an open vocabulary. With metadata, the
/// label must name the same runtime, but aliases must not rewrite stored rows.
#[derive(Debug)]
pub(crate) struct PublishedRuntimeLabel(String);

impl PublishedRuntimeLabel {
    pub(crate) fn from_optional(raw: Option<&str>) -> Option<Self> {
        optional_trimmed(raw).map(Self)
    }

    pub(crate) fn try_from_proto(
        raw: Option<&str>,
        execution: Option<&ExecutionProjection>,
    ) -> Result<Option<Self>, Status> {
        let Some(label) = Self::from_optional(raw) else {
            return Ok(None);
        };
        if let Some(execution) = execution {
            let runtime = RuntimeKind::parse(label.as_str())
                .map_err(|error| Status::invalid_argument(error.to_string()))?;
            if runtime != execution.runtime_kind() {
                return Err(Status::invalid_argument(
                    "published sandbox runtime disagrees with execution metadata",
                ));
            }
        }
        Ok(Some(label))
    }

    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }
}

pub(crate) fn policy_authority(raw: i32) -> Result<Option<PolicyAuthority>, Status> {
    PolicyAuthority::from_optional_proto(raw)
        .map_err(|error| Status::invalid_argument(error.to_string()))
}

#[cfg(test)]
mod tests;
