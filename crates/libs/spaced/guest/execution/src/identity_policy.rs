//! Authority of children launched by an execution service.

/// The caller supplies isolation; this policy controls child credentials within it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IdentityPolicy {
    /// Preserve the guest daemon's account-switching and executable privilege semantics.
    Guest,
    /// Linux embedding: install account IDs/groups, clear active capability sets,
    /// and prevent privilege gain across exec. Requires a privileged launcher and
    /// identity workers; Direct filesystem execution cannot enforce this policy.
    Restricted,
}

impl IdentityPolicy {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Guest => "guest",
            Self::Restricted => "restricted",
        }
    }

    pub(crate) fn validate(
        self,
        execution: crate::FilesystemExecutionMode,
    ) -> Result<(), crate::CreateExecutionServiceError> {
        use crate::CreateExecutionServiceError as Error;
        if self == Self::Guest {
            return Ok(());
        }
        if execution != crate::FilesystemExecutionMode::IdentityWorkers {
            return Err(Error::RestrictedRequiresIdentityWorkers);
        }
        #[cfg(target_os = "linux")]
        {
            use rustix::thread::{CapabilitySet, capabilities};
            let sets =
                capabilities(None).map_err(|error| Error::InspectPrivileges(error.into()))?;
            if !nix::unistd::Uid::effective().is_root()
                || !sets
                    .effective
                    .contains(CapabilitySet::SETUID | CapabilitySet::SETGID)
            {
                return Err(Error::RestrictedRequiresPrivilege);
            }
            Ok(())
        }
        #[cfg(not(target_os = "linux"))]
        Err(Error::UnsupportedIdentityWorkers)
    }
}

impl std::fmt::Display for IdentityPolicy {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Debug, thiserror::Error)]
#[error("identity policy must be guest or restricted")]
pub struct ParseIdentityPolicyError;

impl std::str::FromStr for IdentityPolicy {
    type Err = ParseIdentityPolicyError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "guest" => Ok(Self::Guest),
            "restricted" => Ok(Self::Restricted),
            _ => Err(ParseIdentityPolicyError),
        }
    }
}
