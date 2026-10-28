use pplx_space_common_core::types::SandboxId;

use crate::node_publish::validate::sandbox_observation::{
    PublishedActivityTimestamp, ValidatedSandboxObservation,
};

/// Node-reported activity, still subject to database ownership and freshness.
#[derive(Debug)]
pub(crate) struct ActivityClaim {
    pub(crate) sandbox_id: SandboxId,
    pub(crate) last_used_at: Option<PublishedActivityTimestamp>,
    pub(crate) last_access_at: Option<PublishedActivityTimestamp>,
    pub(crate) last_command_at: Option<PublishedActivityTimestamp>,
}

impl ActivityClaim {
    pub(super) fn from_observation(observation: &ValidatedSandboxObservation) -> Option<Self> {
        if observation.last_used_at().is_none()
            && observation.last_access_at().is_none()
            && observation.last_command_at().is_none()
        {
            return None;
        }
        Some(Self {
            sandbox_id: observation.sandbox_id(),
            last_used_at: observation.last_used_at().cloned(),
            last_access_at: observation.last_access_at().cloned(),
            last_command_at: observation.last_command_at().cloned(),
        })
    }
}
