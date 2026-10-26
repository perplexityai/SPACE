//! Recovery lifetime and fixed-size request binding; no request payload is retained.
use aws_lc_rs::digest::{Context, SHA256};
use pplx_space_common_core::types::ProcessRecoveryToken;
use pplx_space_spaced_guest_protocol::{proto::operations::StartRequest, recovery::RecoveryFence};
use prost::Message;
use tonic::Status;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum RequestBinding {
    Legacy,
    Recovery([u8; 32]),
}

impl RequestBinding {
    pub(super) fn require_match(self, supplied: Self) -> Result<(), Status> {
        match (self, supplied) {
            (Self::Legacy, Self::Legacy) => Ok(()),
            (Self::Recovery(a), Self::Recovery(b)) if a == b => Ok(()),
            (Self::Recovery(_), Self::Recovery(_)) => Err(Status::invalid_argument(
                "idempotency_key is bound to a different start request",
            )),
            _ => Err(Status::failed_precondition(
                "idempotency_key belongs to a different start mode",
            )),
        }
    }
}

/// Hash the supplied wire values, sorting only the unordered environment map.
/// Prost preserves optional-field presence. Mutable launch defaults are excluded.
pub(super) fn binding(request: &StartRequest) -> RequestBinding {
    let mut canonical = request.clone();
    let environment = std::mem::take(&mut canonical.env);
    let mut environment: Vec<_> = environment.into_iter().collect();
    environment.sort_unstable_by(|a, b| a.0.cmp(&b.0));
    let mut digest = Context::new(&SHA256);
    let encoded = canonical.encode_to_vec();
    digest.update(&(encoded.len() as u64).to_be_bytes());
    digest.update(&encoded);
    for (key, value) in environment {
        for field in [key, value] {
            digest.update(&(field.len() as u64).to_be_bytes());
            digest.update(field.as_bytes());
        }
    }
    RequestBinding::Recovery(
        digest
            .finish()
            .as_ref()
            .try_into()
            .expect("SHA256 is 32 bytes"),
    )
}

pub(super) struct RecoveryState {
    pub(super) instance: ProcessRecoveryToken,
    pub(super) launches: std::sync::Arc<tokio::sync::RwLock<()>>,
    pub(super) activation: std::sync::Mutex<Option<ProcessRecoveryToken>>,
    pub(super) idempotency: std::time::Duration,
}

impl RecoveryState {
    pub(super) fn check(&self, fence: RecoveryFence) -> Result<(), Status> {
        if self.instance != fence.instance {
            return Err(Status::failed_precondition("execution engine restarted"));
        }
        if *self
            .activation
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            != Some(fence.activation)
        {
            return Err(Status::failed_precondition(
                "execution activation changed or recovery is disabled",
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
