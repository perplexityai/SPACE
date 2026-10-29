//! Validation of the opt-in recovery envelope. Never lower it to a legacy start.
use crate::{invalid_argument_status, proto::operations::StartRequest};
use pplx_space_common_core::types::{
    EnvMap, GuestUserName, IdempotencyKey, ProcessCommand, ProcessId, ProcessRecoveryToken,
};
use tonic::Status;

pub const START_CONTRACT_VERSION: u32 = 1;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RecoveryFence {
    pub instance: ProcessRecoveryToken,
    pub activation: ProcessRecoveryToken,
}

/// Validate without normalizing supplied values: request binding distinguishes
/// omission from an explicitly supplied default. Legacy requests return None.
pub fn validate_start(request: &StartRequest) -> Result<Option<RecoveryFence>, Status> {
    let Some(recovery) = &request.recovery else {
        return Ok(None);
    };
    if recovery.contract_version != START_CONTRACT_VERSION {
        return Err(Status::failed_precondition(
            "unsupported process recovery contract version",
        ));
    }
    if !request.command.is_empty() {
        return Err(Status::invalid_argument(
            "legacy and recovery commands are mutually exclusive",
        ));
    }
    let fence = RecoveryFence {
        instance: ProcessRecoveryToken::parse(&recovery.expected_instance_id)
            .map_err(invalid_argument_status)?,
        activation: ProcessRecoveryToken::parse(&recovery.expected_activation_id)
            .map_err(invalid_argument_status)?,
    };
    ProcessCommand::new(recovery.command.clone()).map_err(invalid_argument_status)?;
    EnvMap::try_from(request.env.clone()).map_err(invalid_argument_status)?;
    IdempotencyKey::parse(
        request
            .idempotency_key
            .as_deref()
            .ok_or_else(|| Status::invalid_argument("recovery start requires idempotency_key"))?,
    )
    .map_err(invalid_argument_status)?;
    request
        .process_id
        .as_ref()
        .map(ProcessId::parse)
        .transpose()
        .map_err(invalid_argument_status)?;
    request
        .user
        .as_ref()
        .map(GuestUserName::parse)
        .transpose()
        .map_err(invalid_argument_status)?;
    crate::validate_optional_timeout(request.timeout)?;
    if let Some(cwd) = &request.cwd {
        pplx_space_common_core::types::validate_linux_guest_path_input(cwd)
            .map_err(invalid_argument_status)?;
    }
    Ok(Some(fence))
}
