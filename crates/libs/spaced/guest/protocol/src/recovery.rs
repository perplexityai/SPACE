//! Validation of the opt-in recovery envelope. Never lower it to a legacy start.
use crate::{invalid_argument_status, proto::operations::StartRequest};
use pplx_space_common_core::types::{
    EnvMap, GuestUserName, IdempotencyKey, ProcessCommand, ProcessId, ProcessRecoveryToken,
};
use tonic::Status;

pub const START_CONTRACT_VERSION: u32 = 1;
pub const CONTROL_CONTRACT_VERSION: u32 = 1;

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
        return Err(RecoveryReason::UnsupportedContract.into_status());
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

/// Stable reason attached to a failed recovery fence. Tokens are not credentials.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RecoveryReason {
    UnsupportedContract,
    EngineRestarted,
    ActivationChanged,
    ProcessReplaced,
    TemporarilyUnavailable,
    StartModeMismatch,
    StartKeyExpired,
}

impl RecoveryReason {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::UnsupportedContract => "unsupported_contract",
            Self::EngineRestarted => "engine_restarted",
            Self::ActivationChanged => "activation_changed",
            Self::ProcessReplaced => "process_replaced",
            Self::TemporarilyUnavailable => "temporarily_unavailable",
            Self::StartModeMismatch => "start_mode_mismatch",
            Self::StartKeyExpired => "start_key_expired",
        }
    }
    pub fn into_status(self) -> Status {
        let message = match self {
            Self::UnsupportedContract => "unsupported process recovery contract version",
            Self::EngineRestarted => "execution engine restarted",
            Self::ActivationChanged => "execution activation changed",
            Self::ProcessReplaced => "process incarnation changed",
            Self::TemporarilyUnavailable => "process recovery is temporarily unavailable",
            Self::StartModeMismatch => "idempotency_key belongs to a different start mode",
            Self::StartKeyExpired => "idempotency_key expired",
        };
        let mut status = if self == Self::TemporarilyUnavailable {
            Status::unavailable(message)
        } else {
            Status::failed_precondition(message)
        };
        status.metadata_mut().insert(
            RECOVERY_REASON_METADATA_KEY,
            tonic::metadata::MetadataValue::from_static(self.as_str()),
        );
        status
    }
    pub fn from_status(status: &Status) -> Option<Self> {
        status
            .metadata()
            .get(RECOVERY_REASON_METADATA_KEY)?
            .to_str()
            .ok()?
            .parse()
            .ok()
    }
}
impl std::fmt::Display for RecoveryReason {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}
#[derive(Debug, thiserror::Error)]
#[error("unknown process recovery reason")]
pub struct ParseRecoveryReason;
impl std::str::FromStr for RecoveryReason {
    type Err = ParseRecoveryReason;
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Ok(match value {
            "unsupported_contract" => Self::UnsupportedContract,
            "engine_restarted" => Self::EngineRestarted,
            "activation_changed" => Self::ActivationChanged,
            "process_replaced" => Self::ProcessReplaced,
            "temporarily_unavailable" => Self::TemporarilyUnavailable,
            "start_mode_mismatch" => Self::StartModeMismatch,
            "start_key_expired" => Self::StartKeyExpired,
            _ => return Err(ParseRecoveryReason),
        })
    }
}
pub const RECOVERY_REASON_METADATA_KEY: &str = "space-process-recovery-reason";

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProcessHandle {
    pub fence: RecoveryFence,
    pub process_id: ProcessId,
    pub incarnation: ProcessRecoveryToken,
}

/// Version 1 fences calls but does not deduplicate input, close or signals.
/// Missing envelopes select legacy validation; a recovery request must leave
/// the legacy ID empty so an older peer cannot silently execute it unfenced.
pub fn validate_handle(
    legacy_id: &str,
    recovery: Option<&crate::proto::operations::RecoveryHandle>,
) -> Result<Option<ProcessHandle>, Status> {
    let Some(recovery) = recovery else {
        return Ok(None);
    };
    if recovery.contract_version != CONTROL_CONTRACT_VERSION {
        return Err(RecoveryReason::UnsupportedContract.into_status());
    }
    if !legacy_id.is_empty() {
        return Err(Status::invalid_argument(
            "legacy and recovery process handles are mutually exclusive",
        ));
    }
    Ok(Some(ProcessHandle {
        fence: RecoveryFence {
            instance: ProcessRecoveryToken::parse(&recovery.expected_instance_id)
                .map_err(invalid_argument_status)?,
            activation: ProcessRecoveryToken::parse(&recovery.expected_activation_id)
                .map_err(invalid_argument_status)?,
        },
        process_id: ProcessId::parse(&recovery.process_id).map_err(invalid_argument_status)?,
        incarnation: ProcessRecoveryToken::parse(&recovery.process_incarnation)
            .map_err(invalid_argument_status)?,
    }))
}
