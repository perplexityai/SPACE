//! Validate update messages once at the maintenance wire boundary.
use crate::proto::update as wire;
use pplx_space_spaced_update::{self as model, artifact::DescriptorError};
use tonic::Status;

fn invalid(error: impl std::fmt::Display) -> Status {
    Status::invalid_argument(error.to_string())
}
fn missing() -> Status {
    Status::invalid_argument("missing update identity")
}

impl TryFrom<wire::Selection> for model::Selection {
    type Error = Status;
    fn try_from(value: wire::Selection) -> Result<Self, Self::Error> {
        Ok(Self {
            slot: match wire::Slot::try_from(value.slot) {
                Ok(wire::Slot::A) => model::Slot::A,
                Ok(wire::Slot::B) => model::Slot::B,
                _ => return Err(missing()),
            },
            digest: value.digest.parse().map_err(invalid)?,
        })
    }
}
impl From<model::Selection> for wire::Selection {
    fn from(value: model::Selection) -> Self {
        Self {
            slot: match value.slot {
                model::Slot::A => wire::Slot::A,
                model::Slot::B => wire::Slot::B,
            } as i32,
            digest: value.digest.to_string(),
        }
    }
}
impl TryFrom<wire::Attempt> for model::Attempt {
    type Error = Status;
    fn try_from(value: wire::Attempt) -> Result<Self, Self::Error> {
        fn id(value: &str) -> Result<uuid::Uuid, Status> {
            let id = value.parse::<uuid::Uuid>().map_err(invalid)?;
            if id.is_nil() {
                return Err(missing());
            }
            Ok(id)
        }
        if value.confirmation_deadline_ms == 0 {
            return Err(missing());
        }
        Ok(Self {
            operation: id(&value.operation_id)?,
            execution: id(&value.execution_id)?,
            boot: id(&value.boot_id)?,
            incarnation: id(&value.incarnation)?,
            configuration_revision: value.configuration_revision,
            target: value.target.ok_or_else(missing)?.try_into()?,
            confirmation_deadline_ms: value.confirmation_deadline_ms,
        })
    }
}
impl From<model::Attempt> for wire::Attempt {
    fn from(value: model::Attempt) -> Self {
        Self {
            operation_id: value.operation.to_string(),
            execution_id: value.execution.to_string(),
            boot_id: value.boot.to_string(),
            incarnation: value.incarnation.to_string(),
            configuration_revision: value.configuration_revision,
            target: Some(value.target.into()),
            confirmation_deadline_ms: value.confirmation_deadline_ms,
        }
    }
}
impl TryFrom<wire::BundleDescriptor> for model::BundleDescriptor {
    type Error = Status;
    fn try_from(value: wire::BundleDescriptor) -> Result<Self, Self::Error> {
        fn binary(value: Option<wire::BinaryIdentity>) -> Result<model::BinaryIdentity, Status> {
            let value = value.ok_or_else(missing)?;
            Ok(model::BinaryIdentity {
                length: value.length,
                digest: value.digest.parse().map_err(invalid)?,
            })
        }
        let descriptor = Self {
            schema: value.schema,
            source_revision: value.source_revision,
            built_at: value.built_at,
            version: value.version.parse().map_err(invalid)?,
            guest_update_required_since: value
                .guest_update_required_since
                .parse()
                .map_err(invalid)?,
            compatibility_family: value.compatibility_family,
            architecture: value.architecture.parse().map_err(invalid)?,
            seed_contracts: value.seed_contracts,
            service: binary(value.service)?,
            cli: binary(value.cli)?,
        };
        descriptor.validate().map_err(invalid)?;
        Ok(descriptor)
    }
}
impl From<model::BundleDescriptor> for wire::BundleDescriptor {
    fn from(value: model::BundleDescriptor) -> Self {
        fn binary(value: model::BinaryIdentity) -> Option<wire::BinaryIdentity> {
            Some(wire::BinaryIdentity {
                length: value.length,
                digest: value.digest.to_string(),
            })
        }
        Self {
            schema: value.schema,
            source_revision: value.source_revision,
            built_at: value.built_at,
            version: value.version.to_string(),
            guest_update_required_since: value.guest_update_required_since.to_string(),
            compatibility_family: value.compatibility_family,
            architecture: value.architecture.to_string(),
            seed_contracts: value.seed_contracts,
            service: binary(value.service),
            cli: binary(value.cli),
        }
    }
}

pub fn binary(value: i32) -> Result<model::Binary, Status> {
    match wire::Binary::try_from(value) {
        Ok(wire::Binary::Service) => Ok(model::Binary::Service),
        Ok(wire::Binary::Cli) => Ok(model::Binary::Cli),
        _ => Err(invalid(DescriptorError::Invalid("binary name"))),
    }
}

pub fn journal_status(journal: model::Journal) -> wire::Status {
    use model::JournalState;
    let restart = match &journal.state {
        JournalState::Restarting { restart } | JournalState::Restarted { restart } => {
            Some(wire::RestartReceipt {
                operation_id: restart.operation.to_string(),
                execution_id: restart.execution.to_string(),
                boot_id: restart.boot.to_string(),
                previous_incarnation: restart.incarnation.to_string(),
            })
        }
        _ => None,
    };
    let (phase, attempt, progress) = match journal.state {
        JournalState::Stable => (wire::Phase::Stable, None, wire::TrialProgress::Unspecified),
        JournalState::Staged { attempt } => (
            wire::Phase::Staged,
            Some(attempt),
            wire::TrialProgress::Unspecified,
        ),
        JournalState::Armed { attempt } => (
            wire::Phase::Armed,
            Some(attempt),
            wire::TrialProgress::Starting,
        ),
        JournalState::Trial { attempt, progress } => {
            (wire::Phase::Trial, Some(attempt), trial_progress(progress))
        }
        JournalState::Committed { attempt } => (
            wire::Phase::Committed,
            Some(attempt),
            wire::TrialProgress::AwaitingHost,
        ),
        JournalState::RolledBack { attempt, progress } => (
            wire::Phase::RolledBack,
            Some(attempt),
            trial_progress(progress),
        ),
        JournalState::RecoveryRequired => (
            wire::Phase::RecoveryRequired,
            None,
            wire::TrialProgress::Unspecified,
        ),
        JournalState::Restarting { .. } => (
            wire::Phase::Restarting,
            None,
            wire::TrialProgress::Unspecified,
        ),
        JournalState::Restarted { .. } => (
            wire::Phase::Restarted,
            None,
            wire::TrialProgress::Unspecified,
        ),
    };
    wire::Status {
        seed_contract: model::SEED_CONTRACT.into(),
        phase: phase as i32,
        committed: Some(journal.committed.into()),
        attempt: attempt.map(Into::into),
        progress: progress as i32,
        restart,
        ..Default::default()
    }
}
fn trial_progress(value: model::TrialProgress) -> wire::TrialProgress {
    match value {
        model::TrialProgress::Starting => wire::TrialProgress::Starting,
        model::TrialProgress::AwaitingHost => wire::TrialProgress::AwaitingHost,
    }
}
