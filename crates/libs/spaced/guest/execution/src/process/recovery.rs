//! Resolve a process once, retaining its identity through the operation.
use super::*;
use pplx_space_spaced_guest_protocol::{
    proto::operations::RecoveryHandle,
    recovery::{ProcessHandle, RecoveryReason, validate_handle},
};

#[derive(Clone, Copy)]
pub(super) struct RecoveryIdentity {
    pub(super) activation: ProcessRecoveryToken,
    pub(super) incarnation: ProcessRecoveryToken,
}

pub(super) enum ProcessAddress {
    Legacy(ProcessId),
    Recovery(ProcessHandle),
}
impl ProcessAddress {
    pub(super) fn parse(legacy: String, recovery: Option<RecoveryHandle>) -> Result<Self, Status> {
        match validate_handle(&legacy, recovery.as_ref())? {
            Some(handle) => Ok(Self::Recovery(handle)),
            None => ProcessId::parse(legacy)
                .map(Self::Legacy)
                .map_err(invalid_argument_status),
        }
    }
}

pub(super) enum TargetState {
    Running,
    Retained,
}

pub(super) struct ProcessTarget {
    pub(super) id: ProcessId,
    pub(super) record: Arc<ProcessRecord>,
    // Activation changes cannot pass a control effect still using this record.
    _activation: Option<tokio::sync::OwnedRwLockReadGuard<()>>,
}

impl ExecutionService {
    pub(super) async fn select_process(
        &self,
        address: ProcessAddress,
        state: TargetState,
    ) -> Result<ProcessTarget, Status> {
        let (id, expected, activation) = match address {
            ProcessAddress::Legacy(id) => (id, None, None),
            ProcessAddress::Recovery(handle) => {
                let activation = self.inner.recovery.launches.clone().read_owned().await;
                self.inner.recovery.check(handle.fence)?;
                (
                    handle.process_id,
                    Some(RecoveryIdentity {
                        activation: handle.fence.activation,
                        incarnation: handle.incarnation,
                    }),
                    Some(activation),
                )
            }
        };
        let record = self
            .inner
            .processes
            .get_record(&id)
            .ok_or_else(|| Status::not_found("process not found"))?;
        if let Some(expected) = expected {
            let actual = record
                .recovery_identity
                .ok_or_else(|| RecoveryReason::ProcessReplaced.into_status())?;
            if actual.incarnation != expected.incarnation {
                return Err(RecoveryReason::ProcessReplaced.into_status());
            }
            if actual.activation != expected.activation {
                return Err(RecoveryReason::ActivationChanged.into_status());
            }
        }
        if matches!(state, TargetState::Running) && record.journal.is_completed() {
            return Err(Status::not_found("process not found"));
        }
        Ok(ProcessTarget {
            id,
            record,
            _activation: activation,
        })
    }
}
