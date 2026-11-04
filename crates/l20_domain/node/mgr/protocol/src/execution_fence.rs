use pplx_space_common_core::types::SandboxActivationAttemptId;

/// Response header older sandbox managers require from `InspectSandbox` before
/// startup admission. Current sandbox managers do not read it.
#[deprecated(note = "remove once no pre-0.8 sandbox-manager can run")]
pub const EXECUTION_AUTHORITY_CAPABILITY: &str = "x-space-execution-authority";

/// Both startup gates stop collecting missing authority at this bound.
pub const EXECUTION_PREFLIGHT_LIMIT: usize = 64;

/// Report a bounded set of missing authorities; a saturated scan is not a total.
/// Callers own discovery and must propagate store/probe failures separately.
pub fn execution_preflight_result(unadopted: &[String]) -> Result<(), tonic::Status> {
    if unadopted.is_empty() {
        return Ok(());
    }
    let count = if unadopted.len() >= EXECUTION_PREFLIGHT_LIMIT {
        format!("at least {EXECUTION_PREFLIGHT_LIMIT}")
    } else {
        unadopted.len().to_string()
    };
    let sample = unadopted.get(..8).unwrap_or(unadopted).join(", ");
    Err(tonic::Status::failed_precondition(format!(
        "{count} sandbox(s) have no execution authority; sample: {sample}; deploy the adoption baseline before enforcement"
    )))
}

/// Target authority for node-manager instance receive/delete. A restore scope
/// names disposable staging; it never grants access to the canonical instance.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InstanceTarget {
    Execution(ExecutionFence),
    RestoreScope(pplx_space_common_core::types::TransferScopeId),
}

impl TryFrom<&crate::proto::InstanceTarget> for InstanceTarget {
    type Error = tonic::Status;

    fn try_from(wire: &crate::proto::InstanceTarget) -> Result<Self, Self::Error> {
        use crate::proto::instance_target::Target;
        match wire.target.as_ref() {
            Some(Target::Execution(fence)) => Ok(Self::Execution(fence.try_into()?)),
            Some(Target::RestoreScopeId(scope)) => {
                scope.parse().map(Self::RestoreScope).map_err(|error| {
                    tonic::Status::invalid_argument(format!("restore_scope_id: {error}"))
                })
            }
            None => Err(tonic::Status::invalid_argument(
                "instance target is required",
            )),
        }
    }
}

impl From<InstanceTarget> for crate::proto::InstanceTarget {
    fn from(target: InstanceTarget) -> Self {
        use crate::proto::instance_target::Target;
        Self {
            target: Some(match target {
                InstanceTarget::Execution(fence) => Target::Execution(fence.into()),
                InstanceTarget::RestoreScope(scope) => Target::RestoreScopeId(scope.to_string()),
            }),
        }
    }
}

/// Durable lifecycle-owner authority, not a network-slot generation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExecutionFence {
    current: SandboxActivationAttemptId,
    previous: Option<SandboxActivationAttemptId>,
}

impl ExecutionFence {
    pub fn new(
        current: SandboxActivationAttemptId,
        previous: Option<SandboxActivationAttemptId>,
    ) -> Self {
        Self { current, previous }
    }

    pub fn current(self) -> SandboxActivationAttemptId {
        self.current
    }

    pub fn previous(self) -> Option<SandboxActivationAttemptId> {
        self.previous
    }

    pub fn owns(self, execution: SandboxActivationAttemptId) -> bool {
        execution == self.current || self.previous == Some(execution)
    }
}

impl TryFrom<&crate::proto::ExecutionFence> for ExecutionFence {
    type Error = tonic::Status;

    fn try_from(wire: &crate::proto::ExecutionFence) -> Result<Self, Self::Error> {
        let current = wire.execution_id.parse().map_err(|error| {
            tonic::Status::invalid_argument(format!("invalid execution_id: {error}"))
        })?;
        let previous = wire
            .previous_id
            .as_deref()
            .map(str::parse)
            .transpose()
            .map_err(|error| {
                tonic::Status::invalid_argument(format!("invalid previous_id: {error}"))
            })?;
        if previous == Some(current) {
            return Err(tonic::Status::invalid_argument(
                "execution_id and previous_id must differ",
            ));
        }
        Ok(Self::new(current, previous))
    }
}

impl From<ExecutionFence> for crate::proto::ExecutionFence {
    fn from(fence: ExecutionFence) -> Self {
        Self {
            execution_id: fence.current.to_string(),
            previous_id: fence.previous.map(|id| id.to_string()),
        }
    }
}
