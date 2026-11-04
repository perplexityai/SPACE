use super::*;
use pplx_space_common_core::types::SandboxActivationAttemptId;
use pplx_space_node_runtime_core::proto::{BindExecutionRequest, RetireExecutionRequest};

pub(super) fn pin_caller<T>(request: &Request<T>) -> Result<launch::ProcessHandle, Status> {
    let pid = pplx_space_common_svc::peer_identity_from_tonic_request(request)
        .and_then(|peer| peer.pid())
        .and_then(|pid| u32::try_from(pid).ok())
        .ok_or_else(|| {
            Status::permission_denied("execution mutations require a kernel peer process identity")
        })?;
    launch::ProcessHandle::open(pid)
        .map_err(|error| Status::unavailable(format!("pin lifecycle caller: {error}")))
}

pub(super) fn check_caller(caller: Option<&launch::ProcessHandle>) -> Result<(), Status> {
    if let Some(caller) = caller
        && caller
            .has_exited()
            .map_err(|error| Status::unavailable(format!("observe lifecycle caller: {error}")))?
    {
        return Err(Status::aborted(
            "lifecycle caller exited before runtime mutation",
        ));
    }
    Ok(())
}

pub(super) fn parse_execution(raw: &str) -> Result<SandboxActivationAttemptId, Status> {
    raw.parse()
        .map_err(|error| Status::invalid_argument(format!("invalid execution identity: {error}")))
}

fn require_quiescence(sandbox_id: SandboxId) -> Result<(), Status> {
    if !cgroup::sandbox_is_quiescent(&sandbox_id.to_string())
        .map_err(|error| Status::unavailable(format!("runtime occupancy unavailable: {error}")))?
    {
        return Err(Status::failed_precondition(
            "stop the current runtime before changing execution identity",
        ));
    }
    Ok(())
}

impl RuntimeSupervisorService {
    pub(super) async fn execution_io<T: Send + 'static>(
        &self,
        work: impl FnOnce(crate::execution_store::ExecutionStore) -> Result<T, Status> + Send + 'static,
    ) -> Result<T, Status> {
        let slot = self.acquire_blocking_slot().await?;
        let store = self.execution_store.clone();
        tokio::task::spawn_blocking(move || {
            let _slot = slot;
            work(store)
        })
        .await
        .map_err(|error| Status::unavailable(format!("execution store task failed: {error}")))?
    }

    pub(super) async fn check_execution(
        &self,
        sandbox: SandboxId,
        requested: &str,
        previous: Option<&str>,
        allow_absent: bool,
    ) -> Result<SandboxActivationAttemptId, Status> {
        if requested.is_empty() {
            return Err(Status::failed_precondition(
                "runtime execution identity is missing",
            ));
        }
        let requested = parse_execution(requested)?;
        let previous = previous.map(parse_execution).transpose()?;
        self.check_parsed_execution(sandbox, requested, previous, allow_absent)
            .await
    }

    pub(super) async fn check_parsed_execution(
        &self,
        sandbox: SandboxId,
        requested: SandboxActivationAttemptId,
        previous: Option<SandboxActivationAttemptId>,
        allow_absent: bool,
    ) -> Result<SandboxActivationAttemptId, Status> {
        self.execution_io(move |store| {
            let current = store.read(sandbox).map_err(store_status)?;
            match current {
                Some(current) if requested == current || previous == Some(current) => Ok(requested),
                None if allow_absent => {
                    require_quiescence(sandbox)?;
                    Ok(requested)
                }
                _ => Err(Status::failed_precondition(
                    "runtime execution identity is missing or superseded",
                )),
            }
        })
        .await
    }

    pub(super) async fn bind_execution_owned(
        &self,
        request: BindExecutionRequest,
        caller: launch::ProcessHandle,
        _guard: tokio::sync::OwnedRwLockWriteGuard<()>,
    ) -> Result<(), Status> {
        let sandbox = parse_sandbox_id(&request.sandbox_id)?;
        let execution = parse_execution(&request.execution_id)?;
        let previous = request
            .previous_id
            .as_deref()
            .map(parse_execution)
            .transpose()?;
        check_caller(Some(&caller))?;
        self.execution_io(move |store| {
            check_caller(Some(&caller))?;
            let current = store.read(sandbox).map_err(store_status)?;
            if current == Some(execution) {
                return Ok(());
            }
            if current.is_some() && current != previous {
                return Err(Status::failed_precondition(
                    "runtime execution binding was superseded",
                ));
            }
            require_quiescence(sandbox)?;
            store.write(sandbox, execution).map_err(store_status)
        })
        .await
    }

    pub(super) async fn retire_execution_owned(
        &self,
        request: RetireExecutionRequest,
        caller: launch::ProcessHandle,
    ) -> Result<(), Status> {
        let sandbox = parse_sandbox_id(&request.sandbox_id)?;
        let execution = parse_execution(&request.execution_id)?;
        let lock = self.runtime_lock(sandbox);
        let _guard = await_runtime_lock(lock.write()).await?;
        check_caller(Some(&caller))?;
        self.execution_io(move |store| {
            check_caller(Some(&caller))?;
            if store
                .read(sandbox)
                .map_err(store_status)?
                .is_some_and(|id| id != execution)
            {
                return Err(Status::failed_precondition(
                    "runtime execution retirement was superseded",
                ));
            }
            require_quiescence(sandbox)?;
            store.remove(sandbox).map_err(store_status)
        })
        .await
    }
}

fn store_status(error: std::io::Error) -> Status {
    Status::unavailable(format!("runtime execution store unavailable: {error}"))
}
