use super::*;
use pplx_space_node_mgr_client::execution_fence::{
    EXECUTION_PREFLIGHT_LIMIT, ExecutionFence, execution_preflight_result,
};

impl NodeManagerService {
    /// Include surviving runtimes without a slot: checking only lease rows
    /// would miss unadopted runtime cleanup. Names are discovery, not
    /// permission to change a lease or stop a process.
    pub(super) async fn execution_candidates(&self) -> Result<Vec<String>, Status> {
        let pool = self.pool.clone();
        self.off_runtime("read execution candidates", move || {
            let mut ids: std::collections::BTreeSet<_> = pool
                .all_records()
                .map_err(slot_store_status)?
                .into_iter()
                .map(|record| record.info.sandbox_id)
                .filter(|id| !id.is_empty())
                .collect();
            for root in [RUN_DIR, "/sys/fs/cgroup/space"] {
                let entries = match std::fs::read_dir(root) {
                    Ok(entries) => entries,
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                    Err(error) => return Err(Status::unavailable(format!("read {root}: {error}"))),
                };
                for (index, entry) in entries.enumerate() {
                    if index >= 65_536 {
                        return Err(Status::resource_exhausted(
                            "execution discovery limit exceeded",
                        ));
                    }
                    let entry = entry
                        .map_err(|error| Status::unavailable(format!("read {root}: {error}")))?;
                    let name = entry.file_name();
                    let Some(name) = name.to_str() else { continue };
                    let id = name.strip_prefix("sbx-").unwrap_or(name);
                    if id.parse::<SandboxId>().is_ok()
                        && !crate::cgroup::sandbox_is_quiescent(id).map_err(|error| {
                            Status::unavailable(format!("inspect {id}: {error}"))
                        })?
                    {
                        ids.insert(id.to_owned());
                    }
                }
            }
            Ok(ids.into_iter().collect())
        })
        .await
    }

    pub(crate) async fn verify_execution_authority(&self) -> Result<(), Status> {
        let mut unadopted = Vec::new();
        for sandbox_id in self.execution_candidates().await? {
            // Recovery starts before preflight and can own this sandbox.
            // Wait without host locks, then revalidate its current authority.
            let _claim = tokio::select! {
                biased;
                _ = self.pool.shutdown_token.cancelled() => {
                    return Err(Status::unavailable("node-manager is shutting down"));
                }
                claim = self.pool.claim_sandbox_mutation(&sandbox_id) => claim,
            };
            let _gate = self.lock_sandbox_cgroups(&sandbox_id).await;
            let pool = self.pool.clone();
            let missing = self
                .off_runtime("verify execution authority", move || {
                    if pool
                        .execution_fence(&sandbox_id)
                        .map_err(slot_store_status)?
                        .is_none()
                        && (pool
                            .get_record(&sandbox_id)
                            .map_err(slot_store_status)?
                            .is_some()
                            || !crate::cgroup::sandbox_is_quiescent(&sandbox_id).map_err(
                                |error| {
                                    Status::unavailable(format!("inspect {sandbox_id}: {error}"))
                                },
                            )?)
                    {
                        return Ok(Some(sandbox_id));
                    }
                    Ok(None)
                })
                .await?;
            if let Some(id) = missing {
                unadopted.push(id);
                if unadopted.len() == EXECUTION_PREFLIGHT_LIMIT {
                    break;
                }
            }
        }
        execution_preflight_result(&unadopted)
    }
}

impl NodeManagerService {
    /// The caller holds artifact exclusion and the cgroup gate through mutation.
    /// Canonical restore may precede Allocate, or replace its stopped predecessor;
    /// it cannot touch a successor's instance. Scoped work never touches it.
    pub(super) async fn instance_target_root(
        &self,
        sandbox_id: &str,
        target: pplx_space_node_mgr_client::execution_fence::InstanceTarget,
    ) -> Result<PathBuf, Status> {
        use pplx_space_node_mgr_client::execution_fence::InstanceTarget;
        match target {
            InstanceTarget::RestoreScope(scope) => Ok(PathBuf::from(
                pplx_space_common_core::host::linux::paths::sandbox_restore_staging_dir(
                    sandbox_id,
                    &scope.to_string(),
                ),
            )),
            InstanceTarget::Execution(fence) => {
                self.check_execution(sandbox_id, Some(&fence.into()), true)?;
                let sandbox_id = sandbox_id.to_owned();
                self.off_runtime("verify stopped instance owner", move || {
                    require_stopped_instance(&sandbox_id)?;
                    Ok(PathBuf::from(CACHE_DIR).join(sandbox_id))
                })
                .await
            }
        }
    }
}

fn require_stopped_instance(sandbox_id: &str) -> Result<(), Status> {
    if super::runtime::firecracker_before_jail_rebuild(sandbox_id)?.is_some()
        || !crate::cgroup::sandbox_is_quiescent(sandbox_id).map_err(|error| {
            Status::unavailable(format!("runtime occupancy unavailable: {error}"))
        })?
    {
        return Err(Status::failed_precondition(
            "stop the runtime before accessing its unbound or replacement filesystem",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests;

pub(super) fn pin_caller<T>(
    request: &Request<T>,
    fenced: bool,
) -> Result<Option<launch::ProcessHandle>, Status> {
    if !fenced {
        return Ok(None);
    }
    let pid = pplx_space_common_svc::peer_identity_from_tonic_request(request)
        .and_then(|peer| peer.pid())
        .and_then(|pid| u32::try_from(pid).ok())
        .ok_or_else(|| {
            Status::permission_denied("execution mutations require a kernel peer process identity")
        })?;
    launch::ProcessHandle::open(pid)
        .map(Some)
        .map_err(|error| Status::unavailable(format!("pin lifecycle caller: {error}")))
}

pub(super) fn check_caller(caller: Option<&launch::ProcessHandle>) -> Result<(), Status> {
    if let Some(caller) = caller
        && caller
            .has_exited()
            .map_err(|error| Status::unavailable(format!("observe lifecycle caller: {error}")))?
    {
        return Err(Status::aborted(
            "lifecycle caller exited before node mutation",
        ));
    }
    Ok(())
}

/// Call only while holding the sandbox's cgroup gate. Reads alone are not an
/// authorization: that gate must remain owned through all host mutation.
impl NodeManagerService {
    /// Filesystem-only work may outlive every lease, including legacy stopped
    /// instances that never had a receiver token. Under artifact exclusion and
    /// the cgroup gate, accept an explicit unbound token only after proving no
    /// lease or runtime remains. Never grant access to a bound predecessor.
    pub(super) async fn check_filesystem_execution(
        &self,
        sandbox_id: &str,
        requested: Option<&crate::proto::ExecutionFence>,
    ) -> Result<(), Status> {
        let unbound = requested.is_some()
            && self
                .pool
                .execution_fence(sandbox_id)
                .map_err(slot_store_status)?
                .is_none();
        // The gate keeps binding stable across these reads. Enable the existing
        // staging rule only for absence, never for a different current owner.
        self.check_execution(sandbox_id, requested, unbound)?;
        if unbound {
            let pool = self.pool.clone();
            let sandbox_id = sandbox_id.to_owned();
            self.off_runtime("verify unbound filesystem", move || {
                if pool.get(&sandbox_id).map_err(slot_store_status)?.is_some() {
                    return Err(Status::failed_precondition(
                        "unbound filesystem still has a network lease",
                    ));
                }
                require_stopped_instance(&sandbox_id)
            })
            .await?;
        }
        Ok(())
    }

    pub(super) fn check_execution(
        &self,
        sandbox_id: &str,
        requested: Option<&crate::proto::ExecutionFence>,
        allow_predecessor: bool,
    ) -> Result<Option<ExecutionFence>, Status> {
        if requested.is_some() {
            super::trusted_paths::canonical_sandbox_id(sandbox_id)?;
        }
        let requested = requested.map(ExecutionFence::try_from).transpose()?;
        self.check_parsed_execution(sandbox_id, requested, allow_predecessor)
    }

    pub(super) fn check_parsed_execution(
        &self,
        sandbox_id: &str,
        requested: Option<ExecutionFence>,
        allow_predecessor: bool,
    ) -> Result<Option<ExecutionFence>, Status> {
        let current = self
            .pool
            .execution_fence(sandbox_id)
            .map_err(slot_store_status)?;
        match (requested, current) {
            (Some(requested), Some(current))
                if requested.current() == current.current()
                    || (allow_predecessor && requested.previous() == Some(current.current())) =>
            {
                Ok(Some(current))
            }
            // An activation can fail before Allocate, leaving only unbound
            // initial artifacts. The gate excludes a replacement binding.
            (Some(_), None) if allow_predecessor => Ok(None),
            _ => Err(Status::failed_precondition(
                "sandbox execution identity is missing or superseded",
            )),
        }
    }

    pub(super) async fn bind_execution_for_allocation(
        &self,
        sandbox_id: &str,
        requested: Option<&crate::proto::ExecutionFence>,
    ) -> Result<(), Status> {
        let requested = requested
            .ok_or_else(|| Status::failed_precondition("execution identity is required"))?;
        super::trusted_paths::canonical_sandbox_id(sandbox_id)?;
        let fence = ExecutionFence::try_from(requested)?;
        let current = self
            .pool
            .execution_fence(sandbox_id)
            .map_err(slot_store_status)?;
        if current.is_none_or(|current| current.current() != fence.current()) {
            if current.map(ExecutionFence::current) != fence.previous()
                || self
                    .pool
                    .get(sandbox_id)
                    .map_err(slot_store_status)?
                    .is_some()
            {
                return Err(Status::failed_precondition(
                    "release the previous execution before allocating its replacement",
                ));
            }
            self.pool
                .bind_execution(sandbox_id, fence)
                .map_err(slot_store_status)?;
        }
        self.runtime_supervisor
            .bind_execution(pplx_space_node_runtime_core::proto::BindExecutionRequest {
                sandbox_id: sandbox_id.to_owned(),
                execution_id: fence.current().to_string(),
                previous_id: fence.previous().map(|id| id.to_string()),
            })
            .await?;
        Ok(())
    }
}
