use std::{fs, sync::Arc, time::Duration};

use pplx_space_common_core::types::SandboxId;
use pplx_space_node_runtime_core::cgroup;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tonic::Status;

const CGROUP_TERMINATION_TIMEOUT: Duration = Duration::from_secs(3);
const CGROUP_POLL_INTERVAL: Duration = Duration::from_millis(50);

pub(crate) async fn apply_firecracker_cgroup_fixups(
    sandbox_id: SandboxId,
    fc_pid: u32,
    blocking_slot: OwnedSemaphorePermit,
) {
    let sandbox_id = sandbox_id.to_string();
    let sandbox_id_for_log = sandbox_id.clone();
    if let Err(error) = tokio::task::spawn_blocking(move || {
        let _blocking_slot = blocking_slot;
        let swap_max = format!("/sys/fs/cgroup/space/{sandbox_id}/memory.swap.max");
        if let Err(error) = fs::write(&swap_max, "0") {
            tracing::debug!(sandbox_id, path = %swap_max, error = %error, "failed to set firecracker swap limit");
        }

        let task_dir = format!("/proc/{fc_pid}/task");
        let cgroup_procs = format!("/sys/fs/cgroup/space/{sandbox_id}/cgroup.procs");
        let Ok(entries) = fs::read_dir(&task_dir) else {
            return;
        };
        for entry in entries.flatten() {
            let comm_path = entry.path().join("comm");
            if let Ok(name) = fs::read_to_string(&comm_path)
                && name.trim().starts_with("kvm-pit")
            {
                let tid = entry.file_name().to_string_lossy().to_string();
                if let Err(error) = fs::write(&cgroup_procs, &tid) {
                    tracing::debug!(sandbox_id, tid, path = %cgroup_procs, error = %error, "failed to move kvm-pit thread into firecracker cgroup");
                }
            }
        }
    })
    .await
    {
        tracing::debug!(sandbox_id = %sandbox_id_for_log, error = %error, "firecracker cgroup fixup task failed");
    }
}

pub(crate) async fn terminate_sandbox_cgroups(
    sandbox_id: SandboxId,
    blocking_slots: Arc<Semaphore>,
    scope: cgroup::CgroupScope,
) -> Result<Vec<String>, Status> {
    let deadline = tokio::time::Instant::now() + CGROUP_TERMINATION_TIMEOUT;
    let started = tokio::time::Instant::now();
    let mut polls = 0;
    let report = loop {
        let permit = tokio::time::timeout_at(deadline, blocking_slots.clone().acquire_owned())
            .await
            .map_err(|_| {
                Status::unavailable("cgroup termination deadline expired awaiting capacity")
            })?
            .map_err(|_| Status::unavailable("cgroup cleanup admission closed"))?;
        // The owned caller retains its runtime/launch guards while this work
        // runs. Never abandon a blocking poll on timeout and release those guards.
        polls += 1;
        let result = tokio::task::spawn_blocking(move || {
            let _permit = permit;
            cgroup::try_terminate_sandbox_cgroups(sandbox_id, scope)
        })
        .await
        .map_err(|error| Status::internal(format!("cgroup cleanup task failed: {error}")))?;
        match result {
            Ok(cgroup::CgroupRemovalOutcome::Removed(report)) => break report,
            Ok(cgroup::CgroupRemovalOutcome::Busy { path, observation }) => {
                if tokio::time::Instant::now() >= deadline {
                    return Err(Status::unavailable(format!(
                        "cgroup termination deadline: {}: {observation}",
                        path.display()
                    )));
                }
                // No blocking-work permit or blocking thread is held here.
                tokio::time::sleep_until(
                    deadline.min(tokio::time::Instant::now() + CGROUP_POLL_INTERVAL),
                )
                .await;
            }
            Err(error) => return Err(Status::internal(error.to_string())),
        }
    };

    if report.paths_present == 0 && report.killed_pids == 0 {
        return Ok(Vec::new());
    }

    tracing::info!(
        %sandbox_id,
        polls,
        final_poll_report = ?report,
        elapsed_ms = started.elapsed().as_millis() as u64,
        "runtime supervisor cgroup termination complete"
    );
    Ok(vec!["Removed sandbox cgroups".to_string()])
}

pub(crate) fn cgroup_update_error_to_status(error: cgroup::CgroupUpdateError) -> Status {
    match error {
        cgroup::CgroupUpdateError::NotFound(_) => Status::not_found(error.to_string()),
        cgroup::CgroupUpdateError::Invalid(_) => Status::invalid_argument(error.to_string()),
        cgroup::CgroupUpdateError::Read { .. } => Status::unavailable(error.to_string()),
        cgroup::CgroupUpdateError::Write { .. } => Status::internal(error.to_string()),
    }
}
