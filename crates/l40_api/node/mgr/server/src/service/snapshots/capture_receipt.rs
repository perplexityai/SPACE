use crate::service::trusted_paths;
use pplx_space_common_core::types::{LocalArtifactId, SandboxId};
use pplx_space_node_mgr_client::capture_receipt::{
    CLAIMED, COMMITTED, FAILED, INTENT, REFUSED, STAGING_PREFIX,
};
use std::{fs::File, path::Path};
use tonic::Status;

/// Pin the validated staging directory before capture. Relative rename and
/// fsync use this handle even if a directory is renamed while work finishes.
pub(super) struct CaptureReceipt {
    directory: File,
    capture_started: bool,
}

impl CaptureReceipt {
    pub(super) fn open(sandbox_id: SandboxId, path: &Path) -> Result<Self, Status> {
        Self::open_under(Path::new(crate::service::CACHE_DIR), sandbox_id, path)
    }

    pub(super) fn open_under(
        cache: &Path,
        sandbox_id: SandboxId,
        path: &Path,
    ) -> Result<Self, Status> {
        let artifact = path
            .file_name()
            .and_then(|n| n.to_str())
            .and_then(|n| n.strip_prefix(STAGING_PREFIX))
            .and_then(|n| n.parse::<LocalArtifactId>().ok())
            .ok_or_else(|| Status::invalid_argument("invalid capture staging identity"))?;
        let expected = cache
            .join(sandbox_id.to_string())
            .join("snapshots")
            .join(format!("{STAGING_PREFIX}{artifact}"));
        if path.as_os_str() != expected.as_os_str() {
            return Err(Status::invalid_argument(
                "capture staging does not belong to sandbox",
            ));
        }
        let directory = trusted_paths::open_directory(path)?;
        let intent = trusted_paths::open_regular_file(&directory, INTENT.as_ref())?;
        #[derive(serde::Deserialize)]
        struct Identity {
            sandbox_id: String,
            local_artifact_id: LocalArtifactId,
        }
        let identity: Identity = serde_json::from_reader(intent)
            .map_err(|e| Status::failed_precondition(format!("invalid capture intent: {e}")))?;
        if identity.sandbox_id != sandbox_id.to_string() || identity.local_artifact_id != artifact {
            return Err(Status::failed_precondition(
                "capture intent identity mismatch",
            ));
        }
        // Exclusive creation fences duplicate RPCs, including after a failed
        // capture or a process restart. A later refusal must never overwrite
        // uncertainty about a previous producer's in-place memory writes.
        let claim = nix::fcntl::openat(
            &directory,
            CLAIMED,
            nix::fcntl::OFlag::O_CREAT | nix::fcntl::OFlag::O_EXCL | nix::fcntl::OFlag::O_WRONLY,
            nix::sys::stat::Mode::S_IRUSR | nix::sys::stat::Mode::S_IWUSR,
        )
        .map_err(|e| Status::failed_precondition(format!("claim capture attempt: {e}")))?;
        File::from(claim)
            .sync_all()
            .and_then(|()| directory.sync_all())
            .map_err(|e| Status::internal(format!("sync capture admission: {e}")))?;
        Ok(Self {
            directory,
            capture_started: false,
        })
    }

    /// Called immediately before the runtime operation that can mutate memory.
    pub(super) fn begin_capture(&mut self) {
        self.capture_started = true;
    }

    pub(super) fn capture_started(&self) -> bool {
        self.capture_started
    }

    pub(super) async fn commit(&self) -> Result<(), Status> {
        if !self.capture_started {
            return Err(Status::failed_precondition("capture has not started"));
        }
        let directory = self
            .directory
            .try_clone()
            .map_err(|e| Status::internal(format!("clone capture directory: {e}")))?;
        Self::persist(directory, COMMITTED).await
    }

    /// Called after the owned operation returns, including its stop attempts.
    /// A terminal producer error cannot promise that the runtime stopped or its
    /// in-place writes ceased. It only proves no completion receipt will follow
    /// from this owner. Panic/shutdown retains the intent without doing Drop IO.
    pub(super) async fn settle_error(self) -> Result<(), Status> {
        let outcome = if self.capture_started {
            FAILED
        } else {
            REFUSED
        };
        // Rename only the intent: a commit whose directory sync failed must
        // keep its committed receipt, never replace it with a failure marker.
        Self::persist(self.directory, outcome).await
    }

    async fn persist(directory: File, name: &'static str) -> Result<(), Status> {
        tokio::task::spawn_blocking(move || {
            nix::fcntl::renameat(&directory, INTENT, &directory, name)
                .map_err(|e| Status::internal(format!("persist capture outcome: {e}")))?;
            directory
                .sync_all()
                .map_err(|e| Status::internal(format!("sync capture outcome: {e}")))
        })
        .await
        .map_err(|e| Status::internal(format!("capture outcome task: {e}")))?
    }
}

#[cfg(test)]
mod tests;
