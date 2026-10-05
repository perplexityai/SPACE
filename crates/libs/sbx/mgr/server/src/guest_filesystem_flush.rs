use std::time::Duration;

pub(crate) const GUEST_FILESYSTEM_FLUSH_TIMEOUT: Duration = Duration::from_secs(30);
const STOP_FLUSH_FIRST_RETRY: Duration = Duration::from_millis(50);
const STOP_FLUSH_MAX_RETRY: Duration = Duration::from_secs(1);

/// Why a guest filesystem flush, or the snapshot preparation that includes
/// one, did not complete.
#[derive(Debug, thiserror::Error)]
pub(crate) enum GuestFlushError {
    #[error("cannot connect to the sandbox's spaced-host: {0}")]
    Connect(#[source] pplx_space_spaced_host_client::ConnectError),
    #[error("{0}")]
    Guest(#[source] tonic::Status),
    #[error("{operation} timed out after {budget:?}")]
    TimedOut {
        operation: &'static str,
        budget: Duration,
    },
}

const FLUSH: &str = "guest filesystem flush";
const SNAPSHOT_PREPARATION: &str = "guest snapshot preparation";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum GuestSnapshotPreparationBudget {
    Standard,
    AutoLifecycle,
}

impl GuestSnapshotPreparationBudget {
    const fn timeout(self) -> Duration {
        match self {
            Self::Standard => GUEST_FILESYSTEM_FLUSH_TIMEOUT,
            // Background auto-lifecycle work can allow a longer flush budget
            // without extending foreground suspend latency.
            Self::AutoLifecycle => Duration::from_secs(180),
        }
    }
}

/// Flush storage for a disk-only snapshot that does not capture VM memory.
///
/// This may run earlier in a scheduler tick that later creates a distinct
/// memory-bearing checkpoint. For the memory-bearing snapshot itself, use
/// [`prepare_guest_for_vm_snapshot_via_spaced_host`] instead.
pub(crate) async fn flush_guest_filesystems_via_spaced_host(
    sandbox_id: &str,
) -> Result<(), GuestFlushError> {
    tokio::time::timeout(
        GUEST_FILESYSTEM_FLUSH_TIMEOUT,
        flush_guest_filesystems_via_spaced_host_unbounded(sandbox_id),
    )
    .await
    .map_err(|_| GuestFlushError::TimedOut {
        operation: FLUSH,
        budget: GUEST_FILESYSTEM_FLUSH_TIMEOUT,
    })?
}

/// Flush storage and release cached guest bridge clients before a VM memory snapshot.
///
/// This is the complete pre-memory-snapshot operation; callers must not add
/// [`flush_guest_filesystems_via_spaced_host`] for that same memory snapshot.
pub(crate) async fn prepare_guest_for_vm_snapshot_via_spaced_host(
    sandbox_id: &str,
    budget: GuestSnapshotPreparationBudget,
) -> Result<(), GuestFlushError> {
    let timeout = budget.timeout();
    tokio::time::timeout(
        timeout,
        prepare_guest_for_vm_snapshot_via_spaced_host_unbounded(sandbox_id, timeout),
    )
    .await
    .map_err(|_| GuestFlushError::TimedOut {
        operation: SNAPSHOT_PREPARATION,
        budget: timeout,
    })?
}

/// Attempt to flush the guest's root filesystem before a stop terminates its
/// VM, which does not shut the guest down. The whole attempt, connecting and
/// retrying included, shares one budget. A flush the guest is already running
/// may have started before the latest acknowledged writes, so a busy guest is
/// retried until this call's own flush runs. An attempt that runs out of
/// budget does not stop a `syncfs` the guest has begun; the caller stops the
/// VM regardless.
pub(crate) async fn flush_guest_root_filesystem_before_stop(
    sandbox_id: &str,
) -> Result<(), GuestFlushError> {
    let budget = GUEST_FILESYSTEM_FLUSH_TIMEOUT;
    let started = tokio::time::Instant::now();
    let config = pplx_space_spaced_host_client::SpacedHostClientConfig::default().with_socket_path(
        pplx_space_common_core::host::linux::paths::spaced_host_socket(sandbox_id),
    );
    let client = tokio::time::timeout(
        budget,
        pplx_space_spaced_host_client::SpacedHostClient::new_with_config(config),
    )
    .await
    .map_err(|_| GuestFlushError::TimedOut {
        operation: FLUSH,
        budget,
    })?
    .map_err(GuestFlushError::Connect)?;
    flush_until_own_attempt_runs(budget.saturating_sub(started.elapsed()), |remaining| {
        client.flush_guest_filesystems(remaining)
    })
    .await
}

async fn flush_until_own_attempt_runs<F, Fut>(
    budget: Duration,
    mut attempt: F,
) -> Result<(), GuestFlushError>
where
    F: FnMut(Duration) -> Fut,
    Fut: std::future::Future<Output = Result<(), tonic::Status>>,
{
    let deadline = tokio::time::Instant::now() + budget;
    let timed_out = || GuestFlushError::TimedOut {
        operation: FLUSH,
        budget,
    };
    let mut retry = STOP_FLUSH_FIRST_RETRY;
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            return Err(timed_out());
        }
        match tokio::time::timeout(remaining, attempt(remaining)).await {
            Ok(Ok(())) => return Ok(()),
            Ok(Err(status)) if status.code() == tonic::Code::ResourceExhausted => {
                tokio::time::sleep_until(deadline.min(tokio::time::Instant::now() + retry)).await;
                retry = (retry * 2).min(STOP_FLUSH_MAX_RETRY);
            }
            Ok(Err(status)) => return Err(GuestFlushError::Guest(status)),
            Err(_) => return Err(timed_out()),
        }
    }
}

async fn flush_guest_filesystems_via_spaced_host_unbounded(
    sandbox_id: &str,
) -> Result<(), GuestFlushError> {
    let socket_path = pplx_space_common_core::host::linux::paths::spaced_host_socket(sandbox_id);
    flush_guest_filesystems_at_socket(socket_path).await
}

async fn prepare_guest_for_vm_snapshot_via_spaced_host_unbounded(
    sandbox_id: &str,
    timeout: Duration,
) -> Result<(), GuestFlushError> {
    let started_at = tokio::time::Instant::now();
    let socket_path = pplx_space_common_core::host::linux::paths::spaced_host_socket(sandbox_id);
    let config = pplx_space_spaced_host_client::SpacedHostClientConfig::default()
        .with_socket_path(socket_path);
    let client = pplx_space_spaced_host_client::SpacedHostClient::new_with_config(config)
        .await
        .map_err(GuestFlushError::Connect)?;
    client
        .prepare_suspend(timeout.saturating_sub(started_at.elapsed()))
        .await
        .map_err(GuestFlushError::Guest)
}

async fn flush_guest_filesystems_at_socket(socket_path: String) -> Result<(), GuestFlushError> {
    let started_at = tokio::time::Instant::now();
    let config = pplx_space_spaced_host_client::SpacedHostClientConfig::default()
        .with_socket_path(socket_path);
    let client = pplx_space_spaced_host_client::SpacedHostClient::new_with_config(config)
        .await
        .map_err(GuestFlushError::Connect)?;
    client
        .flush_guest_filesystems(
            GUEST_FILESYSTEM_FLUSH_TIMEOUT.saturating_sub(started_at.elapsed()),
        )
        .await
        .map_err(GuestFlushError::Guest)
}

#[cfg(test)]
mod tests;
