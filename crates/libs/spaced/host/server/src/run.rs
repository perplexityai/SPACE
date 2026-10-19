//! The spaced-host service body: resolving the binary's flags into a
//! [`SpacedHostServiceConfig`], the best-effort guest resume notification, and
//! the call that serves until shutdown. This is what the `space-spaced-host-svc`
//! binary's `main` used to hold; the binary now only parses flags, installs
//! telemetry, and calls [`run`].

use std::path::PathBuf;

use pplx_space_spaced_common::proto::bridge_guest::ResumeReason;
use tokio_util::sync::CancellationToken;

use crate::{
    BridgeGuestConnector, RunSpacedHostServerError, SpacedHostService, SpacedHostServiceConfig,
};

#[cfg(test)]
mod tests;

/// The service name spaced-host reports to telemetry. Distinct from the
/// binary name (`space-spaced-host-svc`).
pub const SERVICE_NAME: &str = "space-spaced-host";

/// How the guest this spaced-host fronts was brought up. Selects whether the
/// guest is told it resumed once the bridge is reachable.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum SpacedHostBootType {
    #[default]
    Fresh,
    Snapshot,
    BaseSnapshot,
    Restore,
    Resume,
}

impl SpacedHostBootType {
    /// The boot type as it appears in the log context, matching the spelling
    /// the binary's `--boot-type` flag accepts.
    pub fn as_str(self) -> &'static str {
        match self {
            SpacedHostBootType::Fresh => "fresh",
            SpacedHostBootType::Snapshot => "snapshot",
            SpacedHostBootType::BaseSnapshot => "base-snapshot",
            SpacedHostBootType::Restore => "restore",
            SpacedHostBootType::Resume => "resume",
        }
    }

    /// Why the guest should be told it resumed, or `None` when this boot path
    /// gives the guest no resume to observe.
    fn resume_reason(self) -> Option<ResumeReason> {
        match self {
            SpacedHostBootType::Fresh
            | SpacedHostBootType::Snapshot
            | SpacedHostBootType::BaseSnapshot
            | SpacedHostBootType::Restore => None,
            SpacedHostBootType::Resume => Some(ResumeReason::SuspendResume),
        }
    }
}

/// Everything [`run`] needs from the process that hosts it: the binary's
/// flags, already parsed.
///
/// Every socket path is optional and additive: `None` leaves the
/// corresponding [`SpacedHostServiceConfig`] default untouched, so a caller
/// that sets nothing gets production behavior.
#[derive(Clone, Debug, Default)]
pub struct SpacedHostRunConfig {
    /// spaced-host's own gRPC Unix socket.
    pub socket: Option<PathBuf>,
    /// The sandbox host-control Unix socket spaced-host calls out on.
    pub host_control_socket: Option<PathBuf>,
    /// Firecracker vsock UDS. When set, both bridge directions run over
    /// Firecracker vsock and `firecracker_pid`/`firecracker_uid` are required.
    pub vsock_uds: Option<PathBuf>,
    /// PID of the Firecracker process expected on the far end of `vsock_uds`.
    pub firecracker_pid: Option<u32>,
    /// UID of the Firecracker process expected on the far end of `vsock_uds`.
    pub firecracker_uid: Option<u32>,
    /// Reach the guest's bridge over this UDS instead of a Firecracker vsock.
    /// Ignored when `vsock_uds` is set.
    pub bridge_guest_uds: Option<PathBuf>,
    /// Listen for the guest's host-bound bridge on this UDS instead of a
    /// Firecracker vsock. Ignored when `vsock_uds` is set.
    pub bridge_host_uds: Option<PathBuf>,
    /// The sandbox this spaced-host serves. Log context only.
    pub sandbox_id: Option<String>,
    /// How the guest was booted.
    pub boot_type: SpacedHostBootType,
}

impl SpacedHostRunConfig {
    /// The log context spaced-host runs under: the boot path that produced
    /// this process, plus the sandbox it serves when one was named. The host
    /// process installs this *before* calling [`run`] so startup failures are
    /// already attributed to the right sandbox and boot type.
    pub fn log_context(&self) -> pplx_space_common_svc::LogContext {
        let mut log_context =
            pplx_space_common_svc::LogContext::default().with_boot_type(self.boot_type.as_str());
        if let Some(sandbox_id) = &self.sandbox_id {
            log_context = log_context.with_sandbox_id(sandbox_id.clone());
        }
        log_context
    }
}

/// Why [`run`] returned without a clean shutdown.
#[derive(Debug, thiserror::Error)]
pub enum RunSpacedHostError {
    #[error("--firecracker-pid is required with --vsock-uds")]
    MissingFirecrackerPid,
    #[error("--firecracker-uid is required with --vsock-uds")]
    MissingFirecrackerUid,
    #[error("--firecracker-pid exceeds the supported process ID range")]
    FirecrackerPidOutOfRange,
    /// A Firecracker peer identity was supplied with no vsock to check it on.
    /// The binary's clap `requires` makes this unreachable from the command
    /// line; it is rejected here so an in-process caller assembling a
    /// [`SpacedHostRunConfig`] cannot silently fall through to the UDS bridge
    /// with its peer expectation dropped.
    #[error(
        "--firecracker-pid/--firecracker-uid require --vsock-uds: without it spaced-host reaches \
         the guest over a UDS and cannot check the Firecracker peer they name"
    )]
    FirecrackerPeerWithoutVsockUds,
    #[error(transparent)]
    Serve(#[from] RunSpacedHostServerError),
}

/// Fold the host process's flags onto the service defaults.
fn resolve_service_config(
    config: SpacedHostRunConfig,
) -> Result<SpacedHostServiceConfig, RunSpacedHostError> {
    let mut service_config = SpacedHostServiceConfig::default();
    if let Some(socket) = config.socket {
        service_config.socket_path = socket;
    }
    if let Some(host_control_socket) = config.host_control_socket {
        service_config.host_control_socket_path = host_control_socket;
    }
    if let Some(vsock_uds_path) = config.vsock_uds {
        let firecracker_pid = config
            .firecracker_pid
            .ok_or(RunSpacedHostError::MissingFirecrackerPid)?;
        let firecracker_uid = config
            .firecracker_uid
            .ok_or(RunSpacedHostError::MissingFirecrackerUid)?;
        service_config.bridge_host_socket_path =
            pplx_space_spaced_common::firecracker_vsock_port_path(
                &vsock_uds_path,
                pplx_space_spaced_common::BRIDGE_HOST_VSOCK_PORT,
            );
        service_config.bridge_guest_connector = BridgeGuestConnector::FirecrackerVsock {
            vsock_uds_path,
            port: pplx_space_spaced_common::BRIDGE_GUEST_VSOCK_PORT,
            expected_peer: pplx_space_spaced_common::ExpectedUdsPeer::new(
                i32::try_from(firecracker_pid)
                    .map_err(|_| RunSpacedHostError::FirecrackerPidOutOfRange)?,
                firecracker_uid,
            ),
        };
    } else {
        if config.firecracker_pid.is_some() || config.firecracker_uid.is_some() {
            return Err(RunSpacedHostError::FirecrackerPeerWithoutVsockUds);
        }
        // UDS-bridge mode (local non-Firecracker runtime). Each flag is
        // additive: unset leaves the default UDS paths untouched, so production
        // behavior is unchanged.
        if let Some(bridge_guest_uds) = config.bridge_guest_uds {
            service_config.bridge_guest_connector = BridgeGuestConnector::Uds(bridge_guest_uds);
        }
        if let Some(bridge_host_uds) = config.bridge_host_uds {
            service_config.bridge_host_socket_path = bridge_host_uds;
        }
    }
    Ok(service_config)
}

async fn notify_resume_until_success(
    service: SpacedHostService,
    shutdown: CancellationToken,
    reason: ResumeReason,
) {
    let mut retry = 0usize;
    loop {
        if shutdown.is_cancelled() {
            return;
        }

        match service.notify_resume(reason).await {
            Ok(()) => return,
            Err(error) => {
                let Some(delay) =
                    pplx_space_spaced_common::DEFAULT_CONNECT_RETRY_POLICY.delay(retry)
                else {
                    tracing::warn!(
                        %error,
                        retry,
                        "failed to notify guest resume; retry policy exhausted"
                    );
                    return;
                };
                retry += 1;
                tracing::warn!(
                    %error,
                    retry,
                    retry_in = ?delay,
                    "failed to notify guest resume; will retry"
                );
                tokio::select! {
                    () = shutdown.cancelled() => return,
                    () = tokio::time::sleep(delay) => {}
                }
            }
        }
    }
}

/// Serve spaced-host until `shutdown` is cancelled or the gRPC server fails.
///
/// On a resume boot the guest is notified in the background, retried under
/// [`pplx_space_spaced_common::DEFAULT_CONNECT_RETRY_POLICY`], so serving is
/// never blocked on a guest that is still coming up.
pub async fn run(
    config: SpacedHostRunConfig,
    shutdown: CancellationToken,
) -> Result<(), RunSpacedHostError> {
    let boot_type = config.boot_type;
    let sandbox_id = config.sandbox_id.clone();
    let service_config = resolve_service_config(config)?;
    tracing::debug!(
        sandbox_id = ?sandbox_id,
        boot_type = ?boot_type,
        "spaced host service config loaded"
    );

    let service = SpacedHostService::new_with_config(shutdown.clone(), service_config);
    if let Some(reason) = boot_type.resume_reason() {
        tokio::spawn(notify_resume_until_success(
            service.clone(),
            shutdown.clone(),
            reason,
        ));
    }
    service.run().await?;

    Ok(())
}
