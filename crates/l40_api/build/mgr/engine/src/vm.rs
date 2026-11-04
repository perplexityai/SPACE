// VM boot and lifecycle for the template builder.
//
// Delegates to sbx-manager for sandbox lifecycle (Create/Start/Stop/Delete),
// which in turn delegates privileged operations to node-manager.

pub(crate) const TEMPLATE_DIR: &str = pplx_space_common_core::host::linux::paths::TEMPLATE_DIR;

/// Fixed guest IP — same for every sandbox, isolation via namespace NAT.
pub(crate) const GUEST_IP: std::net::Ipv4Addr = pplx_space_net_common::guest_link::GUEST;

/// Linux build guests need the exit event their VMM implements. Firecracker
/// exits on reset; Cloud Hypervisor handles reset by rebooting the guest and
/// requires power-off to exit. Resolve this before creating the build sandbox.
/// Flush first; the detached delay lets SSH acknowledge the request, and
/// confirmed Stop remains the barrier before copying the rootfs.
pub(crate) fn guest_exit_command(
    runtime: pplx_space_common_core::types::RuntimeKind,
) -> Result<&'static str, super::BuildError> {
    use pplx_space_common_core::types::RuntimeKind;
    match runtime {
        RuntimeKind::Firecracker => Ok(
            "nohup sh -c 'sync; echo 1 > /proc/sys/kernel/sysrq 2>/dev/null || true; if [ -w /proc/sysrq-trigger ]; then echo s > /proc/sysrq-trigger; echo u > /proc/sysrq-trigger; sleep 1; echo b > /proc/sysrq-trigger; else reboot -f; fi' >/dev/null 2>&1 </dev/null &",
        ),
        RuntimeKind::CloudHypervisor => {
            Ok("nohup sh -c 'sync; sleep 1; exec poweroff -f' >/dev/null 2>&1 </dev/null &")
        }
        RuntimeKind::Gvisor => Err(super::BuildError::UnsupportedGuestExit(runtime)),
    }
}

/// Observe graceful exit in the host's PID namespace. This is only a grace
/// window: acknowledged Stop remains the barrier before copying the rootfs.
pub(crate) async fn wait_for_exit(
    sandbox_id: &str,
    pid: u32,
    budget: std::time::Duration,
) -> Result<(), tonic::Status> {
    tokio::time::timeout(budget, async {
        let node = pplx_space_node_mgr_client::connect()
            .await
            .map_err(|error| {
                tonic::Status::unavailable(format!("connect for VM exit observation: {error}"))
            })?;
        wait_for_exit_with(pid, || {
            let mut node = node.clone();
            let request = pplx_space_node_mgr_client::proto::InspectSandboxRequest {
                sandbox_id: sandbox_id.to_owned(),
                ..Default::default()
            };
            async move {
                node.inspect_sandbox(request)
                    .await
                    .map(tonic::Response::into_inner)
            }
        })
        .await
    })
    .await
    .map_err(|_| {
        tonic::Status::deadline_exceeded(
            "build VM graceful exit was not confirmed before shutdown deadline",
        )
    })?
}

async fn wait_for_exit_with<F, Fut>(pid: u32, mut inspect: F) -> Result<(), tonic::Status>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<
            Output = Result<
                pplx_space_node_mgr_client::proto::InspectSandboxResponse,
                tonic::Status,
            >,
        >,
{
    loop {
        match inspect().await {
            Ok(observed) => match observed.runtime_pid {
                Some(current) if current != pid => {
                    return Err(tonic::Status::aborted(
                        "build VM execution changed during shutdown",
                    ));
                }
                Some(_) if !observed.runtime_alive => return Ok(()),
                // Missing identity is unknown, not evidence of a dead VM.
                _ => {}
            },
            Err(error)
                if matches!(
                    error.code(),
                    tonic::Code::Unavailable
                        | tonic::Code::ResourceExhausted
                        | tonic::Code::DeadlineExceeded
                ) => {}
            Err(error) => return Err(error),
        }
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }
}

#[cfg(test)]
mod tests;
