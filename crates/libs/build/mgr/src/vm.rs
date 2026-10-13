// VM boot and lifecycle for the template builder.
//
// Delegates to sbx-manager for sandbox lifecycle (Create/Start/Stop/Delete),
// which in turn delegates privileged operations to node-manager.

pub(crate) const TEMPLATE_DIR: &str = pplx_space_common_core::host::linux::paths::TEMPLATE_DIR;

/// Fixed guest IP — same for every sandbox, isolation via namespace NAT.
pub(crate) const GUEST_IP: &str = "169.254.0.21";

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
            Ok(observed) => match observed.fc_pid {
                Some(current) if current != pid => {
                    return Err(tonic::Status::aborted(
                        "build VM execution changed during shutdown",
                    ));
                }
                Some(_) if !observed.fc_alive => return Ok(()),
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
