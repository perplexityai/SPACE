use super::*;
impl SpacedGuestService {
    pub(super) async fn restart_after_update(&self) {
        // Task Scheduler runs outside the daemon's workload job. The periodic
        // expiry task reconciles durable intent if this immediate request fails.
        let mut command = tokio::process::Command::new(r"C:\Windows\System32\schtasks.exe");
        command
            .args(["/Run", "/TN", "SPACE-Guest-Recovery"])
            .kill_on_drop(true)
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null());
        match tokio::time::timeout(Duration::from_secs(5), command.status()).await {
            Ok(Ok(status)) if status.success() => {}
            result => tracing::warn!(
                ?result,
                "immediate guest recovery request failed; periodic recovery owns intent"
            ),
        }
        self.inner.shutdown.cancel();
    }

    pub(super) async fn update_self_test(&self) -> Result<(), Status> {
        use pplx_space_spaced_guest_protocol::proto::bridge_guest::{
            StatRequest, filesystem_server::Filesystem,
        };
        let filesystem = self.inner.execution.filesystem.as_ref().ok_or_else(|| {
            Status::failed_precondition("candidate has no provisioned workload account")
        })?;
        let result = tokio::time::timeout(
            Duration::from_secs(10),
            filesystem.stat_info(Request::new(StatRequest {
                path: r"C:\".into(),
                user: None,
            })),
        )
        .await
        .map_err(|_| Status::deadline_exceeded("candidate self-test deadline"))??;
        if result.into_inner().info.is_none() {
            return Err(Status::internal(
                "candidate filesystem self-test omitted root",
            ));
        }
        Ok(())
    }
    pub(super) async fn platform_update_blockers(
        &self,
    ) -> Vec<pplx_space_spaced_guest_protocol::proto::update::DeferReason> {
        let mut reasons = self.inner.execution.update_defer_reasons().await;
        if !matches!(
            tokio::task::spawn_blocking(super::recovery::has_descendants).await,
            Ok(Ok(false))
        ) {
            reasons.push(pplx_space_spaced_guest_protocol::proto::update::DeferReason::Descendants);
        }
        reasons
    }
}

pub(super) fn parse_environment(
    values: std::collections::HashMap<String, String>,
) -> Result<pplx_space_guest_process::WindowsEnv, Status> {
    pplx_space_guest_process::WindowsEnv::parse(values)
        .map_err(|e| Status::invalid_argument(e.to_string()))
}
