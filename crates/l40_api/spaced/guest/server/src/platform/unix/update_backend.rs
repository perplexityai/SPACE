use super::*;
use pplx_space_spaced_guest_protocol::proto::update as wire;

impl SpacedGuestService {
    pub(super) async fn publish_platform_readiness(&self) -> Result<(), Status> {
        // systemd consumes the existing readiness notification after the gate opens.
        Ok(())
    }

    pub(super) async fn restart_after_update(&self) {
        self.inner.shutdown.cancel();
    }

    pub(super) async fn update_self_test(&self) -> Result<(), Status> {
        // No listeners exist yet. Exercise the real filesystem worker before
        // closing admission; this read creates no customer retry obligation.
        use pplx_space_spaced_common::proto::bridge_guest::filesystem_server::Filesystem;
        let response = tokio::time::timeout(
            Duration::from_secs(10),
            self.inner
                .execution
                .filesystem_service()
                .stat_info(Request::new(
                    pplx_space_spaced_common::proto::bridge_guest::StatRequest {
                        path: "/".into(),
                        user: None,
                    },
                )),
        )
        .await
        .map_err(|_| Status::deadline_exceeded("candidate filesystem self-test timed out"))??;
        if response.into_inner().info.is_none() {
            return Err(Status::internal(
                "candidate filesystem self-test omitted root",
            ));
        }
        Ok(())
    }
    pub(super) async fn platform_update_blockers(&self) -> Vec<wire::DeferReason> {
        let reasons = self.inner.execution.update_defer_reasons().await;
        #[cfg(target_os = "linux")]
        let mut reasons = reasons;
        // The leaf excludes the holder; populated includes nested descendants.
        // An unreadable cgroup cannot establish idleness, in status or Arm.
        #[cfg(target_os = "linux")]
        match crate::platform::unix::cgroup::GuestCgroups::initialize_for_process() {
            Some(cgroups) => {
                match tokio::fs::read_to_string(cgroups.workloads.join("cgroup.events")).await {
                    Ok(events) if events.lines().any(|v| v == "populated 0") => {}
                    _ => reasons.push(wire::DeferReason::Descendants),
                }
            }
            None => reasons.push(wire::DeferReason::Descendants),
        }
        reasons
    }
}

pub(super) fn parse_environment(
    values: std::collections::HashMap<String, String>,
) -> Result<pplx_space_common_core::types::EnvMap, Status> {
    Ok(parse_env(values))
}
