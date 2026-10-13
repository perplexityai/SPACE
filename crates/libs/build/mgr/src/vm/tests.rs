use super::*;
use pplx_space_node_mgr_client::proto::InspectSandboxResponse;
use std::time::Duration;

#[tokio::test(start_paused = true)]
async fn graceful_exit_requires_host_evidence_for_the_original_vm() {
    let mut observations = Vec::from([
        Err(tonic::Status::unavailable(
            "host observation temporarily unavailable",
        )),
        Ok(InspectSandboxResponse::default()),
        Ok(InspectSandboxResponse {
            fc_pid: Some(42),
            fc_alive: true,
            ..Default::default()
        }),
        Ok(InspectSandboxResponse {
            fc_pid: Some(42),
            fc_alive: false,
            ..Default::default()
        }),
    ])
    .into_iter();
    let started = tokio::time::Instant::now();
    wait_for_exit_with(42, || std::future::ready(observations.next().unwrap()))
        .await
        .unwrap();
    assert_eq!(started.elapsed(), Duration::from_millis(1500));
    let error = wait_for_exit_with(42, || {
        std::future::ready(Ok(InspectSandboxResponse {
            fc_pid: Some(43),
            fc_alive: false,
            ..Default::default()
        }))
    })
    .await
    .unwrap_err();
    assert_eq!(error.code(), tonic::Code::Aborted);
    tokio::time::timeout(
        Duration::from_secs(30),
        wait_for_exit_with(42, || {
            std::future::ready(Ok(InspectSandboxResponse {
                fc_pid: Some(42),
                fc_alive: true,
                ..Default::default()
            }))
        }),
    )
    .await
    .unwrap_err();
}
