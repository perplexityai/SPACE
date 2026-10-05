use super::*;
use tonic::codegen::{Service, http};

/// Send `requests` concurrently while every slot is held for 500 ms, then
/// return their gRPC statuses once the slots are released.
async fn statuses_of_a_queued_burst<S>(
    grpc: S,
    admission: pplx_space_common_grpc::QueuedSlots,
    permits: u32,
    path: String,
) -> Vec<Option<String>>
where
    S: Service<http::Request<tonic::body::Body>, Response = http::Response<tonic::body::Body>>
        + Clone
        + Send
        + 'static,
    S::Future: Send,
    S::Error: std::fmt::Debug,
{
    let request = || {
        http::Request::builder()
            .uri(&path)
            .header("content-type", "application/grpc")
            .body(tonic::body::Body::empty())
            .unwrap()
    };
    let occupied = admission.acquire(permits).await.unwrap();
    let mut calls = Vec::new();
    for _ in 0..30 {
        let mut grpc = grpc.clone();
        let request = request();
        calls.push(tokio::spawn(
            async move { grpc.call(request).await.unwrap() },
        ));
    }
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    drop(occupied);
    let mut statuses = Vec::new();
    for call in calls {
        let response = call.await.unwrap();
        statuses.push(
            response
                .headers()
                .get("grpc-status")
                .map(|value| value.to_str().unwrap().to_string()),
        );
    }
    statuses
}

// A burst that finds every payload slot busy is queued and, once slots free,
// answered exactly as an uncontended request is, for both payload services.
#[tokio::test(start_paused = true)]
async fn payload_bursts_queue_until_slots_free() {
    let service = SpacedHostService::new(CancellationToken::new());
    let process_path = format!(
        "/{}/Start",
        <proto::process_server::ProcessServer<SpacedHostService> as tonic::server::NamedService>::NAME
    );
    let filesystem_path = format!(
        "/{}/Write",
        <FilesystemServer<SpacedHostService> as tonic::server::NamedService>::NAME
    );
    let uncontended = |path: &str| {
        http::Request::builder()
            .uri(path)
            .header("content-type", "application/grpc")
            .body(tonic::body::Body::empty())
            .unwrap()
    };
    let expected_process = service
        .process_grpc_service()
        .call(uncontended(&process_path))
        .await
        .unwrap()
        .headers()
        .get("grpc-status")
        .map(|value| value.to_str().unwrap().to_string());
    let expected_filesystem = service
        .filesystem_grpc_service()
        .call(uncontended(&filesystem_path))
        .await
        .unwrap()
        .headers()
        .get("grpc-status")
        .map(|value| value.to_str().unwrap().to_string());
    assert_ne!(expected_process.as_deref(), Some("8"));
    assert_ne!(expected_filesystem.as_deref(), Some("8"));

    let process = statuses_of_a_queued_burst(
        service.process_grpc_service(),
        service.inner.process_admission.clone(),
        pplx_space_spaced_common::process_limits::PAYLOAD_SLOTS as u32,
        process_path,
    )
    .await;
    assert!(
        process.iter().all(|status| *status == expected_process),
        "{process:?}"
    );
    let filesystem = statuses_of_a_queued_burst(
        service.filesystem_grpc_service(),
        service.inner.filesystem_admission.clone(),
        pplx_space_spaced_common::filesystem_limits::PAYLOAD_SLOTS as u32,
        filesystem_path,
    )
    .await;
    assert!(
        filesystem
            .iter()
            .all(|status| *status == expected_filesystem),
        "{filesystem:?}"
    );
}
