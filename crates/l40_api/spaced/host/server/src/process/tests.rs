use super::*;

// Paused time lets the bounded queue's wait elapse without real sleeping.
#[tokio::test(start_paused = true)]
async fn process_payload_overload_is_rejected_before_decode_or_bridge_lookup() {
    use tonic::codegen::{Service, http};
    let service = SpacedHostService::new(CancellationToken::new());
    let _occupied = service
        .inner
        .process_admission
        .acquire(pplx_space_spaced_common::process_limits::PAYLOAD_SLOTS as u32)
        .await
        .unwrap();
    let mut grpc = service.process_grpc_service();
    for (method, _) in pplx_space_spaced_common::process_limits::PAYLOAD_METHODS {
        let request = http::Request::builder()
            .uri(format!(
                "/{}/{method}",
                <ProcessServer<SpacedHostService> as tonic::server::NamedService>::NAME
            ))
            .header("content-type", "application/grpc")
            .body(tonic::body::Body::empty())
            .unwrap();
        let response = grpc.call(request).await.unwrap();
        assert_eq!(response.headers()["grpc-status"], "8");
    }
}
