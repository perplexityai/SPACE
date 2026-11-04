use super::*;

#[test]
fn template_policy_dependency_survives_forwarded_grpc_trailers() {
    for code in [
        tonic::Code::Unavailable,
        tonic::Code::DeadlineExceeded,
        tonic::Code::Internal,
        tonic::Code::FailedPrecondition,
    ] {
        let original = tonic::Status::new(code, "policy owner unavailable");
        assert!(!is_template_policy_dependency(&original));
        let marked = template_policy_dependency_status(original);
        let mut trailers = tonic::codegen::http::HeaderMap::new();
        marked.add_header(&mut trailers).unwrap();
        let decoded = tonic::Status::from_header_map(&trailers).unwrap();
        assert!(is_template_policy_dependency(&decoded));
        assert_eq!(decoded.code(), code);
        assert_eq!(decoded.message(), "policy owner unavailable");
    }
}

#[test]
fn restart_retry_requires_explicit_successor_metadata() {
    let unavailable = tonic::Status::unavailable("transport unavailable");
    assert!(!is_restart_retry_on_successor(&unavailable));

    let restart = restart_retry_on_successor_status("restart drain");
    assert!(is_restart_retry_on_successor(&restart));

    let mut internal = tonic::Status::internal("restart drain");
    internal.metadata_mut().insert(
        RESTART_RETRY_ON_SUCCESSOR_METADATA,
        tonic::metadata::MetadataValue::from_static("true"),
    );
    assert!(!is_restart_retry_on_successor(&internal));
}

#[test]
fn restart_retry_status_round_trips_through_grpc_trailers() {
    let status = restart_retry_on_successor_status("sandbox-manager handoff");
    let mut trailers = tonic::codegen::http::HeaderMap::new();
    status.add_header(&mut trailers).unwrap();

    let decoded = tonic::Status::from_header_map(&trailers).unwrap();
    assert_eq!(decoded.code(), tonic::Code::Unavailable);
    assert_eq!(decoded.message(), "sandbox-manager handoff");
    assert!(is_restart_retry_on_successor(&decoded));
}

#[test]
fn authoritative_route_identity_requirement_round_trips_through_grpc_trailers() {
    let status = authoritative_route_identity_required_status("identity required");
    let mut trailers = tonic::codegen::http::HeaderMap::new();
    status.add_header(&mut trailers).unwrap();

    let decoded = tonic::Status::from_header_map(&trailers).unwrap();
    assert!(is_authoritative_route_identity_required(&decoded));
    assert!(!is_authoritative_route_identity_required(
        &tonic::Status::failed_precondition("identity required")
    ));
}

#[test]
fn suspend_publication_pending_requires_explicit_reason_metadata() {
    assert!(!is_suspend_publication_pending_status(
        &tonic::Status::aborted("concurrent cleanup refusal",)
    ));

    let pending = suspend_publication_pending_status("suspend publication pending");
    assert!(is_suspend_publication_pending_status(&pending));

    let wrong_code = tonic::Status::internal(pending.message());
    assert!(!is_suspend_publication_pending_status(&wrong_code));
}

#[test]
fn suspend_publication_pending_reason_round_trips_through_grpc_trailers() {
    let status = suspend_publication_pending_status("suspend publication pending");
    let mut trailers = tonic::codegen::http::HeaderMap::new();
    status.add_header(&mut trailers).unwrap();

    let decoded = tonic::Status::from_header_map(&trailers).unwrap();
    assert!(is_suspend_publication_pending_status(&decoded));
}

#[test]
fn retained_filesystem_only_copy_is_terminal_and_round_trips() {
    let status = retained_filesystem_only_copy_status("retained filesystem is the only copy");
    assert!(is_retained_filesystem_only_copy_status(&status));
    assert!(!is_suspend_publication_pending_status(&status));

    let mut trailers = tonic::codegen::http::HeaderMap::new();
    status.add_header(&mut trailers).unwrap();
    let decoded = tonic::Status::from_header_map(&trailers).unwrap();
    assert!(is_retained_filesystem_only_copy_status(&decoded));
    assert!(!is_suspend_publication_pending_status(&decoded));
}
