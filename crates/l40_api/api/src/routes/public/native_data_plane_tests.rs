//! Only Native data-plane operations mark their requests for worker
//! failover; lifecycle, policy and control-plane operations do not.

use axum::{Router, body::Body, http::Request, middleware, routing::get};
use pplx_space_public_api::routes as native;
use tower::ServiceExt as _;

use super::{fails_over_from_silent_pods, mounted_operations, native_data_plane};
use crate::controllers::substrate::native_access;

#[test]
fn data_plane_operations_fail_over_and_lifecycle_ones_do_not() {
    for operation in [
        native::WRITE_FILE,
        native::WRITE_FILE_BATCH,
        native::CREATE_DIRECTORY,
        native::READ_FILE,
        native::STAT_FILE,
        native::CREATE_COMMAND,
        native::CREATE_PROCESS_SESSION,
        native::KILL_PROCESS,
        native::GET_SANDBOX,
    ] {
        assert!(fails_over_from_silent_pods(operation), "{operation:?}");
    }
    for operation in [
        native::CREATE_SANDBOX,
        native::UPDATE_SANDBOX,
        native::DELETE_SANDBOX,
        native::PAUSE_SANDBOX,
        native::RESUME_SANDBOX,
        native::WAKE_SANDBOX,
        native::SET_SANDBOX_TIMEOUT,
        native::GET_SANDBOX_NETWORK_POLICY,
        native::CHECKPOINT_SANDBOX,
        native::LIST_SNAPSHOTS,
        native::GET_OPERATION,
    ] {
        assert!(!fails_over_from_silent_pods(operation), "{operation:?}");
    }
}

#[test]
fn every_failover_operation_is_a_mounted_one() {
    let mounted: Vec<_> = mounted_operations()
        .into_iter()
        .map(|(operation, _)| operation)
        .collect();
    let failing_over = mounted
        .iter()
        .filter(|operation| fails_over_from_silent_pods(**operation))
        .count();
    assert!(
        failing_over >= 20,
        "the allowlist matches mounted operations"
    );
}

#[tokio::test]
async fn the_layer_marks_only_the_requests_it_wraps() {
    async fn marked() -> &'static str {
        if native_access::active() {
            "native"
        } else {
            "default"
        }
    }
    let app = Router::new()
        .route(
            "/data-plane",
            get(marked).layer(middleware::from_fn(native_data_plane)),
        )
        .route("/other", get(marked));
    for (uri, expected) in [("/data-plane", "native"), ("/other", "default")] {
        let response = app
            .clone()
            .oneshot(Request::get(uri).body(Body::empty()).unwrap())
            .await
            .unwrap();
        let body = axum::body::to_bytes(response.into_body(), 64)
            .await
            .unwrap();
        assert_eq!(body, expected.as_bytes(), "{uri}");
    }
}
