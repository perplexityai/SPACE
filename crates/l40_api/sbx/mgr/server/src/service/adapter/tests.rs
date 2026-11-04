use super::super::tests::call_through_restart_admission;
use super::*;
use crate::service::activation_support::internal_request;

fn test_service() -> SandboxManagerService {
    let connection = crate::db::test_conn();
    SandboxManagerService::for_test(connection)
}

fn expect_authorization_error<T>(result: Result<T, Status>) -> Status {
    match result {
        Ok(_) => panic!("empty request should fail authorization"),
        Err(error) => error,
    }
}

#[tokio::test]
async fn restart_drain_does_not_intercept_read_only_rpcs() {
    let service = test_service();
    service.begin_draining_for_restart();

    let errors =
        [
            expect_authorization_error(
                <SandboxManagerService as SandboxManager>::list(
                    &service,
                    Request::new(ListRequest::default()),
                )
                .await,
            ),
            expect_authorization_error(
                <SandboxManagerService as SandboxManager>::spaced_stat(
                    &service,
                    Request::new(SpacedStatRequest::default()),
                )
                .await,
            ),
            expect_authorization_error(
                <SandboxManagerService as SandboxManager>::spaced_list(
                    &service,
                    Request::new(SpacedListRequest::default()),
                )
                .await,
            ),
            expect_authorization_error(
                call_through_restart_admission(
                    &service,
                    "/space.sandbox.manager.v1.SandboxManager/SpacedRead",
                    Request::new(SpacedReadRequest::default()),
                    |service, request| async move {
                        SandboxManager::spaced_read(&service, request).await
                    },
                )
                .await,
            ),
            expect_authorization_error(
                call_through_restart_admission(
                    &service,
                    "/space.sandbox.manager.v1.SandboxManager/SpacedWatch",
                    Request::new(SpacedWatchRequest::default()),
                    |service, request| async move {
                        SandboxManager::spaced_watch(&service, request).await
                    },
                )
                .await,
            ),
            expect_authorization_error(
                <SandboxManagerService as SandboxManager>::spaced_normalize_watch_root(
                    &service,
                    Request::new(SpacedNormalizeWatchRootRequest::default()),
                )
                .await,
            ),
            expect_authorization_error(
                <SandboxManagerService as SandboxManager>::spaced_get_watcher_events(
                    &service,
                    Request::new(SpacedGetWatcherEventsRequest::default()),
                )
                .await,
            ),
            expect_authorization_error(
                <SandboxManagerService as SandboxManager>::spaced_get_modified_files(
                    &service,
                    Request::new(SpacedGetModifiedFilesRequest::default()),
                )
                .await,
            ),
            expect_authorization_error(
                <SandboxManagerService as SandboxManager>::spaced_connect_process(
                    &service,
                    Request::new(SpacedConnectProcessRequest::default()),
                )
                .await,
            ),
            expect_authorization_error(
                <SandboxManagerService as SandboxManager>::spaced_list_processes(
                    &service,
                    Request::new(SpacedListProcessesRequest::default()),
                )
                .await,
            ),
        ];

    for error in errors {
        assert_ne!(error.code(), tonic::Code::Unavailable);
    }
}

#[tokio::test]
async fn restart_drain_allows_authorized_read_only_rpcs() {
    let service = test_service();
    service.begin_draining_for_restart();

    let listed = <SandboxManagerService as SandboxManager>::list(
        &service,
        internal_request(ListRequest::default()),
    )
    .await
    .expect("authorized list should remain available during drain")
    .into_inner();
    let missing_id = SandboxId::new().to_string();
    let state = <SandboxManagerService as SandboxManager>::state(
        &service,
        internal_request(StateRequest {
            sandbox_ids: vec![missing_id.clone()],
        }),
    )
    .await
    .expect("authorized state should remain available during drain")
    .into_inner();

    assert!(listed.sandboxes.is_empty());
    assert!(state.sandboxes.is_empty());
    assert_eq!(state.missing_ids, vec![missing_id]);
}

#[tokio::test]
async fn artifact_transfer_handlers_admit_only_local_operators() {
    use pplx_space_common_core::host::linux::identities::users;
    use pplx_space_common_svc::peer_auth::UdsPeerAccount;
    use std::{
        marker::PhantomData,
        sync::atomic::{AtomicBool, Ordering},
    };
    struct EmptyBody(Arc<AtomicBool>);
    impl http_body::Body for EmptyBody {
        type Data = bytes::Bytes;
        type Error = std::convert::Infallible;
        fn poll_frame(
            self: Pin<&mut Self>,
            _: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
            self.0.store(true, Ordering::SeqCst);
            std::task::Poll::Ready(None)
        }
    }
    struct NoMessages<T>(PhantomData<T>);
    impl<T> tonic::codec::Decoder for NoMessages<T> {
        type Item = T;
        type Error = Status;
        fn decode(&mut self, _: &mut tonic::codec::DecodeBuf<'_>) -> Result<Option<T>, Status> {
            panic!("empty stream has no message")
        }
    }
    fn request<T>(value: T, uid: u32) -> Request<T> {
        let mut request = Request::new(value);
        request
            .extensions_mut()
            .insert(PeerIdentity::new(uid, uid, None));
        request
    }
    fn stream<T: Send + 'static>(
        uid: u32,
        polled: Arc<AtomicBool>,
    ) -> Request<tonic::Streaming<T>> {
        request(
            tonic::Streaming::new_request(NoMessages(PhantomData), EmptyBody(polled), None, None),
            uid,
        )
    }
    let mut service = test_service();
    service.peer_authorizer = UdsPeerAuthorizer::new(
        "artifact-test",
        [
            UdsPeerAccount::with_linux_user(users::SPACE_NODEAPI),
            UdsPeerAccount::role_with_fallback_uid(
                "artifact-test-operator",
                LocalRole::Operator,
                16001,
            ),
            UdsPeerAccount::role_with_fallback_uid(
                "artifact-test-observer",
                LocalRole::Observer,
                16002,
            ),
        ],
    );
    for (uid, allowed) in [
        (0, true),
        (16001, true),
        (users::SPACE_NODEAPI.uid, false),
        (16002, false),
        (16003, false),
    ] {
        let sandbox_polled = Arc::new(AtomicBool::new(false));
        let snapshot_polled = Arc::new(AtomicBool::new(false));
        let codes = [
            expect_authorization_error(
                SandboxManager::export_sandbox(
                    &service,
                    request(ExportSandboxRequest::default(), uid),
                )
                .await,
            )
            .code(),
            expect_authorization_error(
                SandboxManager::export_snapshot(
                    &service,
                    request(ExportSnapshotRequest::default(), uid),
                )
                .await,
            )
            .code(),
            expect_authorization_error(
                SandboxManager::import_sandbox(&service, stream(uid, sandbox_polled.clone())).await,
            )
            .code(),
            expect_authorization_error(
                SandboxManager::import_snapshot(&service, stream(uid, snapshot_polled.clone()))
                    .await,
            )
            .code(),
        ];
        if allowed {
            for code in codes {
                assert!(
                    !matches!(
                        code,
                        tonic::Code::PermissionDenied | tonic::Code::Unauthenticated
                    ),
                    "uid {uid}"
                );
            }
            assert!(sandbox_polled.load(Ordering::SeqCst));
            assert!(snapshot_polled.load(Ordering::SeqCst));
        } else {
            assert_eq!(codes, [tonic::Code::PermissionDenied; 4], "uid {uid}");
            assert!(
                !sandbox_polled.load(Ordering::SeqCst),
                "denied import consumed input"
            );
            assert!(
                !snapshot_polled.load(Ordering::SeqCst),
                "denied import consumed input"
            );
        }
    }
}
