use super::*;
use crate::node_manager_transport::{
    ConnectError, NodeManagerConnector, NodeManagerGrpcClient, NodeManagerTransport, fake,
};
use crate::service::tests::service_tests::{
    insert_platform_owned_sandbox, route_authorized_service, runtime_fields,
};
use pplx_space_common_svc::space_api_route_ed25519::insert_space_api_route_ed25519_token;
use pplx_space_spaced_common::recovery::RecoveryFence;
use std::sync::Arc;
use tokio::sync::Notify;

struct BlockedConnect {
    inner: NodeManagerConnector,
    entered: Notify,
    release: Notify,
}

#[async_trait::async_trait]
impl NodeManagerTransport for BlockedConnect {
    async fn connect(&self) -> Result<NodeManagerGrpcClient, ConnectError> {
        self.entered.notify_one();
        self.release.notified().await;
        self.inner.connect().await
    }

    async fn connect_once(&self) -> Result<NodeManagerGrpcClient, ConnectError> {
        self.inner.connect_once().await
    }

    fn lazy(&self) -> NodeManagerGrpcClient {
        self.inner.lazy()
    }
}

fn resident_service() -> (
    SandboxManagerService,
    String,
    SpaceApiBridgeRouteAuthorization,
    RecoveryFence,
) {
    let conn = crate::db::test_conn();
    let id = SandboxId::new();
    // A pool member skips network-policy installation; this test exercises
    // the resident resume and its database finalization, not the gateway.
    insert_platform_owned_sandbox(&conn, &id.to_string(), SandboxState::Started, Some("seed"));
    crate::db::update_runtime_fields(
        &conn,
        id,
        runtime_fields(ExecutionGeneration::new(1).unwrap(), 7),
    )
    .unwrap();
    let mut policy = AutoLifecyclePolicy::disabled();
    policy
        .wake
        .on
        .push(pplx_space_common_core::types::AutoLifecycleWakeSource::Access);
    crate::db::update_auto_lifecycle(
        &conn,
        &id.to_string(),
        &policy,
        crate::db::PolicyAuthority::Cluster,
    )
    .unwrap();
    let (service, token) = route_authorized_service(conn, id);
    let mut request = tonic::Request::new(());
    insert_space_api_route_ed25519_token(request.metadata_mut(), &token).unwrap();
    let authorization = service
        .prepare_space_api_bridge(&request, &id.to_string(), SpacedRpc::ConnectProcess)
        .unwrap();
    let fence = RecoveryFence {
        instance: ProcessRecoveryToken::new(),
        activation: service
            .lookup_sandbox(&id.to_string())
            .unwrap()
            .node_execution_id
            .unwrap()
            .to_string()
            .parse()
            .unwrap(),
    };
    (service, id.to_string(), authorization, fence)
}

#[tokio::test]
async fn admitted_recovery_resume_finishes_after_deadline_or_caller_drop() {
    if !crate::service::tests::enter_route_test_process(&[]) {
        return;
    }
    for expire in [true, false] {
        let (mut service, id, authorization, fence) = resident_service();
        crate::db::test_fixtures::state_write_fixtures::force_state(
            &service.db_conn().unwrap(),
            &id,
            SandboxState::Paused,
        );
        let blocked = Arc::new(BlockedConnect {
            inner: fake::unbound_in_absent_dir(),
            entered: Notify::new(),
            release: Notify::new(),
        });
        service.node_manager = NodeManagerConnector::new(blocked.clone());
        let budget = pplx_space_common_grpc::GrpcRetryContext::with_timeout(Duration::from_secs(1));
        let mut waking = Box::pin(service.ensure_recovery_active(
            &id,
            &authorization,
            fence,
            &budget,
            GuestActivityKind::Access,
        ));
        tokio::time::timeout(Duration::from_secs(5), async {
            tokio::select! {
                _ = blocked.entered.notified() => {},
                result = &mut waking => panic!("wake must reach node connection: {:?}", result.err()),
            }
        }).await.unwrap();
        assert_eq!(
            service.lookup_sandbox(&id).unwrap().state,
            SandboxState::Resuming
        );
        if expire {
            let status = waking.await.err().unwrap();
            assert_eq!(
                RecoveryReason::from_status(&status),
                Some(RecoveryReason::TemporarilyUnavailable)
            );
        } else {
            drop(waking);
        }
        // A late node connection failure must still restore Paused. Dropping
        // the wake at the caller's deadline would strand the row in Resuming.
        assert_eq!(
            service.active_state_transition_count(),
            1,
            "the admitted wake still owns its transition"
        );
        blocked.release.notify_one();
        tokio::time::timeout(Duration::from_secs(5), async {
            while service.active_state_transition_count() != 0 {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .unwrap();
        assert_eq!(
            service.lookup_sandbox(&id).unwrap().state,
            SandboxState::Paused
        );
        service.shutdown_token.cancel();
    }
}

#[tokio::test]
async fn draining_recovery_access_preserves_residency_and_route_fencing() {
    if !crate::service::tests::enter_route_test_process(&[]) {
        return;
    }
    for (state, expected) in [
        (SandboxState::Started, None),
        (
            SandboxState::Paused,
            Some(RecoveryReason::TemporarilyUnavailable),
        ),
        (
            SandboxState::Suspended,
            Some(RecoveryReason::ActivationChanged),
        ),
        (
            SandboxState::Stopped,
            Some(RecoveryReason::ActivationChanged),
        ),
    ] {
        for stale_route in [false, true] {
            let (service, id, authorization, fence) = resident_service();
            if stale_route {
                advance_route(&service, &id);
            }
            crate::db::test_fixtures::state_write_fixtures::force_state(
                &service.db_conn().unwrap(),
                &id,
                state,
            );
            service.begin_draining_for_restart();
            let budget =
                pplx_space_common_grpc::GrpcRetryContext::with_timeout(Duration::from_secs(1));
            let result = service
                .ensure_recovery_active(
                    &id,
                    &authorization,
                    fence,
                    &budget,
                    GuestActivityKind::Access,
                )
                .await;
            if stale_route {
                assert_eq!(result.err().unwrap().code(), tonic::Code::PermissionDenied);
            } else {
                match expected {
                    None => assert_eq!(result.unwrap().state, SandboxState::Started),
                    Some(reason) => assert_eq!(
                        RecoveryReason::from_status(&result.err().unwrap()),
                        Some(reason)
                    ),
                }
            }
            assert_eq!(service.lookup_sandbox(&id).unwrap().state, state);
            service.shutdown_token.cancel();
        }
    }
}

fn advance_route(service: &SandboxManagerService, id: &str) {
    let mut identity = service.lookup_sandbox(id).unwrap().space_api_route_identity;
    identity.execution_generation = Some(ExecutionGeneration::new(2).unwrap());
    crate::db::update_space_api_route_identity(
        &service.db_conn().unwrap(),
        id.parse().unwrap(),
        &identity,
        crate::auto_lifecycle_delete_receipts::RouteIdentityProvenance::ClusterFenced,
    )
    .unwrap();
}

#[tokio::test]
async fn retired_recovery_routes_preserve_terminal_refusals_and_authorization() {
    if !crate::service::tests::enter_route_test_process(&[]) {
        return;
    }
    for stale_route in [false, true] {
        let (service, id, authorization, fence) = resident_service();
        if stale_route {
            advance_route(&service, &id);
        }
        {
            let db = service.db_conn().unwrap();
            crate::db::test_fixtures::state_write_fixtures::force_state(
                &db,
                &id,
                SandboxState::Paused,
            );
            crate::db::begin_suspend(&db, &id).unwrap();
            crate::db::complete_row_intent_cleanup(
                &db,
                &id,
                crate::service::cleanup_support::CleanupCompletion::MarkSuspended,
                Some((
                    pplx_space_common_core::types::LocalArtifactId::new(),
                    "/cache/test/snapshot",
                )),
            )
            .unwrap();
        }
        let precheck = service
            .require_resident_recovery(&id, &authorization, Some(fence))
            .unwrap_err();
        let wake = service
            .ensure_active_for_guest_activity_with_start(
                Some(&authorization),
                &id,
                true,
                Some(Duration::from_secs(1)),
                GuestActivityKind::Access,
                WakeStartConfig {
                    recovery_fence: Some(fence),
                    ..Default::default()
                },
            )
            .await
            .err()
            .unwrap();
        let rearm = service
            .recovery_activation_scope(&id, Some(&authorization))
            .await
            .err()
            .unwrap();
        for error in [precheck, wake, rearm] {
            if stale_route {
                assert_eq!(error.code(), tonic::Code::PermissionDenied);
            } else {
                assert_eq!(
                    RecoveryReason::from_status(&error),
                    Some(RecoveryReason::ActivationChanged)
                );
            }
        }
        assert_eq!(
            service.lookup_sandbox(&id).unwrap().state,
            SandboxState::Suspended
        );
        service.shutdown_token.cancel();
    }
}
