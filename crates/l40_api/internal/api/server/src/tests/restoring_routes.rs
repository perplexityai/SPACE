//! No data-plane route resolves to a sandbox that is `restoring`, on any
//! transport: the resolver waits on the restore through the activation owner
//! and resolves once the sandbox is `started`, and otherwise refuses with a
//! retryable `Unavailable`, never the backend endpoint of the restoring pod.

use super::*;

/// An activation owner whose restore completes while it is waited on.
struct CompletingActivation {
    pool: sqlx::PgPool,
    calls: std::sync::atomic::AtomicUsize,
}

#[tonic::async_trait]
impl crate::ingress_activation::IngressActivation for CompletingActivation {
    async fn prepare(&self, sandbox_id: SandboxId, _: std::time::Duration) -> Result<(), Status> {
        self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        sqlx::query("UPDATE sandboxes SET lifecycle = 'started' WHERE id = $1")
            .bind(sandbox_id)
            .execute(&self.pool)
            .await
            .map_err(|error| Status::internal(error.to_string()))?;
        Ok(())
    }
}

#[derive(Debug, Clone, Copy)]
enum Owner {
    /// No activation owner is configured.
    Absent,
    /// The owner can't finish the restore.
    Unavailable,
    /// The owner finishes the restore.
    Completes,
}

fn restoring_route_request(
    sandbox_id: SandboxId,
    api_key_hash: String,
) -> Request<UnstableResolveSandboxServiceRouteRequest> {
    Request::new(UnstableResolveSandboxServiceRouteRequest {
        sandbox_id: sandbox_id.to_string(),
        port: 49_983,
        route_kind: SandboxServiceRouteKind::Agent as i32,
        transport: SandboxServiceRouteTransport::Direct as i32,
        credential_proofs: vec![route_credential_proof(
            SandboxServiceRouteCredentialKind::OrgApiKey,
            api_key_hash,
        )],
    })
}

/// A direct (ECW envd) route to a restoring sandbox resolves only once its
/// restore completes, and is refused retryably when no owner can finish it.
#[tokio::test(flavor = "current_thread")]
async fn direct_route_to_a_restoring_sandbox_waits_for_its_restore() -> TestResult {
    if !pplx_space_test_env::enter_test_process(&[
        (
            "SPACE_INTERNAL_RPC_INSECURE_DEV",
            Some(std::ffi::OsStr::new("true")),
        ),
        (
            "SPACE_INTERNAL_TOKEN",
            Some(std::ffi::OsStr::new("restoring-route-test-secret")),
        ),
    ]) {
        return Ok(());
    }

    for owner in [Owner::Absent, Owner::Unavailable, Owner::Completes] {
        let Some(mut fixture) = cluster_fixture().await? else {
            return Ok(());
        };
        let pool = fixture.service.pool.clone();
        let completing = std::sync::Arc::new(CompletingActivation {
            pool: pool.clone(),
            calls: std::sync::atomic::AtomicUsize::new(0),
        });
        let unavailable = std::sync::Arc::new(UnavailableIngressActivation(
            std::sync::atomic::AtomicUsize::new(0),
        ));
        fixture.service.ingress_activation = match owner {
            Owner::Absent => None,
            Owner::Unavailable => Some(unavailable.clone()),
            Owner::Completes => Some(completing.clone()),
        };
        let node_id = NodeId::new();
        let sandbox_id = SandboxId::new();
        let api_key_hash = sha256_hex("restoring-route-api-key");
        insert_data_plane_worker_node(&pool, node_id, "10.0.0.43").await?;
        let node_api = sim_worker_node_api(node_id).await?;
        insert_data_plane_direct_node_endpoint(&pool, node_id, node_api.endpoint()).await?;
        insert_data_plane_active_sandbox(&pool, sandbox_id, node_id).await?;
        insert_active_execution(&pool, sandbox_id, "10.90.0.43", 1).await?;
        insert_e2b_api_key(&pool, api_key_hash.as_str()).await?;
        sqlx::query("UPDATE sandboxes SET lifecycle = 'restoring' WHERE id = $1")
            .bind(sandbox_id)
            .execute(&pool)
            .await?;

        let resolved = fixture
            .service
            .unstable_resolve_sandbox_service_route(restoring_route_request(
                sandbox_id,
                api_key_hash,
            ))
            .await;

        match owner {
            Owner::Absent | Owner::Unavailable => {
                let error = resolved.expect_err("a restoring sandbox must not be routed to");
                assert_eq!(error.code(), Code::Unavailable, "{owner:?}: {error:?}");
                assert!(
                    error.message().contains("restoring"),
                    "{owner:?}: the refusal must be the restoring one, not the owner's: {error:?}"
                );
                assert_eq!(
                    unavailable.0.load(std::sync::atomic::Ordering::SeqCst),
                    usize::from(matches!(owner, Owner::Unavailable)),
                    "{owner:?}: a configured owner is asked to finish the restore first"
                );
            }
            Owner::Completes => {
                let route = resolved?.into_inner();
                assert_eq!(route.lifecycle, "started");
                assert_eq!(
                    completing.calls.load(std::sync::atomic::Ordering::SeqCst),
                    1
                );
            }
        }
    }
    Ok(())
}

/// A terminal session route to a restoring sandbox, which no ingress check
/// covers, resolves only once its restore completes, and is refused
/// retryably when no owner can finish it.
#[tokio::test(flavor = "current_thread")]
async fn session_route_to_a_restoring_sandbox_waits_for_its_restore() -> TestResult {
    for owner in [Owner::Absent, Owner::Unavailable, Owner::Completes] {
        let Some(mut fixture) = cluster_fixture().await? else {
            return Ok(());
        };
        let pool = fixture.service.pool.clone();
        let completing = std::sync::Arc::new(CompletingActivation {
            pool: pool.clone(),
            calls: std::sync::atomic::AtomicUsize::new(0),
        });
        let unavailable = std::sync::Arc::new(UnavailableIngressActivation(
            std::sync::atomic::AtomicUsize::new(0),
        ));
        fixture.service.ingress_activation = match owner {
            Owner::Absent => None,
            Owner::Unavailable => Some(unavailable.clone()),
            Owner::Completes => Some(completing.clone()),
        };
        let node_id = NodeId::new();
        let sandbox_id = SandboxId::new();
        let session_id = format!("term_{}", Uuid::now_v7());
        insert_data_plane_worker_node(&pool, node_id, "10.0.0.44").await?;
        insert_data_plane_active_sandbox(&pool, sandbox_id, node_id).await?;
        insert_active_execution(&pool, sandbox_id, "10.80.0.44", 44).await?;
        insert_data_plane_session(&pool, &session_id, sandbox_id).await?;
        sqlx::query("UPDATE sandboxes SET lifecycle = 'restoring' WHERE id = $1")
            .bind(sandbox_id)
            .execute(&pool)
            .await?;

        let resolved = fixture
            .service
            .resolve_data_plane_session_route(data_plane_request(
                ResolveDataPlaneSessionRouteRequest {
                    session_id: session_id.clone(),
                    protocol_session_id: None,
                    token_sha256: None,
                    required_scope: None,
                    requested_transport: None,
                },
            ))
            .await;

        match owner {
            Owner::Completes => {
                assert_eq!(resolved?.into_inner().session_id, session_id);
                assert_eq!(
                    completing.calls.load(std::sync::atomic::Ordering::SeqCst),
                    1
                );
            }
            Owner::Absent | Owner::Unavailable => {
                let error = resolved.expect_err("a restoring sandbox must not be routed to");
                assert_eq!(error.code(), Code::Unavailable, "{owner:?}: {error:?}");
                assert!(
                    error.message().contains("restoring"),
                    "{owner:?}: {error:?}"
                );
                assert_eq!(
                    unavailable.0.load(std::sync::atomic::Ordering::SeqCst),
                    usize::from(matches!(owner, Owner::Unavailable)),
                    "{owner:?}: a configured owner is asked to finish the restore first"
                );
            }
        }
    }
    Ok(())
}
