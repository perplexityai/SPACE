//! A data-plane session route to a sandbox that still has a node but is at
//! rest is held while a start, resume, restore or wake is about to bring it
//! back: the node would refuse the attach as final first.

use super::*;

async fn resolve(fixture: &ClusterFixture, session_id: &str) -> Result<(), Status> {
    fixture
        .service
        .resolve_data_plane_session_route(data_plane_request(ResolveDataPlaneSessionRouteRequest {
            session_id: session_id.to_owned(),
            protocol_session_id: None,
            token_sha256: None,
            required_scope: None,
            requested_transport: None,
        }))
        .await
        .map(|_| ())
}

#[tokio::test(flavor = "current_thread")]
async fn a_placed_route_waits_for_an_activation_queued_for_its_sandbox() -> TestResult {
    let Some(fixture) = cluster_fixture().await? else {
        return Ok(());
    };
    let pool = fixture.service.pool.clone();
    let node_id = NodeId::new();
    insert_data_plane_worker_node(&pool, node_id, "10.0.0.48").await?;
    for (lifecycle, operation, held) in [
        // A stopped sandbox keeps its node; a client that follows the
        // `:start` hint and attaches before the start is claimed waits.
        ("stopped", Some(("start_sandbox", "queued")), true),
        ("stopped", Some(("start_sandbox", "running")), true),
        ("paused", Some(("resume_sandbox", "queued")), true),
        ("paused", Some(("wake_sandbox", "queued")), true),
        // Nothing coming: routed, for the node to answer.
        ("stopped", None, false),
        ("stopped", Some(("start_sandbox", "succeeded")), false),
        ("paused", Some(("suspend_sandbox", "queued")), false),
        // Running: routed whatever is queued.
        ("started", Some(("wake_sandbox", "queued")), false),
    ] {
        let sandbox_id = SandboxId::new();
        let session_id = format!("term_{}", Uuid::now_v7());
        insert_data_plane_active_sandbox(&pool, sandbox_id, node_id).await?;
        insert_active_execution(&pool, sandbox_id, "10.80.0.48", 2).await?;
        insert_data_plane_session(&pool, &session_id, sandbox_id).await?;
        sqlx::query("UPDATE sandboxes SET lifecycle = $2 WHERE id = $1")
            .bind(sandbox_id)
            .bind(lifecycle)
            .execute(&pool)
            .await?;
        if let Some((kind, state)) = operation {
            sqlx::query(
                "INSERT INTO public_operations (operation_id, org_id, kind, target_type, target_id, state)
                 VALUES ($1, '00000000-0000-0000-0000-000000000001', $2, 'sandbox', $3, $4)",
            )
            .bind(Uuid::now_v7())
            .bind(kind)
            .bind(pplx_space_public_api::PublicSandboxId::from(sandbox_id).to_string())
            .bind(state)
            .execute(&pool)
            .await?;
        }

        let resolved = resolve(&fixture, &session_id).await;
        let case = format!("{lifecycle} with {operation:?}");
        if held {
            let error = resolved.expect_err(&case);
            assert_eq!(error.code(), Code::FailedPrecondition, "{case}: {error:?}");
            // Retryable, not the at-rest mark: the sandbox is on its way.
            assert_eq!(
                pplx_space_common_grpc::session_route_at_rest(&error),
                None,
                "{case}"
            );
        } else {
            resolved.map_err(|error| format!("{case}: {error:?}"))?;
        }
    }
    Ok(())
}
