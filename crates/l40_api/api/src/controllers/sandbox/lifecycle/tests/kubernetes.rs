//! The tests in the parent module whose path differs by substrate, run
//! against the Kubernetes substrate. Each test is the twin of the direct test
//! of the same name and keeps its assertions; its doc says whether the
//! outcome differs.

use crate::test_support::kubernetes::{
    RouteIdentity, insert_kubernetes_worker_node, insert_live_execution, kubernetes,
    substrate_over_pods,
};

use super::*;

async fn insert_stopped_sandbox(pool: &PgPool, sandbox_id: SandboxId) -> TestResult {
    sqlx::query(sql!(
        "INSERT INTO sandboxes (
                 id, user_id, org_id, managed_by, lifecycle, placement,
                 network_policy_generation
             ) VALUES (
                 $1,
                 '00000000-0000-0000-0000-000000000010',
                 $2,
                 'k8s', 'stopped', 'on_node', 1
             )"
    ))
    .bind(sandbox_id)
    .bind(SEED_ORG_ID)
    .execute(pool)
    .await?;
    Ok(())
}

/// Same outcome as on direct. The Kubernetes teardown before the finalize
/// finds no execution to retire, so the delete reaches the same finalize
/// retry loop, and its exhausted budget is a retryable contention 503.
#[tokio::test(flavor = "current_thread")]
async fn exhausting_the_finalize_retry_budget_stays_retryable() -> TestResult {
    use crate::controllers::substrate::kubernetes::tests::substrate_with_store;

    let db = pg_test_db_with_max_connections(3).await?;
    let sandbox_id = SandboxId::new();
    insert_stopped_sandbox(&db.pool, sandbox_id).await?;
    sqlx::query(sql!(
        "CREATE FUNCTION fail_finalize_with_contention_for_test()
         RETURNS trigger
         LANGUAGE plpgsql
         AS $$
         BEGIN
             RAISE EXCEPTION USING
                 ERRCODE = '55P03',
                 MESSAGE = 'injected sustained finalize contention';
         END;
         $$"
    ))
    .execute(&db.pool)
    .await?;
    sqlx::query(sql!(
        "CREATE TRIGGER fail_finalize_with_contention_for_test
         BEFORE DELETE ON sandboxes
         FOR EACH ROW
         EXECUTE FUNCTION fail_finalize_with_contention_for_test()"
    ))
    .execute(&db.pool)
    .await?;

    let sandbox = crate::controllers::sandbox::fetch_sandbox(&db.pool, sandbox_id)
        .await?
        .expect("sandbox row exists");
    let make_substrate = || kubernetes(substrate_with_store(db.pool.clone()));
    let admission = crate::cluster_admission::ClusterSandboxAdmission::with_config(
        make_substrate(),
        crate::cluster_admission::ClusterSandboxAdmissionConfig {
            enabled: false,
            refill_rps: 6.0,
            burst: 60.0,
            redis_url: None,
        },
        pplx_space_feature_flags::FeatureFlagClient::disabled(),
    );
    let controller = SandboxController::with_cluster_admission(make_substrate(), admission);
    let error = controller
        .delete_control_plane(&db.pool, sandbox_id, Some(&sandbox))
        .await
        .expect_err("every finalize attempt must fail");

    assert_eq!(
        error.status(),
        axum::http::StatusCode::SERVICE_UNAVAILABLE,
        "an exhausted retry budget is not a 500"
    );
    assert!(
        error.is_lock_contention(),
        "the exhausted error must keep its contention identity so the public \
         and ECW translators do not re-label it as a fleet condition"
    );
    Ok(())
}

/// Same outcome as on direct: the pre-check is shared and runs before the
/// substrate. Once the lock is released, the retry clears it and, unlike the
/// direct test, which accepts any error but contention, completes the
/// delete: the Kubernetes teardown finds no execution to retire, and the
/// finalize deletes the row.
#[tokio::test(flavor = "current_thread")]
async fn a_contended_delete_precheck_stays_retryable() -> TestResult {
    use crate::controllers::substrate::kubernetes::tests::substrate_with_store;

    let db = pg_test_db_with_max_connections(3).await?;
    let sandbox_id = SandboxId::new();
    insert_stopped_sandbox(&db.pool, sandbox_id).await?;
    let sandbox = crate::controllers::sandbox::fetch_sandbox(&db.pool, sandbox_id)
        .await?
        .expect("sandbox row exists");

    // The role-level `lock_timeout`, applied here per session so the
    // pre-check aborts instead of blocking until the lock is released.
    let bounded = sqlx::postgres::PgPoolOptions::new()
        .max_connections(2)
        .connect_with(
            db.database_url
                .parse::<sqlx::postgres::PgConnectOptions>()?
                .options([("lock_timeout", "250ms")]),
        )
        .await?;

    let mut blocker = db.pool.begin().await?;
    sqlx::query(sql!("LOCK TABLE snapshots IN ACCESS EXCLUSIVE MODE"))
        .execute(&mut *blocker)
        .await?;

    let controller = SandboxController::new(kubernetes(substrate_with_store(bounded.clone())));
    let error = controller
        .delete_control_plane(&bounded, sandbox_id, Some(&sandbox))
        .await
        .expect_err("the pre-check cannot read snapshots while DDL holds the table");

    blocker.rollback().await?;

    assert_eq!(
        error.status(),
        axum::http::StatusCode::SERVICE_UNAVAILABLE,
        "a pre-flight that wrote nothing is retryable, not an internal failure"
    );
    assert!(
        error.is_lock_contention(),
        "the error must carry the contention identity"
    );

    controller
        .delete_control_plane(&bounded, sandbox_id, Some(&sandbox))
        .await
        .map_err(|error| format!("the retry must clear the pre-check and delete: {error}"))?;
    assert!(
        crate::controllers::sandbox::fetch_sandbox(&db.pool, sandbox_id)
            .await?
            .is_none(),
        "the retry's teardown and finalize delete the row"
    );

    Ok(())
}

/// Same outcome as on direct, with the runtime client resolved from the
/// active execution's pod instead of the `node_direct` endpoint.
#[tokio::test(flavor = "current_thread")]
async fn resume_uses_resolved_runtime_without_async_observation() -> TestResult {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let node = insert_kubernetes_worker_node(&db.pool).await?;
    for (generation, supported) in [(Some(1), true), (None, true), (Some(1), false)] {
        let id = SandboxId::new();
        let builder = pplx_space_node_api_sim::SimWorkerHarness::builder(
            crate::test_support::kubernetes::WORKER_NODE,
        )
        .with_internal_api();
        let worker = if supported {
            builder
        } else {
            builder.without_activation_policy_support()
        }
        .start()
        .await?;
        sqlx::query(sql!("INSERT INTO sandboxes (id, user_id, org_id, managed_by, lifecycle, placement, active_node_id, template_id, content_hash, network_policy_generation) VALUES ($1, '00000000-0000-0000-0000-000000000010', '00000000-0000-0000-0000-000000000001', 'k8s', 'paused', 'on_node', $2, 'seed', 'hash-seed', 1)"))
        .bind(id).bind(node).execute(&db.pool).await?;
        let pod = insert_live_execution(&db.pool, id, RouteIdentity::Published)
            .await?
            .pod;
        let mut record = pplx_space_node_api_sim::SandboxRecord::new_creating(
            id.to_string(),
            "seed".into(),
            "fc".into(),
            "test".into(),
            None,
            false,
        );
        record.status = "paused".into();
        record.execution_generation = generation;
        worker.state().insert_new(record).await?;
        let (substrate, _) = substrate_over_pods(&db.pool, worker.port(), vec![pod])?;
        let controller = SandboxController::new(kubernetes(substrate));
        let mut client = controller
            .substrate
            .runtime_client(id, crate::controllers::substrate::RecoveryCharge::Exempt)
            .await?;
        let request = controller
            .resume_request_with_current_policy(
                &db.pool,
                id,
                &mut client,
                crate::controllers::substrate::ActivationOptions {
                    blocking: true,
                    ready_timeout: std::time::Duration::from_secs(5),
                },
            )
            .await;
        if !supported {
            assert_eq!(
                request.unwrap_err().status(),
                axum::http::StatusCode::SERVICE_UNAVAILABLE
            );
            assert_eq!(worker.state().get(&id.to_string()).await?.status, "paused");
            continue;
        }
        let request = request?;
        assert_eq!(request.expected_execution_generation, generation);
        client.resume_sandbox(request).await?;
        assert_eq!(worker.state().get(&id.to_string()).await?.status, "running");
    }
    Ok(())
}
