//! The ingress test in the parent module whose path differs by substrate,
//! run against the Kubernetes substrate under the same name.

use super::*;
use crate::test_support::kubernetes::{
    LiveExecution, RouteIdentity, insert_kubernetes_worker_node, insert_live_execution, kubernetes,
    substrate_over_pods,
};

/// Same outcome as on direct. The paused execution is an active
/// `sandbox_k8s_executions` row whose `route_activation_id` is the attempt
/// the worker reports, the Kubernetes counterpart of the direct route
/// identity, and ingress resumes it through the pod instead of the
/// `node_direct` endpoint.
#[tokio::test(flavor = "current_thread")]
async fn stored_ingress_access_resumes_paused_execution_before_reporting_ready()
-> pplx_space_test_env::TestResult {
    use pplx_space_common_core::types::{AutoLifecycleWakeSource, NodeManagedBy};
    use pplx_space_db_test_fixtures::try_pg_test_db;
    use pplx_space_node_api_sim::{SandboxRecord, SimWorkerHarness};
    use prost::Message;
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let worker = SimWorkerHarness::builder(crate::test_support::kubernetes::WORKER_NODE)
        .with_internal_api()
        .start()
        .await?;
    let node = insert_kubernetes_worker_node(&db.pool).await?;
    for (wake_enabled, pause_after_observation, worker_allows_access) in [
        (true, false, true),
        (false, false, true),
        (true, true, true),
        (false, true, true),
        (true, false, false),
        (true, true, false),
    ] {
        let id = SandboxId::new();
        let mut policy = AutoLifecyclePolicy::disabled();
        if wake_enabled {
            policy.wake.on.push(AutoLifecycleWakeSource::Access);
        }
        let lifecycle = if pause_after_observation {
            "started"
        } else {
            "paused"
        };
        sqlx::query(sql!("INSERT INTO sandboxes (id, user_id, org_id, managed_by, lifecycle, placement, active_node_id, template_id, content_hash, auto_lifecycle_json, network_policy_generation)
            VALUES ($1, '00000000-0000-0000-0000-000000000010', '00000000-0000-0000-0000-000000000001', 'k8s', $2, 'on_node', $3, 'seed', 'hash-seed', $4, 1)"))
            .bind(id.as_uuid()).bind(lifecycle).bind(node).bind(sqlx::types::Json(&policy)).execute(&db.pool).await?;
        let LiveExecution {
            pod,
            route_activation_id,
        } = insert_live_execution(&db.pool, id, RouteIdentity::Published).await?;
        let mut record = SandboxRecord::new_creating(
            id.to_string(),
            "seed".into(),
            "fc".into(),
            "test".into(),
            None,
            false,
        );
        // The worker regression pins Local authority against cluster replay;
        // this fixture models the resulting effective policy at the RPC boundary.
        record.auto_lifecycle_authority = pplx_space_common_core::types::PolicyAuthority::Local;
        let mut effective_policy = AutoLifecyclePolicy::disabled();
        if worker_allows_access {
            effective_policy
                .wake
                .on
                .push(AutoLifecycleWakeSource::Access);
        }
        record.access_idle_policy =
            crate::controllers::auto_lifecycle::policy_to_proto(&effective_policy).encode_to_vec();
        record.status = "paused".into();
        record.route_generation = Some(1);
        record.execution_generation = Some(1);
        record.activation_attempt_id = route_activation_id.map(|id| id.to_string());
        record.route_substrate_kind = Some(NodeManagedBy::Kubernetes);
        worker.state().insert_new(record).await?;
        let (substrate, _) = substrate_over_pods(&db.pool, worker.port(), vec![pod])?;
        let backend = StoredAccess {
            pool: db.pool.clone(),
            controller: SandboxController::new(kubernetes(substrate)),
        };
        let observed = backend.observe(id).await?;
        if pause_after_observation {
            assert_eq!(observed.lifecycle, SandboxLifecycle::Started);
            sqlx::query(sql!(
                "UPDATE sandboxes SET lifecycle = 'paused' WHERE id = $1"
            ))
            .bind(id.as_uuid())
            .execute(&db.pool)
            .await?;
        }
        let outcome = backend.access(id).await;
        if wake_enabled && worker_allows_access {
            assert_eq!(outcome?, AccessOutcome::Ready);
            assert_eq!(
                worker.state().get(&id.to_string()).await?.status,
                "running",
                "fresh ingress preparation must resume the worker before bridge setup"
            );
            let current = fetch_sandbox(&db.pool, id).await?.unwrap();
            assert_eq!(current.lifecycle, "started");
            assert_eq!(current.active_execution_generation, Some(1));
        } else {
            let expected = if wake_enabled {
                tonic::Code::Unavailable
            } else {
                tonic::Code::FailedPrecondition
            };
            assert_eq!(outcome.unwrap_err().code(), expected);
            assert_eq!(worker.state().get(&id.to_string()).await?.status, "paused");
            assert_eq!(
                fetch_sandbox(&db.pool, id).await?.unwrap().lifecycle,
                "paused"
            );
        }
    }
    Ok(())
}

/// Ingress to a `restoring` sandbox whose restore lease has lapsed while its
/// execution still owns the restore (so `observe` reports no owner) reaches
/// `access`, which must not dial the restoring pod: it joins the restore and
/// reports ready only once the sandbox is `started`.
#[tokio::test(flavor = "current_thread")]
async fn stored_ingress_access_joins_a_restore_whose_lease_lapsed()
-> pplx_space_test_env::TestResult {
    use pplx_space_common_core::types::AutoLifecycleWakeSource;
    use pplx_space_db_test_fixtures::try_pg_test_db;
    use pplx_space_node_api_sim::SimWorkerHarness;
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let worker = SimWorkerHarness::builder(crate::test_support::kubernetes::WORKER_NODE)
        .with_internal_api()
        .start()
        .await?;
    let node = insert_kubernetes_worker_node(&db.pool).await?;
    let id = SandboxId::new();
    let mut policy = AutoLifecyclePolicy::disabled();
    policy.wake.on.push(AutoLifecycleWakeSource::Access);
    sqlx::query(sql!("INSERT INTO sandboxes (id, user_id, org_id, managed_by, lifecycle, placement, active_node_id, template_id, content_hash, auto_lifecycle_json, network_policy_generation, restore_lease_expires_at, restore_lease_token)
        VALUES ($1, '00000000-0000-0000-0000-000000000010', '00000000-0000-0000-0000-000000000001', 'k8s', 'restoring', 'on_node', $2, 'seed', 'hash-seed', $3, 1, NOW() - INTERVAL '1 second', $4)"))
        .bind(id.as_uuid()).bind(node).bind(sqlx::types::Json(&policy)).bind(uuid::Uuid::now_v7()).execute(&db.pool).await?;
    let LiveExecution { pod, .. } =
        insert_live_execution(&db.pool, id, RouteIdentity::Published).await?;
    let (substrate, _) = substrate_over_pods(&db.pool, worker.port(), vec![pod])?;
    let backend = StoredAccess {
        pool: db.pool.clone(),
        controller: SandboxController::new(kubernetes(substrate)),
    };
    assert!(
        !backend.observe(id).await?.restore_owned,
        "the lapsed lease leaves ingress with no owner to observe"
    );
    let owner_pool = db.pool.clone();
    let owner = tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        sqlx::query(sql!(
            "UPDATE sandboxes SET lifecycle = 'started' WHERE id = $1"
        ))
        .bind(id.as_uuid())
        .execute(&owner_pool)
        .await
    });

    let outcome = backend.access(id).await;
    let lifecycle_at_return = fetch_sandbox(&db.pool, id).await?.unwrap().lifecycle;
    owner.await??;

    assert_eq!(outcome?, AccessOutcome::Ready);
    assert_eq!(
        lifecycle_at_return, "started",
        "ingress must not reach a restoring pod"
    );
    Ok(())
}
