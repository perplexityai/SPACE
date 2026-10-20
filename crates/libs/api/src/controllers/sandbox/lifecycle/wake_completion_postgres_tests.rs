use pplx_space_common_core::types::SandboxId;

use super::*;
use pplx_space_db_test_fixtures::pg_test_db;
use pplx_space_placement::StoredSandboxLifecycle;

use pplx_space_test_env::TestResult;

async fn insert_sandbox(pool: &PgPool, sandbox_id: SandboxId, lifecycle: &str) -> TestResult {
    sqlx::query(sql!(
        "INSERT INTO sandboxes (
             id, user_id, org_id, lifecycle, placement, managed_by,
             network_policy_override_json, network_policy_generation
         )
         VALUES (
             $1,
             '00000000-0000-0000-0000-000000000010',
             '00000000-0000-0000-0000-000000000001',
             $2, 'unplaced', 'k8s', $3, 1
         )"
    ))
    .bind(sandbox_id)
    .bind(lifecycle)
    .bind(sqlx::types::Json(
        &pplx_space_net_common::NetworkPolicyV1::default(),
    ))
    .execute(pool)
    .await?;
    Ok(())
}

async fn lifecycle_and_placement(
    pool: &PgPool,
    sandbox_id: SandboxId,
) -> TestResult<Option<(String, String)>> {
    Ok(sqlx::query_as::<_, (String, String)>(sql!(
        "SELECT lifecycle, placement FROM sandboxes WHERE id = $1"
    ))
    .bind(sandbox_id)
    .fetch_optional(pool)
    .await?)
}

/// Every stored lifecycle against the fence each substrate's capabilities
/// select. A completion writes `started` / `on_node`; a stale completion behind
/// a newer suspend claim defers to it, and every other refusal is a lost CAS.
/// Refusals leave both `lifecycle` and `placement` untouched.
#[tokio::test(flavor = "current_thread")]
async fn wake_completion_cas_outcome_for_every_prior_lifecycle() -> TestResult {
    use crate::controllers::substrate::{
        Substrate, SubstrateCapabilities, direct::DirectSubstrate,
    };
    use WakeLifecycleCompletion::{Completed, Lost, SupersededBySuspend};

    let db = pg_test_db().await?;
    let pool = &db.pool;

    // The Wake and Restore arms pick the fence from the running substrate's
    // capabilities; the table below runs those constants. Direct's side of
    // the Wake arm's choice is pinned by the controller-level
    // `wake_completion_outcome_maps_to_the_wake_response`; the Kubernetes
    // side, like the Restore arm, is observable only in the race.
    assert_eq!(
        Substrate::from_direct(DirectSubstrate::new(pool.clone())).capabilities(),
        SubstrateCapabilities::DIRECT
    );
    #[cfg(feature = "kubernetes")]
    assert_eq!(
        Substrate::from_kubernetes(
            crate::controllers::substrate::kubernetes::tests::substrate_with_store(pool.clone()),
        )
        .capabilities(),
        SubstrateCapabilities::KUBERNETES
    );

    let mut cases: Vec<(String, &str, PublishedSuspendFence, WakeLifecycleCompletion)> = Vec::new();
    // A row still `suspended` at completion: Kubernetes, whose restore leaves
    // `restoring -> started` to this CAS, fences it as a finalized suspend;
    // Direct's worker-local wake keeps `suspended` as the baseline it advances
    // from.
    let substrates = [
        ("direct", SubstrateCapabilities::DIRECT, Completed),
        (
            "kubernetes",
            SubstrateCapabilities::KUBERNETES,
            SupersededBySuspend,
        ),
    ];
    for (substrate, capabilities, suspended_outcome) in substrates {
        let fence = PublishedSuspendFence::for_capabilities(capabilities);
        for stored in StoredSandboxLifecycle::all() {
            let expected = match stored {
                StoredSandboxLifecycle::Live(
                    SandboxLifecycle::Created
                    | SandboxLifecycle::Creating
                    | SandboxLifecycle::Starting
                    | SandboxLifecycle::Started
                    | SandboxLifecycle::Restoring
                    | SandboxLifecycle::Pausing
                    | SandboxLifecycle::Paused
                    | SandboxLifecycle::Resuming
                    | SandboxLifecycle::Checkpointing,
                ) => Completed,
                StoredSandboxLifecycle::Live(SandboxLifecycle::Suspending) => SupersededBySuspend,
                StoredSandboxLifecycle::Live(SandboxLifecycle::Suspended) => suspended_outcome,
                StoredSandboxLifecycle::Live(
                    SandboxLifecycle::Stopping
                    | SandboxLifecycle::Stopped
                    | SandboxLifecycle::Deleting
                    | SandboxLifecycle::Quarantined
                    | SandboxLifecycle::Failed,
                )
                // The stored values that are not `SandboxLifecycle` variants.
                | StoredSandboxLifecycle::Deleted
                | StoredSandboxLifecycle::CleanupRequired => Lost,
            };
            cases.push((stored.as_str().to_owned(), substrate, fence, expected));
        }
        // The guard compares a trimmed, lower-cased lifecycle.
        cases.push((" Failed ".to_owned(), substrate, fence, Lost));
        cases.push((
            "SUSPENDING".to_owned(),
            substrate,
            fence,
            SupersededBySuspend,
        ));
    }

    for (lifecycle, substrate, fence, expected) in cases {
        let sandbox_id = SandboxId::new();
        insert_sandbox(pool, sandbox_id, &lifecycle).await?;

        let outcome = complete_wake_lifecycle(pool, sandbox_id, fence).await?;

        assert_eq!(
            outcome, expected,
            "prior lifecycle {lifecycle:?}, {substrate} substrate ({fence:?})"
        );
        let expected_row = if expected == Completed {
            ("started".to_owned(), "on_node".to_owned())
        } else {
            (lifecycle.clone(), "unplaced".to_owned())
        };
        assert_eq!(
            lifecycle_and_placement(pool, sandbox_id).await?,
            Some(expected_row),
            "prior lifecycle {lifecycle:?}, {substrate} substrate ({fence:?})"
        );
    }

    // A row deleted while the wake was in flight is a lost CAS, not a
    // completion.
    for (substrate, capabilities, _) in substrates {
        let fence = PublishedSuspendFence::for_capabilities(capabilities);
        assert_eq!(
            complete_wake_lifecycle(pool, SandboxId::new(), fence).await?,
            Lost,
            "{substrate} substrate"
        );
    }
    Ok(())
}

/// On Kubernetes the cancelled-wake redrive asks the substrate to hydrate a
/// `restoring` row whose execution the database still reports unroutable (no
/// pod IP yet): when the apiserver serves the execution's live, matching pod,
/// the hydration makes the row routable and the redrive completes it to
/// `started`. When the pod is gone, the row stays `restoring` for the sweeper;
/// the completion CAS's own routability predicate also holds that case.
#[cfg(feature = "kubernetes")]
#[tokio::test(flavor = "current_thread")]
async fn wake_completion_redrive_on_kubernetes_hydrates_before_completing() -> TestResult {
    use axum::http::StatusCode;

    use crate::controllers::sandbox::wake_completion::wake_completion_redrive;
    use crate::controllers::substrate::Substrate;
    use crate::controllers::substrate::kubernetes::adapter_characterization_tests::sandbox_pod;
    use crate::controllers::substrate::kubernetes::scripted_test_doubles::{
        kube_status, pod_reply, scripted_kube_client,
    };
    use crate::controllers::substrate::kubernetes::tests::{
        insert_k8s_execution, substrate_with_store_and_client,
    };
    use pplx_space_metrics::sandbox::api::WakeCompletionAction;

    let db = pg_test_db().await?;
    let pool = &db.pool;

    for (pod_is_live, expected) in [(true, "started"), (false, "restoring")] {
        let sandbox_id = SandboxId::new();
        let pod_name = format!("space-sbx-{sandbox_id}-g1");
        insert_sandbox(pool, sandbox_id, "restoring").await?;
        insert_k8s_execution(pool, sandbox_id, 1, &pod_name, "", None, true).await?;
        sqlx::query(sql!(
            "UPDATE sandbox_k8s_executions SET state = 'creating' WHERE sandbox_id = $1"
        ))
        .bind(sandbox_id)
        .execute(pool)
        .await?;
        let live_pod = sandbox_pod(
            &pod_name,
            "redrive-uid",
            sandbox_id,
            1,
            "Running",
            Some("192.0.2.9"),
        );
        let (client, _log) = scripted_kube_client(move |_request| {
            if pod_is_live {
                pod_reply(&live_pod)
            } else {
                kube_status(StatusCode::NOT_FOUND, "NotFound")
            }
        });
        let substrate =
            Substrate::from_kubernetes(substrate_with_store_and_client(pool.clone(), client));

        // A zero budget makes exactly one attempt: read, routability check,
        // fenced CAS.
        wake_completion_redrive(
            pool.clone(),
            substrate,
            sandbox_id,
            WakeCompletionAction::Wake,
            std::time::Duration::ZERO,
        )
        .await;

        let lifecycle = lifecycle_and_placement(pool, sandbox_id)
            .await?
            .map(|(lifecycle, _)| lifecycle);
        assert_eq!(
            lifecycle.as_deref(),
            Some(expected),
            "pod is live: {pod_is_live}"
        );
    }
    Ok(())
}
