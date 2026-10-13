use pplx_space_common_core::types::SandboxId;

use super::*;
use pplx_space_db_test_fixtures::pg_test_db;

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

/// Every stored lifecycle against both published-suspend fences. A completion
/// writes `started` / `on_node`; a stale completion behind a newer suspend
/// claim defers to it, and every other refusal is a lost CAS. Refusals leave
/// both `lifecycle` and `placement` untouched.
#[tokio::test(flavor = "current_thread")]
async fn wake_completion_cas_outcome_for_every_prior_lifecycle() -> TestResult {
    use WakeLifecycleCompletion::{Completed, Lost, SupersededBySuspend};

    let db = pg_test_db().await?;
    let pool = &db.pool;

    let mut cases: Vec<(String, PublishedSuspendFence, WakeLifecycleCompletion)> = Vec::new();
    for fence in [
        PublishedSuspendFence::Enforced,
        PublishedSuspendFence::DirectRestoreBaseline,
    ] {
        for lifecycle in SandboxLifecycle::all() {
            let expected = match lifecycle {
                SandboxLifecycle::Created
                | SandboxLifecycle::Creating
                | SandboxLifecycle::Starting
                | SandboxLifecycle::Started
                | SandboxLifecycle::Restoring
                | SandboxLifecycle::Pausing
                | SandboxLifecycle::Paused
                | SandboxLifecycle::Resuming
                | SandboxLifecycle::Checkpointing => Completed,
                SandboxLifecycle::Suspending => SupersededBySuspend,
                // Kubernetes fences a finalized suspend; a direct restore
                // keeps `suspended` as the baseline it advances from.
                SandboxLifecycle::Suspended => match fence {
                    PublishedSuspendFence::Enforced => SupersededBySuspend,
                    PublishedSuspendFence::DirectRestoreBaseline => Completed,
                },
                SandboxLifecycle::Stopping
                | SandboxLifecycle::Stopped
                | SandboxLifecycle::Deleting
                | SandboxLifecycle::Quarantined
                | SandboxLifecycle::Failed => Lost,
            };
            cases.push((lifecycle.as_str().to_owned(), fence, expected));
        }
        // Stored values that are not `SandboxLifecycle` variants.
        for lifecycle in ["deleted", "cleanup_required"] {
            cases.push((lifecycle.to_owned(), fence, Lost));
        }
        // The guard compares a trimmed, lower-cased lifecycle.
        cases.push((" Failed ".to_owned(), fence, Lost));
        cases.push(("SUSPENDING".to_owned(), fence, SupersededBySuspend));
    }

    for (lifecycle, fence, expected) in cases {
        let sandbox_id = SandboxId::new();
        insert_sandbox(pool, sandbox_id, &lifecycle).await?;

        let outcome = complete_wake_lifecycle(pool, sandbox_id, fence).await?;

        assert_eq!(
            outcome, expected,
            "prior lifecycle {lifecycle:?}, {fence:?}"
        );
        let expected_row = if expected == Completed {
            ("started".to_owned(), "on_node".to_owned())
        } else {
            (lifecycle.clone(), "unplaced".to_owned())
        };
        assert_eq!(
            lifecycle_and_placement(pool, sandbox_id).await?,
            Some(expected_row),
            "prior lifecycle {lifecycle:?}, {fence:?}"
        );
    }

    // A row deleted while the wake was in flight is a lost CAS, not a
    // completion.
    for fence in [
        PublishedSuspendFence::Enforced,
        PublishedSuspendFence::DirectRestoreBaseline,
    ] {
        assert_eq!(
            complete_wake_lifecycle(pool, SandboxId::new(), fence).await?,
            Lost
        );
    }
    Ok(())
}

/// The wake and restore arms pick the fence from the running substrate. On a
/// row still `suspended` at completion, Kubernetes, whose restore leaves
/// `restoring -> started` to this CAS, defers to it as a newer suspend; Direct,
/// whose worker-local wake keeps `suspended` as its baseline, advances it.
#[tokio::test(flavor = "current_thread")]
async fn wake_completion_fence_follows_the_running_substrate() -> TestResult {
    use crate::controllers::substrate::{Substrate, direct::DirectSubstrate};

    let db = pg_test_db().await?;
    let pool = &db.pool;

    #[cfg_attr(not(feature = "kubernetes"), allow(unused_mut))]
    let mut cases = vec![(
        Substrate::Direct(Box::new(DirectSubstrate::new(pool.clone()))),
        "started",
    )];
    #[cfg(feature = "kubernetes")]
    cases.push((
        Substrate::Kubernetes(Box::new(
            crate::controllers::substrate::kubernetes::tests::substrate_with_store(pool.clone()),
        )),
        "suspended",
    ));

    for (substrate, expected) in cases {
        let sandbox_id = SandboxId::new();
        insert_sandbox(pool, sandbox_id, "suspended").await?;

        let fence = PublishedSuspendFence::for_capabilities(substrate.capabilities());
        complete_wake_lifecycle(pool, sandbox_id, fence).await?;

        let lifecycle = lifecycle_and_placement(pool, sandbox_id)
            .await?
            .map(|(lifecycle, _)| lifecycle);
        assert_eq!(
            lifecycle.as_deref(),
            Some(expected),
            "{} substrate",
            substrate.managed_by()
        );
    }
    Ok(())
}
