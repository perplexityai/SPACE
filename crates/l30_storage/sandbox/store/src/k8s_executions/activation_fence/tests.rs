//! Each activation-fence write's losing case, where its guard matches no row
//! and nothing changes, beside the case it wins; and the mode and scope of
//! [`lock_attempt_owner`]'s lock.

use pplx_space_db_test_fixtures::{SEED_ORG_ID, SEED_USER_ID, try_pg_test_db};
use pplx_space_node_model::SubstrateKind;
use pplx_space_sandbox_model::{SandboxActivationAttemptId, SandboxId};
use pplx_space_sql_locator::sql;
use pplx_space_test_env::TestResult;
use sqlx::PgPool;
use uuid::Uuid;

use super::*;

const LEASE_TTL_SECS: i64 = 300;

async fn insert_sandbox(pool: &PgPool, lifecycle: &str) -> TestResult<SandboxId> {
    let id = SandboxId::new();
    sqlx::query(sql!(
        "INSERT INTO sandboxes
            (id, user_id, org_id, managed_by, lifecycle, network_policy_generation)
         VALUES ($1, $2::UUID, $3::UUID, $4, $5, 1)"
    ))
    .bind(id)
    .bind(SEED_USER_ID)
    .bind(SEED_ORG_ID)
    .bind(SubstrateKind::Kubernetes)
    .bind(lifecycle)
    .execute(pool)
    .await?;
    Ok(id)
}

/// An execution's fence columns, as a test seeds and reads them back.
#[derive(Debug, Clone, PartialEq, sqlx::FromRow)]
struct Fence {
    state: String,
    pod_uid: Option<String>,
    activation_attempt_id: Option<Uuid>,
    activation_attempt_target_state: Option<String>,
    route_activation_id: Option<Uuid>,
    /// Whether the attempt's lease is still running by the database clock.
    lease_live: Option<bool>,
}

impl Fence {
    /// A `running` execution whose pod UID is recorded, held by `attempt`
    /// toward `started` with a live lease and route `route`.
    fn held(attempt: Option<Uuid>, route: Option<Uuid>) -> Self {
        Self {
            state: "running".to_owned(),
            pod_uid: Some("pod-uid-1".to_owned()),
            activation_attempt_id: attempt,
            activation_attempt_target_state: attempt.map(|_| "started".to_owned()),
            route_activation_id: route,
            lease_live: attempt.map(|_| true),
        }
    }
}

async fn insert_execution(
    pool: &PgPool,
    sandbox_id: SandboxId,
    generation: i64,
    active: bool,
    fence: &Fence,
) -> TestResult {
    sqlx::query(sql!(
        "INSERT INTO sandbox_k8s_executions
            (sandbox_id, generation, pod_name, pod_namespace, state, active,
             pod_uid, activation_attempt_id, activation_attempt_target_state,
             route_activation_id, activation_attempt_expires_at)
         VALUES ($1, $2, $3, 'space-sandboxes', $4, $5, $6, $7, $8, $9,
                 CASE WHEN $10 THEN NOW() + INTERVAL '1 hour'
                      WHEN NOT $10 THEN NOW() - INTERVAL '1 hour' END)"
    ))
    .bind(sandbox_id)
    .bind(generation)
    .bind(format!("sbx-{}-{generation}", sandbox_id.as_uuid()))
    .bind(&fence.state)
    .bind(active)
    .bind(&fence.pod_uid)
    .bind(fence.activation_attempt_id)
    .bind(&fence.activation_attempt_target_state)
    .bind(fence.route_activation_id)
    .bind(fence.lease_live)
    .execute(pool)
    .await?;
    Ok(())
}

async fn read_fence(pool: &PgPool, sandbox_id: SandboxId, generation: i64) -> TestResult<Fence> {
    Ok(sqlx::query_as::<_, Fence>(sql!(
        "SELECT state, pod_uid, activation_attempt_id, activation_attempt_target_state,
                route_activation_id,
                activation_attempt_expires_at > NOW() AS lease_live
           FROM sandbox_k8s_executions
          WHERE sandbox_id = $1 AND generation = $2"
    ))
    .bind(sandbox_id)
    .bind(generation)
    .fetch_one(pool)
    .await?)
}

fn attempt() -> SandboxActivationAttemptId {
    SandboxActivationAttemptId::from_uuid(Uuid::now_v7())
}

#[tokio::test(flavor = "current_thread")]
async fn publishing_a_legacy_route_loses_to_a_moved_execution() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let owner = attempt();
    let other = attempt();

    // Each guard on its own: another owner, a route already published,
    // another pod, another generation, a retired execution.
    for (fence, generation, active, pod_uid) in [
        (
            Fence::held(Some(other.as_uuid()), None),
            1,
            true,
            "pod-uid-1",
        ),
        (
            Fence::held(Some(owner.as_uuid()), Some(other.as_uuid())),
            1,
            true,
            "pod-uid-1",
        ),
        (
            Fence::held(Some(owner.as_uuid()), None),
            1,
            true,
            "pod-uid-2",
        ),
        (
            Fence::held(Some(owner.as_uuid()), None),
            2,
            true,
            "pod-uid-1",
        ),
        (
            Fence::held(Some(owner.as_uuid()), None),
            1,
            false,
            "pod-uid-1",
        ),
    ] {
        let sandbox = insert_sandbox(pool, "started").await?;
        insert_execution(pool, sandbox, generation, active, &fence).await?;
        assert_eq!(
            publish_acknowledged_legacy_route(pool, sandbox, 1, owner, pod_uid).await?,
            None
        );
        assert_eq!(read_fence(pool, sandbox, generation).await?, fence);
    }

    let sandbox = insert_sandbox(pool, "started").await?;
    let fence = Fence::held(Some(owner.as_uuid()), None);
    insert_execution(pool, sandbox, 1, true, &fence).await?;
    assert_eq!(
        publish_acknowledged_legacy_route(pool, sandbox, 1, owner, "pod-uid-1").await?,
        Some(owner.as_uuid())
    );
    assert_eq!(
        read_fence(pool, sandbox, 1).await?,
        Fence {
            route_activation_id: Some(owner.as_uuid()),
            ..fence
        }
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn completing_an_attempt_loses_off_target_or_unpublished_or_to_another_owner() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let owner = attempt();
    let other = attempt();
    let route = Some(owner.as_uuid());

    // A sandbox in the attempt's target lifecycle that owns none of these
    // executions: the lifecycle guard reads the execution's own sandbox.
    insert_sandbox(pool, "started").await?;
    for (lifecycle, fence, generation, active) in [
        // The sandbox isn't in the attempt's target lifecycle.
        (
            "starting",
            Fence::held(Some(owner.as_uuid()), route),
            1,
            true,
        ),
        // No published route.
        ("started", Fence::held(Some(owner.as_uuid()), None), 1, true),
        // Another attempt holds the fence.
        (
            "started",
            Fence::held(Some(other.as_uuid()), route),
            1,
            true,
        ),
        // Another generation, or a retired execution.
        (
            "started",
            Fence::held(Some(owner.as_uuid()), route),
            2,
            true,
        ),
        (
            "started",
            Fence::held(Some(owner.as_uuid()), route),
            1,
            false,
        ),
    ] {
        let sandbox = insert_sandbox(pool, lifecycle).await?;
        insert_execution(pool, sandbox, generation, active, &fence).await?;
        assert_eq!(complete_attempt(pool, sandbox, 1, owner).await?, 0);
        assert_eq!(read_fence(pool, sandbox, generation).await?, fence);
    }

    // The target compares against the trimmed, lowercased lifecycle.
    let sandbox = insert_sandbox(pool, " Started ").await?;
    insert_execution(
        pool,
        sandbox,
        1,
        true,
        &Fence::held(Some(owner.as_uuid()), route),
    )
    .await?;
    assert_eq!(complete_attempt(pool, sandbox, 1, owner).await?, 1);
    assert_eq!(
        read_fence(pool, sandbox, 1).await?,
        Fence::held(None, route)
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn releasing_or_giving_up_an_attempt_loses_to_another_owner() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let owner = attempt();
    let other = attempt();

    for give_up in [false, true] {
        for (fence, generation, active) in [
            (Fence::held(Some(other.as_uuid()), None), 1, true),
            (Fence::held(None, None), 1, true),
            (Fence::held(Some(owner.as_uuid()), None), 2, true),
            (Fence::held(Some(owner.as_uuid()), None), 1, false),
        ] {
            let sandbox = insert_sandbox(pool, "starting").await?;
            insert_execution(pool, sandbox, generation, active, &fence).await?;
            let changed = if give_up {
                give_up_attempt(pool, sandbox, 1, owner).await?
            } else {
                release_finished_attempt(pool, sandbox, 1, owner).await?
            };
            assert_eq!(changed, 0, "give_up={give_up} {fence:?}");
            assert_eq!(read_fence(pool, sandbox, generation).await?, fence);
        }
    }

    // A finished release clears the lease; a give-up keeps it, cut to the
    // transaction's start.
    // Neither looks at the lifecycle.
    let sandbox = insert_sandbox(pool, "starting").await?;
    insert_execution(
        pool,
        sandbox,
        1,
        true,
        &Fence::held(Some(owner.as_uuid()), None),
    )
    .await?;
    assert_eq!(release_finished_attempt(pool, sandbox, 1, owner).await?, 1);
    assert_eq!(read_fence(pool, sandbox, 1).await?, Fence::held(None, None));

    let sandbox = insert_sandbox(pool, "starting").await?;
    insert_execution(
        pool,
        sandbox,
        1,
        true,
        &Fence::held(Some(owner.as_uuid()), None),
    )
    .await?;
    assert_eq!(give_up_attempt(pool, sandbox, 1, owner).await?, 1);
    assert_eq!(
        read_fence(pool, sandbox, 1).await?,
        Fence {
            lease_live: Some(false),
            ..Fence::held(None, None)
        }
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn abandoning_for_a_suspend_loses_outside_suspend_or_to_another_owner() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let owner = attempt();
    let other = attempt();

    // A suspended sandbox that owns none of these executions: the
    // lifecycle guard reads the execution's own sandbox.
    insert_sandbox(pool, "suspended").await?;
    for (lifecycle, fence, generation, active) in [
        ("started", Fence::held(Some(owner.as_uuid()), None), 1, true),
        (
            "suspending",
            Fence::held(Some(owner.as_uuid()), None),
            1,
            true,
        ),
        (
            "suspended",
            Fence::held(Some(other.as_uuid()), None),
            1,
            true,
        ),
        (
            "suspended",
            Fence::held(Some(owner.as_uuid()), None),
            2,
            true,
        ),
        (
            "restoring",
            Fence::held(Some(owner.as_uuid()), None),
            1,
            false,
        ),
    ] {
        let sandbox = insert_sandbox(pool, lifecycle).await?;
        insert_execution(pool, sandbox, generation, active, &fence).await?;
        assert_eq!(
            abandon_attempt_superseded_by_suspend(pool, sandbox, 1, owner).await?,
            0
        );
        assert_eq!(read_fence(pool, sandbox, generation).await?, fence);
    }

    for lifecycle in ["suspended", "restoring"] {
        let sandbox = insert_sandbox(pool, lifecycle).await?;
        insert_execution(
            pool,
            sandbox,
            1,
            true,
            &Fence::held(Some(owner.as_uuid()), None),
        )
        .await?;
        assert_eq!(
            abandon_attempt_superseded_by_suspend(pool, sandbox, 1, owner).await?,
            1
        );
        assert_eq!(read_fence(pool, sandbox, 1).await?, Fence::held(None, None));
    }
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn claims_lose_to_an_execution_their_route_publication_does_not_fit() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let claimant = attempt();
    let route = Some(attempt().as_uuid());
    let fresh = |state: &str| Fence {
        state: state.to_owned(),
        pod_uid: None,
        ..Fence::held(None, None)
    };
    let fresh_with_blank_uid = Fence {
        pod_uid: Some("  ".to_owned()),
        ..fresh("creating")
    };

    #[derive(Clone, Copy, Debug)]
    enum Claim {
        PublishedRoute,
        InitializeRoute,
        AdoptLegacy,
    }
    async fn claim(
        pool: &PgPool,
        kind: Claim,
        sandbox: SandboxId,
        attempt: SandboxActivationAttemptId,
    ) -> Result<Option<Uuid>, sqlx::Error> {
        let mut conn = pool.acquire().await?;
        let conn = &mut *conn;
        match kind {
            Claim::PublishedRoute => claim_attempt_with_published_route(
                conn,
                sandbox,
                1,
                attempt,
                "started",
                LEASE_TTL_SECS,
            )
            .await
            .map_err(sqlx::Error::from),
            Claim::InitializeRoute => claim_attempt_initializing_route(
                conn,
                sandbox,
                1,
                attempt,
                "started",
                LEASE_TTL_SECS,
            )
            .await
            .map_err(sqlx::Error::from),
            Claim::AdoptLegacy => claim_attempt_adopting_legacy_route(
                conn,
                sandbox,
                1,
                attempt,
                "started",
                LEASE_TTL_SECS,
            )
            .await
            .map_err(sqlx::Error::from),
        }
    }

    for (kind, fence, generation, active) in [
        // No published route; another generation; a retired execution.
        (Claim::PublishedRoute, Fence::held(None, None), 1, true),
        (Claim::PublishedRoute, Fence::held(None, route), 2, true),
        (Claim::PublishedRoute, Fence::held(None, route), 1, false),
        // A recorded pod UID; a route already published; past `creating`
        // and `recovering`; another generation; a retired execution.
        (
            Claim::InitializeRoute,
            Fence {
                pod_uid: Some("pod-uid-1".to_owned()),
                ..fresh("creating")
            },
            1,
            true,
        ),
        (
            Claim::InitializeRoute,
            Fence {
                route_activation_id: route,
                ..fresh("creating")
            },
            1,
            true,
        ),
        (Claim::InitializeRoute, fresh("created"), 1, true),
        (Claim::InitializeRoute, fresh("creating"), 2, true),
        (Claim::InitializeRoute, fresh("creating"), 1, false),
        // A route already published; no pod UID, or a blank one; another
        // generation; a retired execution.
        (Claim::AdoptLegacy, Fence::held(None, route), 1, true),
        (Claim::AdoptLegacy, fresh("running"), 1, true),
        (Claim::AdoptLegacy, fresh_with_blank_uid.clone(), 1, true),
        (Claim::AdoptLegacy, Fence::held(None, None), 2, true),
        (Claim::AdoptLegacy, Fence::held(None, None), 1, false),
    ] {
        let sandbox = insert_sandbox(pool, "starting").await?;
        insert_execution(pool, sandbox, generation, active, &fence).await?;
        assert_eq!(
            claim(pool, kind, sandbox, claimant).await?,
            None,
            "{kind:?} {fence:?} generation {generation} active {active}"
        );
        assert_eq!(read_fence(pool, sandbox, generation).await?, fence);
    }

    // Each claim wins on the execution it fits. The claims check no owner,
    // so the published-route case's expired owner is simply replaced.
    let expired = Some(attempt().as_uuid());
    for (kind, fence, claimed) in [
        (
            Claim::PublishedRoute,
            Fence {
                lease_live: Some(false),
                ..Fence::held(expired, route)
            },
            Fence::held(Some(claimant.as_uuid()), route),
        ),
        (
            Claim::InitializeRoute,
            fresh("recovering"),
            Fence {
                state: "recovering".to_owned(),
                pod_uid: None,
                ..Fence::held(Some(claimant.as_uuid()), Some(claimant.as_uuid()))
            },
        ),
        (
            Claim::InitializeRoute,
            fresh_with_blank_uid,
            Fence {
                state: "creating".to_owned(),
                pod_uid: Some("  ".to_owned()),
                ..Fence::held(Some(claimant.as_uuid()), Some(claimant.as_uuid()))
            },
        ),
        (
            Claim::AdoptLegacy,
            Fence::held(None, None),
            Fence::held(Some(claimant.as_uuid()), None),
        ),
    ] {
        let sandbox = insert_sandbox(pool, "starting").await?;
        insert_execution(pool, sandbox, 1, true, &fence).await?;
        assert_eq!(
            claim(pool, kind, sandbox, claimant).await?,
            Some(claimant.as_uuid()),
            "{kind:?} {fence:?}"
        );
        assert_eq!(read_fence(pool, sandbox, 1).await?, claimed, "{kind:?}");
    }
    Ok(())
}

/// Whether another transaction's `FOR KEY SHARE NOWAIT` on the execution
/// row fails with `lock_not_available`. Only `FOR UPDATE` refuses it, so
/// this pins the lock's mode.
async fn key_share_blocked(
    pool: &PgPool,
    sandbox_id: SandboxId,
    generation: i64,
) -> TestResult<bool> {
    let mut probe = pool.begin().await?;
    let result = sqlx::query(sql!(
        "SELECT 1 FROM sandbox_k8s_executions
          WHERE sandbox_id = $1 AND generation = $2
          FOR KEY SHARE NOWAIT"
    ))
    .bind(sandbox_id)
    .bind(generation)
    .fetch_optional(&mut *probe)
    .await;
    probe.rollback().await?;
    match result {
        Ok(_) => Ok(false),
        Err(sqlx::Error::Database(error)) if error.code().as_deref() == Some("55P03") => Ok(true),
        Err(error) => Err(error.into()),
    }
}

#[tokio::test(flavor = "current_thread")]
async fn locking_the_attempt_owner_holds_the_active_generation_until_the_transaction_ends()
-> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let owner = attempt();
    let route = attempt();
    let sandbox = insert_sandbox(pool, "starting").await?;
    insert_execution(pool, sandbox, 1, false, &Fence::held(None, None)).await?;
    insert_execution(
        pool,
        sandbox,
        2,
        true,
        &Fence::held(Some(owner.as_uuid()), Some(route.as_uuid())),
    )
    .await?;
    // Another sandbox's active execution at generation 1, where this
    // sandbox's execution is retired: the lock reads and locks only
    // `sandbox_id`'s rows.
    let neighbour = insert_sandbox(pool, "starting").await?;
    insert_execution(
        pool,
        neighbour,
        1,
        true,
        &Fence::held(Some(attempt().as_uuid()), None),
    )
    .await?;

    // A generation that isn't the active one reads nothing and locks
    // nothing.
    for generation in [1, 3] {
        let mut tx = pool.begin().await?;
        assert_eq!(
            lock_attempt_owner(&mut *tx, sandbox, generation).await?,
            None
        );
        assert!(!key_share_blocked(pool, sandbox, 1).await?);
        assert!(!key_share_blocked(pool, sandbox, 2).await?);
        assert!(!key_share_blocked(pool, neighbour, 1).await?);
        tx.rollback().await?;
    }

    for commit in [true, false] {
        let mut tx = pool.begin().await?;
        let locked = lock_attempt_owner(&mut *tx, sandbox, 2)
            .await?
            .expect("the active generation");
        assert_eq!(locked.activation_attempt_id, Some(owner.as_uuid()));
        assert_eq!(
            locked.activation_attempt_target_state.as_deref(),
            Some("started")
        );
        assert_eq!(locked.route_activation_id, Some(route.as_uuid()));
        assert!(locked.lease_remaining_secs > 0.0);
        assert!(key_share_blocked(pool, sandbox, 2).await?);
        assert!(!key_share_blocked(pool, sandbox, 1).await?);
        assert!(!key_share_blocked(pool, neighbour, 1).await?);
        if commit {
            tx.commit().await?;
        } else {
            tx.rollback().await?;
        }
        assert!(!key_share_blocked(pool, sandbox, 2).await?);
    }

    // An expired lease reads as zero seconds left, never negative.
    let expired = insert_sandbox(pool, "starting").await?;
    insert_execution(
        pool,
        expired,
        1,
        true,
        &Fence {
            lease_live: Some(false),
            ..Fence::held(Some(owner.as_uuid()), None)
        },
    )
    .await?;
    let mut tx = pool.begin().await?;
    let locked = lock_attempt_owner(&mut *tx, expired, 1).await?;
    tx.rollback().await?;
    assert_eq!(locked.map(|owner| owner.lease_remaining_secs), Some(0.0));
    Ok(())
}
