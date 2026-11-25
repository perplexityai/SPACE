//! The claim verbs: the pool-reserved generation, the minter and the claim
//! insert that follows it under one sandbox lock, and the reset to
//! `creating`, each with the case its guard refuses.

use pplx_space_db_test_fixtures::{
    SEED_ORG_ID, SEED_USER_ID, try_pg_test_db, try_pg_test_db_with_max_connections,
};
use pplx_space_sandbox_model::{SandboxActivationAttemptId, SandboxId};
use pplx_space_sql_locator::sql;
use pplx_space_test_env::TestResult;
use sqlx::PgPool;

use super::{
    insert_creating_claim, mint_generation, reset_to_creating, retire_pool_reserved_generation,
};
use crate::row_locks;

const NAMESPACE: &str = "space-sandboxes";

async fn insert_sandbox(pool: &PgPool) -> TestResult<SandboxId> {
    let id = SandboxId::new();
    sqlx::query(sql!(
        "INSERT INTO sandboxes
            (id, user_id, org_id, managed_by, lifecycle, network_policy_generation)
         VALUES ($1, $2, $3, 'k8s', 'started', 1)"
    ))
    .bind(id)
    .bind(SEED_USER_ID)
    .bind(SEED_ORG_ID)
    .execute(pool)
    .await?;
    Ok(id)
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Seed {
    generation: i64,
    state: &'static str,
    active: bool,
    pod_uid: Option<&'static str>,
    pod_ip: Option<&'static str>,
    activation_attempt_id: Option<SandboxActivationAttemptId>,
}

impl Seed {
    fn active(generation: i64, state: &'static str) -> Self {
        Self {
            generation,
            state,
            active: true,
            pod_uid: None,
            pod_ip: None,
            activation_attempt_id: None,
        }
    }
}

async fn insert_execution(pool: &PgPool, sandbox_id: SandboxId, seed: &Seed) -> TestResult {
    sqlx::query(sql!(
        "INSERT INTO sandbox_k8s_executions
            (sandbox_id, generation, pod_name, pod_namespace, state, active,
             pod_uid, pod_ip, activation_attempt_id)
         VALUES ($1, $2, $3, 'space-sandboxes', $4, $5, $6, $7::INET, $8)"
    ))
    .bind(sandbox_id)
    .bind(seed.generation)
    .bind(format!(
        "pod-{}-g{}",
        sandbox_id.as_uuid().simple(),
        seed.generation
    ))
    .bind(seed.state)
    .bind(seed.active)
    .bind(seed.pod_uid)
    .bind(seed.pod_ip)
    .bind(seed.activation_attempt_id)
    .execute(pool)
    .await?;
    Ok(())
}

/// Every execution row of the sandbox, oldest generation first: generation,
/// state, active, pod UID, pod IP, whether it is terminated and whether
/// `pod_materialized_at` is stamped.
type Row = (
    i64,
    String,
    bool,
    Option<String>,
    Option<String>,
    bool,
    bool,
);

async fn rows(pool: &PgPool, sandbox_id: SandboxId) -> TestResult<Vec<Row>> {
    Ok(sqlx::query_as::<_, Row>(sql!(
        "SELECT generation, state, active, pod_uid, host(pod_ip),
                terminated_at IS NOT NULL, pod_materialized_at IS NOT NULL
           FROM sandbox_k8s_executions
          WHERE sandbox_id = $1
          ORDER BY generation"
    ))
    .bind(sandbox_id)
    .fetch_all(pool)
    .await?)
}

#[tokio::test(flavor = "current_thread")]
async fn pool_reserved_generation_is_recorded_only_for_a_sandbox_without_executions() -> TestResult
{
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;

    let fresh = insert_sandbox(pool).await?;
    assert!(retire_pool_reserved_generation(pool, fresh, "pod-g1", NAMESPACE).await?);
    let retired = vec![(1, "replaced".to_string(), false, None, None, true, false)];
    assert_eq!(rows(pool, fresh).await?, retired);
    assert!(!retire_pool_reserved_generation(pool, fresh, "pod-g1-again", NAMESPACE).await?);
    assert_eq!(rows(pool, fresh).await?, retired);

    // A sandbox that already has an execution keeps it as it is.
    let claimed = insert_sandbox(pool).await?;
    insert_execution(pool, claimed, &Seed::active(1, "creating")).await?;
    let before = rows(pool, claimed).await?;
    assert!(!retire_pool_reserved_generation(pool, claimed, "pod-other", NAMESPACE).await?);
    assert_eq!(rows(pool, claimed).await?, before);

    // So does one whose only execution leaves generation 1 free: no unique
    // index refuses the insert, only its `NOT EXISTS`.
    let past_one = insert_sandbox(pool).await?;
    insert_execution(pool, past_one, &Seed::active(2, "creating")).await?;
    let before = rows(pool, past_one).await?;
    assert!(!retire_pool_reserved_generation(pool, past_one, "pod-reserved", NAMESPACE).await?);
    assert_eq!(rows(pool, past_one).await?, before);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn mint_generation_takes_one_past_the_highest_recorded() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let sandbox = insert_sandbox(pool).await?;

    let mut conn = pool.acquire().await?;
    assert_eq!(mint_generation(&mut conn, sandbox).await?, 1);
    for generation in [1, 3] {
        insert_execution(
            pool,
            sandbox,
            &Seed {
                active: false,
                ..Seed::active(generation, "replaced")
            },
        )
        .await?;
    }
    assert_eq!(mint_generation(&mut conn, sandbox).await?, 4);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn creating_claim_inserts_nothing_over_an_active_or_recorded_generation() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let sandbox = insert_sandbox(pool).await?;

    let claimed = insert_creating_claim(pool, sandbox, 1, "pod-g1", NAMESPACE)
        .await?
        .expect("the first claim inserts");
    assert_eq!(claimed.generation, 1);
    assert_eq!(claimed.pod_name, "pod-g1");
    assert_eq!(claimed.state, "creating");
    assert!(!claimed.pod_materialized && !claimed.create_age_expired);
    assert_eq!(claimed.activation_attempt_id, None);
    assert_eq!(claimed.route_activation_id, None);
    let live = rows(pool, sandbox).await?;

    // The active execution's unique index refuses a second live claim.
    assert_eq!(
        insert_creating_claim(pool, sandbox, 2, "pod-g2", NAMESPACE).await?,
        None
    );
    assert_eq!(rows(pool, sandbox).await?, live);

    // Retired, generation 1 still refuses a claim that reuses it.
    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions SET active = FALSE WHERE sandbox_id = $1"
    ))
    .bind(sandbox)
    .execute(pool)
    .await?;
    let retired = rows(pool, sandbox).await?;
    assert_eq!(
        insert_creating_claim(pool, sandbox, 1, "pod-g1-again", NAMESPACE).await?,
        None
    );
    assert_eq!(rows(pool, sandbox).await?, retired);
    Ok(())
}

/// Whether some backend waits on a lock `holder_pid` holds.
async fn blocked_by(pool: &PgPool, holder_pid: i32) -> TestResult<bool> {
    Ok(sqlx::query_scalar::<_, bool>(sql!(
        "SELECT EXISTS (
             SELECT 1 FROM pg_stat_activity
              WHERE datname = current_database()
                AND $1 = ANY(pg_blocking_pids(pid))
         )"
    ))
    .bind(holder_pid)
    .fetch_one(pool)
    .await?)
}

/// One claim as the caller runs it: the sandbox lock, the minter, then the
/// insert, in one transaction.
async fn claim_under_sandbox_lock(
    pool: PgPool,
    sandbox: SandboxId,
) -> TestResult<(i64, Option<i64>)> {
    let mut tx = pool.begin().await?;
    assert_eq!(row_locks::lock_id(&mut tx, sandbox).await?, Some(sandbox));
    let generation = mint_generation(&mut tx, sandbox).await?;
    let claimed = insert_creating_claim(
        &mut *tx,
        sandbox,
        generation,
        &format!("pod-g{generation}"),
        NAMESPACE,
    )
    .await?;
    tx.commit().await?;
    Ok((generation, claimed.map(|record| record.generation)))
}

#[tokio::test(flavor = "current_thread")]
async fn two_claims_under_one_sandbox_lock_cannot_both_go_live() -> TestResult {
    // The first claim's transaction, the second claim and the wait probe.
    let Some(db) = try_pg_test_db_with_max_connections(3).await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let sandbox = insert_sandbox(pool).await?;

    let mut first = pool.begin().await?;
    let first_pid: i32 = sqlx::query_scalar(sql!("SELECT pg_backend_pid()"))
        .fetch_one(&mut *first)
        .await?;
    assert_eq!(
        row_locks::lock_id(&mut first, sandbox).await?,
        Some(sandbox)
    );
    let generation = mint_generation(&mut first, sandbox).await?;
    assert_eq!(generation, 1);

    // The second claim queues on the sandbox lock before it mints.
    let second = tokio::spawn(claim_under_sandbox_lock(pool.clone(), sandbox));
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    while !blocked_by(pool, first_pid).await? {
        assert!(!second.is_finished(), "the second claim never waited");
        assert!(
            std::time::Instant::now() < deadline,
            "the second claim never waited"
        );
        tokio::task::yield_now().await;
    }

    let claimed = insert_creating_claim(&mut *first, sandbox, generation, "pod-g1", NAMESPACE)
        .await?
        .map(|record| record.generation);
    assert_eq!(claimed, Some(1));
    first.commit().await?;

    // Once the first commits, the second mints past it and its insert finds
    // the first claim live.
    let (second_generation, second_claimed) = second.await??;
    assert_eq!(second_generation, 2);
    assert_eq!(second_claimed, None);
    assert_eq!(
        rows(pool, sandbox).await?,
        vec![(1, "creating".to_string(), true, None, None, false, false)]
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn reset_to_creating_spares_a_routable_or_reowned_execution() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let attempt = SandboxActivationAttemptId::new();
    let other_attempt = SandboxActivationAttemptId::new();
    let created = Seed {
        pod_uid: Some("uid-1"),
        activation_attempt_id: Some(attempt),
        ..Seed::active(2, "created")
    };

    let sandbox = insert_sandbox(pool).await?;
    insert_execution(pool, sandbox, &created).await?;
    let before = rows(pool, sandbox).await?;
    // Another attempt, another generation, or a missing attempt refuses it.
    assert!(!reset_to_creating(pool, sandbox, 2, Some(other_attempt)).await?);
    assert!(!reset_to_creating(pool, sandbox, 2, None).await?);
    assert!(!reset_to_creating(pool, sandbox, 1, Some(attempt)).await?);
    assert_eq!(rows(pool, sandbox).await?, before);

    // The matching claim resets, and the UID it cleared leaves its stamp.
    assert!(reset_to_creating(pool, sandbox, 2, Some(attempt)).await?);
    assert_eq!(
        rows(pool, sandbox).await?,
        vec![(2, "creating".to_string(), true, None, None, false, true)]
    );

    // A routable execution, a running one and a retired one keep their rows.
    for seed in [
        Seed {
            pod_ip: Some("10.0.0.5"),
            ..created.clone()
        },
        Seed {
            state: "running",
            ..created.clone()
        },
        Seed {
            active: false,
            ..created.clone()
        },
    ] {
        let sandbox = insert_sandbox(pool).await?;
        insert_execution(pool, sandbox, &seed).await?;
        let before = rows(pool, sandbox).await?;
        assert!(
            !reset_to_creating(pool, sandbox, 2, Some(attempt)).await?,
            "{seed:?}"
        );
        assert_eq!(rows(pool, sandbox).await?, before, "{seed:?}");
    }
    Ok(())
}
