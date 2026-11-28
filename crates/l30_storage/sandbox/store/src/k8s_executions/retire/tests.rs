//! Each retire write ends the active execution only while its guard still
//! matches, and a claim goes live only after the retire that precedes it.

use pplx_space_db_test_fixtures::{SEED_ORG_ID, SEED_USER_ID, try_pg_test_db};
use pplx_space_sandbox_model::{SandboxActivationAttemptId, SandboxId};
use pplx_space_sql_locator::sql;
use pplx_space_test_env::TestResult;
use sqlx::PgPool;

use super::{
    ExecutionFence, abandon_unmaterialized_create_claim, finish_active, finish_fenced,
    finish_generation, finish_generation_attempt, finish_generation_pod_uid,
};
use crate::k8s_executions::claim;

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

#[derive(Debug, Clone)]
struct Seed {
    generation: i64,
    state: &'static str,
    active: bool,
    pod_uid: Option<&'static str>,
    pod_ip: Option<&'static str>,
    activation_attempt_id: Option<SandboxActivationAttemptId>,
    route_activation_id: Option<SandboxActivationAttemptId>,
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
            route_activation_id: None,
        }
    }
}

/// A sandbox holding the single execution `seed` describes.
async fn insert_with_execution(pool: &PgPool, seed: &Seed) -> TestResult<SandboxId> {
    let sandbox_id = insert_sandbox(pool).await?;
    sqlx::query(sql!(
        "INSERT INTO sandbox_k8s_executions
            (sandbox_id, generation, pod_name, pod_namespace, state, active,
             pod_uid, pod_ip, activation_attempt_id, route_activation_id)
         VALUES ($1, $2, $3, 'space-sandboxes', $4, $5, $6, $7::INET, $8, $9)"
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
    .bind(seed.route_activation_id)
    .execute(pool)
    .await?;
    Ok(sandbox_id)
}

/// Every execution row of the sandbox, oldest generation first: generation,
/// state, active, pod IP and whether it is terminated.
type Row = (i64, String, bool, Option<String>, bool);

async fn rows(pool: &PgPool, sandbox_id: SandboxId) -> TestResult<Vec<Row>> {
    Ok(sqlx::query_as::<_, Row>(sql!(
        "SELECT generation, state, active, host(pod_ip), terminated_at IS NOT NULL
           FROM sandbox_k8s_executions
          WHERE sandbox_id = $1
          ORDER BY generation"
    ))
    .bind(sandbox_id)
    .fetch_all(pool)
    .await?)
}

fn retired(generation: i64, state: &str) -> Vec<Row> {
    vec![(generation, state.to_string(), false, None, true)]
}

#[tokio::test(flavor = "current_thread")]
async fn finish_active_retires_only_an_active_execution() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let running = Seed {
        pod_ip: Some("10.0.0.5"),
        ..Seed::active(3, "running")
    };

    let inactive = insert_with_execution(
        pool,
        &Seed {
            active: false,
            ..running.clone()
        },
    )
    .await?;
    let before = rows(pool, inactive).await?;
    assert!(!finish_active(pool, inactive, "stopped").await?);
    assert_eq!(rows(pool, inactive).await?, before);

    let sandbox = insert_with_execution(pool, &running).await?;
    assert!(finish_active(pool, sandbox, "stopped").await?);
    assert_eq!(rows(pool, sandbox).await?, retired(3, "stopped"));
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn finish_fenced_spares_an_execution_that_differs_in_any_fenced_field() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let attempt = SandboxActivationAttemptId::new();
    let route = SandboxActivationAttemptId::new();
    let seed = Seed {
        // A blank UID reads as `NULL`.
        pod_uid: Some(" "),
        pod_ip: Some("10.0.0.5"),
        activation_attempt_id: Some(attempt),
        route_activation_id: Some(route),
        ..Seed::active(2, "running")
    };
    let observed = ExecutionFence {
        generation: Some(2),
        activation_attempt_id: Some(attempt),
        pod_uid: None,
        route_activation_id: Some(route),
    };

    let sandbox = insert_with_execution(pool, &seed).await?;
    let before = rows(pool, sandbox).await?;
    let other = SandboxActivationAttemptId::new();
    for fence in [
        ExecutionFence {
            generation: Some(1),
            ..observed
        },
        ExecutionFence {
            generation: None,
            ..observed
        },
        ExecutionFence {
            activation_attempt_id: Some(other),
            ..observed
        },
        ExecutionFence {
            activation_attempt_id: None,
            ..observed
        },
        ExecutionFence {
            pod_uid: Some("uid-successor"),
            ..observed
        },
        ExecutionFence {
            route_activation_id: None,
            ..observed
        },
    ] {
        assert!(
            !finish_fenced(pool, sandbox, fence, "stopped").await?,
            "{fence:?}"
        );
        assert_eq!(rows(pool, sandbox).await?, before, "{fence:?}");
    }

    // A retired execution that matches every field is left as it is.
    let inactive = insert_with_execution(
        pool,
        &Seed {
            active: false,
            ..seed.clone()
        },
    )
    .await?;
    let inactive_before = rows(pool, inactive).await?;
    assert!(!finish_fenced(pool, inactive, observed, "stopped").await?);
    assert_eq!(rows(pool, inactive).await?, inactive_before);

    assert!(finish_fenced(pool, sandbox, observed, "stopped").await?);
    assert_eq!(rows(pool, sandbox).await?, retired(2, "stopped"));
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn finish_generation_spares_another_generation() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let sandbox = insert_with_execution(pool, &Seed::active(2, "running")).await?;
    let before = rows(pool, sandbox).await?;

    assert!(!finish_generation(pool, sandbox, 1, "replaced").await?);
    assert_eq!(rows(pool, sandbox).await?, before);

    // A retired execution at that generation is left as it is.
    let inactive = insert_with_execution(
        pool,
        &Seed {
            active: false,
            ..Seed::active(2, "running")
        },
    )
    .await?;
    let inactive_before = rows(pool, inactive).await?;
    assert!(!finish_generation(pool, inactive, 2, "replaced").await?);
    assert_eq!(rows(pool, inactive).await?, inactive_before);

    assert!(finish_generation(pool, sandbox, 2, "replaced").await?);
    assert_eq!(rows(pool, sandbox).await?, retired(2, "replaced"));
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn finish_generation_attempt_spares_an_execution_another_attempt_holds() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let attempt = SandboxActivationAttemptId::new();
    let other = SandboxActivationAttemptId::new();

    // An execution holding no attempt matches none.
    let unheld = insert_with_execution(pool, &Seed::active(2, "creating")).await?;
    let before = rows(pool, unheld).await?;
    assert!(!finish_generation_attempt(pool, unheld, 2, attempt, "failed").await?);
    assert_eq!(rows(pool, unheld).await?, before);

    let sandbox = insert_with_execution(
        pool,
        &Seed {
            activation_attempt_id: Some(attempt),
            ..Seed::active(2, "creating")
        },
    )
    .await?;
    let before = rows(pool, sandbox).await?;
    assert!(!finish_generation_attempt(pool, sandbox, 2, other, "failed").await?);
    assert!(!finish_generation_attempt(pool, sandbox, 1, attempt, "failed").await?);
    assert_eq!(rows(pool, sandbox).await?, before);

    // A retired execution the attempt still holds is left as it is.
    let inactive = insert_with_execution(
        pool,
        &Seed {
            active: false,
            activation_attempt_id: Some(attempt),
            ..Seed::active(2, "creating")
        },
    )
    .await?;
    let inactive_before = rows(pool, inactive).await?;
    assert!(!finish_generation_attempt(pool, inactive, 2, attempt, "failed").await?);
    assert_eq!(rows(pool, inactive).await?, inactive_before);

    assert!(finish_generation_attempt(pool, sandbox, 2, attempt, "failed").await?);
    assert_eq!(rows(pool, sandbox).await?, retired(2, "failed"));
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn finish_generation_pod_uid_spares_same_generation_successor_uid() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;

    let sandbox = insert_with_execution(
        pool,
        &Seed {
            pod_uid: Some("uid-successor"),
            pod_ip: Some("10.0.0.5"),
            ..Seed::active(2, "running")
        },
    )
    .await?;
    let before = rows(pool, sandbox).await?;
    assert!(!finish_generation_pod_uid(pool, sandbox, 2, Some("uid-stale"), "forgotten").await?);
    assert!(!finish_generation_pod_uid(pool, sandbox, 2, None, "forgotten").await?);
    assert!(!finish_generation_pod_uid(pool, sandbox, 2, Some(" "), "forgotten").await?);
    assert!(
        !finish_generation_pod_uid(pool, sandbox, 1, Some("uid-successor"), "forgotten").await?
    );
    assert_eq!(rows(pool, sandbox).await?, before);

    assert!(finish_generation_pod_uid(pool, sandbox, 2, Some("uid-successor"), "forgotten").await?);
    assert_eq!(rows(pool, sandbox).await?, retired(2, "forgotten"));

    // Blank stored and expected UIDs both normalize to `NULL` and match only
    // an unmaterialized UID.
    let blank = insert_with_execution(
        pool,
        &Seed {
            pod_uid: Some(" "),
            ..Seed::active(3, "created")
        },
    )
    .await?;
    assert!(!finish_generation_pod_uid(pool, blank, 3, Some("uid-1"), "forgotten").await?);
    assert!(finish_generation_pod_uid(pool, blank, 3, Some(" "), "forgotten").await?);
    assert_eq!(rows(pool, blank).await?, retired(3, "forgotten"));
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn abandoning_a_create_claim_spares_a_materialized_or_reowned_execution() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let attempt = SandboxActivationAttemptId::new();
    let claim = Seed {
        activation_attempt_id: Some(attempt),
        ..Seed::active(2, "creating")
    };

    for (seed, generation, activation_attempt_id) in [
        (
            Seed {
                pod_uid: Some("uid-2"),
                ..claim.clone()
            },
            2,
            Some(attempt),
        ),
        (
            Seed {
                pod_ip: Some("10.0.0.5"),
                ..claim.clone()
            },
            2,
            Some(attempt),
        ),
        (
            Seed {
                state: "created",
                ..claim.clone()
            },
            2,
            Some(attempt),
        ),
        (claim.clone(), 2, Some(SandboxActivationAttemptId::new())),
        (claim.clone(), 2, None),
        (claim.clone(), 1, Some(attempt)),
        (
            Seed {
                active: false,
                ..claim.clone()
            },
            2,
            Some(attempt),
        ),
    ] {
        let sandbox = insert_with_execution(pool, &seed).await?;
        let before = rows(pool, sandbox).await?;
        assert!(
            !abandon_unmaterialized_create_claim(pool, sandbox, generation, activation_attempt_id)
                .await?,
            "{seed:?} at {generation} under {activation_attempt_id:?}"
        );
        assert_eq!(rows(pool, sandbox).await?, before, "{seed:?}");
    }

    let sandbox = insert_with_execution(pool, &claim).await?;
    assert!(abandon_unmaterialized_create_claim(pool, sandbox, 2, Some(attempt)).await?);
    assert_eq!(rows(pool, sandbox).await?, retired(2, "replaced"));
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn a_replacement_claim_goes_live_only_after_the_retire_before_it() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let sandbox = insert_with_execution(pool, &Seed::active(1, "running")).await?;

    // A retire that loses leaves the old execution live, and the
    // `(sandbox_id) WHERE active` unique index refuses the claim after it.
    let mut tx = pool.begin().await?;
    assert!(!finish_generation(&mut *tx, sandbox, 7, "replaced").await?);
    let generation = claim::mint_generation(&mut *tx, sandbox).await?;
    assert_eq!(
        claim::insert_creating_claim(&mut *tx, sandbox, generation, "pod-g2", "space-sandboxes")
            .await?,
        None
    );
    tx.rollback().await?;

    let mut tx = pool.begin().await?;
    assert!(finish_generation(&mut *tx, sandbox, 1, "replaced").await?);
    let generation = claim::mint_generation(&mut *tx, sandbox).await?;
    let claimed =
        claim::insert_creating_claim(&mut *tx, sandbox, generation, "pod-g2", "space-sandboxes")
            .await?;
    assert_eq!(claimed.map(|record| record.generation), Some(2));
    tx.commit().await?;
    assert_eq!(
        rows(pool, sandbox).await?,
        vec![
            (1, "replaced".to_string(), false, None, true),
            (2, "creating".to_string(), true, None, false),
        ]
    );
    Ok(())
}
