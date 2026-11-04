use std::collections::BTreeMap;

use pplx_space_common_core::sql;
use pplx_space_common_core::types::{Digest, EnvironmentReference, UserId};
use pplx_space_db_test_fixtures::{try_pg_test_db, try_pg_test_db_with_max_connections};
use pplx_space_test_env::TestResult;
use sqlx::PgPool;
use uuid::Uuid;

use super::*;

/// Seeded by the PostgreSQL migrations.
const SEED_USER: Uuid = Uuid::from_u128(0x10);

fn environment(byte: u8) -> EnvironmentReference {
    EnvironmentReference::new(Digest::sha256([byte; 32]))
}

fn change(reason: &str) -> PolicyChange {
    PolicyChange {
        actor: Some(UserId::from_uuid(SEED_USER)),
        reason: PolicyChangeReason::new(reason).expect("valid reason"),
    }
}

async fn set_default_committed(
    pool: &PgPool,
    environment: EnvironmentReference,
) -> TestResult<u64> {
    let mut tx = pool.begin().await?;
    let generation = set_default(&mut tx, environment, &change("set default")).await?;
    tx.commit().await?;
    Ok(generation)
}

async fn set_default_committed_result(
    pool: &PgPool,
    environment: EnvironmentReference,
) -> Result<u64, SetDefaultError> {
    let mut tx = pool.begin().await?;
    let generation = set_default(&mut tx, environment, &change("set default")).await?;
    tx.commit().await?;
    Ok(generation)
}

async fn set_state_committed(
    pool: &PgPool,
    environment: EnvironmentReference,
    state: RuntimeEnvironmentState,
) -> Result<u64, SetEnvironmentStateError> {
    let mut tx = pool.begin().await?;
    let generation =
        set_environment_state(&mut tx, environment, state, &change("set state")).await?;
    tx.commit().await?;
    Ok(generation)
}

#[tokio::test(flavor = "current_thread")]
async fn bootstrap_policy_overrides_nothing() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let policy = get(&db.pool).await?;
    assert_eq!(
        policy,
        RuntimeEnvironmentPolicy {
            generation: 0,
            default_environment: None,
            legacy_unbound_allowed: None,
            environments: BTreeMap::new(),
        }
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn setting_a_default_allows_it_and_replacing_keeps_the_old_one_allowed() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let (e0, e1) = (environment(0), environment(1));

    assert_eq!(set_default_committed(&db.pool, e0).await?, 1);
    assert_eq!(set_default_committed(&db.pool, e1).await?, 2);

    let policy = get(&db.pool).await?;
    assert_eq!(policy.generation, 2);
    assert_eq!(policy.default_environment, Some(e1));
    assert_eq!(
        policy.environments,
        BTreeMap::from([
            (e0, RuntimeEnvironmentState::Allowed),
            (e1, RuntimeEnvironmentState::Allowed),
        ])
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn setting_a_deprecated_environment_as_default_allows_it() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let e0 = environment(0);
    set_state_committed(&db.pool, e0, RuntimeEnvironmentState::Deprecated).await?;
    set_default_committed(&db.pool, e0).await?;

    let policy = get(&db.pool).await?;
    assert_eq!(policy.environments[&e0], RuntimeEnvironmentState::Allowed);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn blocking_requires_a_default_and_never_targets_it() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let (e0, e1) = (environment(0), environment(1));

    let refused = set_state_committed(&db.pool, e1, RuntimeEnvironmentState::Blocked).await;
    assert!(
        matches!(refused, Err(SetEnvironmentStateError::BlockRequiresDefault(env)) if env == e1),
        "{refused:?}"
    );

    set_default_committed(&db.pool, e0).await?;
    for requested in [
        RuntimeEnvironmentState::Blocked,
        RuntimeEnvironmentState::Deprecated,
    ] {
        let refused = set_state_committed(&db.pool, e0, requested).await;
        assert!(
            matches!(
                refused,
                Err(SetEnvironmentStateError::DefaultMustStayAllowed { environment, requested: r })
                    if environment == e0 && r == requested
            ),
            "{refused:?}"
        );
    }

    assert_eq!(
        set_state_committed(&db.pool, e1, RuntimeEnvironmentState::Blocked).await?,
        2
    );
    let policy = get(&db.pool).await?;
    assert_eq!(policy.environments[&e1], RuntimeEnvironmentState::Blocked);
    assert_eq!(policy.executing().collect::<Vec<_>>(), vec![e0]);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn a_blocked_environment_cannot_become_the_default() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let (e0, e1) = (environment(0), environment(1));
    set_default_committed(&db.pool, e0).await?;
    set_state_committed(&db.pool, e1, RuntimeEnvironmentState::Blocked).await?;

    let mut tx = db.pool.begin().await?;
    let refused = set_default(&mut tx, e1, &change("promote blocked")).await;
    assert!(
        matches!(refused, Err(SetDefaultError::Blocked(env)) if env == e1),
        "{refused:?}"
    );
    tx.rollback().await?;

    assert_eq!(get(&db.pool).await?.default_environment, Some(e0));
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn a_block_waits_for_a_concurrent_default_change_and_sees_it() -> TestResult {
    // One connection per writer plus one to watch the lock queue.
    let Some(db) = try_pg_test_db_with_max_connections(3).await? else {
        return Ok(());
    };
    let (e0, e1) = (environment(0), environment(1));
    set_default_committed(&db.pool, e0).await?;

    let mut first = db.pool.begin().await?;
    set_default(&mut first, e1, &change("move default")).await?;

    let pool = db.pool.clone();
    let blocker = tokio::spawn(async move {
        set_state_committed(&pool, e1, RuntimeEnvironmentState::Blocked).await
    });
    // Hold the first writer open until the second is queued on the policy row
    // lock, so the second checks its rule against the committed default. Each
    // test has its own database, so counting lock waits in it sees only ours.
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        let (waiting,) = sqlx::query_as::<_, (i64,)>(sql!(
            "SELECT count(*) FROM pg_stat_activity
             WHERE datname = current_database()
               AND wait_event_type = 'Lock'
               AND query LIKE '%FOR UPDATE%'"
        ))
        .fetch_one(&db.pool)
        .await?;
        if waiting > 0 {
            break;
        }
        assert!(
            !blocker.is_finished(),
            "the second writer finished without waiting for the first"
        );
        assert!(
            tokio::time::Instant::now() < deadline,
            "the second writer never queued on the policy row lock"
        );
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    first.commit().await?;

    let refused = blocker.await?;
    assert!(
        matches!(
            refused,
            Err(SetEnvironmentStateError::DefaultMustStayAllowed { environment, .. }) if environment == e1
        ),
        "{refused:?}"
    );
    let policy = get(&db.pool).await?;
    assert_eq!(policy.default_environment, Some(e1));
    assert_eq!(policy.environments[&e1], RuntimeEnvironmentState::Allowed);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn a_default_change_waits_for_a_concurrent_block_and_sees_it() -> TestResult {
    // One connection per writer plus one to watch the lock queue.
    let Some(db) = try_pg_test_db_with_max_connections(3).await? else {
        return Ok(());
    };
    let (e0, e1) = (environment(0), environment(1));
    set_default_committed(&db.pool, e0).await?;

    let mut first = db.pool.begin().await?;
    set_environment_state(
        &mut first,
        e1,
        RuntimeEnvironmentState::Blocked,
        &change("block"),
    )
    .await?;

    let pool = db.pool.clone();
    let promoter = tokio::spawn(async move { set_default_committed_result(&pool, e1).await });
    // Hold the block open until the default change is queued on the policy row
    // lock, so it reads the environment's state only after the block commits.
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        let (waiting,) = sqlx::query_as::<_, (i64,)>(sql!(
            "SELECT count(*) FROM pg_stat_activity
             WHERE datname = current_database()
               AND wait_event_type = 'Lock'
               AND query LIKE '%FOR UPDATE%'"
        ))
        .fetch_one(&db.pool)
        .await?;
        if waiting > 0 {
            break;
        }
        assert!(
            !promoter.is_finished(),
            "the default change finished without waiting for the block"
        );
        assert!(
            tokio::time::Instant::now() < deadline,
            "the default change never queued on the policy row lock"
        );
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    first.commit().await?;

    let refused = promoter.await?;
    assert!(
        matches!(refused, Err(SetDefaultError::Blocked(environment)) if environment == e1),
        "{refused:?}"
    );
    let policy = get(&db.pool).await?;
    assert_eq!(policy.default_environment, Some(e0));
    assert_eq!(policy.environments[&e1], RuntimeEnvironmentState::Blocked);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn executing_lists_allowed_and_deprecated() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let (e0, e1, e2) = (environment(0), environment(1), environment(2));
    set_default_committed(&db.pool, e0).await?;
    set_state_committed(&db.pool, e1, RuntimeEnvironmentState::Deprecated).await?;
    set_state_committed(&db.pool, e2, RuntimeEnvironmentState::Blocked).await?;

    let policy = get(&db.pool).await?;
    assert_eq!(policy.executing().collect::<Vec<_>>(), vec![e0, e1]);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn legacy_permission_is_written_and_bumps_the_generation() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let mut tx = db.pool.begin().await?;
    assert_eq!(
        set_legacy_unbound_allowed(&mut tx, false, &change("disable legacy")).await?,
        1
    );
    tx.commit().await?;

    let policy = get(&db.pool).await?;
    assert_eq!(policy.legacy_unbound_allowed, Some(false));
    assert_eq!(policy.generation, 1);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn a_write_composes_into_the_callers_transaction() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let e0 = environment(0);
    let mut tx = db.pool.begin().await?;
    set_default(&mut tx, e0, &change("set default")).await?;
    set_legacy_unbound_allowed(&mut tx, true, &change("allow legacy")).await?;
    assert_eq!(get(&mut *tx).await?.generation, 2);
    tx.rollback().await?;

    let policy = get(&db.pool).await?;
    assert_eq!(policy.generation, 0);
    assert_eq!(policy.default_environment, None);
    assert!(policy.environments.is_empty());
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn writes_record_the_actor_and_reason() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let e0 = environment(0);
    set_default_committed(&db.pool, e0).await?;

    let (policy_actor, policy_reason) = sqlx::query_as::<_, (Option<Uuid>, Option<String>)>(sql!(
        "SELECT updated_by_user_id, reason FROM runtime_environment_policy"
    ))
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(policy_actor, Some(SEED_USER));
    assert_eq!(policy_reason.as_deref(), Some("set default"));

    let (state_actor, state_reason) = sqlx::query_as::<_, (Option<Uuid>, String)>(sql!(
        "SELECT updated_by_user_id, reason FROM runtime_environment_states"
    ))
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(state_actor, Some(SEED_USER));
    assert_eq!(state_reason, "set default");
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn the_bootstrap_record_has_no_change() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let record = get_record(&db.pool).await?;
    assert_eq!(record.policy, get(&db.pool).await?);
    assert_eq!(record.last_change, None);
    assert!(record.environment_changes.is_empty());
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn the_record_reads_back_each_rows_actor_and_reason() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let (e0, e1) = (environment(0), environment(1));
    set_default_committed(&db.pool, e0).await?;
    let mut tx = db.pool.begin().await?;
    let unattributed = PolicyChange {
        actor: None,
        reason: PolicyChangeReason::new("deprecate e1")?,
    };
    set_environment_state(
        &mut tx,
        e1,
        RuntimeEnvironmentState::Deprecated,
        &unattributed,
    )
    .await?;
    tx.commit().await?;

    let record = get_record(&db.pool).await?;
    assert_eq!(record.policy, get(&db.pool).await?);
    let last_change = record.last_change.expect("a written policy has a change");
    assert_eq!(last_change.updated_by, None);
    assert_eq!(last_change.reason, "deprecate e1");

    let e0_change = &record.environment_changes[&e0];
    assert_eq!(e0_change.updated_by, Some(UserId::from_uuid(SEED_USER)));
    assert_eq!(e0_change.reason, "set default");
    let e1_change = &record.environment_changes[&e1];
    assert_eq!(e1_change.updated_by, None);
    assert_eq!(e1_change.reason, "deprecate e1");
    assert_eq!(
        record.environment_changes.keys().collect::<Vec<_>>(),
        record.policy.environments.keys().collect::<Vec<_>>(),
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn a_missing_policy_row_is_an_error_not_the_bootstrap_policy() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    sqlx::query(sql!("DELETE FROM runtime_environment_policy"))
        .execute(&db.pool)
        .await?;

    assert!(matches!(
        get(&db.pool).await,
        Err(GetPolicyError::PolicyRowMissing)
    ));
    let mut tx = db.pool.begin().await?;
    assert!(matches!(
        set_default(&mut tx, environment(0), &change("set default")).await,
        Err(SetDefaultError::PolicyRowMissing)
    ));
    assert!(matches!(
        set_environment_state(
            &mut tx,
            environment(1),
            RuntimeEnvironmentState::Allowed,
            &change("allow")
        )
        .await,
        Err(SetEnvironmentStateError::PolicyRowMissing)
    ));
    assert!(matches!(
        set_legacy_unbound_allowed(&mut tx, true, &change("allow legacy")).await,
        Err(SetLegacyUnboundAllowedError::PolicyRowMissing)
    ));
    tx.rollback().await?;
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn the_default_and_generation_read_past_an_unknown_state() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    assert_eq!(get_default(&db.pool).await?, None);
    let e0 = environment(0);
    set_default_committed(&db.pool, e0).await?;
    sqlx::query(sql!(
        "INSERT INTO runtime_environment_states (environment_ref, state, reason)
         VALUES ($1, 'retired', 'from a newer build')"
    ))
    .bind(environment(1).to_string())
    .execute(&db.pool)
    .await?;
    assert!(matches!(
        get(&db.pool).await,
        Err(GetPolicyError::UnknownState { .. })
    ));
    assert_eq!(get_default(&db.pool).await?, Some(e0));
    assert_eq!(get_generation(&db.pool).await?, 1);

    sqlx::query(sql!("DELETE FROM runtime_environment_policy"))
        .execute(&db.pool)
        .await?;
    assert!(matches!(
        get_default(&db.pool).await,
        Err(GetPolicyError::PolicyRowMissing)
    ));
    assert!(matches!(
        get_generation(&db.pool).await,
        Err(GetPolicyError::PolicyRowMissing)
    ));
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn an_unknown_stored_state_fails_closed() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let e0 = environment(0);
    sqlx::query(sql!(
        "INSERT INTO runtime_environment_states (environment_ref, state, reason)
         VALUES ($1, 'retired', 'written by a newer binary')"
    ))
    .bind(e0.to_string())
    .execute(&db.pool)
    .await?;

    let read = get(&db.pool).await;
    assert!(
        matches!(read, Err(GetPolicyError::UnknownState { environment, .. }) if environment == e0),
        "{read:?}"
    );
    let mut tx = db.pool.begin().await?;
    let refused = set_default(&mut tx, e0, &change("set default")).await;
    assert!(
        matches!(refused, Err(SetDefaultError::UnknownState { environment, .. }) if environment == e0),
        "{refused:?}"
    );
    tx.rollback().await?;

    // Assigning a known state over it is the repair.
    set_state_committed(&db.pool, e0, RuntimeEnvironmentState::Deprecated).await?;
    assert_eq!(
        get(&db.pool).await?.environments[&e0],
        RuntimeEnvironmentState::Deprecated
    );
    Ok(())
}

#[test]
fn reasons_follow_the_schema_bounds() {
    assert_eq!(
        PolicyChangeReason::new(""),
        Err(PolicyChangeReasonError::Empty)
    );
    let longest = "é".repeat(1024);
    assert_eq!(
        PolicyChangeReason::new(longest.clone()).map(|reason| reason.as_str().to_owned()),
        Ok(longest)
    );
    assert_eq!(
        PolicyChangeReason::new("x".repeat(1025)),
        Err(PolicyChangeReasonError::TooLong(1025))
    );
    assert_eq!(
        PolicyChangeReason::new("a\0b"),
        Err(PolicyChangeReasonError::ContainsNul)
    );
}

#[test]
fn states_round_trip_through_their_wire_values() {
    for state in RuntimeEnvironmentState::ALL {
        assert_eq!(state.to_string(), state.as_str());
        assert_eq!(state.as_str().parse(), Ok(state));
    }
    assert_eq!(
        "retired".parse::<RuntimeEnvironmentState>(),
        Err(UnknownRuntimeEnvironmentState("retired".to_owned()))
    );
}
