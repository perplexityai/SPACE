//! Seeding rollouts for tests, here and in the crates that drive them.
//! Compiled for this crate's tests and behind the `test-support` feature;
//! enable it only from `[dev-dependencies]`.

use std::time::Duration;

use pplx_space_accounts::NodeId;
use pplx_space_postgres_pool::{Db, DbError, Executor, Tx};
use pplx_space_sql_locator::sql;

use crate::RolloutId;

/// A rollout header in `phase`, with the revision it references. The schema
/// admits one unfinished header per database. It takes the pool, `&Db` (or
/// the driver's pool during the transition).
pub async fn rollout(pool: impl Executor<'_> + Copy, phase: &str) -> Result<RolloutId, DbError> {
    let revision = uuid::Uuid::now_v7();
    sqlx::query(sql!(
        "INSERT INTO firewall_rules_revisions (id, ruleset_format_version, ruleset)
         VALUES ($1, 1, '[]'::jsonb)"
    ))
    .bind(revision)
    .execute(pool.into_sqlx())
    .await?;
    let id = uuid::Uuid::now_v7();
    sqlx::query(sql!(
        "INSERT INTO firewall_rules_rollouts (
             id, to_revision_id, phase, unfinished_slot, reason, finished_at
         )
         VALUES ($1, $2, $3,
                 CASE WHEN $3 IN ('completed', 'aborted') THEN NULL ELSE 1 END,
                 'test rollout',
                 CASE WHEN $3 IN ('completed', 'aborted') THEN now() END)"
    ))
    .bind(id)
    .bind(revision)
    .bind(phase)
    .execute(pool.into_sqlx())
    .await?;
    Ok(RolloutId(id))
}

/// Move `rollout` to `phase`, keeping the slot and finish time consistent.
pub async fn set_phase(
    executor: impl Executor<'_>,
    rollout: RolloutId,
    phase: &str,
) -> Result<(), DbError> {
    sqlx::query(sql!(
        "UPDATE firewall_rules_rollouts
         SET phase = $2,
             unfinished_slot = CASE WHEN $2 IN ('completed', 'aborted') THEN NULL ELSE 1 END,
             finished_at = CASE WHEN $2 IN ('completed', 'aborted') THEN now() END
         WHERE id = $1"
    ))
    .bind(rollout)
    .bind(phase)
    .execute(executor.into_sqlx())
    .await?;
    Ok(())
}

/// Save `nodes` as `rollout`'s pending targets. It takes the pool, as
/// [`rollout`] does.
pub async fn save_targets(
    pool: impl Executor<'_> + Copy,
    rollout: RolloutId,
    nodes: &[NodeId],
) -> Result<(), DbError> {
    for node in nodes {
        sqlx::query(sql!(
            "INSERT INTO firewall_rules_rollout_items (rollout_id, node_id, phase)
             VALUES ($1, $2, 'pending')"
        ))
        .bind(rollout)
        .bind(*node)
        .execute(pool.into_sqlx())
        .await?;
    }
    Ok(())
}

/// Make the checks of `rollout`'s issued items due.
pub async fn make_checks_due(
    executor: impl Executor<'_>,
    rollout: RolloutId,
) -> Result<(), DbError> {
    sqlx::query(sql!(
        "UPDATE firewall_rules_rollout_items SET next_check_at = now() - interval '1 second'
         WHERE rollout_id = $1 AND issued_at IS NOT NULL"
    ))
    .bind(rollout)
    .execute(executor.into_sqlx())
    .await?;
    Ok(())
}

/// Lock `rollout`'s header in an open transaction, so claims queue behind it.
pub async fn hold_header(db: &Db, rollout: RolloutId) -> Result<Tx<'static>, DbError> {
    let mut tx = db.begin().await?;
    sqlx::query(sql!(
        "SELECT 1 FROM firewall_rules_rollouts WHERE id = $1 FOR UPDATE"
    ))
    .bind(rollout)
    .execute((&mut tx).into_sqlx())
    .await?;
    Ok(tx)
}

/// Wait, for at most ten seconds, until `count` sessions on this test
/// database wait for a lock: each racing statement is then known to be
/// blocked rather than not started yet. It takes the pool, as [`rollout`]
/// does.
pub async fn wait_for_lock_waiters(
    pool: impl Executor<'_> + Copy,
    count: i64,
) -> Result<(), DbError> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let waiting = sqlx::query_scalar::<_, i64>(sql!(
            "SELECT count(*)
             FROM pg_stat_activity
             WHERE datname = current_database()
               AND wait_event_type = 'Lock'"
        ))
        .fetch_one(pool.into_sqlx())
        .await?;
        if waiting >= count {
            return Ok(());
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "{waiting} of {count} sessions started waiting for a lock"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}
