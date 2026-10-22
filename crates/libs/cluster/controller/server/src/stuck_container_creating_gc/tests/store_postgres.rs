//! The live `PostgresGcStore` sandbox-lifecycle lookup, against a throwaway
//! Postgres.
//!
//! The decision logic is covered in `marker_ownership`; what needs a real
//! database is the half that turns rows into its inputs — that "no row" and
//! "row I can't interpret" stay distinguishable. Getting that wrong inverts the
//! fail-closed direction, so it can't rest on a fake. The warm-pool half is
//! tested in `pplx-space-cluster-pool`, which owns that table and its fixtures.
//!
//! Skips silently when no Docker daemon is reachable, like the other
//! testcontainers suites in this crate.

use std::time::Duration;

use pplx_space_db_test_fixtures::{TestDbResult, try_pg_test_db};

use crate::lifecycle_writer_test_support::{self as seed, ExecutionSeed, SandboxSeed};
use crate::stuck_container_creating_gc::store::STARTED_RECOVERY_LEASE;
use sqlx::PgPool;

use super::*;

async fn insert_sandbox(pool: &PgPool, lifecycle: &str) -> TestDbResult<SandboxId> {
    let sandbox_id = SandboxId::new();
    seed::insert_sandbox(pool, sandbox_id, &SandboxSeed::new(lifecycle, "k8s")).await?;
    Ok(sandbox_id)
}

/// An `active`, `recovering` execution row `age` old — the started-recovery
/// ownership signal that lives outside `sandboxes.lifecycle`.
async fn insert_recovering_execution(
    pool: &PgPool,
    sandbox_id: SandboxId,
    age: Duration,
) -> TestDbResult<()> {
    let execution = ExecutionSeed {
        state: "recovering",
        ..ExecutionSeed::RUNNING
    };
    seed::insert_execution(pool, sandbox_id, execution).await?;
    sqlx::query(
        "UPDATE sandbox_k8s_executions
         SET updated_at = NOW() - make_interval(secs => $2)
         WHERE sandbox_id = $1",
    )
    .bind(sandbox_id)
    .bind(i64::try_from(age.as_secs()).expect("test age fits i64"))
    .execute(pool)
    .await?;
    Ok(())
}

#[tokio::test]
async fn restore_owners_answers_every_requested_id() -> TestDbResult<()> {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let restoring = insert_sandbox(&db.pool, "restoring").await?;
    let started = insert_sandbox(&db.pool, "started").await?;
    let deleted_row = SandboxId::new();
    // A `lifecycle` this build doesn't know, written past the enum to stand in
    // for a value a newer writer introduced.
    let unreadable = insert_sandbox(&db.pool, "started").await?;
    let recovering = insert_sandbox(&db.pool, "started").await?;
    insert_recovering_execution(&db.pool, recovering, Duration::ZERO).await?;
    sqlx::query("UPDATE sandboxes SET lifecycle = 'a_state_from_the_future' WHERE id = $1")
        .bind(unreadable.as_uuid())
        .execute(&db.pool)
        .await?;

    let store = PostgresGcStore::new(db.pool.clone());
    let lookup = store
        .restore_owners(&[restoring, started, deleted_row, unreadable, recovering])
        .await?;

    assert_eq!(
        lookup.get(&restoring),
        Some(&Lookup::Known(Owner::InFlight))
    );
    assert_eq!(lookup.get(&started), Some(&Lookup::Known(Owner::Released)));
    // The one the lifecycle alone gets wrong: a started recovery deliberately
    // leaves the row in `started` and records ownership on a fresh `recovering`
    // execution, so reading `lifecycle` would call this marker stale and delete
    // the pod the recovery is mid-way through creating.
    assert_eq!(
        lookup.get(&recovering),
        Some(&Lookup::Known(Owner::InFlight))
    );
    // No row: provably gone, which releases the pod.
    assert_eq!(lookup.get(&deleted_row), Some(&Lookup::Absent));
    // Row present but uninterpretable: must NOT read as absent, or the pod
    // would be released on an answer we don't actually have.
    assert_eq!(lookup.get(&unreadable), Some(&Lookup::Unknown));

    // An id nobody asked about is unknown by omission — the property the
    // fresh-GET-reveals-a-marker path relies on.
    assert_eq!(lookup.get(&SandboxId::new()), None);

    // Nothing to ask, nothing to answer.
    assert!(store.restore_owners(&[]).await?.is_empty());
    assert!(store.warm_pool_owners(&[]).await?.is_empty());
    Ok(())
}

/// The recovery lease is a *window*, not a flag: an execution row whose
/// `updated_at` has aged past `STARTED_RECOVERY_LEASE` no longer owns the pod,
/// or a crashed recovery would protect its pod forever — exactly the permanent
/// exclusion this whole change exists to remove.
#[tokio::test]
async fn a_stale_recovery_lease_no_longer_owns_the_pod() -> TestDbResult<()> {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let fresh = insert_sandbox(&db.pool, "started").await?;
    let stale = insert_sandbox(&db.pool, "started").await?;
    insert_recovering_execution(&db.pool, fresh, STARTED_RECOVERY_LEASE / 2).await?;
    insert_recovering_execution(&db.pool, stale, STARTED_RECOVERY_LEASE * 2).await?;

    let store = PostgresGcStore::new(db.pool.clone());
    let lookup = store.restore_owners(&[fresh, stale]).await?;

    assert_eq!(lookup.get(&fresh), Some(&Lookup::Known(Owner::InFlight)));
    assert_eq!(lookup.get(&stale), Some(&Lookup::Known(Owner::Released)));
    Ok(())
}

/// Only the *active* execution carries the lease: a superseded `recovering` row
/// left behind by an older generation doesn't own the pod.
#[tokio::test]
async fn an_inactive_recovering_execution_does_not_own_the_pod() -> TestDbResult<()> {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let sandbox_id = insert_sandbox(&db.pool, "started").await?;
    let execution = ExecutionSeed {
        state: "recovering",
        active: false,
        ..ExecutionSeed::RUNNING
    };
    seed::insert_execution(&db.pool, sandbox_id, execution).await?;
    sqlx::query("UPDATE sandbox_k8s_executions SET updated_at = NOW() WHERE sandbox_id = $1")
        .bind(sandbox_id)
        .execute(&db.pool)
        .await?;

    let lookup = PostgresGcStore::new(db.pool.clone())
        .restore_owners(&[sandbox_id])
        .await?;

    assert_eq!(
        lookup.get(&sandbox_id),
        Some(&Lookup::Known(Owner::Released))
    );
    Ok(())
}

/// Only a Kubernetes-managed row can own a pod in this cluster: another
/// substrate's `restoring` row reads as no row at all.
#[tokio::test]
async fn restore_owners_ignores_another_substrates_row() -> TestDbResult<()> {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let sandbox_id = SandboxId::new();
    seed::insert_sandbox(
        &db.pool,
        sandbox_id,
        &SandboxSeed::new("restoring", "space"),
    )
    .await?;

    let lookup = PostgresGcStore::new(db.pool.clone())
        .restore_owners(&[sandbox_id])
        .await?;

    assert_eq!(lookup.get(&sandbox_id), Some(&Lookup::Absent));
    Ok(())
}

/// A pod with no slot row must come back `Absent`, which is what releases it.
/// This pins the pairing of the GC store to `WarmPoolStore`; the row facts are
/// tested in the pool crate and the row-to-owner mapping in `marker_ownership`.
#[tokio::test]
async fn warm_pool_owners_answers_a_pod_with_no_slot_row() -> TestDbResult<()> {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let store = PostgresGcStore::new(db.pool.clone());

    let lookup = store.warm_pool_owners(&["no-slot-pod".to_string()]).await?;

    assert_eq!(lookup.get("no-slot-pod"), Some(&Lookup::Absent));
    Ok(())
}
