use pplx_space_common_core::types::OrgId;
use std::time::Duration;

use pplx_space_common_core::sql;
use pplx_space_common_core::types::NodeGroupName;
use pplx_space_db_test_fixtures::{TestDb, try_pg_test_db, try_pg_test_db_with_max_connections};
use uuid::Uuid;

use super::super::{DefaultNodeGroup, EnsureDefaultGroupError, EnsuredDefaultGroup, NodeGroupSink};
use super::NodeGroupDbStore;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

fn name(value: &str) -> NodeGroupName {
    NodeGroupName::parse(value).expect("test fixture name is a valid node-group name")
}

/// Unwrap an [`EnsuredDefaultGroup`] that must be the INSERT. The variant is
/// what gates the seed's `space.admin.audit` entry, so every call site in this
/// file states which one it expects instead of reaching past it.
#[track_caller]
fn seeded(ensured: EnsuredDefaultGroup) -> DefaultNodeGroup {
    match ensured {
        EnsuredDefaultGroup::Seeded(group) => group,
        EnsuredDefaultGroup::Existing(group) => {
            panic!("expected the seeding INSERT, got an existing group: {group:?}")
        }
    }
}

/// Unwrap an [`EnsuredDefaultGroup`] that must be a read of an existing row.
#[track_caller]
fn existing(ensured: EnsuredDefaultGroup) -> DefaultNodeGroup {
    match ensured {
        EnsuredDefaultGroup::Existing(group) => group,
        EnsuredDefaultGroup::Seeded(group) => {
            panic!("expected an existing group, got a seeding INSERT: {group:?}")
        }
    }
}

/// Grab a migrated test database. `Ok(None)` when Docker/Postgres is
/// unavailable (test skips), matching the shared-fixture contract.
///
/// The `node_groups` table comes from the real migration
/// (`db/postgres/20261002070625_baseline.up.sql`), which the
/// shared fixture applies — so these tests run against production's schema,
/// including its `idx_node_groups_single_default` expression index. A
/// test must not layer a stand-in schema on top: an index declared here under
/// a different name than the migration's would not be deduped by
/// `IF NOT EXISTS`, leaving test databases with a schema production doesn't
/// have (and, for the single-default index, a *partial* one, the shape
/// `scripts/check_postgres_partial_indexes.py` bans).
async fn node_group_test_db() -> TestResult<Option<TestDb>> {
    try_pg_test_db().await
}

/// Seeding is idempotent: the first call creates the default generic
/// row with the requested name and the documented shape; the second
/// call returns the same row instead of inserting another.
#[tokio::test(flavor = "current_thread")]
async fn ensure_seeds_default_group_once() -> TestResult {
    let Some(db) = node_group_test_db().await? else {
        return Ok(());
    };
    let store = NodeGroupDbStore::new(db.pool.clone());

    // First call inserts, so it reports `Seeded` — the variant the tick reads
    // to decide whether to emit the admin audit entry.
    let first = seeded(store.ensure_default_group(&name("workers")).await?);
    assert_eq!(first.name.as_str(), "workers");
    // UUIDv7, not v4: the id is application-generated and the column has no
    // default, so nothing but this asserts the version the module docs and the
    // metrics catalog both promise. A v4 id passes every other assertion here.
    assert_eq!(
        first.id.as_uuid().get_version_num(),
        7,
        "the seeded group id must be a UUIDv7"
    );

    let (org_id, template_policy, is_default): (Option<OrgId>, String, bool) = sqlx::query_as(
        sql!("SELECT org_id, template_policy, is_default FROM node_groups WHERE id = $1"),
    )
    .bind(first.id)
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(org_id, None);
    assert_eq!(template_policy, "default");
    assert!(is_default);

    // Second call reads the row it just wrote: `Existing`, not `Seeded`. This
    // is the steady-state tick, and reporting `Seeded` here would audit the
    // same seed on every tick forever.
    let second = existing(store.ensure_default_group(&name("workers")).await?);
    assert_eq!(second, first);

    let count: i64 = sqlx::query_scalar(sql!("SELECT COUNT(*) FROM node_groups"))
        .fetch_one(&db.pool)
        .await?;
    assert_eq!(count, 1);
    Ok(())
}

/// An existing default row wins over the configured seed name: the
/// stored name is the source of truth once seeded.
#[tokio::test(flavor = "current_thread")]
async fn ensure_returns_existing_default_under_different_name() -> TestResult {
    let Some(db) = node_group_test_db().await? else {
        return Ok(());
    };
    let store = NodeGroupDbStore::new(db.pool.clone());

    let inserted = seeded(store.ensure_default_group(&name("shared-pool")).await?);
    assert_eq!(inserted.name.as_str(), "shared-pool");

    let resolved = existing(store.ensure_default_group(&name("workers")).await?);
    assert_eq!(resolved, inserted);
    Ok(())
}

/// No default exists but other groups do: seeding is refused outright, before
/// the name is even considered. Creating a default here would retroactively
/// change what every `NULL` `node_group_id` resolves to, cluster-wide.
///
/// This is also why `NameConflict` is now only reachable via a race — a name
/// collision implies either a non-default group (caught here) or a default one
/// (found by the initial SELECT), so it takes another writer inserting between
/// this census and the INSERT. That routing is covered at the tick level by
/// `t24` instead.
#[tokio::test(flavor = "current_thread")]
async fn ensure_refuses_to_seed_when_other_groups_exist() -> TestResult {
    let Some(db) = node_group_test_db().await? else {
        return Ok(());
    };
    sqlx::query(sql!(
        "INSERT INTO node_groups (id, name, org_id, template_policy, is_default)
         VALUES ($1, 'workers', NULL, 'default', FALSE)"
    ))
    .bind(Uuid::now_v7())
    .execute(&db.pool)
    .await?;

    let store = NodeGroupDbStore::new(db.pool.clone());
    let err = store
        .ensure_default_group(&name("workers"))
        .await
        .expect_err("seeding must be refused while other groups exist");
    assert!(matches!(
        err,
        EnsureDefaultGroupError::SeedBlockedByExistingGroups { other_groups } if other_groups == 1
    ));
    // Nothing was inserted: the pre-existing non-default row is still the
    // only one, and no default was created.
    let (total, defaults): (i64, i64) = sqlx::query_as(sql!(
        "SELECT COUNT(*), COUNT(*) FILTER (WHERE is_default) FROM node_groups"
    ))
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(total, 1);
    assert_eq!(defaults, 0);
    Ok(())
}

/// The slug grammar shared by the table CHECK, the config validation,
/// and the label value.
/// The `NodeGroupName` grammar and the `node_groups.name` CHECK must accept
/// exactly the same set: a name that parses but violates the CHECK fails the
/// seed INSERT at runtime, and one the CHECK allows but the type rejects
/// blocks a legitimately-stored group from ever being labeled.
///
/// This drives the real table rather than restating the regex — an INSERT per
/// case, expecting SQLSTATE 23514 (check_violation) exactly when
/// `NodeGroupName::parse` rejects. Restating the pattern here would pass even
/// if the migration's CHECK were changed underneath it, which is the whole
/// divergence this is meant to catch.
#[tokio::test(flavor = "current_thread")]
async fn name_grammar_agrees_with_the_table_check() -> TestResult {
    let Some(db) = node_group_test_db().await? else {
        return Ok(());
    };
    let sixty_three = "a".repeat(63);
    let sixty_four = "a".repeat(64);
    let cases = [
        // Accepted by both.
        "workers",
        "a",
        "a1",
        "org-acme-1",
        sixty_three.as_str(),
        // Rejected by both.
        "Workers",
        "-workers",
        "workers-",
        "work_ers",
        "work ers",
        "",
        sixty_four.as_str(),
    ];

    for case in cases {
        let parsed_ok = NodeGroupName::parse(case).is_ok();
        let insert = sqlx::query(sql!(
            "INSERT INTO node_groups (id, name, org_id, template_policy, is_default)
             VALUES ($1, $2, NULL, 'default', FALSE)"
        ))
        .bind(Uuid::now_v7())
        .bind(case)
        .execute(&db.pool)
        .await;

        match insert {
            Ok(_) => assert!(
                parsed_ok,
                "the node_groups.name CHECK accepted {case:?} but NodeGroupName::parse \
                 rejected it"
            ),
            Err(err) => {
                let violated_check = err
                    .as_database_error()
                    .and_then(|db_err| db_err.code())
                    .is_some_and(|code| code == "23514");
                assert!(
                    violated_check,
                    "inserting {case:?} failed for an unexpected reason: {err}"
                );
                assert!(
                    !parsed_ok,
                    "NodeGroupName::parse accepted {case:?} but the node_groups.name \
                     CHECK rejected it"
                );
            }
        }
    }
    Ok(())
}

/// The labeling gate's census counts only groups that are *not* the default,
/// so a freshly-seeded cluster (default group alone) reports zero and
/// labeling is allowed; every additional group closes the gate.
#[tokio::test(flavor = "current_thread")]
async fn non_default_group_count_excludes_the_default() -> TestResult {
    let Some(db) = node_group_test_db().await? else {
        return Ok(());
    };
    let store = NodeGroupDbStore::new(db.pool.clone());

    // Nothing seeded yet.
    assert_eq!(store.non_default_group_count().await?, 0);

    // The default group itself must not count against the gate.
    store.ensure_default_group(&name("workers")).await?;
    assert_eq!(store.non_default_group_count().await?, 0);

    // A generic (org-less) non-default group counts.
    sqlx::query(sql!(
        "INSERT INTO node_groups (id, name, org_id, template_policy, is_default)
         VALUES ($1, 'gpu-pool', NULL, 'default', FALSE)"
    ))
    .bind(Uuid::now_v7())
    .execute(&db.pool)
    .await?;
    assert_eq!(store.non_default_group_count().await?, 1);

    // So does an org-owned dedicated group. `node_groups.org_id` carries a
    // real FK to `organizations`, so the org has to exist first.
    let org_id = OrgId::new();
    sqlx::query(sql!(
        "INSERT INTO organizations (id, name, slug) VALUES ($1, 'acme', 'acme')"
    ))
    .bind(org_id)
    .execute(&db.pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO node_groups (id, name, org_id, template_policy, is_default)
         VALUES ($1, 'acme-dedicated', $2, 'restricted', FALSE)"
    ))
    .bind(Uuid::now_v7())
    .bind(org_id)
    .execute(&db.pool)
    .await?;
    assert_eq!(store.non_default_group_count().await?, 2);
    Ok(())
}

/// Two callers seeding at once converge on one group. Which branch the loser
/// takes isn't pinned — `tokio::join!` gives no ordering guarantee, so it may
/// return from the lock-free fast path rather than the re-read-under-the-lock
/// one. What is pinned, either way, is the invariant that matters: exactly one
/// row, and both callers resolving the same group.
#[tokio::test(flavor = "current_thread")]
async fn concurrent_seeds_converge_on_one_group() -> TestResult {
    let Some(db) = node_group_test_db().await? else {
        return Ok(());
    };
    let left = NodeGroupDbStore::new(db.pool.clone());
    let right = NodeGroupDbStore::new(db.pool.clone());

    let seed = name("workers");
    let (a, b) = tokio::join!(
        left.ensure_default_group(&seed),
        right.ensure_default_group(&seed),
    );
    let (a, b) = (a?, b?);
    // Exactly one caller performed the INSERT, and only that one reports
    // `Seeded`. The loser either took the lock-free fast path or re-read under
    // the lock; both are `Existing`, so two replicas racing produce exactly one
    // audit entry rather than two.
    let seeds = [&a, &b]
        .into_iter()
        .filter(|ensured| matches!(ensured, EnsuredDefaultGroup::Seeded(_)))
        .count();
    assert_eq!(
        seeds, 1,
        "exactly one caller may report the seed: {a:?} {b:?}"
    );
    assert_eq!(
        a.clone().into_group(),
        b.clone().into_group(),
        "both callers must resolve the same default group"
    );

    let count: i64 = sqlx::query_scalar(sql!("SELECT COUNT(*) FROM node_groups"))
        .fetch_one(&db.pool)
        .await?;
    assert_eq!(count, 1, "exactly one group row may exist");
    Ok(())
}

/// A conflicting writer must not park the seed indefinitely: the seed
/// transaction's `lock_timeout` bounds the *wait* for the table lock, not just
/// the hold.
///
/// Why the bound is load-bearing rather than tidy. The tick has no Postgres
/// timeout of its own, and the seed's `LOCK TABLE ... IN SHARE ROW EXCLUSIVE
/// MODE` conflicts with the `ROW EXCLUSIVE` every INSERT/UPDATE/DELETE takes. So
/// one writer holding `node_groups` blocks the seed for as long as it likes,
/// which blows `reconcilers::SHUTDOWN_DRAIN_TIMEOUT` (10 s) — and overrunning
/// that makes the controller *abandon* its leader lease rather than resign it,
/// stopping every singleton reconciler in the process until the lease TTL
/// expires. Deleting the statement leaves the rest of this suite green.
///
/// The blocker takes `ROW EXCLUSIVE`, not `ACCESS EXCLUSIVE`: it has to conflict
/// with the seed's table lock while leaving plain `SELECT`s alone. `ACCESS
/// EXCLUSIVE` conflicts with `ACCESS SHARE` too, so it would block the lock-free
/// fast-path read — which has no `lock_timeout` at all — and the call would hang
/// with the guard present, testing nothing. `ROW EXCLUSIVE` is also the mode the
/// writer this lock exists to exclude actually takes.
///
/// The bound's own duration is production's business, not this test's: what
/// has to hold is that the wait ends, so the store is given a short one and
/// the call must come back inside it.
#[tokio::test(flavor = "current_thread")]
async fn seed_lock_wait_is_bounded_rather_than_parking_the_tick() -> TestResult {
    let Some(db) = try_pg_test_db_with_max_connections(3).await? else {
        return Ok(());
    };
    let store =
        NodeGroupDbStore::with_seed_lock_timeout(db.pool.clone(), Duration::from_millis(500));

    // A writer holding the table for the duration of the call.
    let mut blocker = db.pool.begin().await?;
    sqlx::query(sql!("LOCK TABLE node_groups IN ROW EXCLUSIVE MODE"))
        .execute(&mut *blocker)
        .await?;

    // The outer bound only has to be generous enough to distinguish "returned"
    // from "parked": with the guard the call gives up after its own timeout.
    let started = std::time::Instant::now();
    let outcome = tokio::time::timeout(
        Duration::from_secs(60),
        store.ensure_default_group(&name("workers")),
    )
    .await
    .map_err(|_| {
        "ensure_default_group never returned while a conflicting writer held \
         node_groups: the seed's lock_timeout is not bounding the wait"
    })?;
    let elapsed = started.elapsed();

    let err = outcome.expect_err("the seed cannot succeed while the table is locked");
    assert!(
        matches!(err, EnsureDefaultGroupError::Db(_)),
        "a lock timeout is a transient Db failure, not an operator-attention arm: {err:?}"
    );
    assert!(
        elapsed >= Duration::from_millis(500),
        "the seed gave up after {elapsed:?}, before the lock_timeout it set: \
         something other than the bound ended this call"
    );
    assert!(
        elapsed < Duration::from_secs(3),
        "the seed gave up only after {elapsed:?}, far past its own bound"
    );

    // Nothing was written, and the lock is released on rollback.
    drop(blocker);
    let count: i64 = sqlx::query_scalar(sql!("SELECT COUNT(*) FROM node_groups"))
        .fetch_one(&db.pool)
        .await?;
    assert_eq!(count, 0);
    Ok(())
}

/// In the groups-exist-but-no-default state the seed is refused by the lock-free
/// pre-check, **without** taking the table lock.
///
/// `SeedBlockedByExistingGroups` clears only by operator action, so it can hold
/// for days — and the seed's `SHARE ROW EXCLUSIVE` conflicts with the `ROW
/// EXCLUSIVE` every writer takes, so a blocked cluster re-taking it every 30 s
/// would stall the very admin create surface the lock exists to exclude, for as
/// long as the state lasts. The under-lock check is authoritative, which is why
/// deleting the pre-check leaves the rest of this suite green: the *outcome* is
/// unchanged and only the lock traffic differs.
///
/// So the assertion is about the lock, not the variant: a conflicting writer
/// holds `node_groups` throughout, and the call still returns promptly instead of
/// waiting out its own 5 s `lock_timeout`. Without the pre-check it would block
/// on that lock and come back a `Db` error five seconds later.
#[tokio::test(flavor = "current_thread")]
async fn seed_blocked_by_existing_groups_takes_no_table_lock() -> TestResult {
    let Some(db) = try_pg_test_db_with_max_connections(3).await? else {
        return Ok(());
    };
    sqlx::query(sql!(
        "INSERT INTO node_groups (id, name, org_id, template_policy, is_default)
         VALUES ($1, 'gpu-pool', NULL, 'default', FALSE)"
    ))
    .bind(Uuid::now_v7())
    .execute(&db.pool)
    .await?;

    let mut blocker = db.pool.begin().await?;
    sqlx::query(sql!("LOCK TABLE node_groups IN ROW EXCLUSIVE MODE"))
        .execute(&mut *blocker)
        .await?;

    let store = NodeGroupDbStore::new(db.pool.clone());
    let started = std::time::Instant::now();
    let err = store
        .ensure_default_group(&name("workers"))
        .await
        .expect_err("seeding must be refused while other groups exist");
    let elapsed = started.elapsed();

    assert!(
        matches!(
            err,
            EnsureDefaultGroupError::SeedBlockedByExistingGroups { other_groups } if other_groups == 1
        ),
        "expected the blocked arm, got {err:?}"
    );
    // Comfortably inside the seed's own 5s lock_timeout: reaching the lock at all
    // would have cost all of it.
    assert!(
        elapsed < std::time::Duration::from_secs(2),
        "the refusal took {elapsed:?}, which means it queued behind the table lock \
         instead of short-circuiting before `begin()`"
    );
    Ok(())
}
