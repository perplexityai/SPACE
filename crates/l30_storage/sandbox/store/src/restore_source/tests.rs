//! [`select_restore_source`] against Postgres: capture generation outranks
//! registration time, the pin outranks both, and the retry window bounds
//! which rows can outrank a retry's source.

use pplx_space_accounts::NodeId;
use pplx_space_common_core::sql;
use pplx_space_common_core::types::SandboxId;
use pplx_space_db_test_fixtures::{
    SEED_ORG_ID as SEED_ORG, SEED_USER_ID as SEED_USER, insert_ready_worker_node, try_pg_test_db,
};
use pplx_space_sandbox_portability::{
    fresh_ready_snapshot_exists, latest_full_snapshot_ready, newest_full_snapshot_unambiguous,
    newest_suspend_snapshot_activity_current, newest_suspend_snapshot_current,
    restore_retry_source_snapshot_exists, snapshot_durable_copy_exists,
    snapshot_durable_for_source_release,
};
use pplx_space_test_env::TestResult;
use sqlx::PgPool;
use uuid::Uuid;

use super::{
    RestoreSourceMode, RestoreSourcePlacement, RestoreSourceSelection, select_restore_source,
};

const FRESH: RestoreSourceMode = RestoreSourceMode::PlacementFresh;
const NEWEST: RestoreSourceMode = RestoreSourceMode::NewestFull;
const RETRY: RestoreSourceMode = RestoreSourceMode::Retry;
const NODE_FALLBACK: RestoreSourceMode = RestoreSourceMode::NodeLookup;

/// A sandbox in `lifecycle`, which entered it `lifecycle_minutes_ago`, placed
/// on `node` when one is given.
async fn insert_sandbox(
    pool: &PgPool,
    lifecycle: &str,
    lifecycle_minutes_ago: i32,
    node: Option<NodeId>,
) -> TestResult<Uuid> {
    let id = Uuid::now_v7();
    sqlx::query(sql!(
        "INSERT INTO sandboxes
            (id, user_id, org_id, managed_by, lifecycle, lifecycle_changed_at,
             active_node_id, template_id, network_policy_generation)
         VALUES ($1, $2, $3, 'k8s', $4, NOW() - make_interval(mins => $5::INT),
                 $6, 'tmpl-restore-source-test', 1)"
    ))
    .bind(id)
    .bind(SEED_USER)
    .bind(SEED_ORG)
    .bind(lifecycle)
    .bind(lifecycle_minutes_ago)
    .bind(node)
    .execute(pool)
    .await?;
    Ok(id)
}

async fn pin(pool: &PgPool, sandbox: Uuid, artifact: Uuid) -> TestResult {
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET current_local_artifact_id = $2,
                current_local_artifact_kind = 'full',
                current_local_artifact_origin = 'suspend'
          WHERE id = $1"
    ))
    .bind(sandbox)
    .bind(artifact)
    .execute(pool)
    .await?;
    Ok(())
}

async fn insert_node(pool: &PgPool) -> TestResult<NodeId> {
    let node = NodeId::new();
    insert_ready_worker_node(pool, node).await?;
    Ok(node)
}

/// One backup row of a full artifact.
struct Backup<'a> {
    artifact: Uuid,
    /// `suspend`, or `snapshot` for a user's named snapshot.
    origin: &'a str,
    state: &'a str,
    generation: Option<i64>,
    /// Registered this many minutes ago.
    minutes_ago: i32,
    uri: &'a str,
}

impl<'a> Backup<'a> {
    /// A ready suspend capture of a fresh artifact.
    fn suspend(generation: Option<i64>, minutes_ago: i32, uri: &'a str) -> Self {
        Self {
            artifact: Uuid::now_v7(),
            origin: "suspend",
            state: "ready",
            generation,
            minutes_ago,
            uri,
        }
    }
}

async fn insert_backup(pool: &PgPool, sandbox: Uuid, backup: &Backup<'_>) -> TestResult<Uuid> {
    let snapshot = Uuid::now_v7();
    sqlx::query(sql!(
        "INSERT INTO snapshots (
             id, sandbox_id, type, state, timestamp, local_artifact_id, artifact_kind,
             artifact_origin, artifact_snapshot_name, generation, org_id
         )
         VALUES ($1, $2, 'full', $3, NOW() - make_interval(mins => $4::INT), $5, 'full',
                 $6, CASE WHEN $6 = 'snapshot' THEN 'f-named' END, $7, $8)"
    ))
    .bind(snapshot)
    .bind(sandbox)
    .bind(backup.state)
    .bind(backup.minutes_ago)
    .bind(backup.artifact)
    .bind(backup.origin)
    .bind(backup.generation)
    .bind(SEED_ORG)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO snapshot_storage_locations (snapshot_id, storage_connection_string, org_id)
         VALUES ($1, $2, $3)"
    ))
    .bind(snapshot)
    .bind(backup.uri)
    .bind(SEED_ORG)
    .execute(pool)
    .await?;
    Ok(snapshot)
}

async fn selected_uri(
    pool: &PgPool,
    sandbox: Uuid,
    rule: RestoreSourceMode,
    placement: RestoreSourcePlacement<'_>,
) -> TestResult<Option<String>> {
    Ok(
        select_restore_source(pool, SandboxId::from_uuid(sandbox), rule, placement)
            .await?
            .selected()
            .map(|selected| selected.storage_connection_string),
    )
}

/// Suspends A, B, D and E captured generations 1, 2, 4 and 5. C's upload
/// failed, and its artifact was re-uploaded after E: a fresh row with the
/// newest registration time and generation 3. By registration time the
/// history reads A, B, D, E, C, and restoring C rolls back D and E. Both
/// unpinned rules restore E.
#[tokio::test(flavor = "current_thread")]
async fn a_re_upload_of_an_older_capture_does_not_outrank_the_latest_capture() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let sandbox = insert_sandbox(pool, "suspended", 60, None).await?;
    for backup in [
        Backup::suspend(Some(1), 50, "s3://test/a?region=us-east-1"),
        Backup::suspend(Some(2), 40, "s3://test/b?region=us-east-1"),
        Backup::suspend(Some(4), 20, "s3://test/d?region=us-east-1"),
        Backup::suspend(Some(5), 10, "s3://test/e?region=us-east-1"),
        Backup::suspend(Some(3), 0, "s3://test/c-reupload?region=us-east-1"),
    ] {
        insert_backup(pool, sandbox, &backup).await?;
    }

    for rule in [FRESH, NEWEST] {
        assert_eq!(
            selected_uri(pool, sandbox, rule, RestoreSourcePlacement::Any)
                .await?
                .as_deref(),
            Some("s3://test/e?region=us-east-1"),
            "{rule:?}"
        );
    }
    Ok(())
}

/// What [`select_restore_source`] answers, as one comparable string: the
/// selected URI, `unordered` or `unavailable`.
async fn outcome(pool: &PgPool, sandbox: Uuid, rule: RestoreSourceMode) -> TestResult<String> {
    Ok(
        match select_restore_source(
            pool,
            SandboxId::from_uuid(sandbox),
            rule,
            RestoreSourcePlacement::Any,
        )
        .await?
        {
            RestoreSourceSelection::Selected(selected) => selected.storage_connection_string,
            RestoreSourceSelection::Unordered => "unordered".to_owned(),
            RestoreSourceSelection::Unavailable => "unavailable".to_owned(),
        },
    )
}

/// The execution of generation `generation`, started `minutes_ago`.
async fn insert_execution(
    pool: &PgPool,
    sandbox: Uuid,
    generation: i64,
    minutes_ago: i32,
) -> TestResult {
    sqlx::query(sql!(
        "INSERT INTO sandbox_k8s_executions (
             sandbox_id, generation, pod_name, pod_namespace, state, active, created_at
         )
         VALUES ($1, $2, $3, 'sandboxes', 'suspended', FALSE,
                 NOW() - make_interval(mins => $4::INT))"
    ))
    .bind(sandbox)
    .bind(generation)
    .bind(format!("space-sbx-{}-g{generation}", sandbox.simple()))
    .bind(minutes_ago)
    .execute(pool)
    .await?;
    Ok(())
}

/// A capture without a generation ranks below a later execution's capture
/// only when that execution started after it first registered. Its own
/// registration and that capture's can't show the order: the later capture's
/// first backup may have been pruned, so a re-upload of it registers late
/// (`a` below), or it may first upload long after it was taken (`c`). Either
/// way the execution that took it started before `b` registered, so `b` may be
/// the latest capture, and the history refuses. When the execution started
/// after `b` registered, `b` is older, and its capture wins. A Direct sandbox
/// records no executions and keeps the registration order.
#[tokio::test(flavor = "current_thread")]
async fn a_capture_ranks_below_a_later_execution_only_when_that_execution_started_after_it()
-> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    // (case, execution of generation 5 started minutes ago or none, expected)
    for (case, execution_minutes_ago, expected) in [
        ("started before b", Some(50), "unordered"),
        ("started after b", Some(15), "s3://test/a?region=us-east-1"),
        ("no execution history", None, "s3://test/a?region=us-east-1"),
    ] {
        // `a`'s first backup is gone: its re-upload registered after `b`.
        let sandbox = insert_sandbox(pool, "suspended", 60, None).await?;
        let b = Uuid::now_v7();
        for backup in [
            Backup {
                artifact: b,
                ..Backup::suspend(None, 20, "s3://test/b?region=us-east-1")
            },
            Backup::suspend(Some(5), 10, "s3://test/a?region=us-east-1"),
            Backup {
                artifact: b,
                ..Backup::suspend(None, 5, "s3://test/b-again?region=us-east-1")
            },
        ] {
            insert_backup(pool, sandbox, &backup).await?;
        }
        if let Some(minutes_ago) = execution_minutes_ago {
            insert_execution(pool, sandbox, 5, minutes_ago).await?;
        }
        for rule in [FRESH, NEWEST, NODE_FALLBACK] {
            assert_eq!(
                outcome(pool, sandbox, rule).await?,
                expected,
                "{case}, {rule:?}"
            );
        }
    }

    // `c` first uploaded after `b` registered, though its execution started
    // first.
    let sandbox = insert_sandbox(pool, "suspended", 60, None).await?;
    insert_backup(
        pool,
        sandbox,
        &Backup::suspend(None, 20, "s3://test/b?region=us-east-1"),
    )
    .await?;
    insert_backup(
        pool,
        sandbox,
        &Backup::suspend(Some(5), 10, "s3://test/c?region=us-east-1"),
    )
    .await?;
    insert_execution(pool, sandbox, 5, 30).await?;
    for rule in [FRESH, NEWEST, NODE_FALLBACK] {
        assert_eq!(
            outcome(pool, sandbox, rule).await?,
            "unordered",
            "late first upload, {rule:?}"
        );
    }
    Ok(())
}

/// A capture without a generation registered after one with a generation,
/// with no later execution's capture after it, has no order against it: it
/// may be the latest capture from a node that couldn't vouch for it, or a
/// re-upload of an older one. Every rule refuses that history explicitly,
/// whatever registered last, and whether the row without a generation is
/// ready or still uploading. A later execution's capture registered after it
/// ends the refusal: the capture without a generation is older. A re-upload
/// of an older capture with a generation registered after it doesn't: its
/// registration says nothing about when its capture was taken. Captures
/// without a generation registered before the first one with a generation
/// are ordered too: they predate recorded generations, and a capture with a
/// generation outranks them. A pinned row restores its pin whatever the
/// history.
#[tokio::test(flavor = "current_thread")]
async fn a_capture_without_a_generation_after_one_with_is_refused_unless_ordered() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let uri = |name: &str| format!("s3://test/{name}?region=us-east-1");
    let in_flight = |minutes_ago| Backup {
        state: "creating",
        ..Backup::suspend(None, minutes_ago, "s3://test/in-flight?region=us-east-1")
    };
    let cases: [(&str, Vec<Backup<'_>>, &str); 5] = [
        (
            "re-upload without a generation after the latest capture with one",
            vec![
                Backup::suspend(Some(5), 50, "s3://test/gen-5?region=us-east-1"),
                Backup::suspend(None, 20, "s3://test/no-generation?region=us-east-1"),
            ],
            "unordered",
        ),
        (
            "row without a generation between one with a generation and an outranked re-upload",
            vec![
                Backup::suspend(Some(5), 50, "s3://test/gen-5?region=us-east-1"),
                Backup::suspend(None, 20, "s3://test/no-generation?region=us-east-1"),
                Backup::suspend(Some(3), 0, "s3://test/gen-3-reupload?region=us-east-1"),
            ],
            "unordered",
        ),
        (
            "upload without a generation in flight after the latest capture with one",
            vec![
                Backup::suspend(Some(5), 50, "s3://test/gen-5?region=us-east-1"),
                in_flight(10),
            ],
            "unordered",
        ),
        (
            "a later execution's capture registered after the one without a generation",
            vec![
                Backup::suspend(Some(5), 50, "s3://test/gen-5?region=us-east-1"),
                Backup::suspend(None, 30, "s3://test/no-generation?region=us-east-1"),
                Backup::suspend(Some(6), 10, "s3://test/gen-6?region=us-east-1"),
            ],
            "gen-6",
        ),
        (
            "captures from before generations were recorded",
            vec![
                Backup::suspend(None, 50, "s3://test/pre-1?region=us-east-1"),
                Backup::suspend(None, 40, "s3://test/pre-2?region=us-east-1"),
                Backup::suspend(Some(2), 30, "s3://test/gen-2?region=us-east-1"),
            ],
            "gen-2",
        ),
    ];
    let mut mismatches = Vec::new();
    for (name, backups, expected) in cases {
        let expected = if expected == "unordered" {
            expected.to_owned()
        } else {
            uri(expected)
        };
        let sandbox = insert_sandbox(pool, "suspended", 60, None).await?;
        for backup in &backups {
            insert_backup(pool, sandbox, backup).await?;
        }
        for rule in [FRESH, NEWEST, NODE_FALLBACK] {
            let actual = outcome(pool, sandbox, rule).await?;
            if actual != expected {
                mismatches.push(format!("{name}, {rule:?}: {actual}"));
            }
        }
        // The gates agree: a refused history releases no source and counts
        // as not restorable.
        for (gate, fragment) in [
            (
                "fresh_ready_snapshot_exists",
                fresh_ready_snapshot_exists("s"),
            ),
            (
                "latest_full_snapshot_ready",
                latest_full_snapshot_ready("s"),
            ),
            (
                "snapshot_durable_for_source_release",
                snapshot_durable_for_source_release("s"),
            ),
        ] {
            let holds: bool = sqlx::query_scalar(sqlx::AssertSqlSafe(sql!(format!(
                "SELECT {fragment} FROM sandboxes s WHERE s.id = $1"
            ))))
            .bind(sandbox)
            .fetch_one(pool)
            .await?;
            if holds != (expected != "unordered") {
                mismatches.push(format!("{name}, {gate}: {holds}"));
            }
        }
        // The markerless revert proofs don't vouch for a refused history.
        for (proof, fragment) in [
            (
                "newest_full_snapshot_unambiguous",
                newest_full_snapshot_unambiguous("s"),
            ),
            (
                "newest_suspend_snapshot_activity_current",
                newest_suspend_snapshot_activity_current("s"),
            ),
            (
                "newest_suspend_snapshot_current",
                newest_suspend_snapshot_current("s"),
            ),
        ] {
            let holds: bool = sqlx::query_scalar(sqlx::AssertSqlSafe(sql!(format!(
                "SELECT {fragment} FROM sandboxes s WHERE s.id = $1"
            ))))
            .bind(sandbox)
            .fetch_one(pool)
            .await?;
            if expected == "unordered" && holds {
                mismatches.push(format!("{name}, {proof}: vouches"));
            }
        }
    }
    assert!(mismatches.is_empty(), "{mismatches:#?}");

    // Pinned, the same refused history restores the pin.
    let sandbox = insert_sandbox(pool, "suspended", 60, None).await?;
    let with_generation = Backup::suspend(Some(5), 50, "s3://test/gen-5?region=us-east-1");
    insert_backup(pool, sandbox, &with_generation).await?;
    insert_backup(
        pool,
        sandbox,
        &Backup::suspend(None, 20, "s3://test/no-generation?region=us-east-1"),
    )
    .await?;
    pin(pool, sandbox, with_generation.artifact).await?;
    for rule in [FRESH, NEWEST, NODE_FALLBACK] {
        assert_eq!(
            outcome(pool, sandbox, rule).await?,
            uri("gen-5"),
            "{rule:?}"
        );
    }

    // So does a retry of a claimed restore whose window holds the pin: the
    // state its first attempt loaded.
    let sandbox = insert_sandbox(pool, "restoring", 0, None).await?;
    let with_generation = Backup::suspend(Some(5), 50, "s3://test/gen-5?region=us-east-1");
    insert_backup(pool, sandbox, &with_generation).await?;
    insert_backup(
        pool,
        sandbox,
        &Backup::suspend(None, 20, "s3://test/no-generation?region=us-east-1"),
    )
    .await?;
    pin(pool, sandbox, with_generation.artifact).await?;
    let retry_gate = |sandbox: Uuid| async move {
        sqlx::query_scalar::<_, bool>(sqlx::AssertSqlSafe(sql!(format!(
            "SELECT {} FROM sandboxes s WHERE s.id = $1",
            restore_retry_source_snapshot_exists("s")
        ))))
        .bind(sandbox)
        .fetch_one(pool)
        .await
    };
    // The history is unordered, and the retry and its gate still take the
    // pin. So they do once a later execution's capture in the window, which
    // the one without a generation registered after, outranks it: the first attempt
    // loaded the pin whatever ranked above it.
    for later in [false, true] {
        if later {
            insert_backup(
                pool,
                sandbox,
                &Backup::suspend(Some(7), 30, "s3://test/gen-7?region=us-east-1"),
            )
            .await?;
        }
        for rule in [RETRY, NODE_FALLBACK] {
            assert_eq!(
                outcome(pool, sandbox, rule).await?,
                uri("gen-5"),
                "{later}, {rule:?}"
            );
        }
        assert!(
            retry_gate(sandbox).await?,
            "{later}: the retry gate vouches for the pin"
        );
    }
    // Unpinned, an unordered window refuses, and so does the retry gate.
    let sandbox = insert_sandbox(pool, "restoring", 0, None).await?;
    for backup in [
        Backup::suspend(Some(5), 50, "s3://test/gen-5?region=us-east-1"),
        Backup::suspend(None, 20, "s3://test/no-generation?region=us-east-1"),
    ] {
        insert_backup(pool, sandbox, &backup).await?;
    }
    assert_eq!(outcome(pool, sandbox, RETRY).await?, "unordered");
    assert!(!retry_gate(sandbox).await?);
    Ok(())
}

/// Two histories that look unordered row by row and aren't. An artifact
/// whose first backup records no generation and whose later backup records
/// one is a single capture with a generation. And a restore retry's window
/// bounds its ambiguity: a row without a generation registered after the
/// claim, by the failed branch, doesn't make the pre-claim source ambiguous.
#[tokio::test(flavor = "current_thread")]
async fn rows_outside_the_choice_do_not_make_a_history_unordered() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let sandbox = insert_sandbox(pool, "suspended", 60, None).await?;
    let first = Backup::suspend(None, 50, "s3://test/a-first?region=us-east-1");
    insert_backup(pool, sandbox, &first).await?;
    insert_backup(
        pool,
        sandbox,
        &Backup {
            generation: Some(5),
            minutes_ago: 30,
            uri: "s3://test/a-with-generation?region=us-east-1",
            ..first
        },
    )
    .await?;
    for rule in [FRESH, NEWEST, NODE_FALLBACK] {
        assert_eq!(
            outcome(pool, sandbox, rule).await?,
            "s3://test/a-with-generation?region=us-east-1",
            "{rule:?}"
        );
    }

    let sandbox = insert_sandbox(pool, "restoring", 15, None).await?;
    for backup in [
        Backup::suspend(Some(5), 50, "s3://test/source?region=us-east-1"),
        Backup::suspend(None, 5, "s3://test/failed-branch?region=us-east-1"),
    ] {
        insert_backup(pool, sandbox, &backup).await?;
    }
    for rule in [RETRY, NODE_FALLBACK] {
        assert_eq!(
            outcome(pool, sandbox, rule).await?,
            "s3://test/source?region=us-east-1",
            "{rule:?}"
        );
    }
    let gate: bool = sqlx::query_scalar(sqlx::AssertSqlSafe(sql!(format!(
        "SELECT {} FROM sandboxes s WHERE s.id = $1",
        restore_retry_source_snapshot_exists("s")
    ))))
    .bind(sandbox)
    .fetch_one(pool)
    .await?;
    assert!(gate, "the retry gate agrees");

    // Nor can a later execution's capture the failed branch registered after
    // the claim order the window: before the claim, the capture without a
    // generation came after the one with a generation with nothing after it,
    // so the retry refuses.
    let sandbox = insert_sandbox(pool, "restoring", 15, None).await?;
    for backup in [
        Backup::suspend(Some(5), 50, "s3://test/gen-5?region=us-east-1"),
        Backup::suspend(None, 20, "s3://test/no-generation?region=us-east-1"),
        Backup::suspend(Some(7), 5, "s3://test/failed-branch?region=us-east-1"),
    ] {
        insert_backup(pool, sandbox, &backup).await?;
    }
    for rule in [RETRY, NODE_FALLBACK] {
        assert_eq!(outcome(pool, sandbox, rule).await?, "unordered", "{rule:?}");
    }
    let gate: bool = sqlx::query_scalar(sqlx::AssertSqlSafe(sql!(format!(
        "SELECT {} FROM sandboxes s WHERE s.id = $1",
        restore_retry_source_snapshot_exists("s")
    ))))
    .bind(sandbox)
    .fetch_one(pool)
    .await?;
    assert!(!gate, "the retry gate agrees");

    // A window of captures without a generation stays legacy-ordered even
    // when the failed branch registers the sandbox's first capture with a
    // generation after the claim.
    let sandbox = insert_sandbox(pool, "restoring", 15, None).await?;
    for backup in [
        Backup::suspend(None, 50, "s3://test/older?region=us-east-1"),
        Backup::suspend(None, 30, "s3://test/newer?region=us-east-1"),
        Backup::suspend(Some(7), 5, "s3://test/failed-branch?region=us-east-1"),
    ] {
        insert_backup(pool, sandbox, &backup).await?;
    }
    for rule in [RETRY, NODE_FALLBACK] {
        assert_eq!(
            outcome(pool, sandbox, rule).await?,
            "s3://test/newer?region=us-east-1",
            "{rule:?}"
        );
    }

    // A node's lookup for a pinned restore whose pin's only pre-claim backup
    // failed, under a later execution's capture in the window, returns the
    // pin's backup redone after the claim, never the other capture.
    let sandbox = insert_sandbox(pool, "restoring", 15, None).await?;
    let pinned = Backup {
        state: "failed",
        ..Backup::suspend(Some(3), 50, "s3://test/pin-failed?region=us-east-1")
    };
    insert_backup(pool, sandbox, &pinned).await?;
    insert_backup(
        pool,
        sandbox,
        &Backup::suspend(Some(5), 30, "s3://test/other?region=us-east-1"),
    )
    .await?;
    insert_backup(
        pool,
        sandbox,
        &Backup {
            state: "ready",
            minutes_ago: 5,
            uri: "s3://test/pin-redone?region=us-east-1",
            ..pinned
        },
    )
    .await?;
    pin(pool, sandbox, pinned.artifact).await?;
    assert_eq!(
        outcome(pool, sandbox, NODE_FALLBACK).await?,
        "s3://test/pin-redone?region=us-east-1"
    );

    // A failed restore left the placement clock newer than the pin's backup:
    // the pin still restores, by retry and by a node's lookup, and the retry
    // gate vouches for it.
    let sandbox = insert_sandbox(pool, "restoring", 15, None).await?;
    let pinned = Backup::suspend(Some(5), 50, "s3://test/pinned?region=us-east-1");
    insert_backup(pool, sandbox, &pinned).await?;
    pin(pool, sandbox, pinned.artifact).await?;
    sqlx::query(sql!(
        "UPDATE sandboxes SET placement_changed_at = NOW() - make_interval(mins => 20)
          WHERE id = $1"
    ))
    .bind(sandbox)
    .execute(pool)
    .await?;
    for rule in [RETRY, NODE_FALLBACK] {
        assert_eq!(
            outcome(pool, sandbox, rule).await?,
            "s3://test/pinned?region=us-east-1",
            "{rule:?}"
        );
    }
    let gate: bool = sqlx::query_scalar(sqlx::AssertSqlSafe(sql!(format!(
        "SELECT {} FROM sandboxes s WHERE s.id = $1",
        restore_retry_source_snapshot_exists("s")
    ))))
    .bind(sandbox)
    .fetch_one(pool)
    .await?;
    assert!(gate, "the retry gate vouches for the pin");

    // A re-upload, registered after the last placement, of a capture taken
    // before it isn't fresh: its capture registered before the placement.
    // The sandbox ran since, and its latest capture hasn't registered.
    let sandbox = insert_sandbox(pool, "suspended", 60, None).await?;
    let before = Backup::suspend(None, 50, "s3://test/before?region=us-east-1");
    insert_backup(pool, sandbox, &before).await?;
    sqlx::query(sql!(
        "UPDATE sandboxes SET placement_changed_at = NOW() - make_interval(mins => 30)
          WHERE id = $1"
    ))
    .bind(sandbox)
    .execute(pool)
    .await?;
    insert_backup(
        pool,
        sandbox,
        &Backup {
            minutes_ago: 10,
            uri: "s3://test/before-again?region=us-east-1",
            ..before
        },
    )
    .await?;
    assert_eq!(outcome(pool, sandbox, FRESH).await?, "unavailable");
    for (gate, fragment) in [
        (
            "fresh_ready_snapshot_exists",
            fresh_ready_snapshot_exists("s"),
        ),
        (
            "snapshot_durable_for_source_release",
            snapshot_durable_for_source_release("s"),
        ),
    ] {
        let holds: bool = sqlx::query_scalar(sqlx::AssertSqlSafe(sql!(format!(
            "SELECT {fragment} FROM sandboxes s WHERE s.id = $1"
        ))))
        .bind(sandbox)
        .fetch_one(pool)
        .await?;
        assert!(!holds, "{gate}");
    }
    Ok(())
}

/// The re-upload regression: capture A, then a newer capture B, then a new backup
/// attempt of A's artifact. Every rule, and a retry of a claimed restore,
/// restores B, whether the captures record a generation or not: the
/// re-upload's capture registered with A's first backup.
#[tokio::test(flavor = "current_thread")]
async fn a_new_backup_of_an_older_capture_never_outranks_a_newer_capture() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    for (a_generation, b_generation) in [(Some(1), Some(2)), (None, None)] {
        for lifecycle in ["suspended", "restoring"] {
            // A `restoring` row's claim is newer than every backup.
            let sandbox = insert_sandbox(pool, lifecycle, 0, None).await?;
            let a = Backup::suspend(a_generation, 50, "s3://test/a?region=us-east-1");
            insert_backup(pool, sandbox, &a).await?;
            insert_backup(
                pool,
                sandbox,
                &Backup::suspend(b_generation, 30, "s3://test/b?region=us-east-1"),
            )
            .await?;
            insert_backup(
                pool,
                sandbox,
                &Backup {
                    uri: "s3://test/a-again?region=us-east-1",
                    minutes_ago: 1,
                    ..a
                },
            )
            .await?;
            let rules: &[RestoreSourceMode] = if lifecycle == "restoring" {
                &[RETRY, NODE_FALLBACK]
            } else {
                &[FRESH, NEWEST, NODE_FALLBACK]
            };
            for &rule in rules {
                assert_eq!(
                    outcome(pool, sandbox, rule).await?,
                    "s3://test/b?region=us-east-1",
                    "{a_generation:?}, {lifecycle}, {rule:?}"
                );
            }
        }
    }
    Ok(())
}

/// A history that records no generation keeps registration order.
#[tokio::test(flavor = "current_thread")]
async fn a_history_without_generations_keeps_registration_order() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let sandbox = insert_sandbox(pool, "suspended", 60, None).await?;
    for backup in [
        Backup::suspend(None, 50, "s3://test/old?region=us-east-1"),
        Backup::suspend(None, 10, "s3://test/mid?region=us-east-1"),
        Backup::suspend(None, 0, "s3://test/newest?region=us-east-1"),
    ] {
        insert_backup(pool, sandbox, &backup).await?;
    }

    for rule in [FRESH, NEWEST] {
        assert_eq!(
            selected_uri(pool, sandbox, rule, RestoreSourcePlacement::Any)
                .await?
                .as_deref(),
            Some("s3://test/newest?region=us-east-1"),
            "{rule:?}"
        );
    }
    Ok(())
}

/// A pinned row restores its pinned artifact, the newest of its backup
/// attempts, even when another artifact records a later generation or
/// registered later. A user's full named snapshot backed up after the
/// suspend is the case the Direct substrate used to restore.
#[tokio::test(flavor = "current_thread")]
async fn a_pinned_row_restores_its_pinned_artifact() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let sandbox = insert_sandbox(pool, "suspended", 60, None).await?;
    let pinned = Uuid::now_v7();
    pin(pool, sandbox, pinned).await?;
    for backup in [
        Backup {
            artifact: pinned,
            ..Backup::suspend(Some(4), 30, "s3://test/pinned-1?region=us-east-1")
        },
        Backup {
            artifact: pinned,
            ..Backup::suspend(Some(4), 20, "s3://test/pinned-2?region=us-east-1")
        },
        Backup::suspend(Some(7), 10, "s3://test/other-artifact?region=us-east-1"),
        Backup {
            origin: "snapshot",
            ..Backup::suspend(None, 0, "s3://test/named?region=us-east-1")
        },
    ] {
        insert_backup(pool, sandbox, &backup).await?;
    }

    for rule in [FRESH, NEWEST] {
        assert_eq!(
            selected_uri(pool, sandbox, rule, RestoreSourcePlacement::Any)
                .await?
                .as_deref(),
            Some("s3://test/pinned-2?region=us-east-1"),
            "{rule:?}"
        );
    }
    Ok(())
}

/// A retry restores the latest capture registered before its claim. A
/// re-upload of an older capture inside the window doesn't outrank it, and
/// the failed branch's own later capture, registered after the claim,
/// neither outranks it nor is selected.
#[tokio::test(flavor = "current_thread")]
async fn a_retry_restores_the_latest_capture_in_its_source_window() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let sandbox = insert_sandbox(pool, "restoring", 5, None).await?;
    for backup in [
        Backup::suspend(Some(5), 20, "s3://test/source?region=us-east-1"),
        Backup::suspend(Some(3), 10, "s3://test/old-reupload?region=us-east-1"),
        Backup::suspend(Some(6), 1, "s3://test/failed-branch?region=us-east-1"),
    ] {
        insert_backup(pool, sandbox, &backup).await?;
    }

    for rule in [RETRY, NODE_FALLBACK] {
        assert_eq!(
            selected_uri(pool, sandbox, rule, RestoreSourcePlacement::Any)
                .await?
                .as_deref(),
            Some("s3://test/source?region=us-east-1"),
            "{rule:?}"
        );
    }
    Ok(())
}

/// A full named snapshot backed up after the suspend and before the claim
/// isn't the state the restore started from: the retry, and a node asking on
/// the restore's behalf, load the suspend, whether or not the row is pinned.
#[tokio::test(flavor = "current_thread")]
async fn a_retry_never_restores_a_named_snapshot() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    for pinned_row in [true, false] {
        let sandbox = insert_sandbox(pool, "restoring", 5, None).await?;
        let suspended = Uuid::now_v7();
        if pinned_row {
            pin(pool, sandbox, suspended).await?;
        }
        for backup in [
            Backup {
                artifact: suspended,
                ..Backup::suspend(Some(5), 20, "s3://test/suspend?region=us-east-1")
            },
            Backup {
                origin: "snapshot",
                ..Backup::suspend(Some(5), 10, "s3://test/named?region=us-east-1")
            },
        ] {
            insert_backup(pool, sandbox, &backup).await?;
        }

        for rule in [RETRY, NODE_FALLBACK] {
            assert_eq!(
                selected_uri(pool, sandbox, rule, RestoreSourcePlacement::Any)
                    .await?
                    .as_deref(),
                Some("s3://test/suspend?region=us-east-1"),
                "pinned {pinned_row}, {rule:?}"
            );
        }
    }
    Ok(())
}

/// Whether `fragment`, over the sandbox aliased `s`, holds for `sandbox`.
async fn gate_holds(pool: &PgPool, sandbox: Uuid, fragment: &str) -> TestResult<bool> {
    Ok(sqlx::query_scalar(sqlx::AssertSqlSafe(sql!(format!(
        "SELECT {fragment} FROM sandboxes s WHERE s.id = $1"
    ))))
    .bind(sandbox)
    .fetch_one(pool)
    .await?)
}

/// A named snapshot is taken while the sandbox runs, so one of the same
/// execution registered after its suspend capture is older state: an unpinned
/// first attempt restores the suspend under every rule. One of a later
/// execution outranks the suspend, and the restore waits rather than load
/// either. The source-release gate agrees: with no suspend capture to restore,
/// the source isn't released, though a give-up still counts the named
/// snapshot's backup as a copy to keep the row for. An evacuation-marked row
/// releases on the suspend capture too, newest among suspend captures, as
/// the evacuation restore selects.
#[tokio::test(flavor = "current_thread")]
async fn an_unpinned_restore_never_takes_a_named_snapshot() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let suspend = "s3://test/suspend?region=us-east-1";
    // (case, named snapshot's generation, evacuation-marked, expected, released)
    for (case, named_generation, evacuation, expected, released) in [
        ("same execution", Some(5_i64), false, suspend, true),
        ("same execution, evacuating", Some(5), true, suspend, true),
        ("later execution", Some(6), false, "unavailable", false),
    ] {
        let sandbox = insert_sandbox(pool, "suspended", 30, None).await?;
        if evacuation {
            sqlx::query(sql!(
                "UPDATE sandboxes SET evacuation_restore_target = 'started' WHERE id = $1"
            ))
            .bind(sandbox)
            .execute(pool)
            .await?;
        }
        for backup in [
            Backup::suspend(Some(5), 20, "s3://test/suspend?region=us-east-1"),
            Backup {
                origin: "snapshot",
                ..Backup::suspend(named_generation, 10, "s3://test/named?region=us-east-1")
            },
        ] {
            insert_backup(pool, sandbox, &backup).await?;
        }
        for rule in [FRESH, NEWEST, NODE_FALLBACK] {
            assert_eq!(
                outcome(pool, sandbox, rule).await?,
                expected,
                "{case}, {rule:?}"
            );
        }
        let release = snapshot_durable_for_source_release("s");
        assert_eq!(
            gate_holds(pool, sandbox, &release).await?,
            released,
            "{case}"
        );
        let copy = snapshot_durable_copy_exists("s");
        assert!(gate_holds(pool, sandbox, &copy).await?, "{case}");
    }
    Ok(())
}

/// A later suspend capture holds the source while it can still land, since it
/// may hold bytes only the source node has. An upload in flight holds an
/// unmarked row until it finishes, or the stuck-snapshot reaper fails it. A
/// failed one doesn't, of the same execution or a later one: nothing retries
/// it for an unpinned row on a gentle drain, so the hold would never end. An
/// evacuation-marked row is held by a failed one too, since the evacuating
/// drain re-uploads it, and fails the row if its bytes are gone. A give-up
/// keeps an unmarked row for the ready copy.
#[tokio::test(flavor = "current_thread")]
async fn a_later_capture_holds_the_source_only_while_it_can_land() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    // (later capture's state, its generation, evacuation-marked, released)
    for (state, generation, evacuation, released) in [
        ("creating", 5_i64, false, false),
        ("creating", 6, false, false),
        ("failed", 5, false, true),
        ("failed", 6, false, true),
        ("failed", 5, true, false),
    ] {
        let case = format!("{state}, generation {generation}, evacuation {evacuation}");
        let sandbox = insert_sandbox(pool, "suspended", 30, None).await?;
        if evacuation {
            sqlx::query(sql!(
                "UPDATE sandboxes SET evacuation_restore_target = 'started' WHERE id = $1"
            ))
            .bind(sandbox)
            .execute(pool)
            .await?;
        }
        for backup in [
            Backup::suspend(Some(5), 20, "s3://test/ready?region=us-east-1"),
            Backup {
                state,
                ..Backup::suspend(Some(generation), 10, "s3://test/later?region=us-east-1")
            },
        ] {
            insert_backup(pool, sandbox, &backup).await?;
        }
        let release = snapshot_durable_for_source_release("s");
        assert_eq!(
            gate_holds(pool, sandbox, &release).await?,
            released,
            "{case}"
        );
        // The give-up runs only once no upload is in flight. Then an
        // evacuation-marked row whose newest capture failed is re-uploaded
        // from the node, and failed if its bytes are gone, never restored
        // from the older capture; an unmarked one keeps the ready copy.
        if state == "failed" {
            let copy = snapshot_durable_copy_exists("s");
            assert_eq!(
                gate_holds(pool, sandbox, &copy).await?,
                !evacuation,
                "{case}"
            );
        }
    }
    Ok(())
}

/// A give-up keeps an unordered history's last ready copy though later
/// captures outrank it, as long as none of them uploaded: a capture with a
/// higher generation and a later one without a generation both failed. The
/// source isn't released for it, since its restore refuses.
#[tokio::test(flavor = "current_thread")]
async fn a_give_up_keeps_an_unordered_historys_outranked_copy() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let sandbox = insert_sandbox(pool, "suspended", 30, None).await?;
    for backup in [
        Backup::suspend(Some(3), 25, "s3://test/three?region=us-east-1"),
        Backup {
            state: "failed",
            ..Backup::suspend(Some(5), 20, "s3://test/five?region=us-east-1")
        },
        Backup {
            state: "failed",
            ..Backup::suspend(None, 10, "s3://test/unranked?region=us-east-1")
        },
    ] {
        insert_backup(pool, sandbox, &backup).await?;
    }
    let copy = snapshot_durable_copy_exists("s");
    assert!(gate_holds(pool, sandbox, &copy).await?);
    let release = snapshot_durable_for_source_release("s");
    assert!(!gate_holds(pool, sandbox, &release).await?);
    Ok(())
}

/// The claim's source upload failed and its artifact was backed up again after
/// the claim. A node asking during the restore gets that backup, the same
/// bytes, although the retry window ends at the claim. Before that backup
/// lands, neither the retry nor the node restores another capture, though a
/// later execution's ready capture is in the window: the pin predates the
/// claim, so it's the state the restore started from.
#[tokio::test(flavor = "current_thread")]
async fn a_node_restores_a_source_re_uploaded_after_the_claim() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let sandbox = insert_sandbox(pool, "restoring", 5, None).await?;
    let pinned = Uuid::now_v7();
    pin(pool, sandbox, pinned).await?;
    for backup in [
        Backup {
            artifact: pinned,
            state: "failed",
            ..Backup::suspend(Some(5), 20, "s3://test/failed-upload?region=us-east-1")
        },
        Backup::suspend(Some(6), 15, "s3://test/other?region=us-east-1"),
    ] {
        insert_backup(pool, sandbox, &backup).await?;
    }
    for rule in [RETRY, NODE_FALLBACK] {
        assert_eq!(
            outcome(pool, sandbox, rule).await?,
            "unavailable",
            "{rule:?}"
        );
    }

    insert_backup(
        pool,
        sandbox,
        &Backup {
            artifact: pinned,
            ..Backup::suspend(Some(5), 1, "s3://test/reupload?region=us-east-1")
        },
    )
    .await?;
    assert_eq!(
        selected_uri(pool, sandbox, NODE_FALLBACK, RestoreSourcePlacement::Any)
            .await?
            .as_deref(),
        Some("s3://test/reupload?region=us-east-1"),
    );
    // The retry window ends at the claim, so the retry still waits.
    assert_eq!(outcome(pool, sandbox, RETRY).await?, "unavailable");
    Ok(())
}

/// A node reads only the source of a sandbox placed on it. Its fallback
/// serves a `started` sandbox whose runtime is being recovered, restoring
/// the pinned artifact, which the control plane's own rules refuse for a
/// row that isn't `suspended` or `restoring`.
#[tokio::test(flavor = "current_thread")]
async fn a_node_reads_the_source_of_the_sandboxes_placed_on_it() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let node = insert_node(pool).await?;
    let other = insert_node(pool).await?;
    let sandbox = insert_sandbox(pool, "started", 5, Some(node)).await?;
    let pinned = Uuid::now_v7();
    pin(pool, sandbox, pinned).await?;
    insert_backup(
        pool,
        sandbox,
        &Backup {
            artifact: pinned,
            ..Backup::suspend(Some(2), 20, "s3://test/pinned?region=us-east-1")
        },
    )
    .await?;
    insert_backup(
        pool,
        sandbox,
        &Backup::suspend(Some(3), 10, "s3://test/other?region=us-east-1"),
    )
    .await?;

    assert_eq!(
        selected_uri(
            pool,
            sandbox,
            NODE_FALLBACK,
            RestoreSourcePlacement::PlacedOn(&[other])
        )
        .await?,
        None,
    );
    assert_eq!(
        selected_uri(
            pool,
            sandbox,
            NODE_FALLBACK,
            RestoreSourcePlacement::PlacedOn(&[other, node])
        )
        .await?
        .as_deref(),
        Some("s3://test/pinned?region=us-east-1"),
    );
    assert_eq!(
        selected_uri(pool, sandbox, FRESH, RestoreSourcePlacement::Any).await?,
        None,
    );
    Ok(())
}

/// A capture whose newest backup attempt failed, or is still in flight,
/// isn't restored from an older attempt's generation, and an older capture
/// can't stand in for it under the newest-full rule: the latest capture's
/// failed upload outranks every older capture's re-upload.
#[tokio::test(flavor = "current_thread")]
async fn a_failed_upload_of_the_latest_capture_blocks_older_re_uploads() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let sandbox = insert_sandbox(pool, "suspended", 60, None).await?;
    insert_backup(
        pool,
        sandbox,
        &Backup {
            state: "failed",
            ..Backup::suspend(Some(5), 10, "s3://test/latest-failed?region=us-east-1")
        },
    )
    .await?;
    insert_backup(
        pool,
        sandbox,
        &Backup::suspend(Some(3), 0, "s3://test/old-reupload?region=us-east-1"),
    )
    .await?;

    assert_eq!(
        selected_uri(pool, sandbox, NEWEST, RestoreSourcePlacement::Any).await?,
        None,
    );
    Ok(())
}

/// An unpinned user restore loads only a capture from after the last
/// placement, the capture itself and not merely some snapshot, and only the
/// latest one. A re-upload registered since the placement isn't that: not one
/// of an older capture, which the previous cycle's outranks, nor one of the
/// previous cycle's capture while the current one's upload is in flight.
#[tokio::test(flavor = "current_thread")]
async fn an_unpinned_restore_waits_for_a_capture_since_the_last_placement() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    for (case, backups) in [
        (
            "older re-upload",
            vec![
                Backup::suspend(Some(5), 20, "s3://test/prior-cycle?region=us-east-1"),
                Backup::suspend(Some(3), 10, "s3://test/old-reupload?region=us-east-1"),
            ],
        ),
        (
            "current upload in flight",
            vec![
                Backup::suspend(Some(5), 20, "s3://test/prior-cycle?region=us-east-1"),
                Backup::suspend(
                    Some(5),
                    10,
                    "s3://test/prior-cycle-reupload?region=us-east-1",
                ),
                Backup {
                    state: "creating",
                    ..Backup::suspend(Some(6), 5, "s3://test/current?region=us-east-1")
                },
            ],
        ),
    ] {
        let sandbox = insert_sandbox(pool, "suspended", 60, None).await?;
        sqlx::query(sql!(
            "UPDATE sandboxes
                SET placement_changed_at = NOW() - make_interval(mins => 15)
              WHERE id = $1"
        ))
        .bind(sandbox)
        .execute(pool)
        .await?;
        for backup in &backups {
            insert_backup(pool, sandbox, backup).await?;
        }

        assert_eq!(
            selected_uri(pool, sandbox, FRESH, RestoreSourcePlacement::Any).await?,
            None,
            "{case}"
        );
        // The portability and source-release gates agree: nothing a restore
        // would load is durable, so the source node isn't released.
        for (gate, fragment) in [
            (
                "fresh_ready_snapshot_exists",
                fresh_ready_snapshot_exists("s"),
            ),
            (
                "snapshot_durable_for_source_release",
                snapshot_durable_for_source_release("s"),
            ),
        ] {
            let holds: bool = sqlx::query_scalar(sqlx::AssertSqlSafe(sql!(format!(
                "SELECT {fragment} FROM sandboxes s WHERE s.id = $1"
            ))))
            .bind(sandbox)
            .fetch_one(pool)
            .await?;
            assert!(!holds, "{case}: {gate}");
        }
    }
    Ok(())
}

/// A node recovering a `started` sandbox asks after the recovery placed it,
/// so the capture predates the placement. Its lookup still returns it, by
/// capture, where a user restore's placement-freshness rule wouldn't, but not
/// while a later capture's upload is in flight.
#[tokio::test(flavor = "current_thread")]
async fn a_node_lookup_skips_the_placement_freshness_check() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let sandbox = insert_sandbox(pool, "started", 60, None).await?;
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET placement_changed_at = NOW() - make_interval(mins => 5)
          WHERE id = $1"
    ))
    .bind(sandbox)
    .execute(pool)
    .await?;
    for backup in [
        Backup::suspend(Some(5), 20, "s3://test/latest?region=us-east-1"),
        Backup::suspend(Some(3), 10, "s3://test/old-reupload?region=us-east-1"),
    ] {
        insert_backup(pool, sandbox, &backup).await?;
    }

    assert_eq!(
        selected_uri(pool, sandbox, NODE_FALLBACK, RestoreSourcePlacement::Any)
            .await?
            .as_deref(),
        Some("s3://test/latest?region=us-east-1"),
    );
    // A later capture whose upload is still in flight outranks it: the
    // lookup waits rather than restore older state.
    insert_backup(
        pool,
        sandbox,
        &Backup {
            state: "creating",
            ..Backup::suspend(Some(6), 1, "s3://test/in-flight?region=us-east-1")
        },
    )
    .await?;
    assert_eq!(
        selected_uri(pool, sandbox, NODE_FALLBACK, RestoreSourcePlacement::Any).await?,
        None,
    );
    Ok(())
}

/// The admin runtime-environment population binds a suspended Kubernetes
/// sandbox to the capture its restore loads: for every history the selector
/// tests above use, the population's environment is the selector's pick under
/// the placement-fresh rule, and a sandbox the selector refuses or finds no
/// source for counts as unrecorded.
#[tokio::test(flavor = "current_thread")]
async fn the_population_binds_the_capture_the_selector_restores() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let suspend = |generation, minutes_ago, name: &'static str| {
        Backup::suspend(generation, minutes_ago, name)
    };
    let histories: Vec<Vec<Backup<'_>>> = vec![
        vec![
            suspend(Some(1), 50, "a"),
            suspend(Some(5), 10, "e"),
            suspend(Some(3), 2, "c-reupload"),
        ],
        vec![
            suspend(Some(5), 50, "gen-5"),
            suspend(None, 20, "no-generation"),
        ],
        vec![
            suspend(Some(5), 50, "gen-5"),
            suspend(None, 30, "no-generation"),
            suspend(Some(6), 10, "gen-6"),
        ],
        vec![suspend(None, 50, "pre-1"), suspend(Some(2), 30, "gen-2")],
        vec![
            suspend(Some(5), 50, "gen-5"),
            suspend(None, 5, "late-no-generation"),
        ],
        vec![
            suspend(None, 50, "old"),
            suspend(None, 10, "mid"),
            suspend(None, 2, "newest"),
        ],
    ];
    // Each history as a suspended sandbox, and as a restoring one claimed
    // after every backup (placement-fresh and retry rules), once pinned to
    // its first capture, and once claimed 15 minutes ago, so later rows fall
    // outside the retry window.
    let cases = histories.iter().flat_map(|history| {
        [
            ("suspended", 60, FRESH, false),
            ("restoring", 0, RETRY, false),
            ("restoring", 0, RETRY, true),
            ("restoring", 15, RETRY, false),
        ]
        .map(|(lifecycle, claimed, rule, pinned)| (history, lifecycle, claimed, rule, pinned))
    });
    let mut mismatches = Vec::new();
    let mut previous: Option<Uuid> = None;
    for (index, (history, lifecycle, claimed, rule, pinned)) in cases.enumerate() {
        let mut history: Vec<Backup<'_>> =
            history.iter().map(|backup| Backup { ..*backup }).collect();
        if let Some(previous) = previous {
            sqlx::query(sql!(
                "UPDATE sandboxes SET lifecycle = 'deleted' WHERE id = $1"
            ))
            .bind(previous)
            .execute(pool)
            .await?;
        }
        let sandbox = insert_sandbox(pool, lifecycle, claimed, None).await?;
        previous = Some(sandbox);
        // A re-upload of the first capture's artifact, registered after every
        // other row.
        let reupload = Backup {
            uri: "again",
            minutes_ago: 1,
            ..history[0]
        };
        history.push(reupload);
        for backup in &history {
            let snapshot = insert_backup(pool, sandbox, backup).await?;
            sqlx::query(sql!(
                "UPDATE snapshots SET execution_metadata = jsonb_build_object('environment', $2::TEXT)
                  WHERE id = $1"
            ))
            .bind(snapshot)
            .bind(backup.uri)
            .execute(pool)
            .await?;
        }
        if pinned {
            pin(pool, sandbox, history[0].artifact).await?;
        }
        let selected = outcome(pool, sandbox, rule).await?;
        let population = crate::runtime_population::sandbox_population(pool).await?;
        let bound: Vec<_> = population
            .iter()
            .filter(|row| row.lifecycle == lifecycle)
            .map(|row| (row.recorded, row.environment_ref.clone()))
            .collect();
        let expected = match selected.as_str() {
            "unordered" | "unavailable" => vec![(false, None)],
            uri => vec![(true, Some(uri.to_owned()))],
        };
        if bound != expected {
            mismatches.push(format!(
                "case {index} ({lifecycle}, claimed {claimed}m ago, pinned {pinned}): selector {selected}, population {bound:?}"
            ));
        }
    }
    assert!(mismatches.is_empty(), "{mismatches:#?}");
    Ok(())
}

/// A Direct sandbox restoring from a pinned capture is attributed, like the
/// selector restores it, to the pin, even below a later execution's capture
/// in its window.
#[tokio::test(flavor = "current_thread")]
async fn the_population_binds_a_direct_retry_to_its_pin() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let sandbox = insert_sandbox(pool, "restoring", 0, None).await?;
    sqlx::query(sql!(
        "UPDATE sandboxes SET managed_by = 'space' WHERE id = $1"
    ))
    .bind(sandbox)
    .execute(pool)
    .await?;
    let pinned = Backup::suspend(Some(3), 50, "pinned");
    for backup in [&pinned, &Backup::suspend(Some(5), 30, "later")] {
        let snapshot = insert_backup(pool, sandbox, backup).await?;
        sqlx::query(sql!(
            "UPDATE snapshots SET execution_metadata = jsonb_build_object('environment', $2::TEXT)
              WHERE id = $1"
        ))
        .bind(snapshot)
        .bind(backup.uri)
        .execute(pool)
        .await?;
    }
    pin(pool, sandbox, pinned.artifact).await?;
    let population = crate::runtime_population::sandbox_population(pool).await?;
    let bound: Vec<_> = population
        .iter()
        .filter(|row| row.lifecycle == "restoring")
        .map(|row| row.environment_ref.clone())
        .collect();
    assert_eq!(bound, [Some("pinned".to_owned())]);
    Ok(())
}
