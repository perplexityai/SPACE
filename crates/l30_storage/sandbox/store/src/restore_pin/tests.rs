//! [`restore_pin_state`] and [`pin_restore_source`] against Postgres: an
//! operator can pin either side of an unordered history, and nothing a
//! later capture provably follows.

use pplx_space_db_test_fixtures::{
    SEED_ORG_ID as SEED_ORG, SEED_USER_ID as SEED_USER, try_pg_test_db,
};
use pplx_space_node_model::SubstrateKind;
use pplx_space_sandbox_model::SandboxId;
use pplx_space_snapshot_model::LocalArtifactId;
use pplx_space_sql_locator::sql;
use pplx_space_test_env::TestResult;
use sqlx::PgPool;
use uuid::Uuid;

use super::{pin_restore_source, restore_pin_state};

async fn insert_sandbox(pool: &PgPool, lifecycle: &str) -> TestResult<SandboxId> {
    let id = SandboxId::new();
    sqlx::query(sql!(
        "INSERT INTO sandboxes
            (id, user_id, org_id, managed_by, lifecycle, template_id, network_policy_generation)
         VALUES ($1, $2, $3, 'k8s', $4, 'tmpl-restore-pin-test', 1)"
    ))
    .bind(id)
    .bind(SEED_USER)
    .bind(SEED_ORG)
    .bind(lifecycle)
    .execute(pool)
    .await?;
    Ok(id)
}

/// A full backup of `artifact` with `origin`, registered `minutes_ago`.
async fn insert_backup(
    pool: &PgPool,
    sandbox: SandboxId,
    artifact: Uuid,
    origin: &str,
    generation: Option<i64>,
    minutes_ago: i32,
) -> TestResult {
    let snapshot = Uuid::now_v7();
    sqlx::query(sql!(
        "INSERT INTO snapshots (
             id, sandbox_id, type, state, timestamp, local_artifact_id, artifact_kind,
             artifact_origin, artifact_snapshot_name, generation, org_id
         )
         VALUES ($1, $2, 'full', 'ready', NOW() - make_interval(mins => $3::INT), $4, 'full',
                 $5, CASE WHEN $5 = 'snapshot' THEN 'named' END, $6, $7)"
    ))
    .bind(snapshot)
    .bind(sandbox)
    .bind(minutes_ago)
    .bind(artifact)
    .bind(origin)
    .bind(generation)
    .bind(SEED_ORG)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO snapshot_storage_locations (snapshot_id, storage_connection_string, org_id)
         VALUES ($1, 's3://test/restore-pin?region=us-east-1', $2)"
    ))
    .bind(snapshot)
    .bind(SEED_ORG)
    .execute(pool)
    .await?;
    Ok(())
}

/// An unordered history lists the latest capture of each side, and an
/// operator can pin either: the capture with a generation, or the later one
/// without. An outranked capture and a named snapshot's backup aren't listed
/// and can't be pinned, and neither can any capture once the row's
/// `lifecycle_changed_at` moved, or while it's pinned or not `suspended`.
#[tokio::test(flavor = "current_thread")]
async fn an_operator_pins_either_side_of_an_unordered_history_and_nothing_older() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let sandbox = insert_sandbox(pool, "suspended").await?;
    let (older, stamped, unstamped, named) = (
        Uuid::now_v7(),
        Uuid::now_v7(),
        Uuid::now_v7(),
        Uuid::now_v7(),
    );
    insert_backup(pool, sandbox, older, "suspend", Some(4), 50).await?;
    insert_backup(pool, sandbox, stamped, "suspend", Some(5), 40).await?;
    insert_backup(pool, sandbox, unstamped, "suspend", None, 30).await?;
    insert_backup(pool, sandbox, named, "snapshot", None, 20).await?;

    let state = restore_pin_state(pool, sandbox).await?.expect("sandbox");
    assert!(state.unordered);
    assert_eq!(state.pinned_artifact, None);
    let listed: Vec<Uuid> = state
        .captures
        .iter()
        .map(|capture| capture.local_artifact_id.as_uuid())
        .collect();
    assert_eq!(listed, [unstamped, stamped]);
    let token = state.lifecycle_changed_at.expect("lifecycle_changed_at");

    for refused in [older, named] {
        assert!(
            !pin_restore_source(
                pool,
                sandbox,
                LocalArtifactId::from_uuid(refused),
                token,
                None
            )
            .await?
        );
    }
    let moved = token - time::Duration::seconds(1);
    assert!(
        !pin_restore_source(
            pool,
            sandbox,
            LocalArtifactId::from_uuid(unstamped),
            moved,
            None
        )
        .await?
    );
    assert!(
        pin_restore_source(
            pool,
            sandbox,
            LocalArtifactId::from_uuid(unstamped),
            token,
            None
        )
        .await?
    );
    let state = restore_pin_state(pool, sandbox).await?.expect("sandbox");
    assert_eq!(state.pinned_artifact.map(|a| a.as_uuid()), Some(unstamped));
    // Pinned: a second pin is refused.
    assert!(
        !pin_restore_source(
            pool,
            sandbox,
            LocalArtifactId::from_uuid(stamped),
            token,
            None
        )
        .await?
    );

    let other = insert_sandbox(pool, "suspended").await?;
    insert_backup(pool, other, stamped, "suspend", Some(5), 40).await?;
    insert_backup(pool, other, unstamped, "suspend", None, 30).await?;
    let token = restore_pin_state(pool, other)
        .await?
        .and_then(|state| state.lifecycle_changed_at)
        .expect("lifecycle_changed_at");
    assert!(
        pin_restore_source(
            pool,
            other,
            LocalArtifactId::from_uuid(stamped),
            token,
            None
        )
        .await?
    );

    let started = insert_sandbox(pool, "started").await?;
    insert_backup(pool, started, unstamped, "suspend", None, 30).await?;
    let token = restore_pin_state(pool, started)
        .await?
        .and_then(|state| state.lifecycle_changed_at)
        .expect("lifecycle_changed_at");
    assert!(
        !pin_restore_source(
            pool,
            started,
            LocalArtifactId::from_uuid(unstamped),
            token,
            None
        )
        .await?
    );
    Ok(())
}

/// A capture lists the generation any backup of its artifact records, as its
/// rank reads it, though its newest backup, from before backups inherited
/// it, records none.
#[tokio::test(flavor = "current_thread")]
async fn a_capture_lists_the_generation_its_artifact_records() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let sandbox = insert_sandbox(pool, "suspended").await?;
    let artifact = Uuid::now_v7();
    insert_backup(pool, sandbox, artifact, "suspend", Some(5), 30).await?;
    insert_backup(pool, sandbox, artifact, "suspend", None, 10).await?;
    let state = restore_pin_state(pool, sandbox).await?.expect("sandbox");
    let generations: Vec<Option<i64>> = state
        .captures
        .iter()
        .map(|capture| capture.generation)
        .collect();
    assert_eq!(generations, [Some(5)]);
    Ok(())
}

/// A capture outranked only by a later execution's failed upload can be
/// pinned, and so can one followed only by a failed upload of the same rank:
/// that upload won't land, and the operator chooses. One outranked or
/// followed by an upload still in flight can't.
#[tokio::test(flavor = "current_thread")]
async fn a_capture_outranked_only_by_a_failed_upload_is_pinnable() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    // (later upload's state, its generation, the ready capture is pinnable)
    for (state, generation, pinnable) in [
        ("failed", 6_i64, true),
        ("creating", 6, false),
        ("failed", 5, true),
        ("creating", 5, false),
    ] {
        let sandbox = insert_sandbox(pool, "suspended").await?;
        let ready = Uuid::now_v7();
        insert_backup(pool, sandbox, ready, "suspend", Some(5), 30).await?;
        sqlx::query(sql!(
            "INSERT INTO snapshots (
                 id, sandbox_id, type, state, timestamp, local_artifact_id, artifact_kind,
                 artifact_origin, generation, org_id
             )
             VALUES ($1, $2, 'full', $3, NOW() - INTERVAL '10 minutes', $4, 'full', 'suspend', $6,
                     $5)"
        ))
        .bind(Uuid::now_v7())
        .bind(sandbox)
        .bind(state)
        .bind(Uuid::now_v7())
        .bind(SEED_ORG)
        .bind(generation)
        .execute(pool)
        .await?;
        let state_now = restore_pin_state(pool, sandbox).await?.expect("sandbox");
        let listed = state_now
            .captures
            .iter()
            .any(|capture| capture.local_artifact_id.as_uuid() == ready);
        assert_eq!(listed, pinnable, "{state}, {generation}");
        let token = state_now
            .lifecycle_changed_at
            .expect("lifecycle_changed_at");
        assert_eq!(
            pin_restore_source(
                pool,
                sandbox,
                LocalArtifactId::from_uuid(ready),
                token,
                state_now.restore_node_id,
            )
            .await?,
            pinnable,
            "{state}, {generation}"
        );
    }
    Ok(())
}

/// Two distinct unstamped captures after a stamped one that tie on first
/// registration are both listed, and an operator can pin either: neither
/// provably follows the other. When one registered after the other, only the
/// later one is, besides the stamped capture.
#[tokio::test(flavor = "current_thread")]
async fn an_operator_pins_either_of_two_tied_unstamped_captures() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    // (case, the second capture ties the first, the first is pinnable)
    for (case, tied, first_pinnable) in [("tied", true, true), ("later", false, false)] {
        for pick_first in [true, false] {
            let sandbox = insert_sandbox(pool, "suspended").await?;
            let (stamped, first, second) = (Uuid::now_v7(), Uuid::now_v7(), Uuid::now_v7());
            insert_backup(pool, sandbox, stamped, "suspend", Some(4), 40).await?;
            insert_backup(pool, sandbox, first, "suspend", None, 30).await?;
            insert_backup(pool, sandbox, second, "suspend", None, 20).await?;
            if tied {
                sqlx::query(sql!(
                    "UPDATE snapshots
                        SET timestamp = (SELECT timestamp FROM snapshots
                                          WHERE sandbox_id = $1 AND local_artifact_id = $2)
                      WHERE sandbox_id = $1 AND local_artifact_id = $3"
                ))
                .bind(sandbox)
                .bind(first)
                .bind(second)
                .execute(pool)
                .await?;
            }
            let state = restore_pin_state(pool, sandbox).await?.expect("sandbox");
            assert!(state.unordered, "{case}");
            let mut listed: Vec<Uuid> = state
                .captures
                .iter()
                .map(|capture| capture.local_artifact_id.as_uuid())
                .collect();
            listed.sort();
            let mut expected = if first_pinnable {
                vec![stamped, first, second]
            } else {
                vec![stamped, second]
            };
            expected.sort();
            assert_eq!(listed, expected, "{case}");
            let token = state.lifecycle_changed_at.expect("lifecycle_changed_at");
            let (pick, pinnable) = if pick_first {
                (first, first_pinnable)
            } else {
                (second, true)
            };
            assert_eq!(
                pin_restore_source(pool, sandbox, LocalArtifactId::from_uuid(pick), token, None)
                    .await?,
                pinnable,
                "{case}, pick first {pick_first}"
            );
        }
    }
    Ok(())
}

/// A Kubernetes node named `name` in `state`, or a node with no Kubernetes
/// mapping when `name` is `None`.
async fn insert_node(
    pool: &PgPool,
    name: Option<&str>,
    state: &str,
) -> TestResult<pplx_space_accounts::NodeId> {
    insert_node_as(pool, name, state, "worker", SubstrateKind::Kubernetes).await
}

/// [`insert_node`] for a node registered as `role` by `managed_by`.
async fn insert_node_as(
    pool: &PgPool,
    name: Option<&str>,
    state: &str,
    role: &str,
    managed_by: SubstrateKind,
) -> TestResult<pplx_space_accounts::NodeId> {
    let node = pplx_space_accounts::NodeId::new();
    sqlx::query(sql!(
        "INSERT INTO nodes (id, address, role, managed_by, state)
         VALUES ($1, '127.0.0.1', $3, $4, $2)"
    ))
    .bind(node)
    .bind(state)
    .bind(role)
    .bind(managed_by)
    .execute(pool)
    .await?;
    if let Some(name) = name {
        sqlx::query(sql!(
            "INSERT INTO node_k8s (node_id, k8s_node_name) VALUES ($1, $2)"
        ))
        .bind(node)
        .bind(name)
        .execute(pool)
        .await?;
    }
    Ok(node)
}

/// A retired execution of `sandbox` at `generation` that ran on `node_name`.
async fn insert_execution(
    pool: &PgPool,
    sandbox: SandboxId,
    generation: i64,
    node_name: Option<&str>,
) -> TestResult {
    sqlx::query(sql!(
        "INSERT INTO sandbox_k8s_executions (
             sandbox_id, generation, pod_name, pod_namespace, state, active, k8s_node_name
         )
         VALUES ($1, $2, $3, 'sandboxes', 'suspended', FALSE, $4)"
    ))
    .bind(sandbox)
    .bind(generation)
    .bind(format!(
        "space-sbx-{}-g{generation}",
        sandbox.as_uuid().simple()
    ))
    .bind(node_name)
    .execute(pool)
    .await?;
    Ok(())
}

/// The node the pin asks is the one the Kubernetes restore loads from
/// (`restore_source_node_name`): the placement's mapped node, else the
/// newest execution's node, even once the placement pointers are cleared.
/// A node holding a local copy restores it in place of the pinned backup,
/// so asking only the placement would let a pin through that restores
/// another capture.
#[tokio::test(flavor = "current_thread")]
async fn the_pin_asks_the_node_the_restore_loads_from() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let prefix = Uuid::now_v7().simple().to_string();
    let name = |suffix: &str| format!("restore-pin-{prefix}-{suffix}");
    let placed = insert_node(pool, Some(&name("placed")), "ready").await?;
    let _older = insert_node(pool, Some(&name("older")), "ready").await?;
    let newest = insert_node(pool, Some(&name("newest")), "ready").await?;
    let unmapped = insert_node(pool, None, "ready").await?;
    let deleted_k8s = insert_node(pool, Some(&name("deleted-k8s")), "deleted").await?;
    let deleted_direct = insert_node(pool, None, "deleted").await?;
    // Two registrations of one name: the ready one is the node.
    let _demoted = insert_node(pool, Some(&name("reused")), "registered").await?;
    let reused = insert_node(pool, Some(&name("reused")), "ready").await?;

    // (placement, executions oldest first, expected)
    let cases = [
        (Some(placed), vec![Some(name("newest"))], Some(placed)),
        (
            None,
            vec![Some(name("older")), Some(name("newest")), None],
            Some(newest),
        ),
        // The newest execution names a node nothing registered: the restore
        // pins that name, not an older execution's node.
        (None, vec![Some(name("older")), Some(name("gone"))], None),
        (None, vec![Some(name("reused"))], Some(reused)),
        (Some(unmapped), vec![Some(name("newest"))], Some(newest)),
        (Some(unmapped), vec![], Some(unmapped)),
        // Soft-deleted tombstone rows never host a live local copy.
        (Some(deleted_k8s), vec![Some(name("deleted-k8s"))], None),
        (Some(deleted_direct), vec![], None),
        (None, vec![], None),
    ];
    for (placement, executions, expected) in cases {
        let sandbox = insert_sandbox(pool, "suspended").await?;
        sqlx::query(sql!(
            "UPDATE sandboxes SET active_node_id = $2 WHERE id = $1"
        ))
        .bind(sandbox)
        .bind(placement)
        .execute(pool)
        .await?;
        for (generation, node_name) in (1_i64..).zip(&executions) {
            insert_execution(pool, sandbox, generation, node_name.as_deref()).await?;
        }
        let state = restore_pin_state(pool, sandbox).await?.expect("sandbox");
        assert_eq!(
            state.restore_node_id, expected,
            "placement {placement:?}, executions {executions:?}"
        );
    }
    Ok(())
}

/// The node the pin asks is a Kubernetes worker, on the host the restore
/// schedules onto. A builder registers under the same host name with its own
/// row and holds no sandbox, and so does another substrate's node that
/// carries the same mapping. Of two workers under one name the restore runs
/// on the ready one, else on the one that registered last. A worker that
/// registered again has a new row under the name its placement's old row
/// still maps to, and the restore runs there whatever host an execution
/// named. A placement with no Kubernetes mapping is its own node only when
/// it's a worker, and a `target_node_id` that's gone isn't replaced by
/// `active_node_id`: neither substrate restores from that node.
#[tokio::test(flavor = "current_thread")]
async fn the_pin_asks_the_live_worker_on_the_restore_host() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let prefix = Uuid::now_v7().simple().to_string();
    let name = |suffix: &str| format!("restore-pin-{prefix}-{suffix}");
    let kubernetes = SubstrateKind::Kubernetes;
    // The builder and the other substrate's worker are the ready, newer rows:
    // only the role rules out the one, and only `managed_by` the other.
    let worker = insert_node(pool, Some(&name("shared")), "registered").await?;
    insert_node_as(pool, Some(&name("shared")), "ready", "builder", kubernetes).await?;
    insert_node_as(
        pool,
        Some(&name("shared")),
        "ready",
        "worker",
        SubstrateKind::Space,
    )
    .await?;
    insert_node_as(
        pool,
        Some(&name("builder-only")),
        "ready",
        "builder",
        kubernetes,
    )
    .await?;
    // The ready worker registered first, and the newer one isn't ready.
    let ready = insert_node(pool, Some(&name("pair")), "ready").await?;
    let unready = insert_node(pool, Some(&name("pair")), "registered").await?;
    // Neither is ready, so the node is the one whose mapping was written
    // last, which isn't the row created last.
    let remapped = insert_node(pool, Some(&name("twice")), "registered").await?;
    let stale = insert_node(pool, Some(&name("twice")), "registered").await?;
    // Neither is ready and their mappings were written at the same time, so
    // the node is the row created last.
    insert_node(pool, Some(&name("tied")), "registered").await?;
    let tied_last = insert_node(pool, Some(&name("tied")), "registered").await?;
    let now: time::OffsetDateTime = sqlx::query_scalar(sql!("SELECT NOW()"))
        .fetch_one(pool)
        .await?;
    sqlx::query(sql!(
        "UPDATE node_k8s SET updated_at = $2 - INTERVAL '4 hours' WHERE k8s_node_name = $1"
    ))
    .bind(name("tied"))
    .bind(now)
    .execute(pool)
    .await?;
    for (node, mapped_ago) in [(ready, 3), (unready, 0), (remapped, 1), (stale, 2)] {
        sqlx::query(sql!(
            "UPDATE node_k8s
                SET updated_at = $3 - make_interval(hours => $2::INT)
              WHERE node_id = $1"
        ))
        .bind(node)
        .bind(mapped_ago)
        .bind(now)
        .execute(pool)
        .await?;
    }
    let retired = insert_node(pool, Some(&name("again")), "deleted").await?;
    let again = insert_node(pool, Some(&name("again")), "ready").await?;
    insert_node(pool, Some(&name("other")), "ready").await?;
    let unmapped = insert_node(pool, None, "ready").await?;
    let deleted_unmapped = insert_node(pool, None, "deleted").await?;
    let unmapped_builder = insert_node_as(pool, None, "ready", "builder", kubernetes).await?;

    // (target, active, executions oldest first, expected)
    let cases = [
        (None, None, vec![name("shared")], Some(worker)),
        (None, None, vec![name("builder-only")], None),
        (None, None, vec![name("pair")], Some(ready)),
        (None, None, vec![name("twice")], Some(remapped)),
        (None, None, vec![name("tied")], Some(tied_last)),
        (Some(retired), None, vec![name("other")], Some(again)),
        (None, Some(retired), vec![], Some(again)),
        (Some(unmapped), None, vec![], Some(unmapped)),
        (None, Some(unmapped), vec![], Some(unmapped)),
        (Some(unmapped_builder), None, vec![], None),
        (Some(deleted_unmapped), Some(unmapped), vec![], None),
    ];
    for (target, active, executions, expected) in cases {
        let sandbox = insert_sandbox(pool, "suspended").await?;
        sqlx::query(sql!(
            "UPDATE sandboxes SET target_node_id = $2, active_node_id = $3 WHERE id = $1"
        ))
        .bind(sandbox)
        .bind(target)
        .bind(active)
        .execute(pool)
        .await?;
        for (generation, node_name) in (1_i64..).zip(&executions) {
            insert_execution(pool, sandbox, generation, Some(node_name.as_str())).await?;
        }
        let state = restore_pin_state(pool, sandbox).await?.expect("sandbox");
        assert_eq!(
            state.restore_node_id, expected,
            "target {target:?}, active {active:?}, executions {executions:?}"
        );
    }
    Ok(())
}

/// If `restore_node_id` changes between [`restore_pin_state`] and
/// [`pin_restore_source`] without moving `lifecycle_changed_at` (for instance a
/// node or execution attribution lands after a `None` read, or placement moves
/// between nodes), the guarded write refuses so a pin never bypasses asking the
/// node that would actually restore its local copy.
#[tokio::test(flavor = "current_thread")]
async fn pin_restore_source_refuses_when_restore_node_changes_after_read() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let prefix = Uuid::now_v7().simple().to_string();
    let node_a_name = format!("restore-pin-cas-{prefix}-a");
    let node_b_name = format!("restore-pin-cas-{prefix}-b");
    let node_a = insert_node(pool, Some(&node_a_name), "ready").await?;
    let node_b = insert_node(pool, Some(&node_b_name), "ready").await?;

    let sandbox = insert_sandbox(pool, "suspended").await?;
    let (stamped, unstamped) = (Uuid::now_v7(), Uuid::now_v7());
    insert_backup(pool, sandbox, stamped, "suspend", Some(5), 40).await?;
    insert_backup(pool, sandbox, unstamped, "suspend", None, 30).await?;

    // Read sees `restore_node_id = None` (caller skips asking any node);
    // before the write executes, an execution row resolves `restore_node_id` to
    // `Some(node_a)` without touching `sandboxes.lifecycle_changed_at`.
    let initial = restore_pin_state(pool, sandbox).await?.expect("sandbox");
    assert_eq!(initial.restore_node_id, None);
    let token = initial.lifecycle_changed_at.expect("lifecycle_changed_at");

    insert_execution(pool, sandbox, 1, Some(&node_a_name)).await?;
    let after_exec = restore_pin_state(pool, sandbox).await?.expect("sandbox");
    assert_eq!(after_exec.lifecycle_changed_at, Some(token));
    assert_eq!(after_exec.restore_node_id, Some(node_a));

    assert!(
        !pin_restore_source(
            pool,
            sandbox,
            LocalArtifactId::from_uuid(unstamped),
            token,
            initial.restore_node_id,
        )
        .await?,
        "None -> Some(node_a) race must refuse the pin"
    );
    assert_eq!(
        restore_pin_state(pool, sandbox)
            .await?
            .expect("sandbox")
            .pinned_artifact,
        None
    );

    // Read sees `Some(node_a)`; placement moves to `Some(node_b)` without
    // changing `lifecycle_changed_at`.
    sqlx::query(sql!(
        "UPDATE sandboxes SET target_node_id = $2 WHERE id = $1"
    ))
    .bind(sandbox)
    .bind(node_b)
    .execute(pool)
    .await?;
    let after_move = restore_pin_state(pool, sandbox).await?.expect("sandbox");
    assert_eq!(after_move.lifecycle_changed_at, Some(token));
    assert_eq!(after_move.restore_node_id, Some(node_b));

    assert!(
        !pin_restore_source(
            pool,
            sandbox,
            LocalArtifactId::from_uuid(unstamped),
            token,
            Some(node_a),
        )
        .await?,
        "Some(node_a) -> Some(node_b) race must refuse the pin"
    );
    assert!(
        pin_restore_source(
            pool,
            sandbox,
            LocalArtifactId::from_uuid(unstamped),
            token,
            Some(node_b),
        )
        .await?,
        "matching expected_restore_node_id succeeds"
    );
    Ok(())
}
