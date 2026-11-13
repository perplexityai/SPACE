//! [`repair_restore_pin`] and [`restore_pin_repair_candidates`] against
//! Postgres: the repair pins only the node's latest capture, and never a row
//! that ran elsewhere since, moved, or was pinned meanwhile.

use pplx_space_accounts::NodeId;
use pplx_space_common_core::sql;
use pplx_space_common_core::types::{LocalArtifactId, SandboxId, SubstrateKind};
use pplx_space_db_test_fixtures::{
    SEED_ORG_ID as SEED_ORG, SEED_USER_ID as SEED_USER, insert_ready_worker_node, try_pg_test_db,
};
use pplx_space_test_env::TestResult;
use sqlx::PgPool;
use uuid::Uuid;

use super::{RestorePinRepairCandidate, repair_restore_pin, restore_pin_repair_candidates};

async fn insert_node(pool: &PgPool) -> TestResult<NodeId> {
    let node = NodeId::new();
    insert_ready_worker_node(pool, node).await?;
    Ok(node)
}

/// An unpinned Kubernetes sandbox in `lifecycle`, targeted at `node`.
async fn insert_sandbox(pool: &PgPool, lifecycle: &str, node: NodeId) -> TestResult<SandboxId> {
    let id = SandboxId::new();
    sqlx::query(sql!(
        "INSERT INTO sandboxes
            (id, user_id, org_id, managed_by, lifecycle, target_node_id, template_id,
             network_policy_generation)
         VALUES ($1, $2, $3, 'k8s', $4, $5, 'tmpl-restore-pin-repair-test', 1)"
    ))
    .bind(id)
    .bind(SEED_USER)
    .bind(SEED_ORG)
    .bind(lifecycle)
    .bind(node)
    .execute(pool)
    .await?;
    Ok(id)
}

/// A full suspend backup of `artifact` from `node`, registered `minutes_ago`.
async fn insert_backup(
    pool: &PgPool,
    sandbox: SandboxId,
    node: NodeId,
    artifact: Uuid,
    (state, generation, minutes_ago): (&str, Option<i64>, i32),
) -> TestResult {
    let snapshot = Uuid::now_v7();
    sqlx::query(sql!(
        "INSERT INTO snapshots (
             id, sandbox_id, type, state, timestamp, source_node_id, local_artifact_id,
             artifact_kind, artifact_origin, generation, org_id
         )
         VALUES ($1, $2, 'full', $3, NOW() - make_interval(mins => $4::INT), $5, $6, 'full',
                 'suspend', $7, $8)"
    ))
    .bind(snapshot)
    .bind(sandbox)
    .bind(state)
    .bind(minutes_ago)
    .bind(node)
    .bind(artifact)
    .bind(generation)
    .bind(SEED_ORG)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO snapshot_storage_locations (snapshot_id, storage_connection_string, org_id)
         VALUES ($1, 's3://test/restore-pin-repair?region=us-east-1', $2)"
    ))
    .bind(snapshot)
    .bind(SEED_ORG)
    .execute(pool)
    .await?;
    Ok(())
}

async fn pin(pool: &PgPool, sandbox: SandboxId) -> TestResult<Option<Uuid>> {
    Ok(sqlx::query_scalar(sql!(
        "SELECT current_local_artifact_id FROM sandboxes WHERE id = $1"
    ))
    .bind(sandbox)
    .fetch_one(pool)
    .await?)
}

/// The node's artifact is pinned when its ready backup from that node is the
/// latest capture. Each refusal leaves the row unpinned: the artifact has no
/// ready backup, its backup came from another node, a later execution's
/// capture outranks it, a capture of another artifact registered after it
/// (from another node, or from this one without a generation, while the node was
/// asked), or the row moved, isn't suspended, or was pinned meanwhile. A
/// named snapshot's backup registered after it isn't a capture, and doesn't
/// refuse.
#[tokio::test(flavor = "current_thread")]
async fn the_repair_pins_only_the_nodes_latest_capture() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let node = insert_node(pool).await?;
    let other = insert_node(pool).await?;
    const READY: (&str, Option<i64>, i32) = ("ready", None, 10);

    #[derive(Debug, Clone, Copy)]
    enum Case {
        Latest,
        NotReady,
        FromOtherNode,
        Outranked,
        CapturedElsewhereLater,
        CapturedHereLater,
        NamedSnapshotLater,
        Moved,
        NotSuspended,
        AlreadyPinned,
        ResuspendedSince,
    }
    let mut mismatches = Vec::new();
    for case in [
        Case::Latest,
        Case::NotReady,
        Case::FromOtherNode,
        Case::Outranked,
        Case::CapturedElsewhereLater,
        Case::CapturedHereLater,
        Case::NamedSnapshotLater,
        Case::Moved,
        Case::NotSuspended,
        Case::AlreadyPinned,
        Case::ResuspendedSince,
    ] {
        let lifecycle = if matches!(case, Case::NotSuspended) {
            "started"
        } else {
            "suspended"
        };
        let sandbox = insert_sandbox(pool, lifecycle, node).await?;
        let artifact = Uuid::now_v7();
        match case {
            Case::NotReady => {
                insert_backup(pool, sandbox, node, artifact, ("failed", None, 10)).await?;
            }
            Case::FromOtherNode => insert_backup(pool, sandbox, other, artifact, READY).await?,
            // Generation 1, so a generation 2 registered before it
            // outranks it without registering after it.
            Case::Outranked => {
                insert_backup(pool, sandbox, node, artifact, ("ready", Some(1), 10)).await?;
            }
            _ => insert_backup(pool, sandbox, node, artifact, READY).await?,
        }
        match case {
            Case::Outranked => {
                insert_backup(pool, sandbox, node, Uuid::now_v7(), ("failed", Some(2), 20)).await?;
            }
            Case::CapturedElsewhereLater => {
                insert_backup(pool, sandbox, other, Uuid::now_v7(), ("creating", None, 5)).await?;
            }
            Case::CapturedHereLater => {
                insert_backup(pool, sandbox, node, Uuid::now_v7(), ("creating", None, 5)).await?;
            }
            // A named snapshot's backup isn't a capture the sandbox resumes
            // from: it doesn't show the sandbox ran since.
            Case::NamedSnapshotLater => {
                sqlx::query(sql!(
                    "INSERT INTO snapshots (
                         id, sandbox_id, type, state, timestamp, source_node_id,
                         local_artifact_id, artifact_kind, artifact_origin,
                         artifact_snapshot_name, org_id
                     )
                     VALUES ($1, $2, 'full', 'ready', NOW() - INTERVAL '5 minutes', $3, $4,
                             'full', 'snapshot', 'named', $5)"
                ))
                .bind(Uuid::now_v7())
                .bind(sandbox)
                .bind(node)
                .bind(Uuid::now_v7())
                .bind(SEED_ORG)
                .execute(pool)
                .await?;
            }
            Case::Moved => {
                sqlx::query(sql!(
                    "UPDATE sandboxes SET target_node_id = $2 WHERE id = $1"
                ))
                .bind(sandbox)
                .bind(other)
                .execute(pool)
                .await?;
            }
            Case::AlreadyPinned => {
                sqlx::query(sql!(
                    "UPDATE sandboxes
                        SET current_local_artifact_id = $2,
                            current_local_artifact_kind = 'full',
                            current_local_artifact_origin = 'suspend'
                      WHERE id = $1"
                ))
                .bind(sandbox)
                .bind(Uuid::now_v7())
                .execute(pool)
                .await?;
            }
            _ => {}
        }
        // The candidate as the repair read it, before asking the node.
        let lifecycle_changed_at: time::OffsetDateTime = sqlx::query_scalar(sql!(
            "SELECT lifecycle_changed_at FROM sandboxes WHERE id = $1"
        ))
        .bind(sandbox)
        .fetch_one(pool)
        .await?;
        if matches!(case, Case::ResuspendedSince) {
            // The sandbox restored and suspended again while the node was
            // asked: its answer describes the earlier suspend.
            sqlx::query(sql!(
                "UPDATE sandboxes SET lifecycle_changed_at = lifecycle_changed_at + INTERVAL '1 second'
                  WHERE id = $1"
            ))
            .bind(sandbox)
            .execute(pool)
            .await?;
        }
        let before = pin(pool, sandbox).await?;
        let repaired = repair_restore_pin(
            pool,
            RestorePinRepairCandidate {
                sandbox_id: sandbox,
                node_id: node,
                lifecycle_changed_at,
            },
            LocalArtifactId::from_uuid(artifact),
        )
        .await?;
        let after = pin(pool, sandbox).await?;
        let expected = matches!(case, Case::Latest | Case::NamedSnapshotLater);
        let expected_pin = if expected { Some(artifact) } else { before };
        if repaired != expected || after != expected_pin {
            mismatches.push(format!("{case:?}: repaired {repaired}, pin {after:?}"));
        }
    }
    assert!(mismatches.is_empty(), "{mismatches:#?}");
    Ok(())
}

/// The candidates are the unpinned suspended Kubernetes rows placed on a node
/// with a ready suspend backup, in id order after the cursor.
#[tokio::test(flavor = "current_thread")]
async fn candidates_are_unpinned_suspended_rows_with_a_ready_backup() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let node = insert_node(pool).await?;
    let ready = (("ready", None, 10), "suspended");
    let mut listed = Vec::new();
    for (backup, lifecycle, expected) in [
        (ready.0, ready.1, true),
        (("failed", None, 10), "suspended", false),
        (ready.0, "started", false),
        (ready.0, ready.1, true),
    ] {
        let sandbox = insert_sandbox(pool, lifecycle, node).await?;
        insert_backup(pool, sandbox, node, Uuid::now_v7(), backup).await?;
        if expected {
            let lifecycle_changed_at: time::OffsetDateTime = sqlx::query_scalar(sql!(
                "SELECT lifecycle_changed_at FROM sandboxes WHERE id = $1"
            ))
            .bind(sandbox)
            .fetch_one(pool)
            .await?;
            listed.push(RestorePinRepairCandidate {
                sandbox_id: sandbox,
                node_id: node,
                lifecycle_changed_at,
            });
        }
    }
    let all =
        restore_pin_repair_candidates(pool, SubstrateKind::Kubernetes.as_str(), None, 1000).await?;
    let ours: Vec<_> = all
        .iter()
        .copied()
        .filter(|candidate| candidate.node_id == node)
        .collect();
    assert_eq!(ours, listed);
    let after_first = restore_pin_repair_candidates(
        pool,
        SubstrateKind::Kubernetes.as_str(),
        Some(listed[0].sandbox_id),
        1000,
    )
    .await?
    .into_iter()
    .filter(|candidate| candidate.node_id == node)
    .collect::<Vec<_>>();
    assert_eq!(after_first, listed[1..]);

    // Each substrate lists only its own rows.
    sqlx::query(sql!(
        "UPDATE sandboxes SET managed_by = 'space' WHERE id = $1"
    ))
    .bind(listed[0].sandbox_id)
    .execute(pool)
    .await?;
    let ours = |all: Vec<RestorePinRepairCandidate>| -> Vec<RestorePinRepairCandidate> {
        all.into_iter()
            .filter(|candidate| candidate.node_id == node)
            .collect()
    };
    assert_eq!(
        ours(restore_pin_repair_candidates(pool, SubstrateKind::Space.as_str(), None, 1000).await?),
        listed[..1]
    );
    assert_eq!(
        ours(
            restore_pin_repair_candidates(pool, SubstrateKind::Kubernetes.as_str(), None, 1000)
                .await?
        ),
        listed[1..]
    );
    Ok(())
}
