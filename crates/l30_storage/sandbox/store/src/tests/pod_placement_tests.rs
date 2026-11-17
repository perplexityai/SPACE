//! [`wake::record_pod_node_placement`] projects a pod's node onto the row
//! only while the pod's execution is the sandbox's active one and the row is
//! in a lifecycle a pod can back it in.

use super::*;
use crate::test_support::stored_lifecycle_matrix;
use crate::wake;

/// `active_node_id`, `target_node_id`, `placement`, `placement_changed_at`
/// (as text, compared exactly), as stored.
type PodPlacement = (Option<NodeId>, Option<NodeId>, String, Option<String>);

/// The `placement_changed_at` the tests seed, which a write that places the
/// row moves.
const SEEDED_CHANGED_AT: &str = "2026-01-02 03:04:05+00";

async fn read_pod_placement(pool: &PgPool, id: Uuid) -> TestResult<PodPlacement> {
    Ok(sqlx::query_as::<_, PodPlacement>(sql!(
        "SELECT active_node_id, target_node_id, placement, placement_changed_at::TEXT
           FROM sandboxes WHERE id = $1"
    ))
    .bind(id)
    .fetch_one(pool)
    .await?)
}

/// A pod's node is recorded while the row is in a lifecycle a pod can back
/// it in: those `ensure_pod_exists` materializes one in, and `suspending` and
/// `stopping`, while the pod a suspend or stop acts on is still the
/// sandbox's. A row being deleted, failed, quarantined, a tombstone or a
/// value outside the vocabulary keeps its placement, and the write reports
/// the lifecycle that refused it. `A`: recorded; `.`: refused, row unchanged.
/// The pod's execution is active in every cell, so only the lifecycle
/// decides.
#[tokio::test(flavor = "current_thread")]
async fn record_pod_node_placement_records_only_lifecycles_a_pod_backs() -> TestResult {
    const EXPECTED: &[&str] = &[
        //              placement
        "created            A",
        "creating           A",
        "starting           A",
        "started            A",
        "restoring          A",
        "pausing            A",
        "paused             A",
        "resuming           A",
        "suspending         A",
        "suspended          A",
        "stopping           A",
        "stopped            A",
        "deleting           .",
        "checkpointing      A",
        "quarantined        .",
        "failed             .",
        "deleted            .",
        "bogus              .",
        "_Suspended_        .",
    ];
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };

    let mut mismatches = Vec::new();
    for (stored, [(_, expect)]) in stored_lifecycle_matrix(["placement"], EXPECTED) {
        let id = insert_sandbox(&db.pool, None, stored).await?;
        insert_k8s_execution(&db.pool, id, 2, true).await?;
        let node = NodeId::new();

        sqlx::query(sql!(
            "UPDATE sandboxes SET placement_changed_at = $2::TIMESTAMPTZ WHERE id = $1"
        ))
        .bind(id)
        .bind(SEEDED_CHANGED_AT)
        .execute(&db.pool)
        .await?;

        let written = wake::record_pod_node_placement(
            &db.pool,
            SandboxId::from_uuid(id),
            node,
            generation(2),
        )
        .await?;

        let (active, target, placement, changed_at) = read_pod_placement(&db.pool, id).await?;
        let (expected_write, expected_row) = if expect.applies() {
            (
                wake::PodNodePlacementWrite::Placed,
                (Some(node), Some(node), "on_node"),
            )
        } else {
            (
                wake::PodNodePlacementWrite::LifecycleRefused {
                    lifecycle: stored.to_owned(),
                },
                (None, None, "unplaced"),
            )
        };
        let stamped = changed_at.as_deref() != Some(SEEDED_CHANGED_AT) && changed_at.is_some();
        if written != expected_write
            || (active, target, placement.as_str()) != expected_row
            || stamped != expect.applies()
        {
            mismatches.push(format!(
                "{stored:?}: expected {expect:?}, got {written:?} \
                 placement={active:?} {target:?} {placement:?} changed_at={changed_at:?}"
            ));
        }
    }
    assert!(mismatches.is_empty(), "{}", mismatches.join("\n"));
    Ok(())
}

/// A late placement write for a pod whose execution is no longer the active
/// one, because a newer execution replaced it or a suspend retired it with
/// none active, records nothing: the row keeps the placement it moved on to.
/// The active execution's own pod is recorded, stamping
/// `placement_changed_at` anew, and one already placed there is found so
/// without being rewritten.
#[tokio::test(flavor = "current_thread")]
async fn record_pod_node_placement_records_only_the_active_execution() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let current_node = NodeId::new();
    let left_node = NodeId::new();

    // Re-placed: generation 3 runs on `current_node`; a late write for
    // generation 2's pod names the node it left.
    let replaced = insert_sandbox(&db.pool, None, "started").await?;
    insert_k8s_execution(&db.pool, replaced, 2, false).await?;
    insert_k8s_execution(&db.pool, replaced, 3, true).await?;
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET active_node_id = $2, target_node_id = $2, placement = 'on_node',
                placement_changed_at = NOW() - INTERVAL '1 hour'
          WHERE id = $1"
    ))
    .bind(replaced)
    .bind(current_node)
    .execute(&db.pool)
    .await?;
    let before = read_pod_placement(&db.pool, replaced).await?;
    let written = wake::record_pod_node_placement(
        &db.pool,
        SandboxId::from_uuid(replaced),
        left_node,
        generation(2),
    )
    .await?;
    assert_eq!(
        written,
        wake::PodNodePlacementWrite::NotActive,
        "a replaced execution's pod matches nothing"
    );
    assert_eq!(
        read_pod_placement(&db.pool, replaced).await?,
        before,
        "the re-placed row keeps its node"
    );

    // Suspended: the suspend retired generation 2 and unplaced the row.
    let suspended = insert_sandbox(&db.pool, None, "suspended").await?;
    insert_k8s_execution(&db.pool, suspended, 2, false).await?;
    let written = wake::record_pod_node_placement(
        &db.pool,
        SandboxId::from_uuid(suspended),
        left_node,
        generation(2),
    )
    .await?;
    assert_eq!(
        written,
        wake::PodNodePlacementWrite::NotActive,
        "a retired execution's pod matches nothing"
    );
    assert_eq!(
        read_pod_placement(&db.pool, suspended).await?,
        (None, None, "unplaced".to_owned(), None),
        "the suspended row stays unplaced"
    );

    // The active execution's pod, already placed there: found so, and not
    // rewritten.
    let written = wake::record_pod_node_placement(
        &db.pool,
        SandboxId::from_uuid(replaced),
        current_node,
        generation(3),
    )
    .await?;
    assert_eq!(written, wake::PodNodePlacementWrite::AlreadyPlaced);
    assert_eq!(
        read_pod_placement(&db.pool, replaced).await?,
        before,
        "a row already placed on that node is not rewritten"
    );
    let placed = insert_sandbox(&db.pool, None, "started").await?;
    insert_k8s_execution(&db.pool, placed, 4, true).await?;
    sqlx::query(sql!(
        "UPDATE sandboxes SET placement_changed_at = $2::TIMESTAMPTZ WHERE id = $1"
    ))
    .bind(placed)
    .bind(SEEDED_CHANGED_AT)
    .execute(&db.pool)
    .await?;
    let written = wake::record_pod_node_placement(
        &db.pool,
        SandboxId::from_uuid(placed),
        current_node,
        generation(4),
    )
    .await?;
    assert_eq!(written, wake::PodNodePlacementWrite::Placed);
    let (active, target, placement, changed_at) = read_pod_placement(&db.pool, placed).await?;
    assert_eq!(
        (active, target, placement.as_str()),
        (Some(current_node), Some(current_node), "on_node"),
        "the active execution's node is recorded"
    );
    assert!(
        changed_at.is_some() && changed_at.as_deref() != Some(SEEDED_CHANGED_AT),
        "the placement is stamped anew: {changed_at:?}"
    );
    Ok(())
}
