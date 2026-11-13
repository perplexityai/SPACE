use pplx_space_common_core::types::{NodeId, SandboxId, SubstrateKind};

use std::num::NonZeroUsize;
use std::time::Duration;

use pplx_space_common_core::sql;
use pplx_space_db_test_fixtures::{SEED_ORG_ID, SEED_USER_ID, try_pg_test_db};
use pplx_space_sandbox_residency::ResidencyEvidence;
use pplx_space_test_env::TestResult;
use sqlx::PgPool;

use super::{
    NodeHold, NodeHoldPlacement, NodeHoldReason, NodeHoldTerms, STUCK_TRANSIENT_AFTER, list,
};

const MINUTE: Duration = Duration::from_secs(60);
const LIMIT: NonZeroUsize = NonZeroUsize::new(500).expect("non-zero");

/// A worker whose heartbeat and complete census are both fresh, so a row
/// placed long enough ago and missing from the census is provably absent.
async fn insert_node(pool: &PgPool) -> TestResult<NodeId> {
    let node_id = NodeId::new();
    sqlx::query(sql!(
        "INSERT INTO nodes (id, address, role, managed_by, state)
         VALUES ($1, '10.0.0.40'::INET, 'worker', 'k8s', 'registered')"
    ))
    .bind(node_id)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO node_status_snapshots (
             node_id, observed_at, stale_after, reachable, services,
             sandbox_inventory_pruned_at, updated_at
         ) VALUES ($1, NOW(), NOW() + INTERVAL '1 hour', TRUE, '[]'::JSONB, NOW(), NOW())"
    ))
    .bind(node_id)
    .execute(pool)
    .await?;
    Ok(node_id)
}

/// Where a seeded row points and what the node and snapshots say about it.
#[derive(Debug, Clone, Copy)]
struct Seed {
    lifecycle: &'static str,
    active: bool,
    target: bool,
    age: Duration,
    /// The node's census lists the row.
    reported: bool,
    marker: Option<&'static str>,
    /// A `creating` full snapshot uploading from the node.
    uploading: bool,
    /// A `ready` full snapshot with a live storage location.
    durable: bool,
}

impl Seed {
    const fn on_active(lifecycle: &'static str) -> Self {
        Self {
            lifecycle,
            active: true,
            target: false,
            age: Duration::from_secs(3600),
            reported: true,
            marker: None,
            uploading: false,
            durable: false,
        }
    }

    const fn on_target(lifecycle: &'static str) -> Self {
        Self {
            active: false,
            target: true,
            ..Self::on_active(lifecycle)
        }
    }
}

/// Inserts `seed` against `node`. Placement settled two hours ago, so the
/// census can prove absence; snapshots are stamped after it, so they belong
/// to the current placement.
async fn insert(pool: &PgPool, node: NodeId, seed: Seed) -> TestResult<SandboxId> {
    let id = SandboxId::new();
    sqlx::query(sql!(
        "INSERT INTO sandboxes (
             id, user_id, org_id, lifecycle, placement, managed_by,
             active_node_id, target_node_id, evacuation_restore_target,
             restore_attempt_count, next_restore_attempt_at,
             placement_changed_at, lifecycle_changed_at, network_policy_generation
         ) VALUES (
             $1, $2, $3, $4, 'placed', $5,
             CASE WHEN $6 THEN $8::UUID END, CASE WHEN $7 THEN $8::UUID END, $9,
             CASE WHEN $9 IS NULL THEN 0 ELSE 2 END,
             CASE WHEN $9 IS NULL THEN NULL ELSE NOW() + INTERVAL '5 minutes' END,
             NOW() - INTERVAL '2 hours', NOW() - make_interval(secs => $10), 1
         )"
    ))
    .bind(id)
    .bind(SEED_USER_ID)
    .bind(SEED_ORG_ID)
    .bind(seed.lifecycle)
    .bind(SubstrateKind::Kubernetes)
    .bind(seed.active)
    .bind(seed.target)
    .bind(node)
    .bind(seed.marker)
    .bind(seed.age.as_secs_f64())
    .execute(pool)
    .await?;
    if seed.reported {
        sqlx::query(sql!(
            "INSERT INTO sandbox_observed_states (sandbox_id, node_id, status)
             VALUES ($1, $2, $3)"
        ))
        .bind(id)
        .bind(node)
        .bind(seed.lifecycle)
        .execute(pool)
        .await?;
    }
    if seed.uploading {
        sqlx::query(sql!(
            "INSERT INTO snapshots (sandbox_id, type, state, timestamp, source_node_id)
             VALUES ($1, 'full', 'creating', NOW() - INTERVAL '1 minute', $2)"
        ))
        .bind(id)
        .bind(node)
        .execute(pool)
        .await?;
    }
    if seed.durable {
        sqlx::query(sql!(
            "WITH ready AS (
                 INSERT INTO snapshots (sandbox_id, type, state, timestamp, source_node_id)
                 VALUES ($1, 'full', 'ready', NOW() - INTERVAL '30 minutes', $2)
                 RETURNING id
             )
             INSERT INTO snapshot_storage_locations (snapshot_id, storage_connection_string)
             SELECT id, 's3://synthetic-bucket/holds' FROM ready"
        ))
        .bind(id)
        .bind(node)
        .execute(pool)
        .await?;
    }
    Ok(id)
}

/// The row that reaches `reason`, plus a `control` that differs in the one
/// fact the reason turns on and must not reach it. Exhaustive, so a new
/// reason does not compile until it has a row here.
fn case(reason: NodeHoldReason) -> (Seed, Seed) {
    match reason {
        NodeHoldReason::CensusAbsent => (
            Seed {
                reported: false,
                ..Seed::on_active("started")
            },
            Seed::on_active("started"),
        ),
        NodeHoldReason::Evacuating => (
            Seed {
                marker: Some("started"),
                ..Seed::on_active("suspending")
            },
            // Unmarked and young: an ordinary transition.
            Seed {
                age: MINUTE,
                ..Seed::on_active("suspending")
            },
        ),
        NodeHoldReason::UploadInFlight => (
            Seed {
                uploading: true,
                ..Seed::on_target("suspended")
            },
            Seed::on_target("suspended"),
        ),
        NodeHoldReason::StuckTransient => (
            Seed {
                age: STUCK_TRANSIENT_AFTER + MINUTE,
                ..Seed::on_active("starting")
            },
            Seed {
                age: STUCK_TRANSIENT_AFTER - MINUTE,
                ..Seed::on_active("starting")
            },
        ),
        NodeHoldReason::Transient => (
            Seed {
                age: STUCK_TRANSIENT_AFTER - MINUTE,
                ..Seed::on_target("restoring")
            },
            Seed {
                age: STUCK_TRANSIENT_AFTER + MINUTE,
                ..Seed::on_target("restoring")
            },
        ),
        NodeHoldReason::LocalOnlySnapshot => (
            Seed::on_target("suspended"),
            Seed {
                uploading: true,
                ..Seed::on_target("suspended")
            },
        ),
        // An earlier upload still `creating` does not make a resident whose
        // snapshot is already durable an upload in flight.
        NodeHoldReason::Resident => (
            Seed {
                uploading: true,
                durable: true,
                ..Seed::on_active("paused")
            },
            Seed {
                uploading: true,
                ..Seed::on_active("paused")
            },
        ),
        NodeHoldReason::TerminationBlocking => {
            (Seed::on_active("stopped"), Seed::on_active("started"))
        }
    }
}

fn held(holds: &[NodeHold], id: SandboxId) -> &NodeHold {
    holds
        .iter()
        .find(|hold| hold.sandbox_id == id)
        .unwrap_or_else(|| panic!("{id} is not listed"))
}

/// Each reason is reached by its own row and missed by a control that differs
/// in the fact the reason turns on, in both evidence modes. The census-absent
/// row is listed in both, counted under `LifecycleOnly` and discounted under
/// `NodeCensusAware`; the stuck and transient rows sit one minute either side
/// of the threshold.
#[tokio::test(flavor = "current_thread")]
async fn every_reason_is_reached_by_its_row_and_missed_by_its_control() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    for residency in [
        ResidencyEvidence::LifecycleOnly,
        ResidencyEvidence::NodeCensusAware,
    ] {
        for reason in NodeHoldReason::ALL {
            let node = insert_node(pool).await?;
            let (seed, control) = case(reason);
            let row = insert(pool, node, seed).await?;
            let control = insert(pool, node, control).await?;
            let holds = list(pool, node, residency, LIMIT).await?;
            let hold = held(&holds.rows, row);
            assert_eq!(hold.reason, reason, "{residency}: {hold:?}");
            assert_ne!(
                held(&holds.rows, control).reason,
                reason,
                "{residency}: the control must not reach {reason}"
            );
            let count = |r| {
                holds
                    .reason_counts
                    .iter()
                    .find(|(counted, _)| *counted == r)
                    .map(|(_, n)| *n)
            };
            assert_eq!(count(reason), Some(1), "{residency}: {reason} count");
            assert_eq!(holds.total, 2);
        }
    }
    Ok(())
}

/// The census decides only `resident` and `active_resident`, and only under
/// `NodeCensusAware`; a reported row on the same node keeps both. A
/// census-absent drain blocker keeps its blocker reason, and a discounted
/// phantom is listed after every row a term counts.
#[tokio::test(flavor = "current_thread")]
async fn census_absence_moves_only_the_census_terms() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let node = insert_node(pool).await?;
    let phantom = insert(pool, node, case(NodeHoldReason::CensusAbsent).0).await?;
    let reported = insert(pool, node, case(NodeHoldReason::CensusAbsent).1).await?;
    let blocker = insert(
        pool,
        node,
        Seed {
            target: true,
            reported: false,
            age: STUCK_TRANSIENT_AFTER + MINUTE,
            ..Seed::on_active("suspending")
        },
    )
    .await?;

    let counted = NodeHoldTerms {
        resident: true,
        drain_blocker: false,
        active_resident: true,
    };
    let lifecycle_only = list(pool, node, ResidencyEvidence::LifecycleOnly, LIMIT).await?;
    assert_eq!(held(&lifecycle_only.rows, phantom).terms, counted);
    let census_aware = list(pool, node, ResidencyEvidence::NodeCensusAware, LIMIT).await?;
    let discounted = held(&census_aware.rows, phantom);
    assert_eq!(discounted.terms, NodeHoldTerms::default());
    assert!(!discounted.terms.drain());
    assert!(!discounted.terms.drain_with_termination_blocking_gate());
    assert!(!discounted.terms.terminate());
    assert_eq!(held(&census_aware.rows, reported).terms, counted);

    for (holds, terms) in [
        (
            &lifecycle_only,
            NodeHoldTerms {
                resident: true,
                drain_blocker: true,
                active_resident: true,
            },
        ),
        (
            &census_aware,
            NodeHoldTerms {
                resident: false,
                drain_blocker: true,
                active_resident: false,
            },
        ),
    ] {
        let blocker = held(&holds.rows, blocker);
        assert_eq!(
            blocker.reason,
            NodeHoldReason::StuckTransient,
            "{blocker:?}"
        );
        assert!(blocker.facts.census_absent);
        assert!(blocker.terms.drain_blocker);
        assert_eq!(blocker.terms, terms);
    }

    let one = NonZeroUsize::new(1).expect("non-zero");
    let capped = list(pool, node, ResidencyEvidence::NodeCensusAware, one).await?;
    assert!(capped.truncated);
    assert_eq!(capped.rows.len(), 1);
    assert_ne!(capped.rows[0].sandbox_id, phantom);
    assert!(capped.rows[0].terms.counted(), "{:?}", capped.rows[0]);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn facts_and_placement_are_reported_per_row() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let node = insert_node(pool).await?;
    let evacuating = insert(pool, node, case(NodeHoldReason::Evacuating).0).await?;
    let uploading = insert(pool, node, case(NodeHoldReason::UploadInFlight).0).await?;
    let both = insert(
        pool,
        node,
        Seed {
            target: true,
            ..Seed::on_active("started")
        },
    )
    .await?;
    let holds = list(pool, node, ResidencyEvidence::LifecycleOnly, LIMIT).await?;

    let evacuating = held(&holds.rows, evacuating);
    assert_eq!(evacuating.placement, NodeHoldPlacement::Active);
    assert_eq!(
        evacuating.facts.evacuation_restore_target.as_deref(),
        Some("started")
    );
    assert_eq!(evacuating.facts.restore_attempt_count, Some(2));
    assert!(evacuating.facts.next_restore_attempt_at.is_some());

    let uploading = held(&holds.rows, uploading);
    assert_eq!(uploading.placement, NodeHoldPlacement::Target);
    assert!(uploading.facts.upload_in_flight && !uploading.facts.durable_snapshot);
    assert!(uploading.facts.upload_started_at.is_some());
    assert!(uploading.terms.drain_blocker);
    assert_eq!(uploading.facts.restore_attempt_count, None);

    assert_eq!(held(&holds.rows, both).placement, NodeHoldPlacement::Both);
    Ok(())
}

/// History and other nodes' rows are not holds, and the cap trims the rows
/// returned without trimming the counts.
#[tokio::test(flavor = "current_thread")]
async fn only_counted_rows_are_listed_and_the_cap_keeps_the_counts() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let node = insert_node(pool).await?;
    let other = insert_node(pool).await?;
    insert(pool, node, Seed::on_active("failed")).await?;
    insert(pool, node, Seed::on_target("deleted")).await?;
    insert(
        pool,
        node,
        Seed {
            durable: true,
            ..Seed::on_target("suspended")
        },
    )
    .await?;
    insert(pool, other, Seed::on_active("started")).await?;
    let holds = list(pool, node, ResidencyEvidence::LifecycleOnly, LIMIT).await?;
    assert_eq!(holds.rows, Vec::new());
    assert_eq!(holds.total, 0);

    insert(
        pool,
        node,
        Seed {
            age: 10 * MINUTE,
            ..Seed::on_active("started")
        },
    )
    .await?;
    let oldest = insert(pool, node, Seed::on_active("started")).await?;
    insert(
        pool,
        node,
        Seed {
            age: Duration::from_secs(2 * 3600),
            ..Seed::on_active("stopped")
        },
    )
    .await?;
    let one = NonZeroUsize::new(1).expect("non-zero");
    let holds = list(pool, node, ResidencyEvidence::LifecycleOnly, one).await?;
    assert!(holds.truncated);
    assert_eq!(holds.total, 3);
    assert_eq!(holds.term_counts.active_resident, 3);
    assert_eq!(holds.term_counts.resident, 2);
    // Reason order first, so a resident leads the older termination-blocking
    // row; then the oldest change.
    assert_eq!(holds.rows[0].sandbox_id, oldest);
    Ok(())
}

#[test]
fn reasons_serialize_as_their_wire_form() {
    for reason in NodeHoldReason::ALL {
        assert_eq!(
            serde_json::to_value(reason).expect("serializes"),
            serde_json::Value::String(reason.as_str().to_string())
        );
    }
}
