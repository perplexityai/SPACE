//! The admin API's v2 drain statements against Postgres: the start's
//! generation and deadline, a repeat's shorten, and what holds an
//! evacuation slot; and the stored status's frozen keys.

use pplx_space_db_test_fixtures::{TestDbResult, pg_test_db};
use sqlx::PgPool;

use super::*;

/// A Kubernetes node, its heartbeat fresh or stale.
async fn node(pool: &PgPool, fresh: bool) -> TestDbResult<NodeId> {
    let node_id = NodeId::new();
    sqlx::query(sql!(
        "INSERT INTO nodes (id, address, role, managed_by, state)
         VALUES ($1, '10.0.0.50'::INET, 'worker', 'k8s', 'registered')"
    ))
    .bind(node_id)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO node_status_snapshots (
             node_id, observed_at, stale_after, reachable, services,
             sandbox_inventory_pruned_at, updated_at
         ) VALUES (
             $1, NOW(),
             CASE WHEN $2 THEN NOW() + INTERVAL '1 hour' ELSE NOW() - INTERVAL '1 minute' END,
             TRUE, '[]'::JSONB, NOW(), NOW()
         )"
    ))
    .bind(node_id)
    .bind(fresh)
    .execute(pool)
    .await?;
    Ok(node_id)
}

/// Move `node_id` to `draining` as the admin route does, and start a v2
/// drain on it with `hold_seconds`, in one transaction.
async fn start(pool: &PgPool, node_id: NodeId, hold_seconds: i64) -> TestDbResult<()> {
    let mut tx = pool.begin().await?;
    sqlx::query(sql!(
        "UPDATE nodes SET drain_phase = 'draining', drain_mode = 'gentle',
                          drain_started_at = NOW()
          WHERE id = $1"
    ))
    .bind(node_id)
    .execute(&mut *tx)
    .await?;
    start_v2_drain(&mut tx, node_id, hold_seconds).await?;
    tx.commit().await?;
    Ok(())
}

/// Set the drain's phase and merge `status` into its `drain_status`.
async fn set(
    pool: &PgPool,
    node_id: NodeId,
    phase: Option<&str>,
    status: serde_json::Value,
) -> TestDbResult<()> {
    sqlx::query(sql!(
        "UPDATE nodes SET drain_phase = $2, drain_status = drain_status || $3::JSONB
          WHERE id = $1"
    ))
    .bind(node_id)
    .bind(phase)
    .bind(status)
    .execute(pool)
    .await?;
    Ok(())
}

async fn admission(pool: &PgPool, node_id: NodeId) -> TestDbResult<V2StartAdmission> {
    let mut tx = pool.begin().await?;
    let admission = admit_v2_start(&mut tx, node_id).await?;
    tx.commit().await?;
    Ok(admission)
}

/// The start records the engine and the drain's status, its generation and
/// the deadline `hold_seconds` after the drain's start, by the clock that
/// stamped it, exactly, and clears the mode: a v2 drain takes none. The
/// columns read it back typed.
#[tokio::test(flavor = "current_thread")]
async fn a_start_records_its_generation_and_deadline() -> TestDbResult<()> {
    let db = pg_test_db().await?;
    let pool = &db.pool;
    for hold in [0, 600] {
        let node_id = node(pool, true).await?;
        start(pool, node_id, hold).await?;
        let (deadline_at, started, started_us, mode): (i64, i64, i64, Option<String>) =
            sqlx::query_as(sql!(
                "SELECT (drain_status->>'deadline_at')::BIGINT,
                        FLOOR(EXTRACT(EPOCH FROM drain_started_at))::BIGINT,
                        (EXTRACT(EPOCH FROM drain_started_at) * 1000000)::BIGINT,
                        drain_mode
                   FROM nodes WHERE id = $1"
            ))
            .bind(node_id)
            .fetch_one(pool)
            .await?;
        assert_eq!(deadline_at, started + hold, "hold {hold}");
        assert_eq!(mode, None, "a v2 drain takes no mode");
        let columns = drain_v2_columns(pool, node_id).await?.expect("the row");
        assert_eq!(columns.engine, NodeDrainEngine::V2);
        assert_eq!(
            columns.status,
            Some(StoredV2Status {
                generation: u64::try_from(started_us).expect("a generation"),
                deadline_at,
                ..StoredV2Status::default()
            })
        );
    }
    Ok(())
}

/// `drain_status` carries exactly the frozen keys, and reads back the
/// same: the scalars, the two id lists, the fence, its two stamps and the
/// report's six keys.
#[test]
fn the_stored_status_carries_the_frozen_keys() {
    let status = StoredV2Status {
        generation: 7,
        deadline_at: 100,
        observed_running: vec!["a".to_owned()],
        evacuated_total: vec!["a".to_owned(), "b".to_owned()],
        terminate_instance: Some("i-0123456789abcdef0".to_owned()),
        dispatched_at: Some(200),
        terminated_observed_at: Some(300),
        report: Some(StoredV2Report {
            reported_at: 150,
            evacuated_grew_at: 140,
            evacuated: vec![StoredV2Evacuated {
                sandbox_id: "a".to_owned(),
                artifact_kind: StoredV2ArtifactKind::Full,
                artifact_id: "f".to_owned(),
            }],
            pending: vec![StoredV2Pending {
                sandbox_id: "c".to_owned(),
                wait: StoredV2Wait::Hold,
                running: true,
                reason: "held".to_owned(),
                since: Some(90),
                pool_member: true,
            }],
            blocked: vec![StoredV2Blocked {
                reason: "scan failed".to_owned(),
            }],
            unrecorded: vec![StoredV2Unrecorded {
                sandbox_id: "a".to_owned(),
                reason: "not yet".to_owned(),
                since: 145,
            }],
        }),
    };
    let json = serde_json::to_value(&status).expect("serializes");
    let keys = |value: &serde_json::Value| {
        let mut keys: Vec<String> = value
            .as_object()
            .expect("an object")
            .keys()
            .cloned()
            .collect();
        keys.sort();
        keys
    };
    assert_eq!(
        keys(&json),
        [
            "deadline_at",
            "dispatched_at",
            "evacuated_total",
            "generation",
            "observed_running",
            "report",
            "terminate_instance",
            "terminated_observed_at",
        ]
    );
    assert_eq!(
        keys(&json["report"]),
        [
            "blocked",
            "evacuated",
            "evacuated_grew_at",
            "pending",
            "reported_at",
            "unrecorded",
        ]
    );
    assert_eq!(json["report"]["pending"][0]["wait"], "hold");
    assert_eq!(json["report"]["pending"][0]["pool_member"], true);
    assert_eq!(json["report"]["evacuated"][0]["artifact_kind"], "full");
    assert_eq!(
        StoredV2Status::from_column(Some(&json)).expect("reads"),
        Some(status)
    );
    // A drain the admin API just started carries only its generation and
    // deadline; a wait or an artifact kind this build doesn't know reads as
    // unspecified and unknown.
    let started = serde_json::json!({"generation": 7, "deadline_at": 100});
    assert_eq!(
        keys(
            &serde_json::to_value(StoredV2Status::from_column(Some(&started)).expect("reads"))
                .expect("serializes")
        ),
        [
            "deadline_at",
            "evacuated_total",
            "generation",
            "observed_running"
        ]
    );
    let report: StoredV2Report = serde_json::from_value(serde_json::json!({
        "reported_at": 1, "evacuated_grew_at": 1,
        "evacuated": [{"sandbox_id": "a", "artifact_kind": "tape", "artifact_id": "x"}],
        "pending": [{"sandbox_id": "b", "wait": "later"}],
    }))
    .expect("reads");
    assert_eq!(
        report.evacuated[0].artifact_kind,
        StoredV2ArtifactKind::Unknown
    );
    assert_eq!(report.pending[0].wait, StoredV2Wait::Unspecified);
    assert!(
        !report.pending[0].pool_member,
        "an absent pool_member reads false"
    );
    assert!(!report.finished());
}

/// A repeat moves the deadline only earlier: a shorter hold is stored, the
/// same one changes nothing, and a longer one is refused and changes
/// nothing.
#[tokio::test(flavor = "current_thread")]
async fn a_repeat_only_shortens_the_deadline() -> TestDbResult<()> {
    let db = pg_test_db().await?;
    let pool = &db.pool;
    let node_id = node(pool, true).await?;
    start(pool, node_id, 600).await?;
    let deadline = || async {
        sqlx::query_scalar::<_, i64>(sql!(
            "SELECT (drain_status->>'deadline_at')::BIGINT FROM nodes WHERE id = $1"
        ))
        .bind(node_id)
        .fetch_one(pool)
        .await
    };
    let started = deadline().await? - 600;
    for (hold, change, stored) in [
        (900, HoldChange::Refused, started + 600),
        (600, HoldChange::Kept, started + 600),
        (60, HoldChange::Shortened, started + 60),
        (120, HoldChange::Refused, started + 60),
        (0, HoldChange::Shortened, started),
    ] {
        let mut tx = pool.begin().await?;
        assert_eq!(
            shorten_v2_hold(&mut tx, node_id, hold).await?,
            change,
            "{hold}"
        );
        tx.commit().await?;
        assert_eq!(deadline().await?, stored, "{hold}");
    }
    Ok(())
}

/// The start's admission counts the other nodes whose v2 drain holds a
/// slot, its hold included: `draining`, or undrained (no phase) with its
/// cancel unfinished, whatever its instance. A drained or terminating node,
/// a v1 drain, and the node itself hold none. It reads the node's own
/// heartbeat.
#[tokio::test(flavor = "current_thread")]
async fn the_admission_counts_the_slots_other_drains_hold() -> TestDbResult<()> {
    let db = pg_test_db().await?;
    let pool = &db.pool;
    let starting = node(pool, true).await?;
    assert_eq!(
        admission(pool, starting).await?,
        V2StartAdmission {
            heartbeat_fresh: true,
            evacuating: 0
        }
    );
    let report_holds_only = serde_json::json!({"report": {
        "reported_at": 1, "evacuated_grew_at": 1,
        "pending": [{"sandbox_id": "a", "wait": "hold", "running": true}],
    }});
    for (case, phase, status, holds) in [
        ("draining", Some("draining"), serde_json::json!({}), true),
        ("only holding", Some("draining"), report_holds_only, true),
        ("cancelling", None, serde_json::json!({}), true),
        (
            "cancelling, instance terminated",
            None,
            serde_json::json!({"terminated_observed_at": 1}),
            true,
        ),
        ("drained", Some("drained"), serde_json::json!({}), false),
        (
            "terminating",
            Some("terminating"),
            serde_json::json!({}),
            false,
        ),
    ] {
        let other = node(pool, true).await?;
        start(pool, other, 600).await?;
        set(pool, other, phase, status).await?;
        let counted = admission(pool, starting).await?.evacuating;
        // Every other case's node stays; count only this one's change.
        sqlx::query(sql!(
            "UPDATE nodes SET drain_phase = NULL, drain_engine = NULL, drain_status = NULL
              WHERE id = $1"
        ))
        .bind(other)
        .execute(pool)
        .await?;
        assert_eq!(counted, i64::from(holds), "{case}");
    }
    // A v1 drain holds no v2 slot, and the node's own drain doesn't count.
    let v1 = node(pool, true).await?;
    sqlx::query(sql!(
        "UPDATE nodes SET drain_phase = 'draining', drain_started_at = NOW() WHERE id = $1"
    ))
    .bind(v1)
    .execute(pool)
    .await?;
    start(pool, starting, 600).await?;
    assert_eq!(admission(pool, starting).await?.evacuating, 0);
    let stale = node(pool, false).await?;
    assert!(!admission(pool, stale).await?.heartbeat_fresh);
    Ok(())
}
