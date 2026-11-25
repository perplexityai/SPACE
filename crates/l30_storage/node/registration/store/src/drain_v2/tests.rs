//! The v2 drain's statements against Postgres: the start's generation and
//! deadline, a repeat's shorten, what holds an evacuation slot, the stored
//! status's frozen keys, and the loop's listing, report, terminated record
//! and phase moves.

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
    start_v2_drain(&mut *tx, node_id, hold_seconds).await?;
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
    let admission = admit_v2_start(&mut *tx, node_id).await?;
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
/// same: the scalars, the two id lists, the fence, its two stamps, the
/// instance the terminated stamp is about, and the report's six keys.
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
        terminated_observed_instance: Some("i-0123456789abcdef0".to_owned()),
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
            "terminated_observed_instance",
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
            shorten_v2_hold(&mut *tx, node_id, hold).await?,
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

/// A sandbox row in `lifecycle`, its lifecycle unchanged for `age`.
async fn sandbox(pool: &PgPool, lifecycle: &str, age: std::time::Duration) -> TestDbResult<String> {
    let id: String = sqlx::query_scalar(sql!(
        "INSERT INTO sandboxes
            (id, user_id, org_id, managed_by, lifecycle, template_id, network_policy_generation)
         VALUES (space_uuid_v7(), $1, $2, 'k8s', $3, 'tmpl-drain-v2-test', 1)
         RETURNING id::TEXT"
    ))
    .bind(pplx_space_db_test_fixtures::SEED_USER_ID)
    .bind(pplx_space_db_test_fixtures::SEED_ORG_ID)
    .bind(lifecycle)
    .fetch_one(pool)
    .await?;
    // The lifecycle trigger stamps an update; a direct one sets the age.
    sqlx::query(sql!(
        "ALTER TABLE sandboxes DISABLE TRIGGER sandboxes_lifecycle_changed_at"
    ))
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "UPDATE sandboxes SET lifecycle_changed_at = NOW() - make_interval(secs => $2)
          WHERE id = $1::UUID"
    ))
    .bind(&id)
    .bind(age.as_secs_f64())
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "ALTER TABLE sandboxes ENABLE TRIGGER sandboxes_lifecycle_changed_at"
    ))
    .execute(pool)
    .await?;
    Ok(id)
}

async fn generation_of(pool: &PgPool, node_id: NodeId) -> TestDbResult<i64> {
    Ok(sqlx::query_scalar(sql!(
        "SELECT (drain_status->>'generation')::BIGINT FROM nodes WHERE id = $1"
    ))
    .bind(node_id)
    .fetch_one(pool)
    .await?)
}

async fn status_of(pool: &PgPool, node_id: NodeId) -> TestDbResult<StoredV2Status> {
    Ok(drain_v2_columns(pool, node_id)
        .await?
        .and_then(|columns| columns.status)
        .expect("a v2 status"))
}

/// A report with `pending` entries for these ids, and nothing else.
fn report_pending(pending: &[&str]) -> StoredV2Report {
    StoredV2Report {
        reported_at: 1,
        evacuated_grew_at: 1,
        pending: pending
            .iter()
            .map(|sandbox_id| StoredV2Pending {
                sandbox_id: (*sandbox_id).to_owned(),
                wait: StoredV2Wait::Moving,
                running: false,
                reason: String::new(),
                since: None,
                pool_member: false,
            })
            .collect(),
        ..StoredV2Report::default()
    }
}

/// The drained tick adds the durable rows it finds to `evacuated_total`,
/// keeping those a report recorded, sorted and without repeats, and keeps
/// the rest of the status; adding only ids the set holds writes nothing,
/// and under another generation, or in another phase than the one asked
/// for, it adds nothing.
#[tokio::test(flavor = "current_thread")]
async fn the_drained_tick_adds_to_the_evacuated_set_once() -> TestDbResult<()> {
    let db = pg_test_db().await?;
    let pool = &db.pool;
    let node_id = node(pool, true).await?;
    start(pool, node_id, 600).await?;
    let generation = generation_of(pool, node_id).await?;
    let report = report_pending(&[]);
    let recorded = "00000000-0000-4000-8000-0000000000b2".to_owned();
    let unreported = "00000000-0000-4000-8000-0000000000a1".to_owned();
    assert!(
        record_v2_report(
            pool,
            node_id,
            generation,
            &report,
            &[],
            std::slice::from_ref(&recorded)
        )
        .await?
    );
    set(pool, node_id, Some("drained"), serde_json::json!({})).await?;
    assert!(
        add_v2_evacuated(
            pool,
            node_id,
            generation,
            "drained",
            &[recorded.clone(), unreported.clone()]
        )
        .await?
    );
    let status = status_of(pool, node_id).await?;
    assert_eq!(
        status.evacuated_total,
        [unreported.clone(), recorded.clone()]
    );
    assert_eq!(status.report, Some(report));
    assert!(
        !add_v2_evacuated(
            pool,
            node_id,
            generation,
            "drained",
            std::slice::from_ref(&unreported)
        )
        .await?,
        "every id counted already: no write"
    );
    let new = "00000000-0000-4000-8000-0000000000c3".to_owned();
    for (case, generation, phase) in [
        ("another generation", generation + 1, "drained"),
        ("another phase", generation, "terminating"),
    ] {
        assert!(
            !add_v2_evacuated(pool, node_id, generation, phase, std::slice::from_ref(&new)).await?,
            "{case}"
        );
    }
    assert_eq!(
        status_of(pool, node_id).await?.evacuated_total,
        [unreported, recorded]
    );
    Ok(())
}

/// Each stored report adds the sandboxes it lists running to the drain's
/// observed set and the evacuations recorded to `evacuated_total`, both of
/// which only grow; the count takes, of the observed set, the ones
/// `suspended` for longer than the bound: not a paused sandbox, nor one
/// suspended just now, nor an id that names no sandbox. A report under
/// another generation records nothing.
#[tokio::test(flavor = "current_thread")]
async fn a_report_grows_the_observed_and_evacuated_sets() -> TestDbResult<()> {
    let db = pg_test_db().await?;
    let pool = &db.pool;
    let hour = std::time::Duration::from_secs(3_600);
    let node_id = node(pool, true).await?;
    start(pool, node_id, 600).await?;
    let generation = generation_of(pool, node_id).await?;
    let suspended = sandbox(pool, "suspended", hour).await?;
    let paused = sandbox(pool, "paused", hour).await?;
    let fresh = sandbox(pool, "suspended", std::time::Duration::ZERO).await?;
    let never_running = sandbox(pool, "suspended", hour).await?;
    let report = report_pending(&[]);
    for (running, evacuated) in [
        (
            vec![suspended.clone(), paused.clone()],
            vec![suspended.clone()],
        ),
        (vec![fresh.clone(), "not-a-sandbox".to_owned()], vec![]),
        (vec![], vec![fresh.clone(), suspended.clone()]),
    ] {
        assert!(record_v2_report(pool, node_id, generation, &report, &running, &evacuated).await?);
    }
    let status = status_of(pool, node_id).await?;
    let mut observed = vec![
        suspended.clone(),
        paused.clone(),
        fresh.clone(),
        "not-a-sandbox".to_owned(),
    ];
    observed.sort();
    assert_eq!(status.observed_running, observed);
    let mut evacuated = vec![suspended.clone(), fresh.clone()];
    evacuated.sort();
    assert_eq!(status.evacuated_total, evacuated);
    assert_eq!(status.report, Some(report.clone()));
    let counts = evacuated_still_suspended(pool, 900.0).await?;
    assert_eq!(counts, [(node_id, 1)]);
    assert!(
        !record_v2_report(
            pool,
            node_id,
            generation + 1,
            &report,
            std::slice::from_ref(&never_running),
            std::slice::from_ref(&never_running)
        )
        .await?
    );
    assert_eq!(status_of(pool, node_id).await?.evacuated_total, evacuated);
    assert_eq!(
        evacuated_still_suspended(pool, 900.0).await?,
        [(node_id, 1)]
    );
    Ok(())
}

/// EC2's `terminated` is recorded once, with the first time and the
/// instance, for the instance the drain asks about under its generation:
/// the recorded one until the fence is written, then the fenced one; and
/// for any phase, a cancel included. A record about another instance than
/// the one asked about now doesn't count, in the listing or for a new
/// record, which replaces it; one written without an instance, before the
/// key existed, counts for whichever instance is asked about.
#[tokio::test(flavor = "current_thread")]
async fn terminated_is_recorded_once_on_the_asked_instance() -> TestDbResult<()> {
    let db = pg_test_db().await?;
    let pool = &db.pool;
    let node_id = node(pool, true).await?;
    start(pool, node_id, 600).await?;
    let generation = generation_of(pool, node_id).await?;
    sqlx::query(sql!(
        "INSERT INTO node_k8s (node_id, k8s_node_name, k8s_node_uid, instance_id)
         VALUES ($1, 'worker-drain-v2-test', 'uid-1', 'i-0000000a')"
    ))
    .bind(node_id)
    .execute(pool)
    .await?;
    let listed_stamp = || async {
        let listed = list_v2_node_rows(pool).await?;
        TestDbResult::Ok(
            listed
                .iter()
                .find(|row| row.id == node_id)
                .expect("the v2 node")
                .terminated_observed_at,
        )
    };
    let backdate = || async {
        sqlx::query(sql!(
            "UPDATE nodes SET drain_status = drain_status
                     || jsonb_build_object('terminated_observed_at', 1)
              WHERE id = $1"
        ))
        .bind(node_id)
        .execute(pool)
        .await?;
        TestDbResult::Ok(())
    };
    set(pool, node_id, None, serde_json::json!({})).await?;
    assert!(!record_v2_terminated_observed(pool, node_id, generation, "i-0000000b").await?);
    assert!(!record_v2_terminated_observed(pool, node_id, generation + 1, "i-0000000a").await?);
    assert!(record_v2_terminated_observed(pool, node_id, generation, "i-0000000a").await?);
    let first = status_of(pool, node_id).await?;
    assert!(first.terminated_observed_at.is_some());
    assert_eq!(
        first.terminated_observed_instance.as_deref(),
        Some("i-0000000a")
    );
    backdate().await?;
    assert!(record_v2_terminated_observed(pool, node_id, generation, "i-0000000a").await?);
    assert_eq!(
        status_of(pool, node_id).await?.terminated_observed_at,
        Some(1)
    );
    assert_eq!(listed_stamp().await?, Some(1));

    // Once fenced on another instance, the fence is the one asked about: the
    // record about the recorded instance no longer counts, and a record
    // about the fence replaces it.
    set(
        pool,
        node_id,
        Some("terminating"),
        serde_json::json!({"terminate_instance": "i-0000000c"}),
    )
    .await?;
    assert_eq!(
        listed_stamp().await?,
        None,
        "a record about another instance"
    );
    assert!(!record_v2_terminated_observed(pool, node_id, generation, "i-0000000a").await?);
    assert!(record_v2_terminated_observed(pool, node_id, generation, "i-0000000c").await?);
    let replaced = status_of(pool, node_id).await?;
    assert!(replaced.terminated_observed_at.is_some_and(|at| at > 1));
    assert_eq!(
        replaced.terminated_observed_instance.as_deref(),
        Some("i-0000000c")
    );
    assert!(listed_stamp().await?.is_some());

    // A record without an instance, written before the key existed, counts
    // as it always did, and a new record keeps its time.
    sqlx::query(sql!(
        "UPDATE nodes SET drain_status = (drain_status - 'terminated_observed_instance')
                 || jsonb_build_object('terminated_observed_at', 1)
          WHERE id = $1"
    ))
    .bind(node_id)
    .execute(pool)
    .await?;
    assert_eq!(
        listed_stamp().await?,
        Some(1),
        "a record without an instance"
    );
    assert!(record_v2_terminated_observed(pool, node_id, generation, "i-0000000c").await?);
    assert_eq!(
        status_of(pool, node_id).await?.terminated_observed_at,
        Some(1)
    );
    Ok(())
}

/// The re-read before each effect holds only while the row is the v2 drain
/// as listed: in the listed phase, under the listed generation, on the
/// listed Kubernetes UID and recorded instance. A registration for another
/// machine, a corrected instance, a new drain, an undrain of a listed drain
/// and a cleared drain each refuse it; a cancel listed as one holds while
/// it still is; and a row listed with no UID or instance holds only while
/// it still has none.
#[tokio::test(flavor = "current_thread")]
async fn the_row_reads_as_listed_only_on_its_generation_and_machine() -> TestDbResult<()> {
    let db = pg_test_db().await?;
    let pool = &db.pool;
    let undrain = "UPDATE nodes SET drain_phase = NULL WHERE id = $1";
    for (case, listed, moved_on, holds) in [
        ("as listed", Some("draining"), None, true),
        (
            "another uid",
            Some("draining"),
            Some("UPDATE node_k8s SET k8s_node_uid = 'uid-2' WHERE node_id = $1"),
            false,
        ),
        (
            "another instance",
            Some("draining"),
            Some("UPDATE node_k8s SET instance_id = 'i-0000000b' WHERE node_id = $1"),
            false,
        ),
        (
            "no instance any more",
            Some("draining"),
            Some("UPDATE node_k8s SET instance_id = NULL WHERE node_id = $1"),
            false,
        ),
        (
            "another generation",
            Some("draining"),
            Some(
                "UPDATE nodes SET drain_status = drain_status
                          || jsonb_build_object('generation', 1)
                  WHERE id = $1",
            ),
            false,
        ),
        (
            "drain cleared",
            Some("draining"),
            Some(
                "UPDATE nodes SET drain_phase = NULL, drain_engine = NULL, drain_status = NULL
                  WHERE id = $1",
            ),
            false,
        ),
        (
            "listed draining, then undrained",
            Some("draining"),
            Some(undrain),
            false,
        ),
        (
            "listed as a cancel, still a cancel",
            None,
            Some(undrain),
            true,
        ),
        (
            "registration gone",
            Some("draining"),
            Some("DELETE FROM node_k8s WHERE node_id = $1"),
            false,
        ),
    ] {
        let node_id = node(pool, true).await?;
        start(pool, node_id, 600).await?;
        let generation = generation_of(pool, node_id).await?;
        sqlx::query(sql!(
            "INSERT INTO node_k8s (node_id, k8s_node_name, k8s_node_uid, instance_id)
             VALUES ($1, $2, 'uid-1', 'i-0000000a')"
        ))
        .bind(node_id)
        .bind(format!("worker-{}", node_id.as_uuid().simple()))
        .execute(pool)
        .await?;
        if let Some(moved_on) = moved_on {
            sqlx::query(sqlx::AssertSqlSafe(moved_on))
                .bind(node_id)
                .execute(pool)
                .await?;
        }
        assert_eq!(
            v2_node_as_listed(
                pool,
                node_id,
                listed,
                generation,
                Some("uid-1"),
                Some("i-0000000a")
            )
            .await?,
            holds,
            "{case}"
        );
    }
    // Listed with no registration: holds while there is still none.
    let unregistered = node(pool, true).await?;
    start(pool, unregistered, 600).await?;
    let generation = generation_of(pool, unregistered).await?;
    let draining = Some("draining");
    assert!(v2_node_as_listed(pool, unregistered, draining, generation, None, None).await?);
    assert!(
        !v2_node_as_listed(
            pool,
            unregistered,
            draining,
            generation,
            Some("uid-1"),
            None
        )
        .await?
    );
    Ok(())
}

/// The listing reads the generation, the deadline and whether the drain
/// evacuates by the database clock: a hold ahead holds, a hold of 0
/// evacuates at once.
#[tokio::test(flavor = "current_thread")]
async fn the_listing_evacuates_from_the_deadline() -> TestDbResult<()> {
    let db = pg_test_db().await?;
    let pool = &db.pool;
    let holding = node(pool, true).await?;
    start(pool, holding, 600).await?;
    let evacuating = node(pool, true).await?;
    start(pool, evacuating, 0).await?;
    let rows = list_v2_node_rows(pool).await?;
    let row = |node_id| rows.iter().find(|row| row.id == node_id).expect("listed");
    assert!(!row(holding).evacuate);
    assert!(row(evacuating).evacuate);
    assert_eq!(
        row(holding).generation,
        Some(generation_of(pool, holding).await?)
    );
    assert!(
        row(holding)
            .deadline_at
            .is_some_and(|at| at > row(holding).db_now)
    );
    assert_eq!(row(holding).report(), None);
    Ok(())
}

/// `draining -> drained` needs the generation's stored report finished;
/// `terminating -> drained` records why and refuses a fenced row; and the
/// cancel's finish takes only an undrained row, a v2 engine with no phase.
#[tokio::test(flavor = "current_thread")]
async fn the_phase_moves_check_the_stored_state() -> TestDbResult<()> {
    let db = pg_test_db().await?;
    let pool = &db.pool;
    let node_id = node(pool, true).await?;
    start(pool, node_id, 0).await?;
    let generation = generation_of(pool, node_id).await?;
    assert!(
        !advance_v2_drained(pool, node_id, generation).await?,
        "no report yet"
    );
    assert!(record_v2_report(pool, node_id, generation, &report_pending(&["a"]), &[], &[]).await?);
    assert!(
        !advance_v2_drained(pool, node_id, generation).await?,
        "a pending entry"
    );
    assert!(record_v2_report(pool, node_id, generation, &report_pending(&[]), &[], &[]).await?);
    assert!(!advance_v2_drained(pool, node_id, generation + 1).await?);
    assert!(
        !finish_v2_cancel(pool, node_id, generation).await?,
        "a phase"
    );
    assert!(advance_v2_drained(pool, node_id, generation).await?);
    assert!(
        !return_v2_drained(pool, node_id, generation, "refused").await?,
        "not terminating"
    );
    set(pool, node_id, Some("terminating"), serde_json::json!({})).await?;
    assert!(return_v2_drained(pool, node_id, generation, "terminate refused: x").await?);
    let (phase, error): (Option<String>, Option<String>) = sqlx::query_as(sql!(
        "SELECT drain_phase, drain_last_error FROM nodes WHERE id = $1"
    ))
    .bind(node_id)
    .fetch_one(pool)
    .await?;
    assert_eq!(phase.as_deref(), Some("drained"));
    assert_eq!(error.as_deref(), Some("terminate refused: x"));
    set(
        pool,
        node_id,
        Some("terminating"),
        serde_json::json!({"terminate_instance": "i-0000000a"}),
    )
    .await?;
    assert!(
        !return_v2_drained(pool, node_id, generation, "refused").await?,
        "fenced"
    );
    set(pool, node_id, None, serde_json::json!({})).await?;
    assert!(!finish_v2_cancel(pool, node_id, generation + 1).await?);
    assert!(finish_v2_cancel(pool, node_id, generation).await?);
    assert_eq!(
        drain_v2_columns(pool, node_id).await?.map(|c| c.status),
        Some(None)
    );
    Ok(())
}
