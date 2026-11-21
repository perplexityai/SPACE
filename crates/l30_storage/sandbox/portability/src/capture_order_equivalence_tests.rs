//! Postgres equivalence check between the order fragments over
//! [`crate::capture_order`] and the inlined ones they replaced
//! ([`pre_refactor`]).
//!
//! The rewrite only moves where each snapshot's rank and first registration
//! are computed, so every fragment must answer exactly as before, NULL
//! included, for every sandbox and snapshot. The fixture is a seeded random
//! set of histories wide enough to reach every branch: captures with and
//! without a generation or artifact, re-uploads of one artifact, every state
//! and origin, disk rows, executions or none per generation, timestamp ties,
//! pins that match or don't, evacuation marks, and placement and claim clocks
//! on either side of the captures.

mod pre_refactor;

use pplx_space_db_test_fixtures::{SEED_ORG_ID, SEED_USER_ID, try_pg_test_db};
use pplx_space_sql_locator::sql;
use sqlx::PgPool;
use uuid::Uuid;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

const SANDBOXES: usize = 400;
const SEED: u64 = 0x5eed_c0de;

/// A fragment over a sandbox aliased `s`.
type SandboxFragment = fn(&str) -> String;
/// A fragment over a sandbox aliased `s` and its full snapshot aliased `sn`.
type SnapshotFragment = fn(&str, &str) -> String;

#[tokio::test(flavor = "current_thread")]
async fn capture_order_fragments_match_the_inlined_ones() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let sandboxes = insert_histories(pool).await?;

    let sandbox_fragments: [(&str, SandboxFragment, SandboxFragment); 4] = [
        (
            "snapshot_durable_for_source_release",
            pre_refactor::snapshot_durable_for_source_release,
            super::snapshot_durable_for_source_release,
        ),
        (
            "snapshot_durable_copy_exists",
            pre_refactor::snapshot_durable_copy_exists,
            super::snapshot_durable_copy_exists,
        ),
        (
            "restore_order_unordered",
            pre_refactor::restore_order_unordered,
            super::restore_order_unordered,
        ),
        (
            "restore_order_unordered_in_retry_window",
            pre_refactor::restore_order_unordered_in_retry_window,
            super::restore_order_unordered_in_retry_window,
        ),
    ];
    for (name, old, new) in sandbox_fragments {
        let statement = format!(
            "SELECT s.id, {} AS old, {} AS new
               FROM sandboxes s
              WHERE s.id = ANY($1)",
            old("s"),
            new("s")
        );
        assert_same_rows(pool, name, &statement, &sandboxes).await?;
    }

    let snapshot_fragments: [(&str, SnapshotFragment, SnapshotFragment); 7] = [
        (
            "no_newer_full_snapshot",
            pre_refactor::no_newer_full_snapshot,
            super::no_newer_full_snapshot,
        ),
        (
            "no_newer_suspend_capture",
            pre_refactor::no_newer_suspend_capture,
            super::no_newer_suspend_capture,
        ),
        (
            "no_newer_live_suspend_capture",
            pre_refactor::no_newer_live_suspend_capture,
            super::no_newer_live_suspend_capture,
        ),
        (
            "no_newer_full_snapshot_ignoring_failed_retry",
            pre_refactor::no_newer_full_snapshot_ignoring_failed_retry,
            super::no_newer_full_snapshot_ignoring_failed_retry,
        ),
        (
            "full_snapshot_outranked_by_generation",
            pre_refactor::full_snapshot_outranked_by_generation,
            super::full_snapshot_outranked_by_generation,
        ),
        (
            "full_snapshot_outranked_by_live_capture",
            pre_refactor::full_snapshot_outranked_by_live_capture,
            super::full_snapshot_outranked_by_live_capture,
        ),
        (
            "restore_retry_source_snapshot_is_current",
            pre_refactor::restore_retry_source_snapshot_is_current,
            super::restore_retry_source_snapshot_is_current,
        ),
    ];
    for (name, old, new) in snapshot_fragments {
        let statement = format!(
            "SELECT sn.id, {} AS old, {} AS new
               FROM snapshots sn
               JOIN sandboxes s ON s.id = sn.sandbox_id
              WHERE s.id = ANY($1)
                AND sn.type = 'full'",
            old("s", "sn"),
            new("s", "sn")
        );
        assert_same_rows(pool, name, &statement, &sandboxes).await?;
    }
    assert_same_ranks(pool, &sandboxes).await?;
    assert_scalar_ranks_match(pool, &sandboxes).await
}

/// `capture_order`'s columns are [`super::capture_registered_at`],
/// [`super::capture_rank`] and [`super::capture_rank_in_retry_window`] for
/// every full snapshot.
async fn assert_same_ranks(pool: &PgPool, sandboxes: &[Uuid]) -> TestResult {
    let order =
        crate::capture_order::with_capture_order("s", crate::capture_order::Ranks::WithRetry);
    let statement = format!(
        "SELECT sn.id,
                ({})::TEXT AS old_registered, {} AS old_rank, {} AS old_retry_rank,
                ordered.registered_at::TEXT, ordered.rank, ordered.retry_rank,
                sn.timestamp::TEXT, sn.generation
           FROM snapshots sn
           JOIN sandboxes s ON s.id = sn.sandbox_id
           CROSS JOIN LATERAL ({order} SELECT * FROM capture_order me WHERE me.id = sn.id) ordered
          WHERE s.id = ANY($1)
            AND sn.type = 'full'",
        pre_refactor::capture_registered_at("sn"),
        pre_refactor::capture_rank("sn"),
        pre_refactor::capture_rank_in_retry_window("s", "sn"),
    );
    type Row = (
        Uuid,
        String,
        Option<i64>,
        Option<i64>,
        String,
        Option<i64>,
        Option<i64>,
        String,
        Option<i64>,
    );
    let rows: Vec<Row> = sqlx::query_as(sqlx::AssertSqlSafe(sql!(statement.as_str())))
        .bind(sandboxes)
        .fetch_all(pool)
        .await?;
    let full: i64 = sqlx::query_scalar(sql!(
        "SELECT COUNT(*) FROM snapshots WHERE sandbox_id = ANY($1) AND type = 'full'"
    ))
    .bind(sandboxes)
    .fetch_one(pool)
    .await?;
    assert_eq!(
        rows.len() as i64,
        full,
        "capture_order lost a full snapshot"
    );
    for row in &rows {
        assert_eq!(
            (&row.1, row.2, row.3),
            (&row.4, row.5, row.6),
            "capture order of snapshot {} disagrees",
            row.0
        );
    }
    for (name, reached) in [
        ("a NULL rank", rows.iter().any(|row| row.2.is_none())),
        ("rank 0", rows.iter().any(|row| row.2 == Some(0))),
        (
            "an inherited generation",
            rows.iter()
                .any(|row| row.8.is_none() && row.2.is_some_and(|rank| rank > 0)),
        ),
        (
            "a first registration before the row's own",
            rows.iter().any(|row| row.1 != row.7),
        ),
        (
            "a retry rank that differs",
            rows.iter().any(|row| row.2 != row.3),
        ),
    ] {
        assert!(reached, "the fixture never reaches {name}");
    }
    Ok(())
}

/// The scalar rank fragments the restore-source selector reads,
/// [`super::capture_registered_at`], [`super::capture_rank`] and
/// [`super::capture_rank_in_retry_window`], agree with `capture_order`'s
/// columns for every full snapshot. The fixture also reaches a retry rank
/// that a generation recorded only after the claim would change.
async fn assert_scalar_ranks_match(pool: &PgPool, sandboxes: &[Uuid]) -> TestResult {
    let order =
        crate::capture_order::with_capture_order("s", crate::capture_order::Ranks::WithRetry);
    let window = crate::restore_retry_source_snapshot_window("s", "rank_later");
    let statement = format!(
        "SELECT COUNT(*) FILTER (WHERE ({})::TEXT IS DISTINCT FROM ordered.registered_at::TEXT
                                    OR {} IS DISTINCT FROM ordered.rank
                                    OR {} IS DISTINCT FROM ordered.retry_rank),
                COUNT(*) FILTER (WHERE {} IS DISTINCT FROM ordered.retry_rank)
           FROM snapshots sn
           JOIN sandboxes s ON s.id = sn.sandbox_id
           CROSS JOIN LATERAL ({order} SELECT * FROM capture_order me WHERE me.id = sn.id) ordered
          WHERE s.id = ANY($1)
            AND sn.type = 'full'",
        super::capture_registered_at("sn"),
        super::capture_rank("sn"),
        super::capture_rank_in_retry_window("s", "sn"),
        pre_refactor::capture_rank_where("sn", &window, None),
    );
    let (disagree, claim_bound): (i64, i64) =
        sqlx::query_as(sqlx::AssertSqlSafe(sql!(statement.as_str())))
            .bind(sandboxes)
            .fetch_one(pool)
            .await?;
    assert_eq!(
        disagree, 0,
        "scalar capture ranks disagree with capture_order"
    );
    assert!(
        claim_bound > 0,
        "the fixture never reaches a retry rank the claim cutoff changes"
    );
    Ok(())
}

/// Every row's old and new answers agree, and the fixture reaches both
/// answers, so the comparison can't pass on a constant.
async fn assert_same_rows(
    pool: &PgPool,
    name: &str,
    statement: &str,
    sandboxes: &[Uuid],
) -> TestResult {
    let rows: Vec<(Uuid, Option<bool>, Option<bool>)> =
        sqlx::query_as(sqlx::AssertSqlSafe(sql!(statement)))
            .bind(sandboxes)
            .fetch_all(pool)
            .await?;
    let disagreements: Vec<_> = rows.iter().filter(|(_, old, new)| old != new).collect();
    assert!(
        disagreements.is_empty(),
        "{name}: {} of {} rows disagree, first {:?}",
        disagreements.len(),
        rows.len(),
        disagreements.first()
    );
    for answer in [true, false] {
        assert!(
            rows.iter().any(|(_, old, _)| *old == Some(answer)),
            "{name}: the fixture never answers {answer}"
        );
    }
    Ok(())
}

/// A small deterministic generator, so a failure reproduces.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        // xorshift64*
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_f491_4f6c_dd1d)
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }

    fn chance(&mut self, percent: u64) -> bool {
        self.below(100) < percent
    }

    fn pick<'a, T>(&mut self, items: &'a [T]) -> &'a T {
        &items[self.below(items.len() as u64) as usize]
    }
}

#[derive(Default)]
struct Columns {
    sandbox: Vec<Uuid>,
    lifecycle: Vec<String>,
    evacuation: Vec<Option<String>>,
    placement_minute: Vec<Option<i32>>,
    claim_minute: Vec<i32>,
    pin: Vec<Option<Uuid>>,
    pin_kind: Vec<Option<String>>,

    snapshot: Vec<Uuid>,
    snapshot_sandbox: Vec<Uuid>,
    snapshot_type: Vec<String>,
    state: Vec<String>,
    minute: Vec<i32>,
    generation: Vec<Option<i64>>,
    artifact: Vec<Option<Uuid>>,
    kind: Vec<Option<String>>,
    origin: Vec<Option<String>>,
    name: Vec<Option<String>>,

    located: Vec<Uuid>,
    orphaned: Vec<bool>,

    execution_sandbox: Vec<Uuid>,
    execution_generation: Vec<i64>,
    execution_minute: Vec<i32>,
}

/// Inserts [`SANDBOXES`] random histories and returns their sandbox ids.
async fn insert_histories(pool: &PgPool) -> TestResult<Vec<Uuid>> {
    let mut rng = Rng(SEED);
    let mut c = Columns::default();
    for _ in 0..SANDBOXES {
        let sandbox = Uuid::now_v7();
        let artifacts = [Uuid::now_v7(), Uuid::now_v7(), Uuid::now_v7()];
        let snapshots = rng.below(7);
        for _ in 0..snapshots {
            c.snapshot.push(Uuid::now_v7());
            c.snapshot_sandbox.push(sandbox);
            let full = rng.chance(90);
            c.snapshot_type
                .push(if full { "full" } else { "disk" }.into());
            c.state
                .push((*rng.pick(&["creating", "ready", "ready", "failed", "deleting"])).into());
            // A narrow range, so registrations tie.
            c.minute.push(rng.below(12) as i32);
            c.generation
                .push((!rng.chance(45)).then(|| 1 + rng.below(4) as i64));
            let (artifact, kind, origin) = if rng.chance(25) {
                (None, None, None)
            } else if rng.chance(4) {
                (None, Some("full"), Some("fork"))
            } else {
                let origin = *rng.pick(&["suspend", "suspend", "suspend", "snapshot", "stop"]);
                let kind = if rng.chance(85) { "full" } else { "disk" };
                (Some(*rng.pick(&artifacts)), Some(kind), Some(origin))
            };
            c.artifact.push(artifact);
            c.kind.push(kind.map(Into::into));
            c.origin.push(origin.map(Into::into));
            c.name
                .push((origin == Some("snapshot")).then(|| "named".to_owned()));
            if rng.chance(75) {
                c.located.push(*c.snapshot.last().unwrap());
                c.orphaned.push(rng.chance(15));
            }
        }
        for generation in 1..=4 {
            if rng.chance(40) {
                c.execution_sandbox.push(sandbox);
                c.execution_generation.push(generation);
                c.execution_minute.push(rng.below(12) as i32);
            }
        }
        c.sandbox.push(sandbox);
        c.lifecycle
            .push((*rng.pick(&["suspended", "suspended", "restoring"])).into());
        c.evacuation
            .push(rng.chance(40).then(|| "started".to_owned()));
        c.placement_minute
            .push(rng.chance(60).then(|| rng.below(12) as i32));
        // `lifecycle_changed_at` is NOT NULL.
        c.claim_minute.push(rng.below(13) as i32);
        let pin = if rng.chance(55) {
            None
        } else if rng.chance(85) {
            Some(*rng.pick(&artifacts))
        } else {
            Some(Uuid::now_v7())
        };
        c.pin.push(pin);
        c.pin_kind
            .push(pin.map(|_| if rng.chance(85) { "full" } else { "disk" }.to_owned()));
    }
    // A fixed history the random ones don't reach: an unpinned, unmarked row
    // whose placement-fresh ready capture a later execution's failed capture
    // outranks. The source release refuses it; the give-up keeps the row.
    let sandbox = Uuid::now_v7();
    for (state, minute, generation, located) in [("ready", 4, 2, true), ("failed", 6, 3, false)] {
        let snapshot = Uuid::now_v7();
        c.snapshot.push(snapshot);
        c.snapshot_sandbox.push(sandbox);
        c.snapshot_type.push("full".into());
        c.state.push(state.into());
        c.minute.push(minute);
        c.generation.push(Some(generation));
        c.artifact.push(Some(Uuid::now_v7()));
        c.kind.push(Some("full".into()));
        c.origin.push(Some("suspend".into()));
        c.name.push(None);
        if located {
            c.located.push(snapshot);
            c.orphaned.push(false);
        }
        c.execution_sandbox.push(sandbox);
        c.execution_generation.push(generation);
        c.execution_minute.push(minute - 1);
    }
    c.sandbox.push(sandbox);
    c.lifecycle.push("suspended".into());
    c.evacuation.push(None);
    c.placement_minute.push(Some(2));
    c.claim_minute.push(3);
    c.pin.push(None);
    c.pin_kind.push(None);

    sqlx::query(sql!(
        "INSERT INTO sandboxes (
             id, user_id, org_id, lifecycle, placement, managed_by,
             evacuation_restore_target, placement_changed_at, lifecycle_changed_at,
             current_local_artifact_id, current_local_artifact_kind,
             current_local_artifact_origin, network_policy_generation
         )
         SELECT id, $2, $3, lifecycle, 'unplaced', 'k8s', evacuation,
                TIMESTAMPTZ '2026-01-01 00:00:00+00' + placement_minute * INTERVAL '1 minute',
                TIMESTAMPTZ '2026-01-01 00:00:00+00' + claim_minute * INTERVAL '1 minute',
                pin, pin_kind, CASE WHEN pin IS NOT NULL THEN 'suspend' END, 1
           FROM UNNEST($1::UUID[], $4::TEXT[], $5::TEXT[], $6::INT[], $7::INT[],
                       $8::UUID[], $9::TEXT[])
                AS t(id, lifecycle, evacuation, placement_minute, claim_minute, pin, pin_kind)"
    ))
    .bind(&c.sandbox)
    .bind(SEED_USER_ID)
    .bind(SEED_ORG_ID)
    .bind(&c.lifecycle)
    .bind(&c.evacuation)
    .bind(&c.placement_minute)
    .bind(&c.claim_minute)
    .bind(&c.pin)
    .bind(&c.pin_kind)
    .execute(pool)
    .await?;

    sqlx::query(sql!(
        "INSERT INTO snapshots (
             id, sandbox_id, type, state, timestamp, generation,
             local_artifact_id, artifact_kind, artifact_origin, artifact_snapshot_name
         )
         SELECT id, sandbox_id, type, state,
                TIMESTAMPTZ '2026-01-01 00:00:00+00' + minute * INTERVAL '1 minute',
                generation, artifact, kind, origin, name
           FROM UNNEST($1::UUID[], $2::UUID[], $3::TEXT[], $4::TEXT[], $5::INT[],
                       $6::BIGINT[], $7::UUID[], $8::TEXT[], $9::TEXT[], $10::TEXT[])
                AS t(id, sandbox_id, type, state, minute, generation, artifact, kind,
                     origin, name)"
    ))
    .bind(&c.snapshot)
    .bind(&c.snapshot_sandbox)
    .bind(&c.snapshot_type)
    .bind(&c.state)
    .bind(&c.minute)
    .bind(&c.generation)
    .bind(&c.artifact)
    .bind(&c.kind)
    .bind(&c.origin)
    .bind(&c.name)
    .execute(pool)
    .await?;

    sqlx::query(sql!(
        "INSERT INTO snapshot_storage_locations (
             id, snapshot_id, storage_connection_string, orphaned_at
         )
         SELECT gen_random_uuid(), snapshot_id, 's3://space-test/durable',
                CASE WHEN orphaned THEN NOW() END
           FROM UNNEST($1::UUID[], $2::BOOL[]) AS t(snapshot_id, orphaned)"
    ))
    .bind(&c.located)
    .bind(&c.orphaned)
    .execute(pool)
    .await?;

    sqlx::query(sql!(
        "INSERT INTO sandbox_k8s_executions (
             sandbox_id, generation, pod_name, pod_namespace, active, created_at
         )
         SELECT sandbox_id, generation, 'equivalence-' || gen_random_uuid(), 'default', FALSE,
                TIMESTAMPTZ '2026-01-01 00:00:00+00' + minute * INTERVAL '1 minute'
           FROM UNNEST($1::UUID[], $2::BIGINT[], $3::INT[]) AS t(sandbox_id, generation, minute)"
    ))
    .bind(&c.execution_sandbox)
    .bind(&c.execution_generation)
    .bind(&c.execution_minute)
    .execute(pool)
    .await?;

    Ok(c.sandbox)
}
