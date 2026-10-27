//! Postgres-backed coverage for the per-node backup queue read.
//!
//! Four properties, each stated as something an operator would recognise
//! rather than as a restatement of the statement that implements it:
//!
//! 1. **Turning the flag off changes nothing.** Not "the code takes the other
//!    branch" — the statement that runs is the one `main` ships, compared on
//!    its text and on its executed plan.
//! 2. **The queue can only narrow.** Whatever it returns, the history path
//!    would have returned too; and where the queue is complete the two agree
//!    exactly. A queue that could return *more* would be a second opinion
//!    about durability, which is the thing a stale queue must never be able
//!    to express.
//! 3. **A node that has not reported is not believed.** An empty queue is
//!    trusted only behind a fresh gate; stale or never-stamped falls back to
//!    history, and the fixture makes that difference observable by giving
//!    history an answer the queue does not have.
//! 4. **Promotion names the artifact the node reported**, not the marker —
//!    because on legacy rows the marker is NULL and naming it is what sends a
//!    backup down the sandbox-id-only route.

use pplx_space_common_svc::sql::sandbox_residency::CENSUS_INVENTORY_MAX_AGE;
use pplx_space_db_test_fixtures::try_pg_test_db;
use pplx_space_test_env::TestResult;
use uuid::Uuid;

use super::*;

/// A page cap large enough that nothing in this module truncates. Truncation
/// is `pinned_local_only_scale_tests`' subject; conflating the two would make
/// a fixture change there able to fail an assertion here for an unrelated
/// reason.
const TEST_PAGE_CAP: NonZeroUsize = NonZeroUsize::new(64).unwrap();

/// How long ago the node last reported its backup inventory.
#[derive(Clone, Copy)]
enum Gate {
    /// Inside `CENSUS_INVENTORY_MAX_AGE`.
    Fresh,
    /// Older than the window — the node used to report and has gone quiet.
    Stale,
    /// The column is NULL: this node has never reported one. The state of
    /// every node until it runs a build that reports its inventory, which is why
    /// it is a distinct case and not folded into `Stale`.
    NeverReported,
}

impl Gate {
    /// Seconds to subtract from `NOW()`, or `None` to leave the column NULL.
    fn secs_ago(self) -> Option<f64> {
        match self {
            // Comfortably inside the window without being `NOW()` itself, so
            // the test does not depend on the two statements running in the
            // same instant.
            Self::Fresh => Some(30.0),
            // Twice the window, so a change to the constant cannot silently
            // make this case fresh.
            Self::Stale => Some((CENSUS_INVENTORY_MAX_AGE.as_secs() * 2) as f64),
            Self::NeverReported => None,
        }
    }
}

/// Register a worker node and its census satellite row, stamping the
/// backup-inventory gate as `gate` describes.
async fn insert_node_with_gate(pool: &PgPool, gate: Gate) -> TestResult<NodeId> {
    let node_id = NodeId::new();
    sqlx::query(sql!(
        "INSERT INTO nodes (id, address, role, managed_by, state)
         VALUES ($1, '10.0.0.81'::inet, 'worker', 'k8s', 'registered')"
    ))
    .bind(node_id)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO node_status_snapshots (
             node_id, observed_at, stale_after, reachable, role,
             scheduling_status, services, sandbox_inventory_pruned_at,
             local_backup_inventory_reported_at, updated_at
         )
         VALUES (
             $1, NOW(), NOW() + make_interval(secs => 60), TRUE,
             'worker', 'ready', '[]'::jsonb, NOW(),
             CASE WHEN $2::FLOAT8 IS NULL
                  THEN NULL
                  ELSE NOW() - make_interval(secs => $2)
             END,
             NOW()
         )"
    ))
    .bind(node_id)
    .bind(gate.secs_ago())
    .execute(pool)
    .await?;
    Ok(node_id)
}

/// A suspended sandbox pinned to `node` by `target_node_id`, with the legacy
/// NULL `current_local_artifact_id` every legacy row
/// carries. Returns its id.
async fn insert_suspended_row(pool: &PgPool, node: NodeId) -> TestResult<SandboxId> {
    let id = SandboxId::new();
    sqlx::query(sql!(
        "INSERT INTO sandboxes (
             id, user_id, org_id, lifecycle, placement, managed_by,
             target_node_id, placement_changed_at, lifecycle_changed_at,
             network_policy_generation
         )
         VALUES (
             $1, '00000000-0000-0000-0000-000000000010',
             '00000000-0000-0000-0000-000000000001',
             'suspended', 'unplaced', 'k8s',
             $2, NOW() - INTERVAL '30 days', NOW() - INTERVAL '30 days', 1
         )"
    ))
    .bind(id)
    .bind(node)
    .execute(pool)
    .await?;
    Ok(id)
}

/// Make `sandbox` durable: a `state='ready'` full snapshot with a storage
/// location, which is what `snapshot_durable_for_source_release` looks for.
async fn make_durable(pool: &PgPool, sandbox: SandboxId, node: NodeId) -> TestResult {
    sqlx::query(sql!(
        "WITH inserted AS (
             INSERT INTO snapshots (id, sandbox_id, type, state, source_node_id, timestamp)
             VALUES (space_uuid_v7(), $1, 'full', 'ready', $2, NOW())
             RETURNING id
         )
         INSERT INTO snapshot_storage_locations (snapshot_id, storage_connection_string)
         SELECT id, 's3://test-bucket/' || id FROM inserted"
    ))
    .bind(sandbox)
    .bind(node)
    .execute(pool)
    .await?;
    Ok(())
}

/// Queue `sandbox` on `node` as holding an un-backed-up artifact of `kind`.
/// Returns the artifact id, which the promotion assertions key on.
async fn enqueue(pool: &PgPool, node: NodeId, sandbox: SandboxId, kind: &str) -> TestResult<Uuid> {
    let artifact_id = Uuid::now_v7();
    sqlx::query(sql!(
        "INSERT INTO node_local_backup_pending
             (node_id, sandbox_id, artifact_id, artifact_kind)
         VALUES ($1, $2, $3, $4)"
    ))
    .bind(node)
    .bind(sandbox)
    .bind(artifact_id)
    .bind(kind)
    .execute(pool)
    .await?;
    Ok(artifact_id)
}

async fn analyze(pool: &PgPool) -> TestResult {
    sqlx::query(sql!(
        "ANALYZE sandboxes, snapshots, snapshot_storage_locations,
                 node_local_backup_pending, node_status_snapshots"
    ))
    .execute(pool)
    .await?;
    Ok(())
}

/// Run the history statement directly — the oracle every differential here is
/// taken against.
async fn history_answer(
    pool: &PgPool,
    node: NodeId,
    exclude_legacy: bool,
) -> TestResult<Vec<SandboxId>> {
    let query = pinned_local_only_sandboxes_query(exclude_legacy, TEST_PAGE_CAP);
    Ok(
        sqlx::query_scalar::<_, SandboxId>(sqlx::AssertSqlSafe(sql!(query.as_str())))
            .bind(node)
            .fetch_all(pool)
            .await?,
    )
}

/// Whether the gate fragment believes this node's inventory.
async fn gate_is_fresh(pool: &PgPool, node: NodeId) -> TestResult<bool> {
    let query = node_backup_inventory_gate_query();
    Ok(
        sqlx::query_scalar::<_, bool>(sqlx::AssertSqlSafe(sql!(query.as_str())))
            .bind(node)
            .fetch_one(pool)
            .await?,
    )
}

/// The gate believes a recent report, and nothing else.
///
/// Runs first because every fallback assertion below is an assertion that the
/// gate said no. If the gate said no for the wrong reason — or said no
/// always — those tests would still pass while proving nothing, so the three
/// cases are pinned separately here.
#[tokio::test(flavor = "current_thread")]
async fn the_gate_believes_a_recent_report_and_nothing_else() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;

    for (gate, expected, name) in [
        (Gate::Fresh, true, "a report inside the window"),
        (Gate::Stale, false, "a report older than the window"),
        (Gate::NeverReported, false, "a node that never reported"),
    ] {
        let node = insert_node_with_gate(pool, gate).await?;
        assert_eq!(
            gate_is_fresh(pool, node).await?,
            expected,
            "{name} must read as fresh={expected}"
        );
    }

    // A node with no `node_status_snapshots` row at all — the state between
    // registration and first census. Distinct from `NeverReported`, which has
    // a row with a NULL column, and reachable in production, so it gets its
    // own case rather than being assumed to behave like one.
    let bare = NodeId::new();
    sqlx::query(sql!(
        "INSERT INTO nodes (id, address, role, managed_by, state)
         VALUES ($1, '10.0.0.82'::inet, 'worker', 'k8s', 'registered')"
    ))
    .bind(bare)
    .execute(pool)
    .await?;
    assert!(
        !gate_is_fresh(pool, bare).await?,
        "a node with no census row at all must not be believed — the EXISTS \
         has nothing to match, and reading that as fresh would trust an empty \
         queue for a node that has never spoken"
    );
    Ok(())
}

/// With the flag off, the queue is invisible — including where it would have
/// produced a different answer.
///
/// The fixture makes the two arms genuinely differ: two non-durable rows on a
/// node with a fresh gate, only one of which the node reported. Both arms
/// enumerate both rows — history is the population either way — so what the
/// flag changes is the identity and the order: on, the reported row comes
/// first and names its artifact; off, no row names one. A fixture where the
/// node reported nothing would leave the arms identical, and would pass
/// against a build that ignored the flag.
#[tokio::test(flavor = "current_thread")]
async fn the_flag_off_answers_from_history_even_where_the_queue_disagrees() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let node = insert_node_with_gate(pool, Gate::Fresh).await?;

    let queued = insert_suspended_row(pool, node).await?;
    enqueue(pool, node, queued, "full").await?;
    let unqueued = insert_suspended_row(pool, node).await?;
    analyze(pool).await?;

    let mut both = vec![queued, unqueued];
    both.sort();
    let mut seen = history_answer(pool, node, false).await?;
    seen.sort();
    assert_eq!(
        seen, both,
        "fixture check: history must see both rows, or the arms cannot differ \
         and neither assertion below means anything"
    );

    let off = list_pinned_local_candidates(pool, node, false, false).await?;
    assert_eq!(off.source, PinnedLocalSource::History);
    let mut off_ids: Vec<SandboxId> = off.candidates.iter().map(|c| c.sandbox_id).collect();
    off_ids.sort();
    assert_eq!(
        off_ids, both,
        "flag off must answer from history for every node, whatever the gate \
         says — that is what makes merging this inert"
    );

    assert!(
        off.candidates.iter().all(|c| c.queued_artifact.is_none()),
        "flag off must not carry the queue's identity on any row"
    );

    let on = list_pinned_local_candidates(pool, node, false, true).await?;
    assert_eq!(
        on.candidates
            .iter()
            .map(|c| (c.sandbox_id, c.queued_artifact.is_some()))
            .collect::<Vec<_>>(),
        vec![(queued, true), (unqueued, false)],
        "flag on puts the reported row first, naming its artifact, and the row \
         only history sees after it — the flag is the only difference between \
         this answer and the one above"
    );
    assert_eq!(on.source, PinnedLocalSource::HistoryQueueIncomplete);
    Ok(())
}

/// The queue narrows and never widens: no row reaches the page from the queue
/// that history wouldn't return, and when the queue holds every pending
/// artifact the page reads as the queue's (`PinnedLocalSource::Queue`).
///
/// The two halves fail differently. A queue that returned a row history would
/// not is a second, independent opinion about durability — the failure a stale
/// queue must be incapable of. The page can't come up short of history, since
/// `list_pinned_local_candidates` merges history in either way, and release is
/// the drain gate's (`a_wrongly_empty_queue_cannot_release_a_node_still_holding_an_artifact`).
/// So the second half is the label: a complete queue must not read as a census
/// gap.
#[tokio::test(flavor = "current_thread")]
async fn the_queue_answer_is_a_subset_of_history_and_equals_it_when_complete() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let node = insert_node_with_gate(pool, Gate::Fresh).await?;

    // Three shapes across which the two paths must agree, plus one the queue
    // is wrong about in the safe direction.
    let pending = insert_suspended_row(pool, node).await?;
    enqueue(pool, node, pending, "full").await?;

    // Durable, but the node still lists it — the queue has not caught up with
    // a backup that finalized. The predicates must drop it, which is what
    // "the queue narrows; the predicates decide" means in practice.
    let durable_but_queued = insert_suspended_row(pool, node).await?;
    make_durable(pool, durable_but_queued, node).await?;
    enqueue(pool, node, durable_but_queued, "full").await?;

    // Durable and not queued: invisible to both.
    let settled = insert_suspended_row(pool, node).await?;
    make_durable(pool, settled, node).await?;
    analyze(pool).await?;

    let history = history_answer(pool, node, false).await?;
    let queued = list_pinned_local_candidates(pool, node, false, true).await?;
    assert_eq!(queued.source, PinnedLocalSource::Queue);
    let queue_ids: Vec<SandboxId> = queued.candidates.iter().map(|c| c.sandbox_id).collect();

    assert!(
        queue_ids.iter().all(|id| history.contains(id)),
        "the queue returned a row history did not: queue={queue_ids:?} \
         history={history:?}. A row only the queue can see means the two \
         paths disagree about durability, and the queue is the one that can \
         be stale"
    );
    assert_eq!(queue_ids, history, "the page is history's rows and no more");
    assert_eq!(
        queue_ids,
        vec![pending],
        "and the agreed answer is the one genuinely pending row — not zero, \
         which both paths would also agree on and which would make the \
         equality above vacuous"
    );
    Ok(())
}

/// A node that has not reported recently is enumerated from history, whatever
/// its queue says.
///
/// Driven for both ways of not having reported, against a fixture where the
/// queue is empty and history is not: if the fallback failed, the node would
/// read as converged and be released while holding the only copy.
#[tokio::test(flavor = "current_thread")]
async fn an_unreported_node_falls_back_to_history() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;

    for (gate, name) in [
        (Gate::Stale, "a report older than the freshness window"),
        (Gate::NeverReported, "a node that has never reported"),
    ] {
        let node = insert_node_with_gate(pool, gate).await?;
        let pending = insert_suspended_row(pool, node).await?;
        // Deliberately not enqueued: the queue is empty for this node, which
        // is exactly the reading the gate exists to refuse.
        analyze(pool).await?;

        let page = list_pinned_local_candidates(pool, node, false, true).await?;
        assert_eq!(
            page.source,
            PinnedLocalSource::History,
            "{name} must route to history even with the flag on"
        );
        assert_eq!(
            page.candidates
                .iter()
                .map(|c| c.sandbox_id)
                .collect::<Vec<_>>(),
            vec![pending],
            "{name}: the fallback must actually find the pending row. An \
             empty answer here leaves it unpromoted, so the drain gate keeps \
             holding the node with nothing working to clear it"
        );
        assert!(
            page.candidates.iter().all(|c| c.queued_artifact.is_none()),
            "{name}: a history-path row carries no queue identity, so the page \
             labels it as one the queue didn't name"
        );
    }
    Ok(())
}

/// The read never hands a `disk` artifact to the full backup path.
///
/// The table's CHECK admits both because the node reports both, so this is a
/// property of the statement rather than of the schema. The queue read is the
/// full-backup population: a `disk` row on it would mark a sandbox as queued
/// for a backup kind `snapshot_durable_for_source_release` doesn't count. The
/// sandbox itself may still come back from history — it is a non-durable
/// suspended row like any other — and promotion names its marker either way.
/// It comes back unnamed, so a page holding it reads
/// `HistoryQueueIncomplete`.
#[tokio::test(flavor = "current_thread")]
async fn a_disk_artifact_is_not_enumerated_by_the_full_backup_read() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let node = insert_node_with_gate(pool, Gate::Fresh).await?;

    let disk_only = insert_suspended_row(pool, node).await?;
    enqueue(pool, node, disk_only, "disk").await?;
    // The positive half, so the absence above is not read off an empty table:
    // one `full` row on the same node that must still come back.
    let full = insert_suspended_row(pool, node).await?;
    enqueue(pool, node, full, "full").await?;
    analyze(pool).await?;

    let page = list_pinned_local_candidates(pool, node, false, true).await?;
    let named: Vec<(SandboxId, ArtifactKind)> = page
        .candidates
        .iter()
        .filter_map(|c| c.queued_artifact.map(|q| (c.sandbox_id, q.artifact_kind)))
        .collect();
    assert_eq!(
        named,
        vec![(full, ArtifactKind::Full)],
        "only the `full` row may carry a queued identity; got {named:?}"
    );
    assert!(
        page.candidates
            .iter()
            .any(|c| c.sandbox_id == disk_only && c.queued_artifact.is_none()),
        "the `disk` row's sandbox comes back from history, unnamed"
    );
    Ok(())
}

/// A queue-path row carries the artifact identity the node reported, and a
/// history-path row carries none.
///
/// The fixture uses a legacy markerless row — `current_local_artifact_id IS
/// NULL` — because only the queue can supply an identity there. Promotion
/// doesn't send it (`promote_local_snapshot` names the `sandboxes` marker), but the identity is
/// what marks the row as one the queue named, which keeps a gated page from
/// reading it as a census gap (`gated_page_source`).
#[tokio::test(flavor = "current_thread")]
async fn the_queue_path_carries_an_artifact_identity_history_cannot() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let node = insert_node_with_gate(pool, Gate::Fresh).await?;
    let sandbox = insert_suspended_row(pool, node).await?;
    let artifact_id = enqueue(pool, node, sandbox, "full").await?;
    analyze(pool).await?;

    let queued = list_pinned_local_candidates(pool, node, false, true).await?;
    assert_eq!(queued.source, PinnedLocalSource::Queue);
    assert_eq!(
        queued.candidates,
        vec![PinnedLocalCandidate {
            sandbox_id: sandbox,
            queued_artifact: Some(QueuedArtifact {
                artifact_id: LocalArtifactId::from_uuid(artifact_id),
                artifact_kind: ArtifactKind::Full,
            }),
        }],
        "the queue row's own artifact id and kind must reach the caller; the \
         sandbox is markerless, so only the queue can supply this pair"
    );

    let from_history = list_pinned_local_candidates(pool, node, false, false).await?;
    assert_eq!(
        from_history.candidates,
        vec![PinnedLocalCandidate {
            sandbox_id: sandbox,
            queued_artifact: None,
        }],
        "the same sandbox reached through history carries no identity — \
         `current_local_artifact_id` is NULL on it, which is precisely why the \
         history path can only ask for an unnamed current-instance backup"
    );
    Ok(())
}

/// A queue that is wrong in the worst direction cannot lose data.
///
/// Every other test here assumes the queue is right and checks the read
/// handles it correctly. This one assumes it is **empty on a node that
/// genuinely still holds an unbacked artifact**, with a fresh gate vouching
/// for that emptiness — the exact shape a bug anywhere in the node's report
/// or ClusterApi's mirror would produce — and checks nothing is released
/// anyway.
///
/// It holds because the queue and the release decision are different
/// statements. The queue drives *work enumeration*; `draining → drained` is
/// gated by `drain_blocking_sandboxes_query`, which counts from `sandboxes`
/// through `drain_gate_blockers_for_node` and never consults the queue —
/// `count_drain_blocking_sandboxes_on_node` does not even take the flag. A
/// wrong-empty queue is therefore covered twice: the rows it omits are still
/// enumerated from history behind it, and if they were not, the gate would
/// hold the node in `draining`, which is recoverable. It cannot terminate a
/// node holding the only copy, which is not.
///
/// This is the strongest safety statement here, because it does not depend on
/// the node's census or cluster-api's mirror being correct: it is what makes a
/// defect in either survivable rather than terminal.
#[tokio::test(flavor = "current_thread")]
async fn a_wrongly_empty_queue_cannot_release_a_node_still_holding_an_artifact() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let node = insert_node_with_gate(pool, Gate::Fresh).await?;
    let unbacked = insert_suspended_row(pool, node).await?;
    // Deliberately *not* enqueued. The gate is fresh, so this is the queue
    // being wrong in the worst direction — that is the premise, not an
    // oversight.
    analyze(pool).await?;

    // Premise, read off the table rather than off the enumeration: the queue
    // genuinely holds nothing for this node, which is the wrong answer.
    //
    // Deliberately *not* asserted through `list_pinned_local_candidates`.
    // That call also reads history and would list the row anyway, so it never
    // reports the believed-empty state — keying the premise on it would
    // quietly turn this into a test of the enumeration instead of a test of
    // the gate's independence.
    let queued: i64 = sqlx::query_scalar(sql!(
        "SELECT COUNT(*) FROM node_local_backup_pending WHERE node_id = $1"
    ))
    .bind(node)
    .fetch_one(pool)
    .await?;
    assert_eq!(
        queued, 0,
        "premise: the queue must hold nothing for this node, which is the \
         wrong answer while the sandbox is still unbacked"
    );
    assert!(
        gate_is_fresh(pool, node).await?,
        "premise: and the gate must vouch for that emptiness, or the read \
         would have had an independent reason to distrust it"
    );

    // The property. The gate is a different statement and it is not fooled.
    let gate = drain_blocking_sandboxes_query();
    let blockers: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(sql!(gate.as_str())))
        .bind(node)
        .fetch_one(pool)
        .await?;
    assert!(
        blockers >= 1,
        "the release gate must still hold this node: the queue said nothing \
         is pending, but a suspended sandbox with no durable copy is still \
         pinned to it. A zero here is a node terminated while holding the \
         only copy of {unbacked}"
    );

    // Non-vacuity: the gate is counting *this* row for *this* reason, not
    // sitting permanently above zero. Give the sandbox a durable copy and the
    // same gate must clear — otherwise the assertion above would pass on a
    // gate that never returns zero at all.
    make_durable(pool, unbacked, node).await?;
    analyze(pool).await?;
    let cleared: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(sql!(gate.as_str())))
        .bind(node)
        .fetch_one(pool)
        .await?;
    assert_eq!(
        cleared, 0,
        "control: once the artifact is durable the same gate must release the \
         node. Without this, `blockers >= 1` above could hold for a node that \
         can never drain"
    );
    Ok(())
}

/// A node held by bytes the queue cannot represent must still **converge**,
/// not merely fail to be released.
///
/// The sibling test above proves the gate holds such a node, which is the
/// data-safety half. This is the liveness half, and it fails differently: a
/// node that is held forever with nothing enumerated for backup is a hung
/// drain, and today's forced drain does not hang on these rows — the history
/// statement's `exclude_legacy_rows = false` surfaces them and promotion
/// backs up the node's current instance, unnamed.
///
/// The fixture is a suspended sandbox with **no artifact pin** and no durable
/// copy. That is not an exotic state: `LocalEvidence::Unpinned` is the node's
/// own name for a pre-artifact-tracking sandbox, it carries no artifact id at
/// all, and `node_local_backup_pending.artifact_id` is NOT NULL — so the node
/// has nothing it could report for such a row even when working perfectly.
/// Every legacy row is markerless.
///
/// What makes it converge is that a gated read lists history's rows the queue
/// does not name alongside the queued ones. That covers the row by what
/// history holds rather than by recognising the unpinned shape, so it also
/// covers anything else the census misses.
#[tokio::test(flavor = "current_thread")]
async fn a_node_held_by_unrepresentable_bytes_still_converges() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let node = insert_node_with_gate(pool, Gate::Fresh).await?;
    // `insert_suspended_row` leaves `current_local_artifact_id` NULL, which
    // is exactly the unpinned shape. No queue row is possible for it.
    let unpinned = insert_suspended_row(pool, node).await?;
    analyze(pool).await?;

    // Premise: this row really does block the gate, or there is nothing to
    // converge and the assertion below would hold trivially.
    let gate = drain_blocking_sandboxes_query();
    let blockers: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(sql!(gate.as_str())))
        .bind(node)
        .fetch_one(pool)
        .await?;
    assert!(
        blockers >= 1,
        "premise: an unpinned suspended row with no durable copy must block \
         the drain gate"
    );

    let page = list_pinned_local_candidates(pool, node, false, true).await?;
    assert_eq!(
        page.source,
        PinnedLocalSource::HistoryQueueIncomplete,
        "the pass must record *why* it used history here — the node's gate is \
         fresh, so this is not the unreported-node fallback but the queue \
         missing something, and an operator needs to tell those apart"
    );
    assert_eq!(
        page.candidates
            .iter()
            .map(|c| c.sandbox_id)
            .collect::<Vec<_>>(),
        vec![unpinned],
        "the row must be enumerated for backup, or the drain parks: the gate \
         holds the node and nothing ever promotes the thing holding it"
    );
    assert!(
        page.candidates.iter().all(|c| c.queued_artifact.is_none()),
        "and it must carry no queue identity: the queue can't represent an \
         unpinned row, so the page reads it as one the queue didn't name"
    );

    // Non-vacuity: the source follows what history adds, not the flag being
    // on. Give the row a durable copy and history has nothing the queue
    // lacks, so the same call must report the queue as complete.
    make_durable(pool, unpinned, node).await?;
    analyze(pool).await?;
    let settled = list_pinned_local_candidates(pool, node, false, true).await?;
    assert_eq!(
        settled.source,
        PinnedLocalSource::Queue,
        "control: once history holds nothing the queue lacks, the source is \
         the queue. Without this the incomplete label could be firing on \
         every empty queue, and the coverage signal would mean nothing"
    );
    assert!(settled.candidates.is_empty());
    Ok(())
}

/// A row the queue cannot name is still backed up while the queue holds
/// others.
///
/// The operator-visible property: one queued row that never clears must not
/// hold the rest of its node's backlog behind it. The node's census reports a
/// pin whose `local_artifacts` row is gone on purpose (so an empty queue never
/// under-reports), and every backup request for such a row is refused as
/// retryable, so that row can sit in the queue for the whole drain. If a
/// non-empty queue suppressed history, a legacy row on the same node — which
/// the queue cannot represent at all — would never be enumerated.
#[tokio::test(flavor = "current_thread")]
async fn a_queued_row_does_not_hide_a_row_only_history_can_name() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let node = insert_node_with_gate(pool, Gate::Fresh).await?;
    let queued = insert_suspended_row(pool, node).await?;
    enqueue(pool, node, queued, "full").await?;
    // Never enqueued: the legacy shape, with no artifact id to report.
    let legacy = insert_suspended_row(pool, node).await?;
    analyze(pool).await?;

    let page = list_pinned_local_candidates(pool, node, false, true).await?;
    let ids: Vec<SandboxId> = page.candidates.iter().map(|c| c.sandbox_id).collect();
    assert_eq!(
        ids,
        vec![queued, legacy],
        "the queued row first, then the row only history can name; dropping \
         the second would park it behind the first for as long as the first \
         is refused"
    );
    assert!(page.candidates[0].queued_artifact.is_some());
    assert!(
        page.candidates[1].queued_artifact.is_none(),
        "the history row carries no identity, so promotion asks for an \
         unnamed current-instance backup"
    );
    assert_eq!(page.source, PinnedLocalSource::HistoryQueueIncomplete);
    Ok(())
}

/// However many rows the queue holds, a row only history can name stays at
/// the head of the page.
///
/// The operator-visible property: stuck queued rows cannot take every
/// promotion slot. Promotion acts on the first `PROMOTION_MAX_PER_TICK` rows
/// of the page, so a page that listed every queued row first would let that
/// many refused queued rows hold a legacy row off the promoted slice for good.
/// Seeded with more queued rows than there are slots, so the property is not
/// satisfied by the queue simply being short.
#[tokio::test(flavor = "current_thread")]
async fn queued_rows_cannot_push_a_history_row_off_the_head() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let node = insert_node_with_gate(pool, Gate::Fresh).await?;
    for _ in 0..(super::super::PROMOTION_MAX_PER_TICK + 2) {
        let queued = insert_suspended_row(pool, node).await?;
        enqueue(pool, node, queued, "full").await?;
    }
    let legacy = insert_suspended_row(pool, node).await?;
    analyze(pool).await?;

    let page = list_pinned_local_candidates(pool, node, false, true).await?;
    let position = page
        .candidates
        .iter()
        .position(|c| c.sandbox_id == legacy)
        .expect("the legacy row must be on the page at all");
    assert_eq!(
        position, 1,
        "the row only history names is interleaved straight after the first \
         queued row, inside any promotion slice"
    );
    assert!(page.candidates[position].queued_artifact.is_none());
    assert_eq!(
        page.candidates.len(),
        super::super::PROMOTION_MAX_PER_TICK + 3
    );
    Ok(())
}

/// A blocker no promotion can act on does not mark the queue incomplete.
///
/// The operator-visible property: during an ordinary backup, the coverage
/// signal reads the queue as complete. The drain gate counts rows the backup
/// path cannot act on — a `suspending` row, an upload already in flight from
/// the node — so a fallback keyed on the gate count would label the queue
/// incomplete on every tick between a backup's enqueue and its finalize.
#[tokio::test(flavor = "current_thread")]
async fn a_blocker_no_promotion_can_act_on_leaves_the_queue_complete() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let node = insert_node_with_gate(pool, Gate::Fresh).await?;
    let suspending = insert_suspended_row(pool, node).await?;
    sqlx::query(sql!(
        "UPDATE sandboxes SET lifecycle = 'suspending' WHERE id = $1"
    ))
    .bind(suspending)
    .execute(pool)
    .await?;
    analyze(pool).await?;

    // Premise: the gate does count this row, so a gate-keyed fallback would
    // have fired here and the assertion below is not true by construction.
    let gate = drain_blocking_sandboxes_query();
    let blockers: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(sql!(gate.as_str())))
        .bind(node)
        .fetch_one(pool)
        .await?;
    assert!(
        blockers >= 1,
        "premise: a pinned `suspending` row must block the gate"
    );

    let page = list_pinned_local_candidates(pool, node, false, true).await?;
    assert_eq!(
        page.source,
        PinnedLocalSource::Queue,
        "nothing promotable is missing from the queue, so it is complete"
    );
    assert!(page.candidates.is_empty());
    Ok(())
}

/// A queue row whose sandbox has since been re-pinned to another node is not
/// enumerated for the old node.
///
/// The queue is keyed by the node whose *disk* holds the artifact;
/// `target_node_id` is the control plane's placement. They diverge when a pin
/// moves, because the mirror deletes a row only once the node stops reporting
/// it and its artifact is recoverable without that node's disk — so the stale
/// row survives on the old node until then.
///
/// Scoping the read on `q.node_id` alone would return that row, while the
/// history statement (`s.target_node_id = $1`) would not. That breaks
/// `queue ⊆ history`, which is what makes the queue a pure *narrowing* of the
/// candidate set rather than a second opinion about which node owns a
/// sandbox — and the subset differential is the assertion the queue's safety
/// argument leans on, so an exception to it that no test covers is worse than
/// the extra row.
///
/// Nothing is lost by excluding it. History is `target_node_id`-scoped too, so
/// the old node never enumerated that sandbox on `main` either. The drain
/// gate's `source_node_id` arm can still hold the old node while an upload
/// from it is `creating`, and that clears when the upload does.
#[tokio::test(flavor = "current_thread")]
async fn a_repinned_sandbox_is_not_enumerated_for_its_old_node() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let old_node = insert_node_with_gate(pool, Gate::Fresh).await?;
    let new_node = insert_node_with_gate(pool, Gate::Fresh).await?;

    // Queued on `old_node` — its disk really does hold the artifact — while a
    // re-placement has already moved the pin to `new_node`.
    let repinned = insert_suspended_row(pool, old_node).await?;
    enqueue(pool, old_node, repinned, "full").await?;
    sqlx::query(sql!(
        "UPDATE sandboxes SET target_node_id = $1 WHERE id = $2"
    ))
    .bind(new_node)
    .bind(repinned)
    .execute(pool)
    .await?;
    // A genuinely-old-node row, so the assertion below is about the pin and
    // not about the queue read returning nothing at all.
    let still_here = insert_suspended_row(pool, old_node).await?;
    enqueue(pool, old_node, still_here, "full").await?;
    analyze(pool).await?;

    let page = list_pinned_local_candidates(pool, old_node, false, true).await?;
    assert_eq!(page.source, PinnedLocalSource::Queue);
    assert_eq!(
        page.candidates
            .iter()
            .map(|c| c.sandbox_id)
            .collect::<Vec<_>>(),
        vec![still_here],
        "the re-pinned sandbox must not be enumerated for its old node; \
         returning it makes the queue path a superset of the history path, \
         which is the one thing the subset differential promises it is not"
    );

    // And the invariant itself, stated directly against the oracle.
    let history = history_answer(pool, old_node, false).await?;
    assert!(
        page.candidates
            .iter()
            .all(|c| history.contains(&c.sandbox_id)),
        "queue ⊆ history must hold with a moved pin: queue={:?} history={history:?}",
        page.candidates
            .iter()
            .map(|c| c.sandbox_id)
            .collect::<Vec<_>>()
    );
    Ok(())
}
