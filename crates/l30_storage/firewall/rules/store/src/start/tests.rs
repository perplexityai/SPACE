use std::sync::Arc;
use std::time::Duration;

use pplx_space_accounts::{NodeId, UserId};
use pplx_space_db_test_fixtures::{
    SEED_USER_ID, try_pg_test_db, try_pg_test_db_with_max_connections,
};
use pplx_space_firewall_rules_model::{
    FirewallRuleRevision, Match, Name, PayloadHeader, Read, Rule, RuleId, Value, diff,
};
use pplx_space_sql_locator::sql;
use pplx_space_test_env::TestResult;
use sqlx::{PgConnection, PgPool};
use tokio::sync::Barrier;
use tokio::task::JoinHandle;
use uuid::Uuid;

use super::*;
use crate::tests::{online_nodes, set_phase, wait_for_lock_waiters};

const REASON: &str = "test push";

fn id(value: &str) -> RuleId {
    RuleId::new(value).unwrap()
}

/// A rule dropping packets that match every `(protocol, field, value)`, in
/// this order.
fn rule(name: &str, all_of: &[(&str, &str, u64)]) -> Rule {
    let all_of = all_of
        .iter()
        .map(|&(protocol, field, value)| Match {
            left: Read::Payload(PayloadHeader {
                protocol: Name::new(protocol).unwrap(),
                field: Name::new(field).unwrap(),
            }),
            right: Value::Integer(value),
        })
        .collect();
    Rule::new(id(name), all_of).unwrap()
}

fn dns() -> Rule {
    rule("dns", &[("udp", "dport", 53)])
}

/// Two comparisons, so their order is stored too.
fn web() -> Rule {
    rule("web", &[("tcp", "sport", 1024), ("tcp", "dport", 80)])
}

fn ssh() -> Rule {
    rule("ssh", &[("tcp", "dport", 22)])
}

fn revision(rules: &[Rule]) -> FirewallRuleRevision {
    FirewallRuleRevision::new(rules.to_vec()).unwrap()
}

/// Pinned test targets: every `nodes` row is enrolled, and every node
/// qualifies but an `incompatible` one for a candidate holding the rule.
/// With `hold`, the start meets the barrier twice at its freeze, holding the
/// control lock in between.
#[derive(Default)]
struct Fixture {
    incompatible: Option<(NodeId, RuleId)>,
    hold: Option<Arc<Barrier>>,
}

impl Targets for Fixture {
    async fn enrolled(&mut self, tx: &mut PgConnection) -> Result<Vec<NodeId>, StartError> {
        if let Some(hold) = self.hold.take() {
            hold.wait().await;
            hold.wait().await;
        }
        Ok(sqlx::query_scalar(sql!("SELECT id FROM nodes ORDER BY id"))
            .fetch_all(tx)
            .await?)
    }

    fn qualify(
        &self,
        candidate: &FirewallRuleRevision,
        nodes: &[NodeId],
    ) -> Result<(), StartError> {
        match &self.incompatible {
            Some((node, rule)) if nodes.contains(node) && candidate.rule(rule).is_some() => {
                Err(StartError::Unqualified(*node))
            }
            _ => Ok(()),
        }
    }
}

async fn start_by(
    pool: &PgPool,
    expected_adopted: AdoptedRevision,
    revision: &FirewallRuleRevision,
    mut targets: Fixture,
) -> Result<StartedRollout, StartError> {
    let start = StartRollout {
        actor: UserId::from_uuid(SEED_USER_ID),
        reason: REASON.to_owned(),
        revision: revision.clone(),
        expected_adopted,
    };
    let mut conn = pool.acquire().await?;
    start_with(&mut conn, &start, &mut targets).await
}

async fn start(
    pool: &PgPool,
    expected_adopted: AdoptedRevision,
    revision: &FirewallRuleRevision,
) -> Result<StartedRollout, StartError> {
    start_by(pool, expected_adopted, revision, Fixture::default()).await
}

fn spawn_start(
    pool: &PgPool,
    expected_adopted: AdoptedRevision,
    revision: &FirewallRuleRevision,
    targets: Fixture,
) -> JoinHandle<Result<StartedRollout, StartError>> {
    let (pool, revision) = (pool.clone(), revision.clone());
    tokio::spawn(async move { start_by(&pool, expected_adopted, &revision, targets).await })
}

/// Stand in for the completion step: finish `started` and adopt its
/// revision.
async fn complete(pool: &PgPool, started: &StartedRollout) -> TestResult {
    set_phase(pool, started.rollout_id, "completed").await?;
    sqlx::query(sql!(
        "UPDATE firewall_rules_control SET enrollment_default_revision_id = $1"
    ))
    .bind(started.revision_id)
    .execute(pool)
    .await?;
    Ok(())
}

/// Push `revision` first and complete it, so it's the adopted default.
async fn adopt(pool: &PgPool, revision: &FirewallRuleRevision) -> TestResult<RevisionId> {
    let started = start(pool, AdoptedRevision::NoRevision, revision).await?;
    complete(pool, &started).await?;
    Ok(started.revision_id)
}

/// What a start must never change (the default and every delivery record),
/// then what it saves, as text.
async fn state(pool: &PgPool) -> TestResult<(String, String)> {
    Ok(sqlx::query_as(sql!(
        "SELECT
             jsonb_build_array(
                 (SELECT jsonb_agg(to_jsonb(c)) FROM firewall_rules_control c),
                 (SELECT jsonb_agg(to_jsonb(a)) FROM firewall_rules_node_assignments a),
                 (SELECT jsonb_agg(to_jsonb(o)) FROM firewall_rules_node_observations o),
                 (SELECT jsonb_agg(to_jsonb(t)) FROM node_reconcile_triggers t),
                 (SELECT jsonb_agg(to_jsonb(q)) FROM node_reconcile_queue q))::text,
             jsonb_build_array(
                 (SELECT jsonb_agg(to_jsonb(r) ORDER BY r.id) FROM firewall_rules_revisions r),
                 (SELECT jsonb_agg(to_jsonb(r) ORDER BY r.id) FROM firewall_rules_rollouts r),
                 (SELECT jsonb_agg(to_jsonb(i) ORDER BY i.rollout_id, i.node_id)
                  FROM firewall_rules_rollout_items i))::text"
    ))
    .fetch_one(pool)
    .await?)
}

async fn targets_of(pool: &PgPool, rollout: RolloutId) -> TestResult<Vec<NodeId>> {
    Ok(sqlx::query_scalar(sql!(
        "SELECT node_id FROM firewall_rules_rollout_items WHERE rollout_id = $1 ORDER BY node_id"
    ))
    .bind(rollout)
    .fetch_all(pool)
    .await?)
}

/// The fixture enrollment writer: under the control lock enrollment takes,
/// register a node that has never reported, so it's offline.
async fn enroll(tx: &mut PgConnection) -> sqlx::Result<NodeId> {
    sqlx::query(sql!("SELECT 1 FROM firewall_rules_control FOR SHARE"))
        .execute(&mut *tx)
        .await?;
    let node = NodeId::new();
    sqlx::query(sql!("INSERT INTO nodes (id, role) VALUES ($1, 'worker')"))
        .bind(node)
        .execute(&mut *tx)
        .await?;
    Ok(node)
}

/// Meet a held start at its freeze, failing rather than hanging if it ended
/// before getting there.
async fn meet(hold: &Barrier) -> TestResult {
    tokio::time::timeout(Duration::from_secs(10), hold.wait())
        .await
        .map_err(|_| "the held start never reached its freeze")?;
    Ok(())
}

fn sorted(mut nodes: Vec<NodeId>) -> Vec<NodeId> {
    nodes.sort_by_key(|node| node.as_uuid());
    nodes
}

/// Catches a start that adopts its candidate or writes delivery state, saves
/// the wrong header or targets, or answers a repeated request with
/// a second rollout or a stored success; a slot check that misses an
/// unfinished phase or runs after the targets are judged.
#[tokio::test(flavor = "current_thread")]
async fn a_start_saves_the_reviewed_rollout_without_adopting_it() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let a = AdoptedRevision::Revision(adopt(pool, &revision(&[dns(), web()])).await?);
    let nodes = online_nodes(pool, 2).await?;
    let candidate = revision(&[dns(), web(), ssh()]);
    let (unchanged, _) = state(pool).await?;

    let started = start(pool, a, &candidate).await?;
    let header = sqlx::query_as::<_, (RevisionId, String, String, Uuid, Uuid, i64)>(sql!(
        "SELECT to_revision_id, phase, reason, created_by_user_id, updated_by_user_id, generation
         FROM firewall_rules_rollouts WHERE id = $1"
    ))
    .bind(started.rollout_id)
    .fetch_one(pool)
    .await?;
    assert_eq!(
        header,
        (
            started.revision_id,
            "canary".to_owned(),
            REASON.to_owned(),
            SEED_USER_ID,
            SEED_USER_ID,
            i64::try_from(started.generation)?
        )
    );
    let items = sqlx::query_as::<_, (NodeId, String, i64, bool)>(sql!(
        "SELECT node_id, phase, attempts,
                issued_at IS NULL AND next_check_at IS NULL AND last_error IS NULL
         FROM firewall_rules_rollout_items WHERE rollout_id = $1 ORDER BY node_id"
    ))
    .bind(started.rollout_id)
    .fetch_all(pool)
    .await?;
    let pending: Vec<_> = nodes
        .iter()
        .map(|&node| (node, "pending".to_owned(), 0, true))
        .collect();
    assert_eq!(items, pending);
    assert_eq!(state(pool).await?.0, unchanged);

    // The response is lost and the request repeated: refused, not answered
    // from a receipt, in every unfinished phase, before a target that would
    // fail is judged.
    for phase in ["canary", "rolling", "paused"] {
        set_phase(pool, started.rollout_id, phase).await?;
        let pinned = Fixture {
            incompatible: Some((nodes[0], id("dns"))),
            hold: None,
        };
        let repeated = start_by(pool, a, &candidate, pinned).await;
        assert!(
            matches!(repeated, Err(StartError::RolloutInProgress(id)) if id == started.rollout_id),
            "{phase}: {repeated:?}"
        );
    }
    Ok(())
}

/// Catches a precondition that treats either case as a wildcard or compares
/// NULL unsafely, a first push that adopts its candidate or invents a base,
/// a production start that falls back to some membership or qualification,
/// and a missing control row seeded rather than refused.
#[tokio::test(flavor = "current_thread")]
async fn a_start_matches_exactly_the_expected_default() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let candidate = revision(&[dns()]);
    let unknown = AdoptedRevision::Revision(RevisionId(Uuid::now_v7()));
    let refused = start(pool, unknown, &candidate).await;
    assert!(
        matches!(
            refused,
            Err(StartError::Stale {
                adopted: AdoptedRevision::NoRevision
            })
        ),
        "{refused:?}"
    );
    let mut conn = pool.acquire().await?;
    let production = StartRollout {
        actor: UserId::from_uuid(SEED_USER_ID),
        reason: REASON.to_owned(),
        revision: candidate.clone(),
        expected_adopted: AdoptedRevision::NoRevision,
    };
    let refused = start_rollout(&mut conn, &production).await;
    assert!(
        matches!(refused, Err(StartError::NotEnabled)),
        "{refused:?}"
    );

    let first = start(pool, AdoptedRevision::NoRevision, &candidate).await?;
    assert_eq!(first.review, Review::FirstPush);

    complete(pool, &first).await?;
    let a = AdoptedRevision::Revision(first.revision_id);
    // A real revision that isn't adopted: an aborted candidate.
    let aborted = start(pool, a, &revision(&[web()])).await?;
    set_phase(pool, aborted.rollout_id, "aborted").await?;
    for expected in [
        AdoptedRevision::NoRevision,
        AdoptedRevision::Revision(aborted.revision_id),
    ] {
        let refused = start(pool, expected, &candidate).await;
        assert!(
            matches!(refused, Err(StartError::Stale { adopted }) if adopted == a),
            "{expected:?}: {refused:?}"
        );
    }

    sqlx::query(sql!("DELETE FROM firewall_rules_control"))
        .execute(pool)
        .await?;
    let refused = start(pool, a, &candidate).await;
    assert!(
        matches!(refused, Err(StartError::MissingControlRow)),
        "{refused:?}"
    );
    Ok(())
}

/// Two starts of their own candidates against `expected`. The first holds
/// the control lock at its freeze until the second waits for it, then
/// commits, or fails on an incompatible `node` and rolls back.
async fn race(
    pool: &PgPool,
    expected: AdoptedRevision,
    [first_candidate, second_candidate]: [&FirewallRuleRevision; 2],
    node: NodeId,
    first_commits: bool,
) -> TestResult<(
    Result<StartedRollout, StartError>,
    Result<StartedRollout, StartError>,
)> {
    let hold = Arc::new(Barrier::new(2));
    let first = Fixture {
        incompatible: (!first_commits).then(|| (node, id("dns"))),
        hold: Some(Arc::clone(&hold)),
    };
    let first = spawn_start(pool, expected, first_candidate, first);
    meet(&hold).await?;
    let second = spawn_start(pool, expected, second_candidate, Fixture::default());
    wait_for_lock_waiters(pool, 1).await?;
    meet(&hold).await?;
    Ok((first.await?, second.await?))
}

/// How many revision, header and item rows are saved.
async fn saved_rows(pool: &PgPool) -> TestResult<(i64, i64, i64)> {
    Ok(sqlx::query_as(sql!(
        "SELECT (SELECT count(*) FROM firewall_rules_revisions),
                (SELECT count(*) FROM firewall_rules_rollouts),
                (SELECT count(*) FROM firewall_rules_rollout_items)"
    ))
    .fetch_one(pool)
    .await?)
}

/// Catches starts that don't serialize on the control row, which would let
/// two pushes both pass the precondition and the slot check, a lock that
/// outlives a failed start, a blocked start that doesn't name the winner or
/// saves anything, and a slot-key violation not reported as the slot taken.
#[tokio::test(flavor = "current_thread")]
async fn racing_starts_serialize_on_the_control_row() -> TestResult {
    let Some(db) = try_pg_test_db_with_max_connections(5).await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let node = online_nodes(pool, 1).await?[0];
    let candidates = [&revision(&[dns()]), &revision(&[ssh()])];
    let a = AdoptedRevision::Revision(adopt(pool, &revision(&[web()])).await?);
    let (first, second) = race(pool, a, candidates, node, false).await?;
    assert!(
        matches!(first, Err(StartError::Unqualified(_))),
        "{first:?}"
    );
    set_phase(pool, second?.rollout_id, "aborted").await?;

    let before = saved_rows(pool).await?;
    let (first, second) = race(pool, a, candidates, node, true).await?;
    let first = first?;
    assert!(
        matches!(second, Err(StartError::RolloutInProgress(id)) if id == first.rollout_id),
        "{second:?}"
    );
    // Only the winner's revision, header and item.
    let (revisions, headers, items) = before;
    assert_eq!(
        saved_rows(pool).await?,
        (revisions + 1, headers + 1, items + 1)
    );
    complete(pool, &first).await?;
    let expected = AdoptedRevision::Revision(first.revision_id);

    // A writer that skips the control lock meets the slot key instead.
    let mut bypass = pool.begin().await?;
    sqlx::query(sql!(
        "INSERT INTO firewall_rules_rollouts (id, to_revision_id, phase, unfinished_slot, reason)
         SELECT $1, enrollment_default_revision_id, 'canary', 1, 'bypass'
         FROM firewall_rules_control"
    ))
    .bind(Uuid::now_v7())
    .execute(&mut *bypass)
    .await?;
    let racing = spawn_start(pool, expected, candidates[0], Fixture::default());
    wait_for_lock_waiters(pool, 1).await?;
    bypass.commit().await?;
    let refused = racing.await?;
    assert!(matches!(refused, Err(StartError::SlotTaken)), "{refused:?}");
    Ok(())
}

/// Catches targets read before the control lock, or under a lock enrollment
/// doesn't wait for, which would miss a node enrolled just before the start
/// or save one enrolled during it; and targets filtered to online nodes.
#[tokio::test(flavor = "current_thread")]
async fn targets_are_frozen_under_the_control_lock() -> TestResult {
    let Some(db) = try_pg_test_db_with_max_connections(5).await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let online = online_nodes(pool, 1).await?[0];
    let candidate = revision(&[dns()]);

    // Enrolled before the freeze: the start waits for it and includes it.
    let mut enrolling = pool.begin().await?;
    let offline = enroll(&mut enrolling).await?;
    let racing = spawn_start(
        pool,
        AdoptedRevision::NoRevision,
        &candidate,
        Fixture::default(),
    );
    wait_for_lock_waiters(pool, 1).await?;
    enrolling.commit().await?;
    let first = racing.await??;
    let frozen = sorted(vec![online, offline]);
    assert_eq!(targets_of(pool, first.rollout_id).await?, frozen);
    set_phase(pool, first.rollout_id, "aborted").await?;

    // Enrolled during the freeze: it waits for the start and isn't included.
    let hold = Arc::new(Barrier::new(2));
    let holding = Fixture {
        incompatible: None,
        hold: Some(Arc::clone(&hold)),
    };
    let starting = spawn_start(pool, AdoptedRevision::NoRevision, &candidate, holding);
    meet(&hold).await?;
    let enrolling = tokio::spawn({
        let pool = pool.clone();
        async move {
            let mut tx = pool.begin().await?;
            enroll(&mut tx).await?;
            tx.commit().await
        }
    });
    wait_for_lock_waiters(pool, 1).await?;
    meet(&hold).await?;
    let second = starting.await??;
    enrolling.await??;
    assert_eq!(targets_of(pool, second.rollout_id).await?, frozen);
    Ok(())
}

/// Catches a candidate stored out of order or not as validated, a review not
/// diffed against the adopted default or diffed against one this version
/// can't read, a candidate past nft's limits accepted because the model
/// built it, and qualification of only the changed rules, which would start
/// a rollout some target can't enforce.
#[tokio::test(flavor = "current_thread")]
async fn the_whole_candidate_is_validated_and_stored_exactly() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let base = revision(&[dns(), web()]);
    let base_id = adopt(pool, &base).await?;
    let a = AdoptedRevision::Revision(base_id);
    let nodes = online_nodes(pool, 2).await?;

    // The model reports a reorder alone as a change (`a_pure_reorder_is_reported`).
    let reordered = revision(&[web(), dns()]);
    let started = start(pool, a, &reordered).await?;
    assert_eq!(read_revision(pool, started.revision_id).await?, reordered);
    assert_eq!(
        started.review,
        Review::Against {
            base: base_id,
            diff: diff(&base, &reordered)
        }
    );
    set_phase(pool, started.rollout_id, "aborted").await?;

    let past_limit = revision(&[dns(), rule("big", &[("tcp", "dport", u64::MAX)])]);
    let invalid = start(pool, a, &past_limit).await;
    assert!(
        matches!(invalid, Err(StartError::Invalid(_))),
        "{invalid:?}"
    );
    // `web` is unchanged from A, and one target can't enforce it.
    let pinned = Fixture {
        incompatible: Some((nodes[1], id("web"))),
        hold: None,
    };
    let refused = start_by(pool, a, &revision(&[dns(), web(), ssh()]), pinned).await;
    assert!(
        matches!(refused, Err(StartError::Unqualified(node)) if node == nodes[1]),
        "{refused:?}"
    );

    // A default in a format a newer version might write.
    let newer = RevisionId(Uuid::now_v7());
    sqlx::query(sql!(
        "WITH newer AS (
             INSERT INTO firewall_rules_revisions (id, ruleset_format_version, ruleset)
             VALUES ($1, 2, '[]') RETURNING id)
         UPDATE firewall_rules_control SET enrollment_default_revision_id = (SELECT id FROM newer)"
    ))
    .bind(newer)
    .execute(pool)
    .await?;
    let unreadable = start(pool, AdoptedRevision::Revision(newer), &reordered).await;
    assert!(
        matches!(unreadable, Err(StartError::StoredRevision { id, .. }) if id == newer),
        "{unreadable:?}"
    );
    Ok(())
}

/// Catches a start that isn't one transaction, leaving a revision or header
/// without its targets, or the slot held, after a failed write; and an
/// unrelated unique violation reported as a rollout in progress.
#[tokio::test(flavor = "current_thread")]
async fn a_failed_write_leaves_nothing_behind() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    online_nodes(pool, 1).await?;
    let candidate = revision(&[dns()]);
    sqlx::raw_sql(sql!(
        "CREATE FUNCTION fail_insert() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN RAISE unique_violation USING MESSAGE = 'injected', CONSTRAINT = 'injected'; END $$"
    ))
    .execute(pool)
    .await?;
    let before = state(pool).await?;
    // After the revision insert, then after the header insert.
    for table in [
        sql!(
            "CREATE TRIGGER fail_insert BEFORE INSERT ON firewall_rules_rollouts
             FOR EACH ROW EXECUTE FUNCTION fail_insert();"
        ),
        sql!(
            "CREATE TRIGGER fail_insert BEFORE INSERT ON firewall_rules_rollout_items
             FOR EACH ROW EXECUTE FUNCTION fail_insert();"
        ),
    ] {
        sqlx::raw_sql(table).execute(pool).await?;
        let failed = start(pool, AdoptedRevision::NoRevision, &candidate).await;
        assert!(matches!(failed, Err(StartError::Database(_))), "{failed:?}");
        assert_eq!(state(pool).await?, before);
        sqlx::raw_sql(sql!(
            "DROP TRIGGER IF EXISTS fail_insert ON firewall_rules_rollouts;
             DROP TRIGGER IF EXISTS fail_insert ON firewall_rules_rollout_items;"
        ))
        .execute(pool)
        .await?;
    }
    start(pool, AdoptedRevision::NoRevision, &candidate).await?;
    Ok(())
}
