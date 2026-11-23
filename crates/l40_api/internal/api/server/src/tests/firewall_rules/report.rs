//! `ReportFirewallRules` through the real auth layer, the round opener and
//! Fetch. A row snapshot is the whole observation row as JSON, so "unchanged"
//! includes `updated_at`, which any UPDATE moves.

use std::time::Duration;

use super::*;
use crate::firewall_rules::REPORT_MAX_AGE;
use crate::proto::{
    FirewallRulesCensus as Census, FirewallRulesCensusGap, FirewallRulesCensusGapKind as Gap,
    FirewallRulesReportOutcome as Outcome, FirewallRulesSlotEvidence,
    FirewallRulesSlotOutcome as Slot, FirewallRulesSlotReason as Reason,
    ReportFirewallRulesRequest,
};

const MAX_AGE: Duration = REPORT_MAX_AGE;
/// Bounds every wait on a lock or the clock.
const WAIT: Duration = Duration::from_secs(10);

async fn report(
    client: &mut ClusterApiClient<Channel>,
    bearer: &str,
    request: ReportFirewallRulesRequest,
) -> TestResult<Result<Outcome, (Code, String)>> {
    let mut request = Request::new(request);
    request
        .metadata_mut()
        .insert("authorization", format!("Bearer {bearer}").parse()?);
    Ok(client
        .report_firewall_rules(request)
        .await
        .map(|response| response.into_inner().outcome())
        .map_err(|status| (status.code(), status.message().to_owned())))
}

fn slot(
    slot_id: u32,
    slot_generation: u64,
    outcome: Slot,
    reason: Reason,
) -> FirewallRulesSlotEvidence {
    FirewallRulesSlotEvidence {
        slot_id: Some(slot_id),
        slot_generation: Some(slot_generation),
        outcome: outcome.into(),
        reason: reason.into(),
    }
}

fn gap(kind: Gap, slot_id: Option<u32>) -> FirewallRulesCensusGap {
    FirewallRulesCensusGap {
        kind: kind.into(),
        slot_id,
    }
}

/// `request` with an incomplete census: slot 4's namespace is missing, and
/// the host names no managed-namespace directory.
fn incomplete(mut request: ReportFirewallRulesRequest) -> ReportFirewallRulesRequest {
    request.census = Census::Incomplete.into();
    request.census_gaps = vec![gap(Gap::Missing, Some(4)), gap(Gap::Unowned, None)];
    request
}

/// A complete census in which leased slot 3 matched and idle slot 9 had the
/// wrong rules.
fn answer(
    node: NodeId,
    round: u64,
    (generation, revision): (u64, Uuid),
    epoch: u64,
) -> ReportFirewallRulesRequest {
    ReportFirewallRulesRequest {
        node_id: node.to_string(),
        round: Some(round),
        assignment_generation: Some(generation),
        revision_id: revision.to_string(),
        manager_epoch: Some(epoch),
        census: Census::Complete.into(),
        census_gaps: Vec::new(),
        slots: vec![
            slot(9, 2, Slot::Failed, Reason::RulesMismatch),
            slot(3, 5, Slot::Verified, Reason::Unspecified),
        ],
    }
}

/// `node`'s whole observation row, or `None` without one.
async fn row(pool: &sqlx::PgPool, node: NodeId) -> TestResult<Option<String>> {
    Ok(sqlx::query_scalar::<_, String>(sql!(
        "SELECT row_to_json(o)::text FROM firewall_rules_node_observations o WHERE node_id = $1"
    ))
    .bind(node)
    .fetch_optional(pool)
    .await?)
}

/// Assign `node` `generation` of `revision` and ask it round `round` about
/// that generation under `epoch`, issued `age` before the database clock.
async fn ask(
    pool: &sqlx::PgPool,
    node: NodeId,
    (generation, revision): (u64, Uuid),
    round: u64,
    epoch: u64,
    age: Duration,
) -> TestResult {
    sqlx::query(sql!(
        "INSERT INTO firewall_rules_node_assignments (node_id, assignment_generation, revision_id)
         VALUES ($1, $2, $3)
         ON CONFLICT (node_id) DO UPDATE
         SET assignment_generation = EXCLUDED.assignment_generation,
             revision_id = EXCLUDED.revision_id"
    ))
    .bind(node)
    .bind(i64::try_from(generation)?)
    .bind(revision)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO firewall_rules_node_observations (
             node_id, manager_epoch, pending_round, pending_round_issued_at,
             pending_assignment_generation
         )
         VALUES ($1, $2, $3, clock_timestamp() - make_interval(secs => $5), $4)
         ON CONFLICT (node_id) DO UPDATE
         SET manager_epoch = EXCLUDED.manager_epoch,
             pending_round = EXCLUDED.pending_round,
             pending_round_issued_at = EXCLUDED.pending_round_issued_at,
             pending_assignment_generation = EXCLUDED.pending_assignment_generation"
    ))
    .bind(node)
    .bind(i64::try_from(epoch)?)
    .bind(i64::try_from(round)?)
    .bind(i64::try_from(generation)?)
    .bind(age.as_secs_f64())
    .execute(pool)
    .await?;
    Ok(())
}

/// Wait until `count` backends wait on a lock.
async fn wait_for_lock_waiters(pool: &sqlx::PgPool, count: i64) -> TestResult {
    let deadline = tokio::time::Instant::now() + WAIT;
    while sqlx::query_scalar::<_, i64>(sql!(
        "SELECT count(*) FROM pg_stat_activity
         WHERE datname = current_database() AND wait_event_type = 'Lock'"
    ))
    .fetch_one(pool)
    .await?
        < count
    {
        assert!(
            tokio::time::Instant::now() < deadline,
            "{count} lock waiters never appeared"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    Ok(())
}

/// Lock `node`'s observation row until the returned transaction ends.
async fn hold_observation(
    pool: &sqlx::PgPool,
    node: NodeId,
) -> TestResult<sqlx::Transaction<'static, sqlx::Postgres>> {
    let mut holder = pool.begin().await?;
    sqlx::query(sql!(
        "SELECT 1 FROM firewall_rules_node_observations WHERE node_id = $1 FOR UPDATE"
    ))
    .bind(node)
    .execute(&mut *holder)
    .await?;
    Ok(holder)
}

/// One enrolled worker whose node-ctl may fetch and report, with a revision.
struct Reporter {
    harness: ReviewedClusterApi,
    token: String,
    node: NodeId,
    revision: Uuid,
    pool: sqlx::PgPool,
    _db: TestDb,
}

async fn reporter(max_connections: u32) -> TestResult<Option<Reporter>> {
    let Some(ClusterFixture {
        mut service,
        _db: db,
    }) = cluster_fixture_with_max_connections(max_connections).await?
    else {
        return Ok(None);
    };
    let pool = service.pool.clone();
    let node = register(&pool, "worker", "ready", "192.0.2.10", "worker-a", "uid-a").await?;
    let revision = insert_revision(&pool, SSH).await?;
    service.firewall_enrollment = Enrollment::Fixture(vec![node]);
    let token = projected_token("node-ctl-a");
    let reviews = vec![(token.clone(), node_ctl_on("worker-a", "uid-a"))];
    Ok(Some(Reporter {
        harness: serve_granting(service, reviews, true, true).await?,
        token,
        node,
        revision,
        pool,
        _db: db,
    }))
}

/// The stack end to end: the opener's round, fetched through Fetch, answered
/// with a mismatch and an incomplete census, is stored as reported with the
/// round's original issue time, and Fetch no longer offers it. Catches a
/// decoder that refuses unfavorable evidence, a write that stores another
/// time, leaves part of the pending tuple, or drops a census gap or the slot
/// order, and an acceptance that touches the rollout item.
#[tokio::test(flavor = "current_thread")]
async fn report_answers_the_round_its_opener_opened() -> TestResult {
    let Some(fixture) = cluster_fixture().await? else {
        return Ok(());
    };
    let mut service = fixture.service;
    let pool = service.pool.clone();
    let pool = &pool;
    let a = register(pool, "worker", "ready", "192.0.2.10", "worker-a", "uid-a").await?;
    upsert_snapshot(pool, a, SnapshotSeed::default()).await?;
    service.firewall_enrollment = Enrollment::Fixture(vec![a]);
    let mut conn = pool.acquire().await?;
    let rollout = rollout(pool, "rolling").await?;
    save_targets(pool, rollout, &[a]).await?;
    let (target, _) = issue_one(&mut conn, rollout).await?;
    let Opening::Opened(question) = open_round(&mut conn, &target, 1, &mut Wake).await? else {
        panic!("no round was opened");
    };
    let token = projected_token("node-ctl-a");
    let reviews = vec![(token.clone(), node_ctl_on("worker-a", "uid-a"))];
    let mut harness = serve_granting(service, reviews, true, true).await?;
    let client = &mut harness.client;

    let fetched = fetch(client, Some(&token)).await?.expect("A fetches");
    let (assignment, pending) = (fetched.assignment.unwrap(), fetched.pending_round.unwrap());
    let generation = pending.assignment_generation.unwrap();
    let request = incomplete(answer(
        a,
        pending.round,
        (generation, assignment.revision_id.parse()?),
        1,
    ));
    let item = || {
        sqlx::query_scalar::<_, String>(sql!(
            "SELECT row_to_json(i)::text FROM firewall_rules_rollout_items i
             WHERE rollout_id = $1 AND node_id = $2"
        ))
        .bind(rollout)
        .bind(a)
        .fetch_one(pool)
    };
    let item_before = item().await?;
    assert_eq!(
        report(client, &token, request).await?,
        Ok(Outcome::Accepted)
    );

    let (accepted_round, accepted_at, document, pending_left) = sqlx::query_as::<
        _,
        (
            Option<i64>,
            Option<time::OffsetDateTime>,
            Option<String>,
            bool,
        ),
    >(sql!(
        "SELECT accepted_round, accepted_round_issued_at, accepted_report::text,
                        pending_round IS NULL AND pending_round_issued_at IS NULL
                            AND pending_assignment_generation IS NULL
                 FROM firewall_rules_node_observations
                 WHERE node_id = $1"
    ))
    .bind(a)
    .fetch_one(pool)
    .await?;
    assert_eq!(accepted_round, Some(i64::try_from(question.round)?));
    assert_eq!(
        accepted_at,
        Some(time::OffsetDateTime::from_unix_timestamp_nanos(
            question.issued_at.as_nanosecond()
        )?)
    );
    assert!(pending_left, "acceptance left part of the pending round");
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&document.unwrap())?,
        serde_json::json!({
            "schema_version": 1,
            "round": question.round,
            "assignment_generation": generation,
            "revision_id": assignment.revision_id,
            "manager_epoch": 1,
            "census": "incomplete",
            "census_gaps": [{"gap": "unowned"}, {"gap": "missing", "slot_id": 4}],
            "slots": [
                {"slot_id": 3, "slot_generation": 5, "outcome": "verified"},
                {"slot_id": 9, "slot_generation": 2, "outcome": "failed", "reason": "rules_mismatch"},
            ],
        })
    );
    assert_eq!(item().await?, item_before);
    let after = fetch(client, Some(&token)).await?.expect("A fetches");
    assert_eq!(after.pending_round, None);
    assert_eq!(after.assignment, Some(assignment));
    Ok(())
}

/// After a lost acknowledgement and a newer round, a replay of the accepted
/// report, even with its slots reordered, is a duplicate, and an altered one a
/// conflict; neither touches round 8 or round 7's evidence. Catches duplicate
/// detection by round alone or by raw text, a conflict that replaces evidence,
/// and a late answer that clears the newer round or refreshes the old one.
#[tokio::test(flavor = "current_thread")]
async fn replayed_reports_after_a_newer_round_write_nothing() -> TestResult {
    let Some(mut r) = reporter(2).await? else {
        return Ok(());
    };
    let (pool, client) = (&r.pool, &mut r.harness.client);
    ask(pool, r.node, (12, r.revision), 7, 1, Duration::ZERO).await?;
    let accepted = incomplete(answer(r.node, 7, (12, r.revision), 1));
    assert_eq!(
        report(client, &r.token, accepted.clone()).await?,
        Ok(Outcome::Accepted)
    );
    // The opener's round 8, past the accepted round 7.
    sqlx::query(sql!(
        "UPDATE firewall_rules_node_observations
         SET pending_round = 8, pending_round_issued_at = clock_timestamp(),
             pending_assignment_generation = 12
         WHERE node_id = $1"
    ))
    .bind(r.node)
    .execute(pool)
    .await?;
    let before = row(pool, r.node).await?;

    let mut reordered = accepted.clone();
    reordered.slots.reverse();
    reordered.census_gaps.reverse();
    let mut altered = accepted.clone();
    altered.slots[0] = slot(9, 2, Slot::Verified, Reason::Unspecified);
    for (request, expected) in [
        (accepted, Outcome::Duplicate),
        (reordered, Outcome::Duplicate),
        (altered, Outcome::Conflict),
    ] {
        assert_eq!(report(client, &r.token, request).await?, Ok(expected));
        assert_eq!(row(pool, r.node).await?, before, "{expected:?}");
    }
    Ok(())
}

/// A round bound to B/12 while the assignment moved to C/13: B's answer is
/// stale and C's can't borrow B's round. Catches acceptance that skips the
/// current-assignment check or matches the round to the current generation.
#[tokio::test(flavor = "current_thread")]
async fn answers_for_another_assignment_than_the_round_write_nothing() -> TestResult {
    let Some(mut r) = reporter(2).await? else {
        return Ok(());
    };
    let (pool, client) = (&r.pool, &mut r.harness.client);
    let c = insert_revision(pool, ORDERED).await?;
    ask(pool, r.node, (12, r.revision), 7, 1, Duration::ZERO).await?;
    sqlx::query(sql!(
        "UPDATE firewall_rules_node_assignments
         SET assignment_generation = 13, revision_id = $2
         WHERE node_id = $1"
    ))
    .bind(r.node)
    .bind(c)
    .execute(pool)
    .await?;
    let before = row(pool, r.node).await?;
    for (asked, expected) in [
        ((12, r.revision), Outcome::Superseded),
        ((13, c), Outcome::GenerationMismatch),
    ] {
        let request = answer(r.node, 7, asked, 1);
        assert_eq!(report(client, &r.token, request).await?, Ok(expected));
        assert_eq!(row(pool, r.node).await?, before, "{expected:?}");
    }
    Ok(())
}

/// Two reports wait together on the observation row: identical ones yield
/// one acceptance and one duplicate, conflicting ones one acceptance and one
/// conflict that leaves the winner stored. Catches a decision made on a read
/// from before the lock.
#[tokio::test(flavor = "current_thread")]
async fn racing_reports_are_accepted_once() -> TestResult {
    let Some(r) = reporter(5).await? else {
        return Ok(());
    };
    let pool = &r.pool;
    let base = answer(r.node, 7, (12, r.revision), 1);
    let mut other = base.clone();
    other.slots[0] = slot(9, 2, Slot::Unknown, Reason::TableUnreadable);
    for (round, pair, loser) in [
        (7, [base.clone(), base.clone()], Outcome::Duplicate),
        (8, [base.clone(), other], Outcome::Conflict),
    ] {
        ask(pool, r.node, (12, r.revision), round, 1, Duration::ZERO).await?;
        let holder = hold_observation(pool, r.node).await?;
        let racing = pair.clone().map(|mut request| {
            request.round = Some(round);
            let (mut client, token) = (r.harness.client.clone(), r.token.clone());
            tokio::spawn(async move { report(&mut client, &token, request).await.unwrap() })
        });
        wait_for_lock_waiters(pool, 2).await?;
        holder.commit().await?;
        let mut outcomes = Vec::new();
        for racer in racing {
            outcomes.push(tokio::time::timeout(WAIT, racer).await??);
        }
        let winner = match outcomes[..] {
            [Ok(Outcome::Accepted), Ok(other)] if other == loser => 0,
            [Ok(other), Ok(Outcome::Accepted)] if other == loser => 1,
            _ => panic!("round {round}: {outcomes:?}"),
        };
        let mut won = pair[winner].clone();
        won.round = Some(round);
        // The winner is stored: replaying it is the duplicate.
        assert_eq!(
            report(&mut r.harness.client.clone(), &r.token, won).await?,
            Ok(Outcome::Duplicate),
            "round {round}"
        );
    }
    Ok(())
}

/// Expired, wrong-epoch, wrong-round and unasked reports change nothing and
/// create no row; a round just inside the limit is accepted. Catches a
/// missing epoch, round or freshness check, freshness measured from
/// `updated_at`, and a report that creates the observation row.
#[tokio::test(flavor = "current_thread")]
async fn reports_outside_the_pending_round_write_nothing() -> TestResult {
    let Some(mut r) = reporter(2).await? else {
        return Ok(());
    };
    let (pool, client) = (&r.pool, &mut r.harness.client);
    let current = (12, r.revision);
    let unasked = answer(r.node, 7, current, 1);
    assert_eq!(
        report(client, &r.token, unasked).await?,
        Ok(Outcome::NotPending)
    );
    assert_eq!(row(pool, r.node).await?, None);

    let inside = MAX_AGE - Duration::from_secs(60);
    for (age, request, expected) in [
        (MAX_AGE, answer(r.node, 7, current, 1), Outcome::Expired),
        (
            Duration::ZERO,
            answer(r.node, 7, current, 2),
            Outcome::EpochMismatch,
        ),
        (
            Duration::ZERO,
            answer(r.node, 6, current, 1),
            Outcome::NotPending,
        ),
        (inside, answer(r.node, 7, current, 1), Outcome::Accepted),
    ] {
        ask(pool, r.node, current, 7, 1, age).await?;
        let before = row(pool, r.node).await?;
        assert_eq!(report(client, &r.token, request).await?, Ok(expected));
        if expected != Outcome::Accepted {
            assert_eq!(row(pool, r.node).await?, before, "{expected:?}");
        }
    }
    // Accepted: nothing is pending, so the same report for the next round
    // number isn't either.
    let before = row(pool, r.node).await?;
    let next = answer(r.node, 8, current, 1);
    assert_eq!(
        report(client, &r.token, next).await?,
        Ok(Outcome::NotPending)
    );
    assert_eq!(row(pool, r.node).await?, before);
    Ok(())
}

/// A report waiting on the observation row holds the assignment it checked,
/// and judges freshness on a clock read after the wait: a round fresh when the
/// report began but expired when its lock was granted is refused. A report
/// waiting on a superseding assignment write sees the new assignment. Catches
/// `now()` for the clock, an unlocked assignment read, and locking the
/// observation before the assignment.
#[tokio::test(flavor = "current_thread")]
async fn reports_are_judged_after_their_lock_waits() -> TestResult {
    let Some(r) = reporter(5).await? else {
        return Ok(());
    };
    let pool = &r.pool;
    let request = answer(r.node, 7, (12, r.revision), 1);
    let spawn_report = || {
        let (mut client, token, request) =
            (r.harness.client.clone(), r.token.clone(), request.clone());
        tokio::spawn(async move { report(&mut client, &token, request).await.unwrap() })
    };

    // Two seconds from expiry.
    ask(
        pool,
        r.node,
        (12, r.revision),
        7,
        1,
        MAX_AGE - Duration::from_secs(2),
    )
    .await?;
    let before = row(pool, r.node).await?;
    let holder = hold_observation(pool, r.node).await?;
    let waiting = spawn_report();
    wait_for_lock_waiters(pool, 1).await?;
    let mut probe = pool.begin().await?;
    let pinned = sqlx::query(sql!(
        "SELECT 1 FROM firewall_rules_node_assignments WHERE node_id = $1 FOR UPDATE NOWAIT"
    ))
    .bind(r.node)
    .execute(&mut *probe)
    .await
    .expect_err("the waiting report holds no assignment lock");
    assert_eq!(
        pinned.as_database_error().and_then(|error| error.code()),
        Some("55P03".into())
    );
    probe.rollback().await?;
    let started_fresh = sqlx::query_scalar::<_, bool>(sql!(
        "SELECT bool_and(a.xact_start < o.pending_round_issued_at + make_interval(secs => $2))
         FROM pg_stat_activity a, firewall_rules_node_observations o
         WHERE o.node_id = $1
           AND a.datname = current_database()
           AND a.wait_event_type = 'Lock'"
    ))
    .bind(r.node)
    .bind(MAX_AGE.as_secs_f64())
    .fetch_one(pool)
    .await?;
    assert!(started_fresh, "the report began after its round expired");
    let deadline = tokio::time::Instant::now() + WAIT;
    while !sqlx::query_scalar::<_, bool>(sql!(
        "SELECT clock_timestamp() >= pending_round_issued_at + make_interval(secs => $2)
         FROM firewall_rules_node_observations
         WHERE node_id = $1"
    ))
    .bind(r.node)
    .bind(MAX_AGE.as_secs_f64())
    .fetch_one(pool)
    .await?
    {
        assert!(tokio::time::Instant::now() < deadline, "never expired");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    holder.commit().await?;
    assert_eq!(
        tokio::time::timeout(WAIT, waiting).await??,
        Ok(Outcome::Expired)
    );
    assert_eq!(row(pool, r.node).await?, before);

    // A supersede uncommitted when the report starts.
    ask(pool, r.node, (12, r.revision), 7, 1, Duration::ZERO).await?;
    let before = row(pool, r.node).await?;
    let mut supersede = pool.begin().await?;
    sqlx::query(sql!(
        "UPDATE firewall_rules_node_assignments
         SET assignment_generation = 13
         WHERE node_id = $1"
    ))
    .bind(r.node)
    .execute(&mut *supersede)
    .await?;
    let waiting = spawn_report();
    wait_for_lock_waiters(pool, 1).await?;
    supersede.commit().await?;
    assert_eq!(
        tokio::time::timeout(WAIT, waiting).await??,
        Ok(Outcome::Superseded)
    );
    assert_eq!(row(pool, r.node).await?, before);
    Ok(())
}

/// An answer to a round the opener has since replaced, before the answer
/// arrives or while it waits on the observation row, is not pending and
/// leaves the newer round alone. Catches a write keyed by the node or the
/// generation rather than the pending round, and a round read before the
/// lock.
#[tokio::test(flavor = "current_thread")]
async fn delayed_answers_leave_a_newer_round_pending() -> TestResult {
    let Some(fixture) = cluster_fixture_with_max_connections(5).await? else {
        return Ok(());
    };
    let mut service = fixture.service;
    let pool = service.pool.clone();
    let pool = &pool;
    let a = register(pool, "worker", "ready", "192.0.2.10", "worker-a", "uid-a").await?;
    upsert_snapshot(pool, a, SnapshotSeed::default()).await?;
    service.firewall_enrollment = Enrollment::Fixture(vec![a]);
    let mut conn = pool.acquire().await?;
    let revision_of = async |rollout: RolloutId| -> TestResult<Uuid> {
        Ok(sqlx::query_scalar::<_, Uuid>(sql!(
            "SELECT to_revision_id FROM firewall_rules_rollouts WHERE id = $1"
        ))
        .bind(rollout)
        .fetch_one(pool)
        .await?)
    };

    // Round 1 asks about the first rollout's generation; the second rollout's
    // opener replaces it with round 2.
    let first = rollout(pool, "rolling").await?;
    save_targets(pool, first, &[a]).await?;
    let (target, old) = issue_one(&mut conn, first).await?;
    let Opening::Opened(stale) = open_round(&mut conn, &target, 1, &mut Wake).await? else {
        panic!("no first round");
    };
    assert!(requeue(&mut *conn, &target, &Requeue::Release { error: None }).await?);
    set_phase(pool, first, "completed").await?;
    let next = rollout(pool, "rolling").await?;
    save_targets(pool, next, &[a]).await?;
    let (target, current) = issue_one(&mut conn, next).await?;
    let Opening::Opened(newer) = open_round(&mut conn, &target, 1, &mut Wake).await? else {
        panic!("no second round");
    };
    assert_ne!(newer.round, stale.round);

    let token = projected_token("node-ctl-a");
    let reviews = vec![(token.clone(), node_ctl_on("worker-a", "uid-a"))];
    let harness = serve_granting(service, reviews, false, true).await?;
    let before = row(pool, a).await?;
    let late = answer(a, stale.round, (old, revision_of(first).await?), 1);
    assert_eq!(
        report(&mut harness.client.clone(), &token, late).await?,
        Ok(Outcome::NotPending)
    );
    assert_eq!(row(pool, a).await?, before);

    // Round 2 is replaced while its answer waits.
    let mut holder = hold_observation(pool, a).await?;
    let (mut client, bearer) = (harness.client.clone(), token.clone());
    let request = answer(a, newer.round, (current, revision_of(next).await?), 1);
    let waiting = tokio::spawn(async move { report(&mut client, &bearer, request).await.unwrap() });
    wait_for_lock_waiters(pool, 1).await?;
    sqlx::query(sql!(
        "UPDATE firewall_rules_node_observations
         SET pending_round = pending_round + 1, pending_round_issued_at = clock_timestamp()
         WHERE node_id = $1"
    ))
    .bind(a)
    .execute(&mut *holder)
    .await?;
    holder.commit().await?;
    let replaced = row(pool, a).await?;
    assert_eq!(
        tokio::time::timeout(WAIT, waiting).await??,
        Ok(Outcome::NotPending)
    );
    assert_eq!(row(pool, a).await?, replaced);
    Ok(())
}

/// Only a report-authorized node-ctl reporting for its own resolved, enrolled
/// worker writes: the fetch authority alone,
/// another workload, a stale UID, an ambiguous name, missing claims, the static
/// token and a report naming worker B all write nothing, and the report
/// authority doesn't permit fetching. Catches report authorized by the fetch
/// authority, a row chosen by the request's node ID, and acceptance without
/// enrollment.
#[tokio::test(flavor = "current_thread")]
async fn report_writes_only_for_an_authorized_callers_own_worker() -> TestResult {
    let Some(fixture) = cluster_fixture().await? else {
        return Ok(());
    };
    let pool = fixture.service.pool.clone();
    let pool = &pool;
    let revision = insert_revision(pool, SSH).await?;
    let a = register(pool, "worker", "ready", "192.0.2.10", "worker-a", "uid-a").await?;
    let b = register(pool, "worker", "ready", "192.0.2.11", "worker-b", "uid-b").await?;
    let twin = register(pool, "worker", "ready", "192.0.2.14", "worker-t", "uid-t").await?;
    register(pool, "worker", "ready", "192.0.2.15", "worker-t", "uid-t2").await?;
    for node in [a, b, twin] {
        ask(pool, node, (12, revision), 7, 1, Duration::ZERO).await?;
    }
    let rows = async || -> TestResult<_> { Ok((row(pool, a).await?, row(pool, b).await?)) };
    let before = rows().await?;

    let enrolled = || {
        let mut service = fixture.service.clone();
        service.firewall_enrollment = Enrollment::Fixture(vec![a, b, twin]);
        service
    };
    let other_workload = "system:serviceaccount:space:space-template-manager";
    let reviewed = [
        ("node-ctl-a", node_ctl_on("worker-a", "uid-a")),
        (
            "other-workload-a",
            pod_bound_service_account_status(AUDIENCE, other_workload, "worker-a", "uid-a"),
        ),
        ("stale-uid", node_ctl_on("worker-a", "stale-uid")),
        ("ambiguous", node_ctl_on("worker-t", "uid-t")),
        ("no-claims", service_account_status(AUDIENCE, NODE_CTL)),
    ]
    .map(|(label, status)| (projected_token(label), status));
    let token = |label: usize| reviewed[label].0.clone();
    let reviews = reviewed.to_vec();
    let own = answer(a, 7, (12, revision), 1);
    let report_denied = Err((
        Code::PermissionDenied,
        "the caller may not report firewall rules".to_owned(),
    ));

    // The fetch authority alone fetches but doesn't report.
    let mut fetch_only = serve_granting(enrolled(), reviews.clone(), true, false).await?;
    let fetched = fetch(&mut fetch_only.client, Some(&token(0))).await?;
    assert_eq!(fetched.map(|fetched| fetched.node_id), Ok(a.to_string()));
    assert_eq!(
        report(&mut fetch_only.client, &token(0), own.clone()).await?,
        report_denied
    );

    let mut report_only = serve_granting(enrolled(), reviews.clone(), false, true).await?;
    let client = &mut report_only.client;
    assert_eq!(fetch(client, Some(&token(0))).await?, denied());
    let for_b = answer(b, 7, (12, revision), 1);
    for (bearer, request) in [
        (token(1), own.clone()),
        (token(2), own.clone()),
        (token(3), answer(twin, 7, (12, revision), 1)),
        (token(4), own.clone()),
        (REVIEWED_STATIC_TOKEN.to_owned(), own.clone()),
        (token(0), for_b.clone()),
    ] {
        assert_eq!(report(client, &bearer, request).await?, report_denied);
    }
    assert_eq!(rows().await?, before);

    // Production's default enrolls no worker. Naming another worker is denied
    // before enrollment is consulted.
    let mut unenrolled = enrolled();
    unenrolled.firewall_enrollment = Enrollment::Unapproved;
    let mut harness = serve_granting(unenrolled, reviews.clone(), false, true).await?;
    let refused = &mut harness.client;
    assert_eq!(
        report(refused, &token(0), for_b.clone()).await?,
        report_denied
    );
    assert_eq!(
        report(refused, &token(0), own.clone()).await?,
        Err((
            Code::FailedPrecondition,
            format!(
                "firewall rules aren't enabled for node {a}: no enrollment predicate is approved"
            )
        ))
    );
    assert_eq!(rows().await?, before);

    // A's acceptance and its replay are A's alone.
    assert_eq!(
        report(client, &token(0), own.clone()).await?,
        Ok(Outcome::Accepted)
    );
    assert_eq!(
        report(client, &token(0), own).await?,
        Ok(Outcome::Duplicate)
    );
    assert_eq!(row(pool, b).await?, before.1);
    Ok(())
}

/// Each malformation of an otherwise acceptable report, whose integers are
/// all 0, is INVALID_ARGUMENT and writes nothing, and the report itself is
/// accepted: 0 is a value, and an unset field never reads as 0. Catches a
/// decoder that defaults an unset field or enum, accepts an unknown enum
/// value, a duplicate slot, a reason or gap slot that doesn't fit, an
/// incomplete census without gaps, an out-of-range integer, or an oversized
/// report, and a bare census standing in for evidence.
#[tokio::test(flavor = "current_thread")]
async fn malformed_reports_write_nothing() -> TestResult {
    let Some(mut r) = reporter(2).await? else {
        return Ok(());
    };
    let (pool, client) = (&r.pool, &mut r.harness.client);
    ask(pool, r.node, (0, r.revision), 0, 0, Duration::ZERO).await?;
    let mut base = answer(r.node, 0, (0, r.revision), 0);
    base.census = Census::Incomplete.into();
    base.census_gaps = vec![gap(Gap::Missing, Some(0)), gap(Gap::Extra, None)];
    base.slots = vec![
        slot(0, 0, Slot::Verified, Reason::Unspecified),
        slot(1, 0, Slot::Failed, Reason::RulesMismatch),
    ];
    let before = row(pool, r.node).await?;
    let too_large = u64::try_from(i64::MAX)? + 1;
    type Malform = fn(&mut ReportFirewallRulesRequest, u64);
    let malformations: [(&str, Malform); 29] = [
        ("node_id unset", |q, _| q.node_id.clear()),
        ("round unset", |q, _| q.round = None),
        ("generation unset", |q, _| q.assignment_generation = None),
        ("epoch unset", |q, _| q.manager_epoch = None),
        ("revision not a UUID", |q, _| q.revision_id = "ssh".into()),
        ("bare census", |q, _| {
            q.census = 0;
            q.census_gaps.clear();
            q.slots.clear();
        }),
        ("unknown census", |q, _| {
            q.census = 3;
            q.census_gaps.clear();
        }),
        ("complete census with gaps", |q, _| {
            q.census = Census::Complete.into();
        }),
        ("incomplete census without", |q, _| q.census_gaps.clear()),
        ("gap kind unset", |q, _| q.census_gaps[1].kind = 0),
        ("unknown gap kind", |q, _| q.census_gaps[1].kind = 7),
        ("slot gap without its slot", |q, _| {
            q.census_gaps[0].slot_id = None
        }),
        ("slot on a gap without one", |q, _| {
            q.census_gaps[1].slot_id = Some(0);
        }),
        ("too many gaps", |q, _| {
            q.census_gaps = vec![gap(Gap::Extra, None); 32_769];
        }),
        ("slot_id unset", |q, _| q.slots[0].slot_id = None),
        ("slot generation unset", |q, _| {
            q.slots[0].slot_generation = None;
        }),
        ("slot outcome unset", |q, _| q.slots[0].outcome = 0),
        ("unknown slot outcome", |q, _| q.slots[0].outcome = 5),
        ("verified with a reason", |q, _| {
            q.slots[0].reason = Reason::RulesMismatch.into();
        }),
        ("failed without one", |q, _| q.slots[1].reason = 0),
        ("failed with an unknown's reason", |q, _| {
            q.slots[1].reason = Reason::TableUnreadable.into();
        }),
        ("unknown reason", |q, _| q.slots[1].reason = 7),
        ("duplicate slot", |q, _| q.slots[1].slot_id = Some(0)),
        ("round out of range", |q, n| q.round = Some(n)),
        ("generation out of range", |q, n| {
            q.assignment_generation = Some(n);
        }),
        ("epoch out of range", |q, n| q.manager_epoch = Some(n)),
        ("slot generation out of range", |q, n| {
            q.slots[0].slot_generation = Some(n);
        }),
        ("too many slots", |q, _| {
            q.slots = (0..16_385)
                .map(|id| slot(id, 0, Slot::Verified, Reason::Unspecified))
                .collect();
        }),
        ("node_id not a node", |q, _| q.node_id = "worker-a".into()),
    ];
    for (label, malform) in malformations {
        let mut request = base.clone();
        malform(&mut request, too_large);
        let outcome = report(client, &r.token, request).await?;
        assert_eq!(
            outcome.map_err(|(code, _)| code),
            Err(Code::InvalidArgument),
            "{label}"
        );
        assert_eq!(row(pool, r.node).await?, before, "{label}");
    }
    assert_eq!(report(client, &r.token, base).await?, Ok(Outcome::Accepted));
    Ok(())
}
