//! The canary's first-assignment claim and its issue-and-release. The
//! controller's tick tests cover the positive path through both.

use super::*;
use crate::test_support::make_checks_due;

async fn claim(pool: &PgPool) -> TestResult<Option<LeasedTarget>> {
    let mut conn = pool.acquire().await?;
    Ok(lease_first_canary(&mut conn, LEASE, SCAN).await?)
}

async fn issue_canary(
    pool: &PgPool,
    target: &LeasedTarget,
    mut queue: Queue,
) -> Result<Issuance, sqlx::Error> {
    let mut conn = pool.acquire().await?;
    issue_canary_and_release(&mut conn, target, FIRST_CHECK_IN, &mut queue).await
}

/// `node`'s item's phase, lease token, lease expiry and attempts.
async fn lease_of(
    pool: &PgPool,
    rollout: RolloutId,
    node: NodeId,
) -> TestResult<(String, Option<Uuid>, Option<OffsetDateTime>, i64)> {
    Ok(sqlx::query_as(sql!(
        "SELECT phase, lease_token, lease_expires_at, attempts
         FROM firewall_rules_rollout_items WHERE rollout_id = $1 AND node_id = $2"
    ))
    .bind(rollout)
    .bind(node)
    .fetch_one(pool)
    .await?)
}

/// Catches a claim, or a lapsed claim taken back, in a rollout that isn't an
/// unfinished canary, which would issue outside canary; and a canary phase
/// read only before the header lock, or only at the claim, which would
/// claim, or issue, after an operator moved the rollout on.
#[tokio::test(flavor = "current_thread")]
async fn only_a_canary_is_claimed_and_issued() -> TestResult {
    let Some(db) = try_pg_test_db_with_max_connections(4).await? else {
        return Ok(());
    };
    let nodes = online_nodes(&db.pool, 2).await?;
    assert_eq!(claim(&db.pool).await?, None, "no rollout");
    for phase in ["paused", "rolling", "completed", "aborted"] {
        let rollout = rollout(&db.pool, phase).await?;
        save_targets(&db.pool, rollout, &nodes).await?;
        held(&db.pool, rollout, nodes[1]).await?;
        lapse(&db.pool, rollout, nodes[1]).await?;
        let lapsed = lease_of(&db.pool, rollout, nodes[1]).await?;
        assert_eq!(claim(&db.pool).await?, None, "{phase}");
        assert_eq!(item(&db.pool, rollout, nodes[0]).await?.0, 0, "{phase}");
        assert_eq!(
            lease_of(&db.pool, rollout, nodes[1]).await?,
            lapsed,
            "{phase}"
        );
        set_phase(&db.pool, rollout, "aborted").await?;
    }

    let rollout = rollout(&db.pool, "canary").await?;
    save_targets(&db.pool, rollout, &nodes).await?;
    let mut rolling = hold_header(&db.pool, rollout).await?;
    let pool = db.pool.clone();
    let claiming = tokio::spawn(async move { claim(&pool).await });
    wait_for_lock_waiters(&db.pool, 1).await?;
    set_phase(&mut *rolling, rollout, "rolling").await?;
    rolling.commit().await?;
    let raced = tokio::time::timeout(Duration::from_secs(10), claiming).await;
    assert_eq!(raced???, None);
    assert_eq!(item(&db.pool, rollout, nodes[0]).await?.0, 0);

    set_phase(&db.pool, rollout, "canary").await?;
    let target = claim(&db.pool).await?.expect("a canary target");
    let claimed = lease_of(&db.pool, rollout, target.node_id).await?;
    set_phase(&db.pool, rollout, "rolling").await?;
    assert_eq!(
        issue_canary(&db.pool, &target, Queue::Wake).await?,
        Issuance::Refused(Refusal::NotIssuing)
    );
    assert_eq!(
        effects(&db.pool, rollout, target.node_id).await?,
        Effects::NONE
    );
    assert_eq!(lease_of(&db.pool, rollout, target.node_id).await?, claimed);
    Ok(())
}

/// Catches a dead claimant's unissued claim that holds the canary forever;
/// a stale claimant that issues under, or changes, its successor's lease; and a
/// lapsed lease on an issued item taken back as if its issuance were
/// abandoned, when it may be a check's.
#[tokio::test(flavor = "current_thread")]
async fn only_an_unissued_lapsed_claim_is_taken_back() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let rollout = rollout(&db.pool, "canary").await?;
    let nodes = online_nodes(&db.pool, 2).await?;
    save_targets(&db.pool, rollout, &nodes).await?;
    let stale = claim(&db.pool).await?.expect("a canary target");
    assert_eq!(claim(&db.pool).await?, None, "a second claim in flight");

    lapse(&db.pool, rollout, stale.node_id).await?;
    let successor = claim(&db.pool).await?.expect("the lapsed claim taken back");
    assert_eq!((successor.node_id, successor.attempts), (stale.node_id, 2));
    let held = lease_of(&db.pool, rollout, successor.node_id).await?;
    assert_eq!(held.1, Some(successor.lease_token));
    assert_eq!(
        issue_canary(&db.pool, &stale, Queue::Wake).await?,
        Issuance::Refused(Refusal::LeaseLost)
    );
    assert_eq!(
        effects(&db.pool, rollout, stale.node_id).await?,
        Effects::NONE
    );
    assert_eq!(lease_of(&db.pool, rollout, successor.node_id).await?, held);

    assert!(matches!(
        issue_canary(&db.pool, &successor, Queue::Wake).await?,
        Issuance::Issued(_)
    ));
    make_checks_due(&db.pool, rollout).await?;
    let check = lease(&db.pool, rollout, 1).await?.remove(0);
    lapse(&db.pool, rollout, check.node_id).await?;
    let lapsed = lease_of(&db.pool, rollout, check.node_id).await?;
    assert_eq!(claim(&db.pool).await?, None);
    assert_eq!(lease_of(&db.pool, rollout, check.node_id).await?, lapsed);
    Ok(())
}

/// Catches a release that fails or matches nothing yet commits, leaving an
/// assignment, a trigger change or a stamp behind with the claim still in
/// flight, or a claim changed by the failed attempt.
#[tokio::test(flavor = "current_thread")]
async fn a_failed_release_rolls_back_the_whole_issuance() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let rollout = rollout(&db.pool, "canary").await?;
    let nodes = online_nodes(&db.pool, 1).await?;
    let node = nodes[0];
    save_targets(&db.pool, rollout, &nodes).await?;
    enqueue_triggers(&db.pool, [node], ReconcileReason::NodeUpdated).await?;
    let target = claim(&db.pool).await?.expect("a canary target");
    let before = effects(&db.pool, rollout, node).await?;
    let claimed = lease_of(&db.pool, rollout, node).await?;
    assert_eq!(claimed.3, 1);

    for queue in [Queue::ThenStealsLease, Queue::ThenBlocksRelease] {
        let outcome = issue_canary(&db.pool, &target, queue).await;
        assert!(outcome.is_err(), "{queue:?}: {outcome:?}");
        assert_eq!(effects(&db.pool, rollout, node).await?, before, "{queue:?}");
        assert_eq!(
            lease_of(&db.pool, rollout, node).await?,
            claimed,
            "{queue:?}"
        );
    }
    Ok(())
}

/// Catches a replay after a lost reply that issues again, releases, or
/// otherwise writes once a check holds the item: the stamp is recognized
/// before the lease.
#[tokio::test(flavor = "current_thread")]
async fn a_replay_after_a_lost_reply_leaves_the_check_lease_alone() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let rollout = rollout(&db.pool, "canary").await?;
    let nodes = online_nodes(&db.pool, 1).await?;
    let node = nodes[0];
    save_targets(&db.pool, rollout, &nodes).await?;
    let target = claim(&db.pool).await?.expect("a canary target");
    let Issuance::Issued(stamp) = issue_canary(&db.pool, &target, Queue::Wake).await? else {
        panic!("the canary wasn't issued");
    };
    make_checks_due(&db.pool, rollout).await?;
    let check = lease(&db.pool, rollout, 1).await?.remove(0);
    assert!(matches!(check.work, Work::Check { .. }));
    let before = (
        effects(&db.pool, rollout, node).await?,
        lease_of(&db.pool, rollout, node).await?,
    );

    // The stamp as issued, with the check time moved above.
    let Issuance::AlreadyIssued(replayed) = issue_canary(&db.pool, &target, Queue::Wake).await?
    else {
        panic!("the replay wasn't recognized as issued");
    };
    assert_eq!(
        (replayed.assignment_generation, replayed.issued_at),
        (stamp.assignment_generation, stamp.issued_at)
    );
    let after = (
        effects(&db.pool, rollout, node).await?,
        lease_of(&db.pool, rollout, node).await?,
    );
    assert_eq!(after, before);
    assert_eq!(after.1.1, Some(check.lease_token));
    Ok(())
}
