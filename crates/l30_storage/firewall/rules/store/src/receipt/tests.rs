use std::time::Duration;

use pplx_space_db_test_fixtures::{TestDb, try_pg_test_db, try_pg_test_db_with_max_connections};
use pplx_space_sql_locator::sql;
use pplx_space_test_env::TestResult;
use serde_json::json;
use sqlx::PgPool;

use super::*;
use crate::MutationRequest;

const ACTOR: &str = "operator:test";
const KEY: &str = "pause-1";
const REASON: &str = "hold";

fn fingerprint(path: &str, body: &[u8], if_match: Option<&str>) -> RequestFingerprint {
    MutationRequest {
        method: "POST",
        path,
        body,
        if_match,
    }
    .fingerprint()
}

fn pause_path(rollout: Uuid) -> String {
    format!("/admin/firewall-rules/rollouts/{rollout}/pause")
}

fn pause_body(reason: &str) -> Vec<u8> {
    json!({ "reason": reason }).to_string().into_bytes()
}

/// `If-Match: generation` on a pause of `rollout` for `reason`.
fn pause_fingerprint(rollout: Uuid, generation: i64, reason: &str) -> RequestFingerprint {
    let generation = generation.to_string();
    fingerprint(&pause_path(rollout), &pause_body(reason), Some(&generation))
}

/// A pause as its route will run one, by `ACTOR` under `KEY`: claim first,
/// and only on `New` check the generation, pause and record the result.
/// Leaves `tx` open.
async fn pause_in(
    tx: &mut Transaction<'_, Postgres>,
    rollout: Uuid,
    generation: i64,
    reason: &str,
) -> TestResult<Claim> {
    let fingerprint = pause_fingerprint(rollout, generation, reason);
    let request = ReceiptRequest {
        actor: ACTOR,
        idempotency_key: KEY,
        fingerprint: &fingerprint,
    };
    let claim = claim_receipt(tx, &request).await?;
    if claim != Claim::New {
        return Ok(claim);
    }
    let paused = sqlx::query_scalar::<_, i64>(sql!(
        "UPDATE firewall_rules_rollouts SET phase = 'paused', reason = $3
         WHERE id = $1 AND generation = $2
         RETURNING generation"
    ))
    .bind(rollout)
    .bind(generation)
    .bind(reason)
    .fetch_optional(&mut **tx)
    .await?
    .ok_or("the rollout isn't at the If-Match generation")?;
    let result =
        json!({ "rollout_id": rollout.to_string(), "phase": "paused", "generation": paused });
    insert_receipt(tx, &request, Uuid::now_v7(), &result).await?;
    Ok(Claim::New)
}

/// Start a rollout as its creation will: claim, allocate the IDs, insert the
/// receipt, then the header that references it. Leaves `tx` open.
async fn create_in(
    tx: &mut Transaction<'_, Postgres>,
    key: &str,
    revision: Uuid,
) -> TestResult<Uuid> {
    let fingerprint = fingerprint(
        "/admin/firewall-rules/rollouts",
        revision.to_string().as_bytes(),
        None,
    );
    let request = ReceiptRequest {
        actor: ACTOR,
        idempotency_key: key,
        fingerprint: &fingerprint,
    };
    assert_eq!(claim_receipt(tx, &request).await?, Claim::New);
    let (receipt, rollout) = (Uuid::now_v7(), Uuid::now_v7());
    insert_receipt(
        tx,
        &request,
        receipt,
        &json!({ "rollout_id": rollout.to_string() }),
    )
    .await?;
    sqlx::query(sql!(
        "INSERT INTO firewall_rules_rollouts
             (id, to_revision_id, created_by_receipt_id, phase, unfinished_slot, reason)
         VALUES ($1, $2, $3, 'rolling', 1, 'test')"
    ))
    .bind(rollout)
    .bind(revision)
    .bind(receipt)
    .execute(&mut **tx)
    .await?;
    Ok(rollout)
}

async fn insert_revision(pool: &PgPool) -> TestResult<Uuid> {
    let revision = Uuid::now_v7();
    sqlx::query(sql!(
        "INSERT INTO firewall_rules_revisions (id, ruleset_format_version, ruleset)
         VALUES ($1, 1, '[]'::jsonb)"
    ))
    .bind(revision)
    .execute(pool)
    .await?;
    Ok(revision)
}

/// A committed rolling rollout at generation 0.
async fn start(pool: &PgPool) -> TestResult<Uuid> {
    let revision = insert_revision(pool).await?;
    let mut tx = pool.begin().await?;
    let rollout = create_in(&mut tx, "create-1", revision).await?;
    tx.commit().await?;
    Ok(rollout)
}

/// Commit a pause of `rollout` from generation 0 for `REASON`.
async fn pause(pool: &PgPool, rollout: Uuid) -> TestResult<Claim> {
    let mut tx = pool.begin().await?;
    let claim = pause_in(&mut tx, rollout, 0, REASON).await?;
    tx.commit().await?;
    Ok(claim)
}

/// `KEY`'s receipts: actor, ID, fingerprint and result.
async fn receipts(pool: &PgPool) -> TestResult<Vec<(String, Uuid, String, serde_json::Value)>> {
    let rows = sqlx::query_as::<_, (String, Uuid, String, Json<serde_json::Value>)>(sql!(
        "SELECT actor, id, request_fingerprint, result
         FROM firewall_rules_mutation_receipts
         WHERE idempotency_key = $1
         ORDER BY actor"
    ))
    .bind(KEY)
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(|(actor, id, fingerprint, Json(result))| (actor, id, fingerprint, result))
        .collect())
}

/// The rollout's phase, reason and generation.
async fn rollout_state(pool: &PgPool, rollout: Uuid) -> TestResult<(String, String, i64)> {
    Ok(sqlx::query_as::<_, (String, String, i64)>(sql!(
        "SELECT phase, reason, generation FROM firewall_rules_rollouts WHERE id = $1"
    ))
    .bind(rollout)
    .fetch_one(pool)
    .await?)
}

fn replay_of(receipt: &(String, Uuid, String, serde_json::Value)) -> Claim {
    Claim::Replay {
        receipt_id: receipt.1,
        result: receipt.3.clone(),
    }
}

/// Wait, for at most ten seconds, until a session on this test database is
/// waiting for an advisory lock: the point at which a racing claim is known
/// to be blocked on the receipt key rather than not started yet.
async fn wait_for_receipt_lock_waiter(pool: &PgPool) -> TestResult {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let waiting = sqlx::query_scalar::<_, bool>(sql!(
            "SELECT EXISTS (
                 SELECT 1 FROM pg_stat_activity
                 WHERE datname = current_database()
                   AND wait_event_type = 'Lock'
                   AND wait_event = 'advisory')"
        ))
        .fetch_one(pool)
        .await?;
        if waiting {
            return Ok(());
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "no claim started waiting for the receipt key"
        );
    }
}

/// Pause `rollout` from generation 0 and hold the transaction open, start a
/// second pause from generation 0 for `second_reason` on another connection,
/// wait until it blocks on the receipt key, then end the first. Returns the
/// second's claim, after it commits.
async fn race(
    db: &TestDb,
    rollout: Uuid,
    second_reason: &'static str,
    first_commits: bool,
) -> TestResult<Claim> {
    let mut first = db.pool.begin().await?;
    assert_eq!(pause_in(&mut first, rollout, 0, REASON).await?, Claim::New);

    let pool = db.pool.clone();
    let second = tokio::spawn(async move {
        let mut tx = pool.begin().await?;
        let claim = pause_in(&mut tx, rollout, 0, second_reason).await?;
        tx.commit().await?;
        TestResult::Ok(claim)
    });
    wait_for_receipt_lock_waiter(&db.pool).await?;
    if first_commits {
        first.commit().await?;
    } else {
        first.rollback().await?;
    }
    second.await?
}

/// Catches a retry whose response was lost running the mutation again
/// instead of returning the first result.
#[tokio::test(flavor = "current_thread")]
async fn a_retry_after_a_lost_response_replays_the_stored_result() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let rollout = start(&db.pool).await?;
    assert_eq!(pause(&db.pool, rollout).await?, Claim::New);
    let stored = receipts(&db.pool).await?;

    assert_eq!(pause(&db.pool, rollout).await?, replay_of(&stored[0]));
    assert_eq!(
        stored[0].3,
        json!({ "rollout_id": rollout.to_string(), "phase": "paused", "generation": 1 })
    );
    assert_eq!(receipts(&db.pool).await?, stored);
    assert_eq!(
        rollout_state(&db.pool, rollout).await?,
        ("paused".to_owned(), REASON.to_owned(), 1)
    );
    Ok(())
}

/// Catches a reused key that replays, or overwrites, the receipt of a
/// different request, and one actor's key blocking another's.
#[tokio::test(flavor = "current_thread")]
async fn a_reused_key_for_a_different_request_conflicts_and_actors_are_independent() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let rollout = start(&db.pool).await?;
    assert_eq!(pause(&db.pool, rollout).await?, Claim::New);
    let stored = receipts(&db.pool).await?;

    let resume = format!("/admin/firewall-rules/rollouts/{rollout}/resume");
    let changed = [
        ("body", pause_fingerprint(rollout, 0, "other")),
        (
            "route",
            fingerprint(&resume, &pause_body(REASON), Some("0")),
        ),
        ("resource", pause_fingerprint(Uuid::now_v7(), 0, REASON)),
        ("precondition", pause_fingerprint(rollout, 1, REASON)),
        (
            "absent precondition",
            fingerprint(&pause_path(rollout), &pause_body(REASON), None),
        ),
    ];
    for (change, fingerprint) in &changed {
        let mut tx = db.pool.begin().await?;
        let request = ReceiptRequest {
            actor: ACTOR,
            idempotency_key: KEY,
            fingerprint,
        };
        assert_eq!(
            claim_receipt(&mut tx, &request).await?,
            Claim::Conflict,
            "{change}"
        );
        tx.rollback().await?;
    }
    assert_eq!(receipts(&db.pool).await?, stored);

    let fingerprint = pause_fingerprint(rollout, 0, REASON);
    let other = ReceiptRequest {
        actor: "operator:other",
        idempotency_key: KEY,
        fingerprint: &fingerprint,
    };
    let mut tx = db.pool.begin().await?;
    assert_eq!(claim_receipt(&mut tx, &other).await?, Claim::New);
    insert_receipt(&mut tx, &other, Uuid::now_v7(), &json!({})).await?;
    tx.commit().await?;
    assert_eq!(receipts(&db.pool).await?.len(), 2);
    Ok(())
}

/// Catches a replay rebuilt from the rollout's current state, or a
/// precondition checked before the receipt, which would refuse an exact
/// retry once the generation moves on.
#[tokio::test(flavor = "current_thread")]
async fn a_retry_after_the_rollout_moves_on_replays_and_a_new_if_match_conflicts() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let rollout = start(&db.pool).await?;
    assert_eq!(pause(&db.pool, rollout).await?, Claim::New);
    let stored = receipts(&db.pool).await?;
    sqlx::query(sql!(
        "UPDATE firewall_rules_rollouts SET phase = 'rolling' WHERE id = $1"
    ))
    .bind(rollout)
    .execute(&db.pool)
    .await?;
    let moved_on = ("rolling".to_owned(), REASON.to_owned(), 2);
    assert_eq!(rollout_state(&db.pool, rollout).await?, moved_on);

    assert_eq!(pause(&db.pool, rollout).await?, replay_of(&stored[0]));
    let mut tx = db.pool.begin().await?;
    assert_eq!(
        pause_in(&mut tx, rollout, 2, REASON).await?,
        Claim::Conflict
    );
    tx.commit().await?;
    assert_eq!(receipts(&db.pool).await?, stored);
    assert_eq!(rollout_state(&db.pool, rollout).await?, moved_on);
    Ok(())
}

/// Catches an unlocked read, which lets two identical requests both see no
/// receipt and both mutate.
#[tokio::test(flavor = "current_thread")]
async fn a_waiter_replays_what_the_winner_committed() -> TestResult {
    let Some(db) = try_pg_test_db_with_max_connections(3).await? else {
        return Ok(());
    };
    let rollout = start(&db.pool).await?;

    let second = race(&db, rollout, REASON, true).await?;
    let stored = receipts(&db.pool).await?;
    assert_eq!(stored.len(), 1);
    assert_eq!(second, replay_of(&stored[0]));
    assert_eq!(
        rollout_state(&db.pool, rollout).await?,
        ("paused".to_owned(), REASON.to_owned(), 1)
    );
    Ok(())
}

/// Catches an unlocked read, which lets a different request on the same key
/// see no receipt and mutate.
#[tokio::test(flavor = "current_thread")]
async fn a_waiter_with_a_different_request_conflicts_with_the_winner() -> TestResult {
    let Some(db) = try_pg_test_db_with_max_connections(3).await? else {
        return Ok(());
    };
    let rollout = start(&db.pool).await?;

    assert_eq!(race(&db, rollout, "other", true).await?, Claim::Conflict);
    assert_eq!(receipts(&db.pool).await?.len(), 1);
    assert_eq!(
        rollout_state(&db.pool, rollout).await?,
        ("paused".to_owned(), REASON.to_owned(), 1)
    );
    Ok(())
}

/// Catches a key reserved outside the mutation's transaction, which a
/// rolled-back winner would leave behind with no mutation.
#[tokio::test(flavor = "current_thread")]
async fn a_waiter_proceeds_as_new_after_the_winner_rolls_back() -> TestResult {
    let Some(db) = try_pg_test_db_with_max_connections(3).await? else {
        return Ok(());
    };
    let rollout = start(&db.pool).await?;

    assert_eq!(race(&db, rollout, "second", false).await?, Claim::New);
    let stored = receipts(&db.pool).await?;
    assert_eq!(stored.len(), 1);
    assert_eq!(
        stored[0].2,
        pause_fingerprint(rollout, 0, "second").as_str()
    );
    assert_eq!(
        rollout_state(&db.pool, rollout).await?,
        ("paused".to_owned(), "second".to_owned(), 1)
    );
    Ok(())
}

/// Catches a creation receipt that can't precede its rollout in one
/// transaction, or that commits when the rollout's insert fails.
#[tokio::test(flavor = "current_thread")]
async fn a_creation_receipt_commits_and_rolls_back_with_its_rollout() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let revision = insert_revision(&db.pool).await?;
    let mut tx = db.pool.begin().await?;
    let created = create_in(&mut tx, "create-1", revision).await?;
    tx.commit().await?;

    // The first rollout is unfinished, so a second's header is refused after
    // its receipt is inserted.
    let mut tx = db.pool.begin().await?;
    let refused = create_in(&mut tx, "create-2", revision)
        .await
        .expect_err("a second unfinished rollout");
    let refused = refused.downcast_ref::<sqlx::Error>().and_then(|error| {
        error
            .as_database_error()
            .and_then(|error| error.constraint().map(str::to_owned))
    });
    assert_eq!(
        refused.as_deref(),
        Some("firewall_rules_rollouts_unfinished_slot_key")
    );
    tx.rollback().await?;

    let pairs = sqlx::query_as::<_, (String, Option<Uuid>)>(sql!(
        "SELECT receipt.idempotency_key, rollout.id
         FROM firewall_rules_mutation_receipts AS receipt
         LEFT JOIN firewall_rules_rollouts AS rollout
             ON rollout.created_by_receipt_id = receipt.id"
    ))
    .fetch_all(&db.pool)
    .await?;
    assert_eq!(pairs, [("create-1".to_owned(), Some(created))]);
    Ok(())
}
