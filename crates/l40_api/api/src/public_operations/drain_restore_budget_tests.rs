//! The claim's cap on drain restores ([`ClaimLimits::drain_restores`]), how
//! it meets the organization limit and the claim's window, the keyed queue
//! behind a drain restore, and the cap's exemption for a drain restore past
//! [`DRAIN_RESTORE_WINDOW`].

use std::time::Duration;

use pplx_space_sandbox_model::SandboxId;

use pplx_space_accounts::OrgId;
use pplx_space_db_test_fixtures::pg_test_db;
use pplx_space_test_env::TestResult;

use super::*;

const ORG: OrgId = OrgId::from_uuid(Uuid::from_u128(1));

/// Queue an operation of `kind` for a sandbox of its own.
async fn queue(pool: &PgPool, kind: StoredOperationKind) -> TestResult<OperationId> {
    queue_for(pool, ORG, kind).await
}

/// Queue an operation of `kind` for a sandbox of its own, of `org`.
async fn queue_for(
    pool: &PgPool,
    org: OrgId,
    kind: StoredOperationKind,
) -> TestResult<OperationId> {
    let target_id = pplx_space_public_api::PublicSandboxId::from(SandboxId::new()).to_string();
    queue_on(pool, org, kind, &target_id).await
}

/// Queue an operation of `kind` on sandbox `target_id` of `org`.
async fn queue_on(
    pool: &PgPool,
    org: OrgId,
    kind: StoredOperationKind,
    target_id: &str,
) -> TestResult<OperationId> {
    let mut tx = pool.begin().await?;
    let (created, _) = create_operation_tx(
        &mut tx,
        CreatePublicOperation {
            org_id: org,
            actor_user_id: None,
            api_product: ApiProduct::Native,
            kind,
            target_type: Some(OperationTargetType::Sandbox),
            target_id: Some(target_id),
            payload_json: serde_json::json!({}),
            idempotency: None,
            traceparent: None,
        },
    )
    .await?;
    tx.commit().await?;
    let CreatePublicOperationResult::Created(operation) = created else {
        return Err(format!("not created: {created:?}").into());
    };
    Ok(operation)
}

/// One dispatcher's claim: up to `total` operations of `kinds`, and up to
/// `drain_restores` drain restores running at once.
async fn claim(
    pool: &PgPool,
    kinds: &[OperationKind],
    total: u32,
    drain_restores: u32,
) -> TestResult<Vec<ClaimedPublicOperation>> {
    Ok(PublicOperationStore::new(pool.clone())
        .claim_operations(ClaimPublicOperations {
            limits: ClaimLimits {
                drain_restores,
                ..ClaimLimits::any(total)
            },
            claim_stale_after: Duration::from_secs(60),
            claimable_kinds: kinds,
        })
        .await?
        .operations)
}

fn drain_restores(claimed: &[ClaimedPublicOperation]) -> usize {
    claimed
        .iter()
        .filter(|operation| operation.kind == StoredOperationKind::DrainRestoreSandbox)
        .count()
}

/// The cap counts drain restores running under any claim: claims that each
/// have slots to spare take no more than the cap between them, and one
/// ending frees one.
#[tokio::test(flavor = "current_thread")]
async fn the_budget_caps_running_drain_restores_across_claims() -> TestResult {
    let db = pg_test_db().await?;
    let pool = &db.pool;
    for _ in 0..6 {
        queue(pool, StoredOperationKind::DrainRestoreSandbox).await?;
    }

    let first = claim(pool, &[], 3, 4).await?;
    let second = claim(pool, &[], 16, 4).await?;
    assert_eq!(
        (drain_restores(&first), drain_restores(&second)),
        (3, 1),
        "the second claim takes what the cap leaves"
    );
    assert!(claim(pool, &[], 16, 4).await?.is_empty(), "the cap is full");

    sqlx::query(sql!(
        "UPDATE public_operations SET state = 'succeeded', claim_token = ''
          WHERE operation_id = $1"
    ))
    .bind(first[0].id.as_uuid())
    .execute(pool)
    .await?;
    assert_eq!(drain_restores(&claim(pool, &[], 16, 4).await?), 1);
    Ok(())
}

/// With the cap full, a user's restores are claimed as before; with no
/// budget at all, a drain restore is never claimed and the rest is.
#[tokio::test(flavor = "current_thread")]
async fn the_budget_leaves_user_work_alone() -> TestResult {
    let db = pg_test_db().await?;
    let pool = &db.pool;
    for _ in 0..5 {
        queue(pool, StoredOperationKind::DrainRestoreSandbox).await?;
    }
    for _ in 0..3 {
        queue(pool, OperationKind::RestoreSandbox.into()).await?;
    }
    let restores = [OperationKind::RestoreSandbox];

    let unbudgeted = claim(pool, &restores, 16, 0).await?;
    assert_eq!(
        (unbudgeted.len(), drain_restores(&unbudgeted)),
        (3, 0),
        "no budget claims every user restore and no drain restore"
    );
    sqlx::query(sql!(
        "UPDATE public_operations SET state = 'queued', claim_token = '' WHERE kind = $1"
    ))
    .bind(OperationKind::RestoreSandbox.as_str())
    .execute(pool)
    .await?;

    let capped = claim(pool, &[], 16, 4).await?;
    assert_eq!(drain_restores(&capped), 4);
    let users = claim(pool, &restores, 16, 4).await?;
    assert_eq!(
        (users.len(), drain_restores(&users)),
        (3, 0),
        "a full cap holds back only drain restores"
    );
    Ok(())
}

/// A claim with no drain-restore budget, as a dispatcher that runs none
/// makes, ignores drain restores entirely, as a dispatcher at the base does:
/// one due at the head of a sandbox's queue holds back nothing behind it.
#[tokio::test(flavor = "current_thread")]
async fn without_a_budget_a_drain_restore_holds_nothing_back() -> TestResult {
    let db = pg_test_db().await?;
    let pool = &db.pool;
    let target_id = pplx_space_public_api::PublicSandboxId::from(SandboxId::new()).to_string();
    for kind in [
        StoredOperationKind::DrainRestoreSandbox,
        OperationKind::UpdateSandbox.into(),
    ] {
        let mut tx = pool.begin().await?;
        let (created, _) = create_operation_tx(
            &mut tx,
            CreatePublicOperation {
                org_id: ORG,
                actor_user_id: None,
                api_product: ApiProduct::Native,
                kind,
                target_type: Some(OperationTargetType::Sandbox),
                target_id: Some(&target_id),
                payload_json: serde_json::json!({}),
                idempotency: None,
                traceparent: None,
            },
        )
        .await?;
        tx.commit().await?;
        assert!(matches!(created, CreatePublicOperationResult::Created(_)));
    }

    let claimed = claim(pool, &[OperationKind::UpdateSandbox], 16, 0).await?;
    assert_eq!(
        claimed
            .iter()
            .map(|operation| operation.kind)
            .collect::<Vec<_>>(),
        [StoredOperationKind::Public(OperationKind::UpdateSandbox)],
        "the update behind the drain restore is claimed"
    );
    Ok(())
}

/// A second organization.
async fn org(pool: &PgPool) -> TestResult<OrgId> {
    let id = OrgId::new();
    sqlx::query(sql!(
        "INSERT INTO organizations (id, name, slug) VALUES ($1, $2, $2)"
    ))
    .bind(id)
    .bind(format!("drain-budget-{id}"))
    .execute(pool)
    .await?;
    Ok(id)
}

/// One dispatcher's claim of up to `total` operations of `kinds`, each
/// organization limited to `per_org` other than teardowns while another
/// waits, with `headroom`, and up to `drain_restores` drain restores
/// running at once. Returns the claimed ids.
async fn claim_limited(
    pool: &PgPool,
    kinds: &[OperationKind],
    total: u32,
    per_org: u32,
    headroom: u32,
    drain_restores: u32,
) -> TestResult<Vec<OperationId>> {
    Ok(PublicOperationStore::new(pool.clone())
        .claim_operations(ClaimPublicOperations {
            limits: ClaimLimits {
                per_org_non_teardown: Some(OrgSlotLimit {
                    contended: std::num::NonZeroU32::new(per_org).expect("a limit"),
                    headroom,
                }),
                drain_restores,
                ..ClaimLimits::any(total)
            },
            claim_stale_after: Duration::from_secs(60),
            claimable_kinds: kinds,
        })
        .await?
        .operations
        .into_iter()
        .map(|operation| operation.id)
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect())
}

/// Drain restores the cap holds back take no place in front of their
/// organization's other work: not an organization slot, and not a place in
/// the organization's window, which the claim reads only `2 * non_teardown`
/// of. Each organization's user restore queued behind a backlog of them is
/// claimed beside the drain restores the cap admits.
#[tokio::test(flavor = "current_thread")]
async fn drain_restores_the_cap_holds_back_hide_no_work_behind_them() -> TestResult {
    let db = pg_test_db().await?;
    let pool = &db.pool;
    let restores = [OperationKind::RestoreSandbox];
    // (drain restores ahead, claim total, per-organization limit, cap): an
    // organization slot each would hold the restore past the limit; a place
    // each in the window would leave it outside the window.
    for (backlog, total, per_org, cap) in [(10, 16, 6, 4), (9, 4, 16, 1)] {
        let busy = org(pool).await?;
        let other = org(pool).await?;
        let mut drains = Vec::new();
        for _ in 0..backlog {
            drains.push(queue_for(pool, busy, StoredOperationKind::DrainRestoreSandbox).await?);
        }
        let behind = queue_for(pool, busy, OperationKind::RestoreSandbox.into()).await?;
        let others = queue_for(pool, other, OperationKind::RestoreSandbox.into()).await?;

        let claimed = claim_limited(pool, &restores, total, per_org, 0, cap).await?;

        let mut expected = drains[..cap as usize].to_vec();
        expected.extend([behind, others]);
        expected.sort();
        assert_eq!(claimed, expected, "backlog {backlog}, total {total}");
        sqlx::query(sql!(
            "UPDATE public_operations SET state = 'succeeded', claim_token = ''
              WHERE org_id = ANY($1)"
        ))
        .bind([busy, other])
        .execute(pool)
        .await?;
    }
    Ok(())
}

/// The cap counts only drain restores the organization limit admits. An
/// organization at its limit whose drain restores are due first takes none
/// of the cap, so the next organization's take it; and an organization whose
/// only due work is drain restores the full cap holds back isn't waiting, so
/// it doesn't hold another organization to its limit.
#[tokio::test(flavor = "current_thread")]
async fn the_cap_counts_only_drain_restores_the_organization_limit_admits() -> TestResult {
    let db = pg_test_db().await?;
    let pool = &db.pool;
    let restores = [OperationKind::RestoreSandbox];

    // An organization at its limit of 2 owns the first drain restores.
    let full = org(pool).await?;
    let next = org(pool).await?;
    for _ in 0..2 {
        queue_for(pool, full, OperationKind::RestoreSandbox.into()).await?;
    }
    assert_eq!(claim_limited(pool, &restores, 16, 2, 2, 0).await?.len(), 2);
    for _ in 0..3 {
        queue_for(pool, full, StoredOperationKind::DrainRestoreSandbox).await?;
    }
    let mut next_drains = Vec::new();
    for _ in 0..2 {
        next_drains.push(queue_for(pool, next, StoredOperationKind::DrainRestoreSandbox).await?);
    }
    assert_eq!(
        claim_limited(pool, &restores, 16, 2, 2, 4).await?,
        next_drains,
        "the organization at its limit takes no place in the cap"
    );

    // With the cap full, an organization with only drain restores due isn't
    // waiting: the one other organization with work may go past its limit.
    let drains_only = org(pool).await?;
    queue_for(pool, drains_only, StoredOperationKind::DrainRestoreSandbox).await?;
    let past = org(pool).await?;
    let mut past_restores = Vec::new();
    for _ in 0..3 {
        past_restores.push(queue_for(pool, past, OperationKind::RestoreSandbox.into()).await?);
    }
    sqlx::query(sql!(
        "UPDATE public_operations SET state = 'succeeded', claim_token = ''
          WHERE org_id = $1"
    ))
    .bind(full)
    .execute(pool)
    .await?;
    assert_eq!(
        claim_limited(pool, &restores, 16, 2, 2, 2).await?,
        past_restores,
        "the cap is full, and no other organization waits"
    );
    Ok(())
}

/// A drain restore heads its sandbox's queue as any operation does: while
/// it waits out a retry, nothing due behind it runs but a delete, which runs
/// past any head in backoff, and while it runs, nothing behind it starts.
/// The work behind it waits for it to end, which its window bounds.
#[tokio::test(flavor = "current_thread")]
async fn a_drain_restore_holds_the_work_behind_it_as_any_head_does() -> TestResult {
    let db = pg_test_db().await?;
    let pool = &db.pool;
    let kinds = OperationKind::ALL
        .iter()
        .copied()
        .filter(|kind| !matches!(kind, OperationKind::CreateSession))
        .collect::<Vec<_>>();
    let behind_kinds = OperationKind::ALL.iter().copied().filter(|kind| {
        !OUTSIDE_SANDBOX_QUEUE_KINDS.contains(kind)
            && !matches!(
                kind,
                OperationKind::DeleteTemplate
                    | OperationKind::CreateSession
                    | OperationKind::CreateSandbox
            )
    });
    for behind in behind_kinds {
        let target_id = pplx_space_public_api::PublicSandboxId::from(SandboxId::new()).to_string();
        let restore = queue_on(
            pool,
            ORG,
            StoredOperationKind::DrainRestoreSandbox,
            &target_id,
        )
        .await?;
        sqlx::query(sql!(
            "UPDATE public_operations SET next_attempt_at = NOW() + INTERVAL '1 minute'
              WHERE operation_id = $1"
        ))
        .bind(restore.as_uuid())
        .execute(pool)
        .await?;
        // As an older API admits it, without canceling the restore.
        sqlx::query(sql!(
            "INSERT INTO public_operations
                 (operation_id, org_id, api_product, kind, state, target_type, target_id,
                  requested_at)
             VALUES ($1, $2, 'native', $3, 'queued', 'sandbox', $4, clock_timestamp())"
        ))
        .bind(Uuid::now_v7())
        .bind(ORG)
        .bind(behind.as_str())
        .bind(&target_id)
        .execute(pool)
        .await?;

        let claimed = claim(pool, &kinds, 16, 4).await?;
        if behind == OperationKind::DeleteSandbox {
            assert_eq!(
                claimed
                    .iter()
                    .map(|operation| operation.kind)
                    .collect::<Vec<_>>(),
                [StoredOperationKind::Public(behind)],
                "a delete runs past a retrying restore"
            );
        } else {
            assert!(
                claimed.is_empty(),
                "{behind} behind a retrying restore: {claimed:?}"
            );
        }
        if !claimed.is_empty() {
            sqlx::query(sql!(
                "UPDATE public_operations SET state = 'canceled', claim_token = ''
                  WHERE target_id = $1"
            ))
            .bind(&target_id)
            .execute(pool)
            .await?;
            continue;
        }

        sqlx::query(sql!(
            "UPDATE public_operations SET next_attempt_at = NOW() WHERE operation_id = $1"
        ))
        .bind(restore.as_uuid())
        .execute(pool)
        .await?;
        let claimed = claim(pool, &kinds, 16, 4).await?;
        assert_eq!(
            claimed
                .iter()
                .map(|operation| operation.id)
                .collect::<Vec<_>>(),
            [restore],
            "{behind}: the due restore runs first"
        );
        assert!(
            claim(pool, &kinds, 16, 4).await?.is_empty(),
            "{behind} behind a running restore"
        );
        sqlx::query(sql!(
            "UPDATE public_operations SET state = 'canceled', claim_token = ''
              WHERE target_id = $1"
        ))
        .bind(&target_id)
        .execute(pool)
        .await?;
    }
    Ok(())
}

/// Move `operation`'s enqueue `ago` into the past, as if it had been
/// enqueued then.
async fn enqueued_ago(pool: &PgPool, operation: OperationId, ago: Duration) -> TestResult {
    sqlx::query(sql!(
        "UPDATE public_operations
            SET created_at = created_at - make_interval(secs => $2),
                requested_at = requested_at - make_interval(secs => $2)
          WHERE operation_id = $1"
    ))
    .bind(operation.as_uuid())
    .bind(ago.as_secs_f64())
    .execute(pool)
    .await?;
    Ok(())
}

/// A drain restore the full cap holds back holds the work queued behind it
/// on its sandbox only until its window is spent: past it, the claim takes
/// it beside a full cap, for its executor to end, and it takes no cap place
/// from a restore still inside its window.
#[tokio::test(flavor = "current_thread")]
async fn a_cap_held_restore_past_its_window_is_claimed_beside_a_full_cap() -> TestResult {
    let db = pg_test_db().await?;
    let pool = &db.pool;
    let kinds = [OperationKind::UpdateSandbox];
    for _ in 0..2 {
        queue(pool, StoredOperationKind::DrainRestoreSandbox).await?;
    }
    assert_eq!(drain_restores(&claim(pool, &kinds, 16, 2).await?), 2);
    let target_id = pplx_space_public_api::PublicSandboxId::from(SandboxId::new()).to_string();
    let held = queue_on(
        pool,
        ORG,
        StoredOperationKind::DrainRestoreSandbox,
        &target_id,
    )
    .await?;
    let update = queue_on(pool, ORG, OperationKind::UpdateSandbox.into(), &target_id).await?;
    // A restore inside its window, which the full cap keeps holding.
    queue(pool, StoredOperationKind::DrainRestoreSandbox).await?;

    enqueued_ago(pool, held, DRAIN_RESTORE_WINDOW - Duration::from_secs(30)).await?;
    assert!(
        claim(pool, &kinds, 16, 2).await?.is_empty(),
        "inside its window the full cap holds it, and the update behind it"
    );

    enqueued_ago(pool, held, Duration::from_secs(31)).await?;
    let claimed = claim(pool, &kinds, 16, 2).await?;
    assert_eq!(
        claimed
            .iter()
            .map(|operation| operation.id)
            .collect::<Vec<_>>(),
        [held],
        "past its window it is claimed beside the full cap; the restore inside its window \
         still waits"
    );
    sqlx::query(sql!(
        "UPDATE public_operations SET state = 'failed', claim_token = '' WHERE operation_id = $1"
    ))
    .bind(held.as_uuid())
    .execute(pool)
    .await?;
    assert_eq!(
        claim(pool, &kinds, 16, 2)
            .await?
            .iter()
            .map(|operation| operation.id)
            .collect::<Vec<_>>(),
        [update],
        "the update runs once the restore ends"
    );
    Ok(())
}

/// A drain restore past its window takes no cap place: due first, it is
/// claimed beside the restore inside its window that the one free place
/// admits, not instead of it.
#[tokio::test(flavor = "current_thread")]
async fn a_restore_past_its_window_takes_no_cap_place() -> TestResult {
    let db = pg_test_db().await?;
    let pool = &db.pool;
    queue(pool, StoredOperationKind::DrainRestoreSandbox).await?;
    assert_eq!(drain_restores(&claim(pool, &[], 16, 2).await?), 1);
    let overdue = queue(pool, StoredOperationKind::DrainRestoreSandbox).await?;
    let inside = queue(pool, StoredOperationKind::DrainRestoreSandbox).await?;
    enqueued_ago(pool, overdue, DRAIN_RESTORE_WINDOW + Duration::from_secs(1)).await?;

    let mut claimed = claim(pool, &[], 16, 2)
        .await?
        .iter()
        .map(|operation| operation.id)
        .collect::<Vec<_>>();
    claimed.sort();
    let mut expected = vec![overdue, inside];
    expected.sort();
    assert_eq!(
        claimed, expected,
        "the free place goes to the restore inside its window"
    );
    Ok(())
}
