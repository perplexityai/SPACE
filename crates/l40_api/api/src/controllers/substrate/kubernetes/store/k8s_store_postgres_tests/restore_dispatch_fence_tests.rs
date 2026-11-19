//! Postgres behaviors for the restore dispatch fence: when a re-drive may
//! take over a user restore whose owner never sent its Wake, and when the
//! restore lease and execution-age guards still hold it.
//!
//! A sibling module rather than more lines in the parent, which is already very
//! large. Driven from `k8s_store_postgres_behaviors` against the parent's
//! fixture.

use pplx_space_sandbox_model::SandboxId;

use super::*;

fn fenced(fixture: &PostgresStore) -> K8sStore {
    fixture
        .store
        .clone()
        .with_restore_dispatch_fence(super::super::super::RestoreDispatchFence::Enabled)
}

/// Claim a suspended sandbox's restore and its execution, as the owner's
/// `ensure_pod_exists` does, stopping short of the Wake.
async fn owner_claims_restore_and_execution(
    store: &K8sStore,
    pool: &PgPool,
    sandbox_id: SandboxId,
) -> TestResult<Uuid> {
    insert_sandbox(pool, sandbox_id, "suspended").await?;
    let token = user_fresh_restore_token(store.begin_restore_lifecycle(sandbox_id, false).await?);
    store
        .ensure_active_execution_claim_for_lifecycle(
            sandbox_id,
            SandboxLifecycle::Restoring,
            Some(token),
            Some((
                SandboxActivationAttemptId::new(),
                SandboxTransitionTargetState::Started,
            )),
        )
        .await?;
    Ok(token)
}

async fn expire_fence(pool: &PgPool, sandbox_id: SandboxId) -> TestResult {
    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions
            SET restore_dispatch_fence_expires_at = NOW() - INTERVAL '1 second'
          WHERE sandbox_id = $1 AND active AND restore_dispatch_fence_token IS NOT NULL"
    ))
    .bind(sandbox_id)
    .execute(pool)
    .await?;
    Ok(())
}

async fn expire_restore_lease(pool: &PgPool, sandbox_id: SandboxId) -> TestResult {
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET restore_lease_expires_at = NOW() - INTERVAL '1 second'
          WHERE id = $1"
    ))
    .bind(sandbox_id)
    .execute(pool)
    .await?;
    Ok(())
}

fn assert_in_flight(result: Result<RestoreClaim, SubstrateError>, why: &str) {
    match result {
        Err(SubstrateError::RestoreInFlight { .. }) => {}
        other => panic!("{why}: expected RestoreInFlight, got {other:?}"),
    }
}

/// An owner that dies after claiming its execution and before sending its
/// Wake stops renewing its fence. Once the fence expires a re-drive takes the
/// restore over, though the restore lease and the execution's last write are
/// both seconds old; the takeover expires the dead owner's activation attempt
/// so the re-drive takes its own. The dead owner, were it only slow, can no
/// longer send its Wake.
pub(super) async fn a_dead_owners_unsent_restore_is_taken_over_once_its_fence_expires(
    fixture: &PostgresStore,
) -> TestResult {
    // Arrange.
    let store = fenced(fixture);
    let sandbox_id = SandboxId::new();
    let owner_token = owner_claims_restore_and_execution(&store, &fixture.pool, sandbox_id).await?;
    assert_in_flight(
        store.begin_restore_lifecycle(sandbox_id, false).await,
        "a live owner's fence must keep the restore in flight",
    );
    assert!(matches!(
        store
            .claim_markerless_restoring_restore_lease(sandbox_id)
            .await,
        Err(SubstrateError::RestoreInFlight { .. })
    ));

    // Act.
    expire_fence(&fixture.pool, sandbox_id).await?;
    let takeover = store.begin_restore_lifecycle(sandbox_id, false).await?;

    // Assert.
    let redrive_token = user_redrive_restore_token(takeover);
    assert_ne!(redrive_token, owner_token);
    let (fence_token, attempt_expired): (Option<Uuid>, Option<bool>) = sqlx::query_as(sql!(
        "SELECT restore_dispatch_fence_token,
                activation_attempt_expires_at <= NOW()
           FROM sandbox_k8s_executions
          WHERE sandbox_id = $1 AND active"
    ))
    .bind(sandbox_id)
    .fetch_one(&fixture.pool)
    .await?;
    assert_eq!(
        fence_token, None,
        "the takeover clears the dead owner's fence"
    );
    assert_eq!(
        attempt_expired,
        Some(true),
        "the takeover expires the dead owner's activation attempt"
    );
    assert!(
        !store
            .confirm_user_restore_dispatch(sandbox_id, owner_token)
            .await?,
        "the owner the takeover replaced must not send its Wake"
    );
    assert!(
        store
            .confirm_user_restore_dispatch(sandbox_id, redrive_token)
            .await?
    );
    Ok(())
}

/// The re-driven Wake path claims through the markerless lease claim, which
/// takes an expired fence over the same way.
pub(super) async fn the_markerless_lease_claim_takes_over_an_expired_fence(
    fixture: &PostgresStore,
) -> TestResult {
    // Arrange.
    let store = fenced(fixture);
    let sandbox_id = SandboxId::new();
    let owner_token = owner_claims_restore_and_execution(&store, &fixture.pool, sandbox_id).await?;
    expire_fence(&fixture.pool, sandbox_id).await?;

    // Act.
    let takeover = store
        .claim_markerless_restoring_restore_lease(sandbox_id)
        .await?
        .expect("an expired fence is taken over");

    // Assert.
    assert_ne!(user_redrive_restore_token(takeover), owner_token);
    Ok(())
}

/// Once the owner confirms its dispatch, its Wake may be running on the node:
/// the fence is gone, and the restore keeps the full execution-age window
/// from the dispatch, even after the restore lease expires and however long
/// the owner took to get there.
pub(super) async fn a_dispatched_restore_keeps_its_ownership_window(
    fixture: &PostgresStore,
) -> TestResult {
    // Arrange.
    let store = fenced(fixture);
    let sandbox_id = SandboxId::new();
    let owner_token = owner_claims_restore_and_execution(&store, &fixture.pool, sandbox_id).await?;
    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions
            SET updated_at = NOW() - INTERVAL '290 seconds'
          WHERE sandbox_id = $1 AND active"
    ))
    .bind(sandbox_id)
    .execute(&fixture.pool)
    .await?;
    assert!(
        store
            .confirm_user_restore_dispatch(sandbox_id, owner_token)
            .await?
    );

    // Act.
    expire_fence(&fixture.pool, sandbox_id).await?;
    expire_restore_lease(&fixture.pool, sandbox_id).await?;
    let result = store.begin_restore_lifecycle(sandbox_id, false).await;

    // Assert.
    match result {
        Err(SubstrateError::RestoreInFlight {
            lease_remaining: Some(remaining),
            ..
        }) => assert!(remaining > Duration::from_secs(150)),
        other => panic!("a dispatched restore must stay in flight, got {other:?}"),
    }
    Ok(())
}

/// What a re-drive with the fence on finds on a restore it may not take over.
#[derive(Debug, Clone, Copy)]
enum Unfenced {
    /// The owner ran with the fence off, or predates it: no fence.
    OwnerWithoutFence,
    /// The fence names a token the row no longer carries, as after a
    /// takeover by a re-drive that writes no fence.
    FenceOfAnotherToken,
}

/// A restore without a fence bound to its current token keeps the restore
/// lease and execution-age guards, so mixed versions and mixed switch
/// settings never take over a restore that may have sent its Wake.
pub(super) async fn a_restore_without_its_owners_fence_keeps_its_ownership_window(
    fixture: &PostgresStore,
) -> TestResult {
    for shape in [Unfenced::OwnerWithoutFence, Unfenced::FenceOfAnotherToken] {
        // Arrange.
        let sandbox_id = SandboxId::new();
        match shape {
            Unfenced::OwnerWithoutFence => {
                owner_claims_restore_and_execution(&fixture.store, &fixture.pool, sandbox_id)
                    .await?;
            }
            Unfenced::FenceOfAnotherToken => {
                owner_claims_restore_and_execution(&fenced(fixture), &fixture.pool, sandbox_id)
                    .await?;
                sqlx::query(sql!(
                    "UPDATE sandbox_k8s_executions
                        SET restore_dispatch_fence_token = $2
                      WHERE sandbox_id = $1 AND active"
                ))
                .bind(sandbox_id)
                .bind(Uuid::now_v7())
                .execute(&fixture.pool)
                .await?;
                expire_fence(&fixture.pool, sandbox_id).await?;
            }
        }
        expire_restore_lease(&fixture.pool, sandbox_id).await?;

        // Act.
        let result = fenced(fixture)
            .begin_restore_lifecycle(sandbox_id, false)
            .await;

        // Assert.
        assert_in_flight(result, &format!("{shape:?}"));
    }
    Ok(())
}

/// A re-drive with the switch off ignores fences, so it keeps today's guards
/// whatever the owner wrote.
pub(super) async fn a_re_drive_with_the_switch_off_takes_no_fence_over(
    fixture: &PostgresStore,
) -> TestResult {
    // Arrange.
    let sandbox_id = SandboxId::new();
    owner_claims_restore_and_execution(&fenced(fixture), &fixture.pool, sandbox_id).await?;
    expire_fence(&fixture.pool, sandbox_id).await?;

    // Act.
    let result = fixture
        .store
        .begin_restore_lifecycle(sandbox_id, false)
        .await;

    // Assert.
    assert_in_flight(result, "the switch is off for this re-drive");
    Ok(())
}

/// A live owner's renewal moves its fence's expiry forward, so a re-drive
/// keeps out while the owner renews.
pub(super) async fn a_renewed_fence_keeps_a_live_owners_restore(
    fixture: &PostgresStore,
) -> TestResult {
    // Arrange.
    let store = fenced(fixture);
    let sandbox_id = SandboxId::new();
    let owner_token = owner_claims_restore_and_execution(&store, &fixture.pool, sandbox_id).await?;
    expire_fence(&fixture.pool, sandbox_id).await?;

    // Act.
    let renewed =
        pplx_space_sandbox_store::restore_dispatch_fence::renew_unsent_user_restore_fence(
            &fixture.pool,
            sandbox_id,
            owner_token,
            30,
        )
        .await?;

    // Assert.
    assert!(renewed);
    assert_in_flight(
        store.begin_restore_lifecycle(sandbox_id, false).await,
        "a renewed fence keeps the restore in flight",
    );
    Ok(())
}

/// An owner whose fence expired but who confirms its Wake before any re-drive
/// comes clears the fence under the row lock, so the re-drive that comes next
/// finds nothing to take over and keeps the full window for that Wake.
pub(super) async fn a_confirmed_dispatch_leaves_nothing_to_take_over(
    fixture: &PostgresStore,
) -> TestResult {
    // Arrange: the owner's fence has expired, but the owner is alive and gets
    // to its Wake first.
    let store = fenced(fixture);
    let sandbox_id = SandboxId::new();
    let owner_token = owner_claims_restore_and_execution(&store, &fixture.pool, sandbox_id).await?;
    expire_fence(&fixture.pool, sandbox_id).await?;
    expire_restore_lease(&fixture.pool, sandbox_id).await?;

    // Act.
    let dispatched = store
        .confirm_user_restore_dispatch(sandbox_id, owner_token)
        .await?;
    let takeover = store.begin_restore_lifecycle(sandbox_id, false).await;

    // Assert.
    assert!(dispatched);
    assert_in_flight(takeover, "the owner's Wake may be running");
    Ok(())
}
