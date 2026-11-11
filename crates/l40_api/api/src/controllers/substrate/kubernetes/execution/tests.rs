use super::super::pod_ops::WakeDispatchStatus;
use super::*;

/// A follower whose every join ends `Claimable` must keep backing off
/// across laps rather than restarting at the initial delay per join.
#[tokio::test(start_paused = true)]
async fn user_restore_poll_backoff_carries_across_claimable_laps() {
    let mut backoff = RecoveryJoinBackoff::new(RecoveryJoinBackoffPolicy::default());

    let mut lap_delays = Vec::new();
    for _ in 0..3 {
        let lap_started = Instant::now();
        tokio::time::sleep(backoff.next_delay()).await;
        lap_delays.push(lap_started.elapsed());
    }

    assert_eq!(
        lap_delays,
        vec![
            Duration::from_millis(100),
            Duration::from_millis(200),
            Duration::from_millis(400),
        ],
        "the poll delay must carry and grow across Claimable laps, not \
         restart at the initial delay on every rejoin",
    );
}

/// A wake that lost a lock race must not revert the row to `suspended`.
///
/// The statements that raise it on this path run after `wake_sandbox` already
/// returned success, so the pod and VM are live. `suspended` is swept by
/// neither the orphaned-sandbox reconciler nor the transient-lifecycle
/// sweeper, so reverting orphans a running VM — and since the caller gets a
/// retryable 503, it is simultaneously invited to start a second restore
/// against the same sandbox.
///
/// The predicate is an exclusion list, so this asserts membership rather than
/// a status: a variant absent from it reverts, which is how `LockContention`
/// came to revert when it was added. The `Conflict` half is the contrast that
/// makes the assertion mean something — without it, a predicate that returned
/// `false` for everything would pass.
///
/// Every case passes `NotDispatched` deliberately. `MayHaveLanded` makes
/// `operation_release_is_safe` false, which short-circuits the whole predicate
/// to `false` before the error is examined at all — so asserting the
/// contention case under `MayHaveLanded` would pass without the exclusion-list
/// arm existing. `NotDispatched` is the one value that lets the dispatch gate
/// through and leaves this arm as the only thing preventing the revert.
#[test]
fn a_contended_wake_does_not_revert_the_row_to_suspended() {
    assert!(
        !wake_failure_should_revert_to_suspended(
            Some(&SubstrateError::LockContention {
                message: "lost race after the wake landed".to_string(),
            }),
            WakeDispatchStatus::NotDispatched,
        ),
        "reverting here orphans a live VM and invites a duplicate restore"
    );

    // A genuinely clean failure still reverts: the restore did not take, so
    // `suspended` is the accurate durable state. This is also what proves the
    // dispatch gate above is open, not doing the work for us.
    assert!(
        wake_failure_should_revert_to_suspended(
            Some(&SubstrateError::Conflict {
                message: "wake failed before live work landed".to_string(),
            }),
            WakeDispatchStatus::NotDispatched,
        ),
        "the exclusion list must not have swallowed the clean-failure case"
    );

    // And a fresh-from-suspended claim inherits the same answer, so the
    // restore path cannot revert what the wake path preserved.
    assert!(
        !restore_failure_should_revert_to_suspended(
            Some(RestoreClaim::FreshFromSuspended {
                restore_lease_token: Some(uuid::Uuid::now_v7()),
            }),
            Some(&SubstrateError::LockContention {
                message: "lost race after the wake landed".to_string(),
            }),
            WakeDispatchStatus::NotDispatched,
        ),
        "the restore wrapper delegates to the wake predicate; if this diverges \
         one path reverts a live VM the other preserved"
    );
}
