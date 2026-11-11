use super::*;

#[test]
fn pooled_prepare_failure_falls_back_only_for_fallthrough_create() {
    assert_eq!(
        pooled_prepare_failure_disposition(CreateBootMode::Fallthrough),
        PooledPrepareFailureDisposition::FallbackToOnDemand
    );
    assert_eq!(
        pooled_prepare_failure_disposition(CreateBootMode::Pool),
        PooledPrepareFailureDisposition::Propagate
    );
}

#[test]
fn pooled_prepare_transport_failure_reclaims_before_cold_fallback() {
    for message in [
        "pooled sandbox metadata update failed: status: Unknown, message: \"transport error\"",
        "pooled sandbox metadata update failed: status: Unavailable, message: \"http2 error\"",
        "pooled sandbox metadata update failed: status: Unavailable, message: \"connection reset\"",
        "pooled sandbox metadata update failed: status: DeadlineExceeded, message: \"worker-node-api/update_sandbox deadline exceeded while running RPC\"",
    ] {
        let error = ControllerError::service_unavailable(message);
        assert_eq!(
            pooled_prepare_failure_action(CreateBootMode::Fallthrough, &error, 1),
            PooledPrepareFailureAction::ReclaimPool,
            "{message}"
        );
        assert_eq!(
            pooled_prepare_failure_action(
                CreateBootMode::Fallthrough,
                &error,
                MAX_POOLED_PREPARE_RECLAIM_ATTEMPTS
            ),
            PooledPrepareFailureAction::FallbackToOnDemand,
            "{message}"
        );
        assert_eq!(
            pooled_prepare_failure_action(CreateBootMode::Pool, &error, 1),
            PooledPrepareFailureAction::Propagate,
            "{message}"
        );
    }
}

#[test]
fn pooled_prepare_non_transport_failure_does_not_reclaim() {
    let policy_mismatch = ControllerError::service_unavailable(
        "pooled sandbox gateway confirmed a network policy that differs from the \
         request-resolved policy (sbx-mgr template cache may be stale); rolling back",
    );
    let owner_conflict =
        ControllerError::conflict("pooled sandbox metadata update failed: owner changed");

    assert_eq!(
        pooled_prepare_failure_action(CreateBootMode::Fallthrough, &policy_mismatch, 1),
        PooledPrepareFailureAction::FallbackToOnDemand
    );
    assert_eq!(
        pooled_prepare_failure_action(CreateBootMode::Fallthrough, &owner_conflict, 1),
        PooledPrepareFailureAction::FallbackToOnDemand
    );
}

#[test]
fn pooled_prepare_cancelled_deadline_does_not_reclaim() {
    let error = ControllerError::service_unavailable(
        "pooled sandbox metadata update failed: status: Cancelled, message: \"operation was canceled\"",
    );

    assert_eq!(
        pooled_prepare_failure_action(CreateBootMode::Fallthrough, &error, 1),
        PooledPrepareFailureAction::FallbackToOnDemand
    );
}

#[test]
fn create_runtime_from_request_canonicalizes_operator_spelling() {
    let (runtime, alias) = create_runtime_from_request(Some("firecracker")).unwrap();

    assert_eq!(runtime, RuntimeKind::Firecracker);
    assert_eq!(alias, "fc");
}

/// A pooled create whose claim is still held must not tell the caller to
/// retry; one whose claim was genuinely released must.
///
/// "Released" is narrower than "cleanup returned `Ok`", which is the trap this
/// pins: on the Kubernetes substrate cleanup succeeds by marking the slot
/// `cleanup_pending` and leaving a durable `paused` row, so its `Ok` is not a
/// release. Only `FinalizeSandboxDbDelete` is. Where the claim is still held a
/// retry mints a second sandbox and abandons the first, so the retryable
/// disposition has to go — including the `LockContention` kind, because the
/// public and ECW translators rebuild a retryable answer from the kind alone
/// before they look at the status.
#[test]
fn a_pooled_create_answers_retryable_only_when_its_claim_was_released() {
    let released = pooled_create_terminal_error(ControllerError::lock_contention(), true);
    assert_eq!(
        released.status(),
        StatusCode::SERVICE_UNAVAILABLE,
        "a genuinely released claim leaves nothing to duplicate, so the lost lock race stays retryable"
    );
    assert!(
        released.is_lock_contention(),
        "dropping the marker would let the translators re-label this as a fleet condition"
    );

    let still_held = pooled_create_terminal_error(ControllerError::lock_contention(), false);
    assert_eq!(
        still_held.status(),
        StatusCode::INTERNAL_SERVER_ERROR,
        "a retry here claims a second warm slot while the first stays live"
    );
    assert!(
        !still_held.is_lock_contention(),
        "the marker must go too — `without_retry_after` leaves it set, which is \
         exactly how a demoted error gets rebuilt into a retryable one"
    );
    assert!(
        still_held.retry_after_header().is_none(),
        "no pacing hint may survive either"
    );

    // An error that was never contention keeps its status, so the demotion
    // cannot quietly restate an unrelated failure as a 500. (It still clears
    // any pacing hint — the demotion is unconditional about that part.)
    let unrelated = pooled_create_terminal_error(ControllerError::not_found("gone"), false);
    assert_eq!(
        unrelated.status(),
        StatusCode::NOT_FOUND,
        "demotion must only strip a retryable disposition, not rewrite a status"
    );
}
