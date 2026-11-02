use super::*;

type ReconnectTagTuple = (
    &'static str,
    &'static str,
    &'static str,
    &'static str,
    &'static str,
);

fn tuple_from_tags(tags: ReconnectAttemptTags) -> ReconnectTagTuple {
    (
        tags.outcome.as_str(),
        tags.terminal_phase.as_str(),
        tags.reason.as_str(),
        tags.entrypoint.as_str(),
        tags.initial_lifecycle,
    )
}

fn tags_for_outcome(
    identity: SandboxApiMetricIdentity,
    initial_lifecycle: &str,
    outcome: ReconnectOutcome,
    terminal_phase: ReconnectTerminalPhase,
    reason: ReconnectReason,
) -> ReconnectTagTuple {
    tuple_from_tags(
        reconnect_attempt_tags(identity, initial_lifecycle, outcome, terminal_phase, reason)
            .expect("identity should be a reconnect entrypoint"),
    )
}

fn tags_for_controller_error(
    identity: SandboxApiMetricIdentity,
    initial_lifecycle: &str,
    err: ControllerError,
) -> ReconnectTagTuple {
    let (outcome, terminal_phase, reason) =
        reconnect_controller_error_outcome(initial_lifecycle, &err);
    tags_for_outcome(identity, initial_lifecycle, outcome, terminal_phase, reason)
}

fn retryable_timeout(message: &str, phase: Option<WaitReadyTimeoutPhase>) -> ControllerError {
    ControllerError::from(SubstrateError::Timeout {
        message: message.to_string(),
        phase,
    })
}

#[test]
fn reconnect_tags_classify_restore_lease_already_in_flight() {
    let tags = tags_for_controller_error(
        SandboxApiMetricIdentity::ecw("connect"),
        "restoring",
        ControllerError::from(SubstrateError::restore_in_flight(
            "another recovery owner still holds the restore lease",
        )),
    );

    assert_eq!(
        tags,
        (
            "wake_in_progress",
            "restore",
            "restore_in_flight",
            "connect",
            "restoring",
        )
    );
}

#[test]
fn reconnect_tags_classify_outer_restore_and_wake_budget_exhaustion() {
    for (initial_lifecycle, terminal_phase) in [("suspended", "restore"), ("paused", "wake")] {
        let tags = tags_for_controller_error(
            SandboxApiMetricIdentity::ecw("connect"),
            initial_lifecycle,
            retryable_timeout("sandbox wake did not finish before API wait budget", None),
        );

        assert_eq!(
            tags,
            (
                "exhausted",
                terminal_phase,
                "wait_budget",
                "connect",
                initial_lifecycle,
            )
        );
    }
}

#[test]
fn reconnect_tags_preserve_wait_ready_timeout_phase_for_dataplane() {
    for (phase, terminal_phase, reason) in [
        (
            WaitReadyTimeoutPhase::PodNeverReady,
            "pod_never_ready",
            "pod_never_ready",
        ),
        (
            WaitReadyTimeoutPhase::ProxyUnreachable,
            "proxy_unreachable",
            "proxy_unreachable",
        ),
        (
            WaitReadyTimeoutPhase::GuestNotReady,
            "guest_not_ready",
            "guest_not_ready",
        ),
    ] {
        let tags = tags_for_controller_error(
            SandboxApiMetricIdentity::native("access"),
            "suspended",
            retryable_timeout("sandbox readiness wait timed out", Some(phase)),
        );

        assert_eq!(
            tags,
            (
                "exhausted",
                terminal_phase,
                reason,
                "dataplane",
                "suspended"
            )
        );
    }
}

#[test]
fn reconnect_tags_classify_rejections_and_post_wake_cas_loss() {
    for (outcome, terminal_phase, reason, initial_lifecycle, expected) in [
        (
            ReconnectOutcome::Rejected,
            ReconnectTerminalPhase::Api,
            ReconnectReason::DrainBlocked,
            "paused",
            ("rejected", "api", "drain_blocked", "connect", "paused"),
        ),
        (
            ReconnectOutcome::Rejected,
            ReconnectTerminalPhase::Api,
            ReconnectReason::TerminalLifecycle,
            "failed",
            ("rejected", "api", "terminal_lifecycle", "connect", "failed"),
        ),
        (
            ReconnectOutcome::WakeFailed,
            ReconnectTerminalPhase::Api,
            ReconnectReason::PostWakeCasLost,
            "suspended",
            (
                "wake_failed",
                "api",
                "post_wake_cas_lost",
                "connect",
                "suspended",
            ),
        ),
    ] {
        let tags = tags_for_outcome(
            SandboxApiMetricIdentity::ecw("connect"),
            initial_lifecycle,
            outcome,
            terminal_phase,
            reason,
        );

        assert_eq!(tags, expected);
    }
}

#[test]
fn reconnect_tags_classify_client_limiter_rejection() {
    // The ECW connect handler records this when the per-client concurrency
    // limiter rejects a reconnect in enforce mode — before the sandbox is
    // fetched, so the lifecycle it passes is empty and tags as `unknown`.
    // Keeping this on the reconnect series is what stops the retry-storm
    // alert from going blind to limiter-rejected retries.
    let tags = tags_for_outcome(
        SandboxApiMetricIdentity::ecw("connect"),
        "",
        ReconnectOutcome::Rejected,
        ReconnectTerminalPhase::Api,
        ReconnectReason::RateLimited,
    );

    assert_eq!(
        tags,
        ("rejected", "api", "rate_limited", "connect", "unknown")
    );
}

#[test]
fn reconnect_tags_classify_cluster_admission_rejection() {
    // The cluster activation limiter rejects a wake/restore with a 429.
    // The Wake admission gate records this on the reconnect series instead
    // of 429ing silently, so a control-plane-throttled retry storm stays
    // visible to the monitor.
    // These build the two *actual* admission error shapes (see
    // `cluster_activation_admission_error`): the rate-limited case carries a
    // `ClusterActivationAdmission` cause; the empty-cluster case is a bare
    // 429 with only a Retry-After header (no cause). Both must classify as
    // admission exhaustion, never `wake_failed`/`substrate`.
    let retry_after = std::time::Duration::from_secs(1);
    let rate_limited = || {
        ControllerError::rate_limited("cluster activation rate limited")
            .with_retry_after_header(retry_after)
            .with_retry_after_cause(ControllerRetryAfterCause::ClusterActivationAdmission)
    };
    let empty_cluster = || {
        ControllerError::rate_limited("cluster has no capacity")
            .with_retry_after_header(retry_after)
    };

    for build in [
        &rate_limited as &dyn Fn() -> ControllerError,
        &empty_cluster as &dyn Fn() -> ControllerError,
    ] {
        // Wake path — ECW connect entrypoint.
        assert_eq!(
            tags_for_controller_error(
                SandboxApiMetricIdentity::ecw("connect"),
                "suspended",
                build(),
            ),
            ("exhausted", "api", "wait_budget", "connect", "suspended")
        );

        // Data-plane access path — dataplane entrypoint.
        assert_eq!(
            tags_for_controller_error(
                SandboxApiMetricIdentity::native("access"),
                "paused",
                build(),
            ),
            ("exhausted", "api", "wait_budget", "dataplane", "paused")
        );
    }
}

#[test]
fn reconnect_tags_classify_deferred_restore_admission_rejection() {
    // Waking a suspended/restoring sandbox defers the cluster-activation
    // charge to the substrate RecoveryCharge path, which surfaces a 429 as
    // `SubstrateError::AdmissionRejected`. It must classify the same as a
    // direct-wake admission 429 (exhausted/wait_budget), not the generic
    // `rejected/unavailable`, so restore-path and direct-wake admission
    // rejections don't split across two outcome/reason series.
    for rate_limited in [true, false] {
        let err = SubstrateError::AdmissionRejected {
            message: "cluster activation admission rejected".to_string(),
            retry_after: Some(std::time::Duration::from_secs(1)),
            rate_limited,
        };
        let (outcome, terminal_phase, reason) = reconnect_error_outcome("suspended", &err);
        assert_eq!(
            (outcome.as_str(), terminal_phase.as_str(), reason.as_str()),
            ("exhausted", "api", "wait_budget")
        );
    }
}

/// An attempt that lost a database lock race must not be counted as a wake
/// failure, and must stay distinguishable from budget exhaustion.
///
/// Two separate properties, and each one is what a different consumer needs.
/// `outcome` is what the reconnect health alerting reads: a lost race wrote
/// nothing and the identical request succeeds on retry, so counting it as
/// `wake_failed` would put Postgres contention into the series that says the
/// sandbox path is broken. `reason` is what the per-environment `lock_timeout`
/// rollout reads: folding it into `wait_budget` — where the sibling
/// control-plane-saturation case already sits — would hide "the bound is
/// firing" behind restore-budget exhaustion, which is the one signal the
/// rollout has to watch.
#[test]
fn reconnect_tags_classify_a_lost_database_lock_race() {
    let contended = SubstrateError::LockContention {
        message: "failed to lock sandbox for execution materialization: 55P03".to_string(),
    };
    let (outcome, terminal_phase, reason) = reconnect_error_outcome("suspended", &contended);
    assert_eq!(
        (outcome.as_str(), terminal_phase.as_str(), reason.as_str()),
        ("exhausted", "restore", "lock_contention")
    );

    // Not the wake-fault series a `Failed` takes...
    let failed = SubstrateError::Failed {
        message: "failed to parse sandbox execution lifecycle".to_string(),
    };
    let (failed_outcome, _, failed_reason) = reconnect_error_outcome("suspended", &failed);
    assert_eq!(failed_outcome.as_str(), "wake_failed");
    assert_ne!(reason.as_str(), failed_reason.as_str());

    // ...and not the same reason as the budget/admission cases it shares
    // `exhausted` with.
    let admission = SubstrateError::AdmissionRejected {
        message: "cluster activation admission rejected".to_string(),
        retry_after: Some(std::time::Duration::from_secs(1)),
        rate_limited: true,
    };
    let (admission_outcome, _, admission_reason) = reconnect_error_outcome("suspended", &admission);
    assert_eq!(admission_outcome.as_str(), outcome.as_str());
    assert_ne!(reason.as_str(), admission_reason.as_str());
}

/// The `ControllerError` half of the same rule, asserted because leaving one
/// half of a mirrored pair classified is how the other half silently
/// regresses.
///
/// Two properties: a contention error must not be labelled `wake_failed`, and
/// it must not be folded into `wait_budget`. The fold is guarded twice over —
/// the `is_lock_contention()` arm below returns before the `retry_after`
/// branch, *and* `lock_contention()` leaves the converging marker unset — so
/// either guard alone would hold today. Both are asserted because each is one
/// edit away from being the only one, and the failure would be silent: a
/// contention error filed under `reason:wait_budget` looks like ordinary
/// restore-budget exhaustion, which is precisely the signal the `lock_timeout`
/// rollout needs to see separately.
#[test]
fn reconnect_controller_tags_classify_a_lost_database_lock_race() {
    let mut contended = ControllerError::service_unavailable("contended");
    contended.mark_lock_contention_for_test();

    // The marker the fold would key off must be absent...
    assert_eq!(
        contended.retry_after(),
        None,
        "the converging marker is the other half of the wait_budget guard"
    );
    // ...and the pacing hint must still be present, since an un-hinted
    // retryable 503 is retried at RTT.
    assert_eq!(
        contended.retry_after_header(),
        Some(crate::controllers::substrate::LOCK_CONTENTION_RETRY_AFTER)
    );

    let (outcome, terminal_phase, reason) =
        reconnect_controller_error_outcome("suspended", &contended);
    assert_eq!(
        (outcome.as_str(), terminal_phase.as_str(), reason.as_str()),
        ("exhausted", "restore", "lock_contention")
    );

    // A fleet 503 on the same path keeps the wake-fault label, so the branch
    // is discriminating on the marking and not on the status code.
    let fleet = ControllerError::service_unavailable("no capacity");
    let (fleet_outcome, _, fleet_reason) = reconnect_controller_error_outcome("suspended", &fleet);
    assert_eq!(fleet_outcome.as_str(), "wake_failed");
    assert_eq!(fleet_reason.as_str(), "unavailable");
}
