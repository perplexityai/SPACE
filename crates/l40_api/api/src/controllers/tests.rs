use super::*;

#[test]
fn retry_after_renders_header_in_seconds() {
    let err = ControllerError::service_unavailable("still converging")
        .with_retry_after(Duration::from_secs(7));
    assert_eq!(err.retry_after(), Some(Duration::from_secs(7)));
    assert_eq!(err.retry_after_header(), Some(Duration::from_secs(7)));

    let response = err.into_response();
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        response
            .headers()
            .get(header::RETRY_AFTER)
            .expect("Retry-After header present"),
        "7"
    );
}

#[test]
fn no_retry_after_header_without_hint() {
    let response = ControllerError::service_unavailable("nope").into_response();
    assert!(response.headers().get(header::RETRY_AFTER).is_none());
}

#[test]
fn retry_after_header_does_not_set_converging_marker() {
    let err = ControllerError::rate_limited("slow down")
        .with_retry_after_header(Duration::from_millis(250));
    assert_eq!(err.retry_after(), None);
    assert_eq!(err.retry_after_header(), Some(Duration::from_millis(250)));

    let response = err.into_response();
    assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(
        response
            .headers()
            .get(header::RETRY_AFTER)
            .expect("Retry-After header present"),
        "1"
    );
}

#[test]
fn retry_after_header_value_rounds_a_partial_second_up_and_clamps_to_one() {
    fn rendered(hint: Duration) -> String {
        retry_after_header_value(hint)
            .expect("a bounded hint renders as a header value")
            .to_str()
            .expect("header value is ASCII")
            .to_string()
    }

    // Delay-seconds form is an integer, and the rounding direction is a
    // contract callers build on: `cluster_admission` samples its jitter in
    // whole seconds precisely so a hint reaches the client as the second it
    // picked. Rounding down instead — or truncating — would silently advertise
    // a retry before the wait it was derived from had elapsed.
    assert_eq!(rendered(Duration::from_secs(3)), "3");
    assert_eq!(rendered(Duration::from_millis(3_001)), "4");
    assert_eq!(rendered(Duration::from_nanos(3_000_000_001)), "4");
    // Nothing below one second is representable. Two distinct mechanisms reach
    // that result and both are covered: a sub-second remainder rounds *up* into
    // 1, while an exactly-zero hint has no remainder to round and is carried by
    // the `.max(1)` clamp instead.
    assert_eq!(rendered(Duration::from_millis(1)), "1");
    assert_eq!(rendered(Duration::ZERO), "1");
}

#[test]
fn without_retry_after_drops_the_marker_and_header() {
    let err = ControllerError::service_unavailable("rolled back")
        .with_retry_after(Duration::from_secs(5))
        .with_retry_after_header(Duration::from_secs(3))
        .without_retry_after();
    assert_eq!(err.retry_after(), None);
    assert_eq!(err.retry_after_header(), None);
    let response = err.into_response();
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert!(response.headers().get(header::RETRY_AFTER).is_none());
}

/// `db_contention_error` is what the lock-acquiring guards across this crate
/// share, so its two dispositions are asserted once here rather than at each
/// guard. A guard that regresses to `internal_error` is caught by its own
/// end-to-end test instead.
#[tokio::test(flavor = "current_thread")]
async fn db_contention_error_separates_a_lost_lock_race_from_a_real_failure()
-> pplx_space_test_env::TestResult {
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db().await? else {
        return Ok(());
    };

    for sqlstate in ["55P03", "40001", "40P01"] {
        let error = db_contention_error(
            "failed to lock something",
            &pplx_space_db_test_fixtures::raise_sqlstate(&db.pool, sqlstate).await,
        );
        assert_eq!(
            error.status(),
            StatusCode::SERVICE_UNAVAILABLE,
            "{sqlstate} must be retryable"
        );
        // No database detail in the client-facing message; the log carries it.
        assert!(
            !error.to_string().contains(sqlstate),
            "{sqlstate} leaked into the response: {error}"
        );
        // Paced, and header-only. Without the hint a caller retries at RTT
        // against the row that is already contended.
        //
        // The absent *converging* marker is not observable in production
        // today — both readers of `retry_after()` return on
        // `is_lock_contention()` first — so this pins the semantic choice
        // against a future regression rather than a current behavior: were the
        // marker set and either guard removed, contention would fold into
        // `reason:wait_budget`, incorrectly classifying contention as a timeout.
        assert_eq!(
            error.retry_after_header(),
            Some(crate::controllers::substrate::LOCK_CONTENTION_RETRY_AFTER),
            "{sqlstate} must pace the retry"
        );
        assert_eq!(
            error.retry_after(),
            None,
            "{sqlstate} left nothing converging"
        );
        assert_eq!(
            error
                .clone()
                .into_response()
                .headers()
                .get(header::RETRY_AFTER)
                .expect("a retryable 503 renders the hint"),
            "1"
        );
    }

    // A unique violation is a real conflict, not a lost race: retrying repeats
    // it, so it keeps the 500 and the context string.
    let error = db_contention_error(
        "failed to lock something",
        &pplx_space_db_test_fixtures::raise_sqlstate(&db.pool, "23505").await,
    );
    assert_eq!(error.status(), StatusCode::INTERNAL_SERVER_ERROR);
    assert!(error.to_string().contains("failed to lock something"));
    assert_eq!(error.retry_after_header(), None);
    Ok(())
}

/// The tag the surfaces branch on is set here, and only for contention.
///
/// This is the producer half only — it asserts nothing about any surface. The
/// consumer half is
/// `routes::public::tests::every_public_translator_keeps_contention_attributed_to_this_service`,
/// which drives the six translators. Both are needed: a translator can branch
/// correctly on a tag that never gets set, and the tag can be set correctly for
/// a translator that ignores it.
///
/// Why the tag exists at all: every translator branches on the status code, and
/// a contention 503 is indistinguishable from a fleet 503 that way — so before
/// `ControllerErrorKind::LockContention`, the sandboxes route reported a
/// Postgres lock race as `CapacityUnavailable`/`Cluster` ("the fleet is out of
/// capacity") and the ports/files/processes/sessions routes reported it as
/// `Node`/`BackendStatus`, blaming a node. Both put the wrong `origin` on the
/// response metrics and told the caller the wrong thing to do about it.
#[tokio::test(flavor = "current_thread")]
async fn db_contention_error_tags_only_contention_with_the_kind_surfaces_branch_on()
-> pplx_space_test_env::TestResult {
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db().await? else {
        return Ok(());
    };
    let contended = db_contention_error(
        "failed to lock something",
        &pplx_space_db_test_fixtures::raise_sqlstate(&db.pool, "55P03").await,
    );
    assert!(contended.is_lock_contention());
    assert_eq!(contended.status(), StatusCode::SERVICE_UNAVAILABLE);

    // A fleet 503 must not be mistaken for contention in the other direction.
    let fleet = ControllerError::service_unavailable("no capacity");
    assert!(!fleet.is_lock_contention());

    // Anything that is not contention keeps its own kind.
    let internal = db_contention_error(
        "failed to lock something",
        &pplx_space_db_test_fixtures::raise_sqlstate(&db.pool, "23505").await,
    );
    assert!(!internal.is_lock_contention());
    assert_eq!(internal.status(), StatusCode::INTERNAL_SERVER_ERROR);
    Ok(())
}

/// A create whose irreversible work already landed must not tell the caller to
/// retry — and the *whole* disposition has to go, not just the hints.
///
/// `without_retry_after` was not enough and that is the trap this pins:
/// it clears `retry_after`/`retry_after_header` but leaves
/// `ControllerErrorKind::LockContention` set, and every surface translator
/// rebuilds a retryable 503 from the kind alone
/// (`public::sandboxes::controller_error`, `e2b::translate::controller_error`).
/// So an error that had been "downgraded" still arrived at the client as
/// `retry this` — which, on a create path that mints a fresh `SandboxId` with
/// no idempotency key, provisions a second sandbox.
#[test]
fn demoting_after_a_side_effect_strips_the_whole_retryable_disposition() {
    let demoted = ControllerError::lock_contention().demoted_after_side_effect();

    assert_eq!(
        demoted.status(),
        StatusCode::INTERNAL_SERVER_ERROR,
        "a 503 still reads as retryable to a client, marking or not"
    );
    assert!(
        !demoted.is_lock_contention(),
        "the kind is what the surface translators rebuild a retryable 503 from"
    );
    assert_eq!(demoted.retry_after(), None);
    assert_eq!(demoted.retry_after_header(), None);
    assert_eq!(demoted.retry_after_cause(), None);

    // Contrast with `without_retry_after`, which is the insufficient form: it
    // drops the hints and keeps the identity that regenerates them.
    let half_done = ControllerError::lock_contention().without_retry_after();
    assert!(
        half_done.is_lock_contention(),
        "if this ever stops holding, the two methods have converged and the \
         comment above is stale"
    );

    // An error that was never contention keeps its *status* — the demotion must
    // not quietly rewrite an unrelated 503 into a 500 — but it does lose its
    // pacing hints, because those three fields are cleared unconditionally.
    // The fixture carries a hint and a cause so that second half is observable:
    // with an un-hinted fixture the clearing cannot be seen at all, and this
    // block would pass on a demotion that stripped nothing.
    let fleet = ControllerError::service_unavailable("no capacity")
        .with_retry_after(Duration::from_secs(30))
        .with_retry_after_cause(ControllerRetryAfterCause::RestoreInFlight);
    assert!(
        fleet.retry_after().is_some() && fleet.retry_after_cause().is_some(),
        "fixture must actually carry the hints this asserts the loss of"
    );

    let demoted_fleet = fleet.demoted_after_side_effect();
    assert_eq!(
        demoted_fleet.status(),
        StatusCode::SERVICE_UNAVAILABLE,
        "only a contention error's status is rewritten; an out-of-capacity 503 stays a 503"
    );
    assert!(!demoted_fleet.is_lock_contention());
    assert_eq!(
        demoted_fleet.retry_after(),
        None,
        "the converging marker goes even on a non-contention error: after a side \
         effect landed, nothing may tell the caller to retry this resource"
    );
    assert_eq!(demoted_fleet.retry_after_cause(), None);
    assert_eq!(demoted_fleet.retry_after_header(), None);
}
