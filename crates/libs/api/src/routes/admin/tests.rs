use axum::{
    Router,
    http::Method,
    routing::{get, put},
};

use super::{
    admin_request_should_persist_audit_event,
    support::{self, ContentionScope, contention_disposition, db_error},
};

#[test]
fn admin_request_audit_persistence_skips_safe_polling_methods() {
    assert!(!admin_request_should_persist_audit_event(&Method::GET));
    assert!(!admin_request_should_persist_audit_event(&Method::HEAD));
    assert!(!admin_request_should_persist_audit_event(&Method::OPTIONS));
    assert!(admin_request_should_persist_audit_event(&Method::POST));
    assert!(admin_request_should_persist_audit_event(&Method::PATCH));
    assert!(admin_request_should_persist_audit_event(&Method::PUT));
    assert!(admin_request_should_persist_audit_event(&Method::DELETE));
}

/// Building the admin router with the exact path patterns used in
/// `admin_router` must not panic. matchit 0.8 rejects dynamic suffixes
/// after a named param (e.g. `/{node_id}:drain`), so if anyone reverts to
/// that style this test catches the regression at build time instead of
/// at API startup.
#[test]
fn admin_route_patterns_are_valid() {
    async fn h() -> &'static str {
        ""
    }
    let _router: Router = Router::new().nest(
        "/admin/api",
        Router::new()
            .route("/llms.txt", get(h))
            .route("/openapi.json", get(h))
            .nest(
                "/v1",
                Router::new()
                    .route("/nodes", get(h).post(h))
                    .route("/nodes:refresh-status", axum::routing::post(h))
                    .route("/nodes:reconcile", axum::routing::post(h))
                    .route("/nodes/templates", get(h))
                    .route("/nodes/{spec}", get(h).post(h).patch(h).delete(h))
                    .route("/nodes/{node_id}/health", get(h))
                    .route(
                        "/nodes/{node_id}/network-policy:reconcile",
                        axum::routing::post(h),
                    )
                    .route("/nodes/{node_id}/template-pulls", axum::routing::post(h))
                    .route(
                        "/nodes/{node_id}/templates/{template_name}",
                        axum::routing::delete(h),
                    )
                    .route("/operations", get(h))
                    .route("/operations/{operation_id}", get(h))
                    .route("/audit-events", get(h))
                    .route("/runtime-environments", get(h))
                    .route("/runtime-environments/default", put(h))
                    .route("/runtime-environments/nodes", get(h))
                    .route("/runtime-environments/population", get(h))
                    .route("/runtime-environments/{environment_ref}/state", put(h))
                    .route("/warm-pools", get(h).put(h))
                    .route("/warm-pools/{pool_id}", axum::routing::delete(h))
                    .route("/warm-pool-policies", get(h).put(h))
                    .route("/warm-pool-policy-capabilities", get(h))
                    .route("/templates", get(h).post(h))
                    .route("/templates/{template_name}", get(h).patch(h).delete(h))
                    .route("/templates/{template_name}/builds", axum::routing::post(h))
                    .route("/templates/{template_name}/builds/{job_id}", get(h))
                    .route(
                        "/templates/{template_name}/base-snapshot-policy",
                        get(h).patch(h),
                    )
                    .route(
                        "/templates/{template_name}/base-snapshot-policy:reconcile",
                        axum::routing::post(h),
                    )
                    .route("/templates/{template_name}/pool", get(h).post(h).delete(h))
                    .route("/template-releases", get(h))
                    .route("/template-releases/{release_id}", axum::routing::delete(h))
                    .route("/template-releases/{release_id}/tag", put(h))
                    .route(
                        "/template-releases/{release_id}/tags/{tag}",
                        axum::routing::delete(h),
                    )
                    .route("/cluster", get(h).patch(h))
                    .route("/orgs", get(h).post(h))
                    .route("/orgs/{org_id}", get(h).patch(h).delete(h))
                    .route("/orgs/{org_id}/users", get(h).post(h))
                    .route("/orgs/{org_id}/api-keys", get(h).post(h))
                    .route(
                        "/orgs/{org_id}/api-keys/{api_key_id}",
                        axum::routing::delete(h),
                    )
                    .route("/users/{user_id}", get(h).patch(h).delete(h))
                    .route("/users/{user_id}/role", axum::routing::put(h))
                    .route("/sandboxes", get(h))
                    .route("/sandboxes/{spec}", get(h).patch(h).post(h))
                    .route("/sandboxes/{spec}/commands", axum::routing::post(h)),
            ),
    );
    let _router: Router = Router::new()
        .route("/llms.txt", get(h))
        .route("/openapi.json", get(h))
        .nest(
            "/v1",
            Router::new()
                .route("/nodes", get(h).post(h))
                .route("/nodes:refresh-status", axum::routing::post(h))
                .route("/nodes:reconcile", axum::routing::post(h))
                .route("/nodes/templates", get(h))
                .route("/nodes/{spec}", get(h).post(h).patch(h).delete(h))
                .route("/nodes/{node_id}/health", get(h))
                .route(
                    "/nodes/{node_id}/network-policy:reconcile",
                    axum::routing::post(h),
                )
                .route("/nodes/{node_id}/template-pulls", axum::routing::post(h))
                .route(
                    "/nodes/{node_id}/templates/{template_name}",
                    axum::routing::delete(h),
                )
                .route("/operations", get(h))
                .route("/operations/{operation_id}", get(h))
                .route("/audit-events", get(h))
                .route("/runtime-environments", get(h))
                .route("/runtime-environments/default", put(h))
                .route("/runtime-environments/nodes", get(h))
                .route("/runtime-environments/population", get(h))
                .route("/runtime-environments/{environment_ref}/state", put(h))
                .route("/warm-pools", get(h).put(h))
                .route("/warm-pools/{pool_id}", axum::routing::delete(h))
                .route("/warm-pool-policies", get(h).put(h))
                .route("/warm-pool-policy-capabilities", get(h))
                .route("/templates", get(h).post(h))
                .route("/templates/{template_name}", get(h).patch(h).delete(h))
                .route("/templates/{template_name}/builds", axum::routing::post(h))
                .route("/templates/{template_name}/builds/{job_id}", get(h))
                .route(
                    "/templates/{template_name}/base-snapshot-policy",
                    get(h).patch(h),
                )
                .route(
                    "/templates/{template_name}/base-snapshot-policy:reconcile",
                    axum::routing::post(h),
                )
                .route("/templates/{template_name}/pool", get(h).post(h).delete(h))
                .route("/template-releases", get(h))
                .route("/template-releases/{release_id}", axum::routing::delete(h))
                .route("/template-releases/{release_id}/tag", put(h))
                .route(
                    "/template-releases/{release_id}/tags/{tag}",
                    axum::routing::delete(h),
                )
                .route("/cluster", get(h).patch(h))
                .route("/orgs", get(h).post(h))
                .route("/orgs/{org_id}", get(h).patch(h).delete(h))
                .route("/orgs/{org_id}/users", get(h).post(h))
                .route("/orgs/{org_id}/api-keys", get(h).post(h))
                .route(
                    "/orgs/{org_id}/api-keys/{api_key_id}",
                    axum::routing::delete(h),
                )
                .route("/users/{user_id}", get(h).patch(h).delete(h))
                .route("/users/{user_id}/role", axum::routing::put(h))
                .route("/sandboxes", get(h))
                .route("/sandboxes/{spec}", get(h).patch(h).post(h))
                .route("/sandboxes/{spec}/commands", axum::routing::post(h)),
        );
}

/// The admin surface answers the two contention classes differently, on
/// purpose, and does so through both of its classifiers.
///
/// `40001`/`40P01` mean this request's own writes raced another interleaved
/// write — the guarded writes take their row locks in different orders across
/// handlers (a compat edit locks the group then the templates; a visibility
/// flip locks the template then, via the compat cascade, the group), so
/// Postgres aborts one arm of an AB-BA interleave. "Retry the whole request" is
/// a 409, and the wording describes what happened.
///
/// `55P03` does not fit that wording. Any statement can raise it, including a
/// plain read on a GET handler blocked behind DDL or behind the drained-node
/// terminator's cascade — a request that wrote nothing and raced no admin
/// write. A client-error 409 would blame the caller and nothing upstream
/// retries a 4xx, so it gets the 503 every other surface here gives it.
///
/// `node_maintenance` is in the table because it keeps its own `db_error` for
/// constraint-specific shapes and merely *delegates* the rest. The shadowing is
/// invisible at its call sites — every handler there reads `db_error(err)` — so
/// a grep for the name finds the local definition just as readily as the shared
/// import, and the delegation needs holding explicitly.
#[tokio::test(flavor = "current_thread")]
async fn admin_db_errors_separate_a_lock_wait_from_a_write_interleave()
-> pplx_space_test_env::TestResult {
    type Classifier = fn(sqlx::Error) -> (axum::http::StatusCode, axum::Json<serde_json::Value>);

    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db().await? else {
        return Ok(());
    };

    // The `23505` column differs on purpose: `node_maintenance` catches a unique
    // violation before delegating and reports "Node already exists", which is
    // more specific than anything the shared classifier can say.
    let classifiers: [(&str, Classifier, axum::http::StatusCode); 2] = [
        (
            "support",
            db_error,
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
        ),
        (
            "node_maintenance",
            super::node_maintenance::db_error,
            axum::http::StatusCode::CONFLICT,
        ),
    ];

    for (name, classify, unique_violation_status) in classifiers {
        let (status, _) =
            classify(pplx_space_db_test_fixtures::raise_sqlstate(&db.pool, "55P03").await);
        assert_eq!(
            status,
            axum::http::StatusCode::SERVICE_UNAVAILABLE,
            "{name}: a lock wait is not a client error"
        );

        for sqlstate in ["40001", "40P01"] {
            let (status, _) =
                classify(pplx_space_db_test_fixtures::raise_sqlstate(&db.pool, sqlstate).await);
            assert_eq!(
                status,
                axum::http::StatusCode::CONFLICT,
                "{name}: {sqlstate} is a write interleave; retry the request"
            );
        }

        let (status, _) =
            classify(pplx_space_db_test_fixtures::raise_sqlstate(&db.pool, "23505").await);
        assert_eq!(
            status, unique_violation_status,
            "{name}: a unique violation is a real conflict, not contention"
        );
    }

    Ok(())
}

/// A statement that wrote nothing is never told its write lost a race — but it
/// is still told the failure is retryable.
///
/// This is the exhaustive table for `contention_disposition`, asserted once
/// here. The per-module tests deliberately do not repeat it — they cover only
/// what their own wrapper adds (an envelope field, an error type, a pinned
/// scope), because a single mutation to `contention_disposition` should fail
/// one table rather than six copies of it.
///
/// Scoped to the boundaries that call `contention_disposition` directly.
/// `support::db_error` hardcodes `Writing` for every one of its callers, reads
/// included, so the row below about a read never taking the 409 does not
/// describe those — see `db_error`'s own doc.
///
/// Every SQLSTATE in the contention set is retryable, so the scope changes
/// only *which* retryable status, never whether there is one. `40001`/`40P01`
/// mean this request's own writes interleaved with another admin's, which is
/// what the 409's wording says; a statement passed `ReadOnly` — an authz
/// preflight, a `GET` handler's `SELECT`, anything a write handler issues
/// before its first write — has written nothing for that to be true of, so it
/// takes the generic 503 instead of being told to retry a write it never
/// made.
///
/// The 500 is reserved for SQLSTATEs outside the set. Answering a bounded,
/// retryable race with an internal error is the defect this disposition exists
/// to remove, so no scope may produce one for a code that is in it — which is
/// what makes the `/admin` and `/debug` read handlers survivable once the
/// role-level `lock_timeout` is enabled.
///
/// The `PublicOperationStoreError` arm is here rather than in a route module
/// for the same reason: every admin surface that opens or replays a public
/// operation reaches its database error through that enum, so which of its
/// variants can carry a SQLSTATE is one judgment, not one per module.
#[tokio::test(flavor = "current_thread")]
async fn a_read_only_statement_is_never_blamed_for_a_write_interleave()
-> pplx_space_test_env::TestResult {
    use axum::http::StatusCode;
    use pplx_space_db_test_fixtures::raise_sqlstate;

    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db().await? else {
        return Ok(());
    };
    let scopes = [ContentionScope::ReadOnly, ContentionScope::Writing];

    let lock_wait = raise_sqlstate(&db.pool, "55P03").await;
    for scope in scopes {
        assert_eq!(
            contention_disposition(scope, &lock_wait).map(|(status, _)| status),
            Some(StatusCode::SERVICE_UNAVAILABLE),
            "{scope:?}: a lock wait never takes the write-specific 409"
        );
    }

    for sqlstate in ["40001", "40P01"] {
        let err = raise_sqlstate(&db.pool, sqlstate).await;
        assert_eq!(
            contention_disposition(ContentionScope::Writing, &err).map(|(status, _)| status),
            Some(StatusCode::CONFLICT),
            "{sqlstate}: a write interleave is the caller's to retry"
        );
        // Retryable either way. The 409 is withheld from a read because its
        // wording would be false, not because the abort is unrecoverable.
        assert_eq!(
            contention_disposition(ContentionScope::ReadOnly, &err).map(|(status, _)| status),
            Some(StatusCode::SERVICE_UNAVAILABLE),
            "{sqlstate}: a read wrote nothing that could have interleaved, \
             but the retry still succeeds"
        );
    }

    let violation = raise_sqlstate(&db.pool, "23505").await;
    for scope in scopes {
        assert!(
            contention_disposition(scope, &violation).is_none(),
            "{scope:?}: a unique violation is a real conflict, not contention"
        );
    }

    // The same table through the store enum, which wraps the database error one
    // layer down. A classifier that only saw a bare `sqlx::Error` would leave
    // every store-backed admin arm on a 500.
    for sqlstate in ["55P03", "40001", "40P01", "23505"] {
        let bare = raise_sqlstate(&db.pool, sqlstate).await;
        let expected = contention_disposition(ContentionScope::Writing, &bare);
        let wrapped = crate::public_operations::PublicOperationStoreError::Sql(
            raise_sqlstate(&db.pool, sqlstate).await,
        );
        assert_eq!(
            support::store_contention_disposition(ContentionScope::Writing, &wrapped),
            expected,
            "{sqlstate}: the store-wrapped error must not take a different disposition"
        );
    }

    // A decode failure on a row this service wrote is not contention: no retry
    // re-reads it into something valid.
    let undecodable = crate::public_operations::PublicOperationStoreError::InvalidStoredFingerprint(
        "not-a-digest".into(),
    );
    for scope in scopes {
        assert!(
            support::store_contention_disposition(scope, &undecodable).is_none(),
            "{scope:?}: a stored-value decode failure is a real failure"
        );
    }

    // Closed for the reason on `templates::tests`' contention test: every
    // `raise_sqlstate` deliberately fails a statement on the connection it
    // borrows, and nextest reports the pool's outstanding ones at drop as a
    // leak.
    db.pool.close().await;
    Ok(())
}

/// The middleware over the whole `/admin/api/v1` tree answers a lost lock race
/// as a retryable 503, which is what makes every classified answer beneath it
/// reachable.
///
/// `require_admin_auth` is layered over `v1_router` — including the merged
/// `debug::admin_v1_debug_router` — and runs up to three database statements
/// over four tables before any handler: `get_session_user`'s `users`-`sessions`
/// join and its `organizations` read, then — only for a caller who is an org
/// admin, since `is_managing_org_admin` short-circuits on the role otherwise —
/// the same `SELECT managing_org_id FROM clusters` each module's gate repeats.
/// All of them answered a flat 500, so under the role-level `lock_timeout` a
/// contended `/admin` request was decided here and no handler's 503 could be
/// reached.
///
/// `ReadOnly` scope, asserted through the same shared disposition the handlers
/// use so this layer cannot drift from them: every contention SQLSTATE answers
/// a retryable 503, and none of them takes the write-specific 409. The
/// `Retry-After` comes with `PublicApiError::lock_contention`, which the admin
/// envelope has no field for.
///
/// Asserts `read_contention_response`, whose only purpose is those two arms,
/// rather than driving the middleware — so it would not catch either arm
/// dropping the call.
#[tokio::test(flavor = "current_thread")]
async fn the_admin_auth_middleware_answers_a_lock_wait_as_retryable()
-> pplx_space_test_env::TestResult {
    use axum::http::StatusCode;
    use pplx_space_db_test_fixtures::raise_sqlstate;

    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db().await? else {
        return Ok(());
    };

    let lock_wait = raise_sqlstate(&db.pool, "55P03").await;
    let response = super::read_contention_response(&lock_wait)
        .expect("a lock timeout on the admin gate is contention");
    assert_eq!(
        response.status(),
        StatusCode::SERVICE_UNAVAILABLE,
        "a lock wait in the admin gate is retryable, not internal"
    );
    assert!(
        response
            .headers()
            .contains_key(axum::http::header::RETRY_AFTER),
        "the gate's 503 tells the caller when to come back"
    );

    let deadlock = raise_sqlstate(&db.pool, "40P01").await;
    assert_eq!(
        super::read_contention_response(&deadlock).map(|response| response.status()),
        Some(StatusCode::SERVICE_UNAVAILABLE),
        "the gate's two statements wrote nothing, so no 409 — but the retry succeeds"
    );

    // Outside the contention set, so the gate still answers a real 500.
    let violation = raise_sqlstate(&db.pool, "23505").await;
    assert!(
        super::read_contention_response(&violation).is_none(),
        "a unique violation in the gate is a real failure"
    );

    // Closed for the reason on the disposition table above: each
    // `raise_sqlstate` fails a statement on the connection it borrows, and
    // nextest reports the pool's outstanding ones at drop as a leak.
    db.pool.close().await;
    Ok(())
}

/// The audit-persist failure fails closed either way, and withholds the retry
/// unless the handler's own status already told the caller to come back.
///
/// `require_admin_auth` writes `admin_audit_events` after `next.run`, on every
/// non-GET — including the ones the handler refused — and discards the
/// handler's response. Five wrong rules preceded this one:
///
/// - `internal()` marks every 500 retryable, so a contended audit insert after
///   a committed `POST /admin/api/v1/orgs` invited a replay that creates a
///   second organization.
/// - `not_retryable()` unconditionally hid the actionable 4xx of a handler that
///   refused and wrote nothing.
/// - `is_success()` missed that a handler 500 can be post-commit.
/// - Withholding the whole 5xx band missed that a handler **503** on this
///   surface *is* the retryable answer this disposition produces.
/// - Reading the request's `Idempotency-Key` treated the caller's choice of
///   header as evidence the handler honours it, which only `sandboxes` and
///   `templates` do — so a client that stamps it on everything was told a
///   committed org create was safe to replay.
///
/// What survives: any 4xx or a 503 keeps the retry; a 2xx or a 500/502/504
/// withholds it. See `audit_failure_response` for the cost that leaves on the
/// idempotency-keyed surfaces.
///
/// Asserts `audit_failure_response` itself, not the middleware arm that calls
/// it — reverting that one line to `PublicApiError::internal()` leaves this
/// green, as it does for the other wrapper-level tests in this change.
#[tokio::test(flavor = "current_thread")]
async fn the_audit_persist_failure_keeps_the_retry_only_when_the_status_says_so()
-> pplx_space_test_env::TestResult {
    use axum::body::to_bytes;
    use axum::http::StatusCode;

    async fn retryable(handler_status: StatusCode) -> bool {
        let response = super::audit_failure_response(handler_status);
        assert_eq!(
            response.status(),
            StatusCode::INTERNAL_SERVER_ERROR,
            "{handler_status}: the audit guarantee keeps this fail-closed"
        );
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("error body");
        let envelope: pplx_space_public_api::PublicErrorEnvelope =
            serde_json::from_slice(&body).expect("error body is an envelope");
        envelope.retryable
    }

    for handler_status in [
        StatusCode::BAD_REQUEST,
        StatusCode::NOT_FOUND,
        StatusCode::CONFLICT,
        StatusCode::PRECONDITION_FAILED,
        StatusCode::TOO_MANY_REQUESTS,
        StatusCode::SERVICE_UNAVAILABLE,
    ] {
        assert!(
            retryable(handler_status).await,
            "{handler_status}: the handler already told the caller to come back"
        );
    }

    for handler_status in [
        StatusCode::OK,
        StatusCode::CREATED,
        StatusCode::ACCEPTED,
        StatusCode::INTERNAL_SERVER_ERROR,
        StatusCode::BAD_GATEWAY,
        StatusCode::GATEWAY_TIMEOUT,
    ] {
        assert!(
            !retryable(handler_status).await,
            "{handler_status}: it committed, or whether it committed is not knowable"
        );
    }

    Ok(())
}
