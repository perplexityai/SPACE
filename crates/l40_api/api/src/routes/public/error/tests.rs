use axum::http::StatusCode;
use http_body_util::BodyExt;
use pplx_space_metrics::response::{ResponseKind, ResponseOrigin};
use pplx_space_public_api::{PublicErrorCode, PublicErrorEnvelope};

use super::{PublicApiError, database_error};

/// Whether `error`'s rendered envelope says the request may be retried.
async fn renders_retryable(error: PublicApiError) -> bool {
    use axum::response::IntoResponse;

    let body = error.into_response().into_body().collect().await.unwrap();
    serde_json::from_slice::<PublicErrorEnvelope>(&body.to_bytes())
        .unwrap()
        .retryable
}

/// The public surface's idempotency guards and the fork-source lifecycle guard
/// are advisory locks whose whole purpose is to wait for a peer transaction.
/// Under a role-level `lock_timeout` that wait aborts with `55P03`, and every
/// database error on this surface used to become a 500 — telling an ECW client
/// the request is broken when in fact the identical retry succeeds and lands on
/// the committed idempotency record. Covers all three conversions, because each
/// is reachable from a different handler.
#[tokio::test(flavor = "current_thread")]
async fn database_lock_contention_is_a_retryable_503_not_an_internal_error()
-> pplx_space_test_env::TestResult {
    use axum::http::StatusCode;

    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db().await? else {
        return Ok(());
    };

    for sqlstate in ["55P03", "40001", "40P01"] {
        let raised = || pplx_space_db_test_fixtures::raise_sqlstate(&db.pool, sqlstate);

        let direct = database_error(raised().await);
        let via_operation = PublicApiError::from(
            crate::public_operations::PublicOperationStoreError::Sql(raised().await),
        );
        let via_session = PublicApiError::from(
            crate::public_sessions::PublicSessionStoreError::Sql(raised().await),
        );

        for (label, error) in [
            ("sqlx::Error", direct),
            ("PublicOperationStoreError", via_operation),
            ("PublicSessionStoreError", via_session),
        ] {
            let (status, origin, kind) = error.classification();
            assert_eq!(
                status,
                StatusCode::SERVICE_UNAVAILABLE,
                "{label} must surface {sqlstate} as retryable"
            );
            // Attributed to this service, not the sandbox fleet: the contention
            // is in our own database.
            assert_eq!(origin, ResponseOrigin::Api);
            assert_eq!(kind, ResponseKind::Internal);
            assert!(
                renders_retryable(error).await,
                "{label}/{sqlstate} must be marked retryable"
            );
        }
    }

    // A real conflict still reads as a real failure on all three paths.
    for error in [
        database_error(pplx_space_db_test_fixtures::raise_sqlstate(&db.pool, "23505").await),
        PublicApiError::from(crate::public_operations::PublicOperationStoreError::Sql(
            pplx_space_db_test_fixtures::raise_sqlstate(&db.pool, "23505").await,
        )),
        PublicApiError::from(crate::public_sessions::PublicSessionStoreError::Sql(
            pplx_space_db_test_fixtures::raise_sqlstate(&db.pool, "23505").await,
        )),
    ] {
        assert_eq!(error.classification().0, StatusCode::INTERNAL_SERVER_ERROR);
    }
    Ok(())
}

/// Replaying the create of a session that has ended answers the existing
/// `conflict` code, distinguished by its detail rather than a new code.
#[tokio::test]
async fn ended_session_replay_is_a_non_retryable_conflict_with_a_reason() {
    use axum::response::IntoResponse;

    let error = PublicApiError::from(crate::public_sessions::PublicSessionStoreError::SessionEnded);
    let response = error.into_response();
    assert_eq!(response.status(), StatusCode::CONFLICT);
    let envelope: PublicErrorEnvelope =
        serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes()).unwrap();
    assert_eq!(envelope.code, PublicErrorCode::Conflict);
    assert!(!envelope.retryable);
    assert_eq!(
        envelope.details,
        vec![pplx_space_public_api::PublicErrorDetail {
            key: "reason".to_string(),
            value: "session_ended".to_string(),
        }]
    );
}
