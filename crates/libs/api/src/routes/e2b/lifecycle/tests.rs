use axum::http::{HeaderName, HeaderValue, StatusCode};
use pplx_space_common_core::types::{UpstreamIssuer, UpstreamOrgId, UpstreamSubjectId};

use crate::federated_identity::{
    FEDERATED_SERVICE_GRANT_EXPIRES_AT_HEADER, FEDERATED_SERVICE_GRANT_HEADER,
    FEDERATED_SERVICE_GRANT_ISSUED_AT_HEADER, FederatedOwner,
};

use super::*;

const GRANT_EXPIRES_AT_UNIX: i64 = 1_797_120_000;

fn headers(pairs: &[(&str, &str)]) -> HeaderMap {
    let mut headers = HeaderMap::new();
    for (name, value) in pairs {
        headers.insert(
            HeaderName::try_from(*name).unwrap(),
            HeaderValue::from_str(value).unwrap(),
        );
    }
    headers
}

fn owner() -> FederatedOwner {
    FederatedOwner {
        issuer: UpstreamIssuer::parse("example-issuer").unwrap(),
        org_id: UpstreamOrgId::parse("example-org-1").unwrap(),
        subject_id: UpstreamSubjectId::parse("123").unwrap(),
    }
}

/// Non-federated ECW traffic is untouched by the grant requirement, including
/// when a caller sends grant headers it has no owner for.
#[test]
fn requests_without_a_federated_owner_need_no_grant() {
    assert!(
        request_federated_service_grant(&headers(&[]), None)
            .unwrap()
            .is_none()
    );
    assert!(
        request_federated_service_grant(
            &headers(&[(FEDERATED_SERVICE_GRANT_HEADER, "opaque-grant")]),
            None
        )
        .unwrap()
        .is_none()
    );
}

#[test]
fn federated_owner_grant_carries_the_provider_expiry() {
    let grant = request_federated_service_grant(
        &headers(&[
            (FEDERATED_SERVICE_GRANT_HEADER, "opaque-grant"),
            (
                FEDERATED_SERVICE_GRANT_EXPIRES_AT_HEADER,
                &GRANT_EXPIRES_AT_UNIX.to_string(),
            ),
        ]),
        Some(&owner()),
    )
    .unwrap()
    .expect("a federated owner yields a grant");

    assert_eq!(grant.as_bytes(), b"opaque-grant");
    assert_eq!(grant.expires_at_unix(), GRANT_EXPIRES_AT_UNIX);
}

/// The grant and its expiry are one artifact, so half of one is refused the
/// same way a wholly missing grant is: SPACE has no basis for choosing the
/// lifetime of a blob it cannot read.
#[test]
fn federated_owner_without_a_grant_expiry_is_unauthorized() {
    let err = request_federated_service_grant(
        &headers(&[(FEDERATED_SERVICE_GRANT_HEADER, "opaque-grant")]),
        Some(&owner()),
    )
    .unwrap_err();

    assert_eq!(err.status, StatusCode::UNAUTHORIZED);
}

/// A legacy local sandbox reached through the federated bridge has no owner row
/// to bind a grant to, so refresh persists nothing for it.
#[test]
fn legacy_local_sandboxes_persist_no_grant() {
    use pplx_space_common_core::types::SandboxOwnerSource;

    let owner = owner();
    let grant = request_federated_service_grant(
        &headers(&[
            (FEDERATED_SERVICE_GRANT_HEADER, "opaque-grant"),
            (
                FEDERATED_SERVICE_GRANT_EXPIRES_AT_HEADER,
                &GRANT_EXPIRES_AT_UNIX.to_string(),
            ),
        ]),
        Some(&owner),
    )
    .unwrap()
    .unwrap();

    assert!(grant_to_persist(SandboxOwnerSource::Local, Some(&owner), Some(&grant)).is_none());
    assert!(
        grant_to_persist(
            SandboxOwnerSource::FederatedIdentity,
            Some(&owner),
            Some(&grant)
        )
        .is_some()
    );
    assert!(grant_to_persist(SandboxOwnerSource::FederatedIdentity, None, Some(&grant)).is_none());
}

/// Connect's permissive variant: fully absent grant headers are a no-op even
/// for a federated owner — connect must not gain a hard header requirement —
/// while a partial or malformed grant is rejected exactly like refresh.
#[test]
fn connect_grant_is_optional_when_headers_are_absent() {
    // Fully absent: no renewal, no rejection.
    assert!(
        optional_request_federated_service_grant(&headers(&[]), Some(&owner()), false)
            .unwrap()
            .is_none()
    );
    // Non-federated traffic with stray grant headers stays untouched.
    assert!(
        optional_request_federated_service_grant(
            &headers(&[(FEDERATED_SERVICE_GRANT_HEADER, "opaque-grant")]),
            None,
            false,
        )
        .unwrap()
        .is_none()
    );

    // A complete grant renews.
    let grant = optional_request_federated_service_grant(
        &headers(&[
            (FEDERATED_SERVICE_GRANT_HEADER, "opaque-grant"),
            (
                FEDERATED_SERVICE_GRANT_EXPIRES_AT_HEADER,
                &GRANT_EXPIRES_AT_UNIX.to_string(),
            ),
        ]),
        Some(&owner()),
        false,
    )
    .unwrap()
    .expect("a complete grant must be parsed for renewal");
    assert_eq!(grant.as_bytes(), b"opaque-grant");
    assert_eq!(grant.expires_at_unix(), GRANT_EXPIRES_AT_UNIX);
}

/// Half a grant is not "absent": a blob without its expiry (or vice versa)
/// is malformed and must be rejected, never silently skipped.
#[test]
fn connect_grant_with_partial_headers_is_unauthorized() {
    let err = optional_request_federated_service_grant(
        &headers(&[(FEDERATED_SERVICE_GRANT_HEADER, "opaque-grant")]),
        Some(&owner()),
        false,
    )
    .unwrap_err();
    assert_eq!(err.status, StatusCode::UNAUTHORIZED);

    let err = optional_request_federated_service_grant(
        &headers(&[(
            FEDERATED_SERVICE_GRANT_EXPIRES_AT_HEADER,
            &GRANT_EXPIRES_AT_UNIX.to_string(),
        )]),
        Some(&owner()),
        false,
    )
    .unwrap_err();
    assert_eq!(err.status, StatusCode::UNAUTHORIZED);

    let err = optional_request_federated_service_grant(
        &headers(&[
            (FEDERATED_SERVICE_GRANT_HEADER, "opaque-grant"),
            (FEDERATED_SERVICE_GRANT_EXPIRES_AT_HEADER, "not-a-unix-ts"),
        ]),
        Some(&owner()),
        false,
    )
    .unwrap_err();
    assert_eq!(err.status, StatusCode::UNAUTHORIZED);

    // An issued-at with nothing else is a partial envelope, not absence.
    let err = optional_request_federated_service_grant(
        &headers(&[(
            FEDERATED_SERVICE_GRANT_ISSUED_AT_HEADER,
            &GRANT_EXPIRES_AT_UNIX.to_string(),
        )]),
        Some(&owner()),
        false,
    )
    .unwrap_err();
    assert_eq!(err.status, StatusCode::UNAUTHORIZED);

    let err = optional_request_federated_service_grant(
        &headers(&[
            (FEDERATED_SERVICE_GRANT_HEADER, "opaque-grant"),
            (
                FEDERATED_SERVICE_GRANT_EXPIRES_AT_HEADER,
                &GRANT_EXPIRES_AT_UNIX.to_string(),
            ),
            (FEDERATED_SERVICE_GRANT_ISSUED_AT_HEADER, "not-a-unix-ts"),
        ]),
        Some(&owner()),
        false,
    )
    .unwrap_err();
    assert_eq!(err.status, StatusCode::UNAUTHORIZED);
}

/// The RFC's end-state contract, flippable without a code change: with
/// the grant requirement on, an absent grant pair on a federated
/// connect is unauthorized exactly like refresh; non-federated traffic is
/// still untouched.
#[test]
fn connect_grant_absence_is_unauthorized_in_strict_mode() {
    let err =
        optional_request_federated_service_grant(&headers(&[]), Some(&owner()), true).unwrap_err();
    assert_eq!(err.status, StatusCode::UNAUTHORIZED);

    assert!(
        optional_request_federated_service_grant(&headers(&[]), None, true)
            .unwrap()
            .is_none()
    );
}

mod delete {
    use std::time::Duration;

    use axum::response::IntoResponse;
    use pplx_space_db_test_fixtures::{SEED_ORG_ID, pg_test_db};
    use pplx_space_public_api::{
        OperationKind, PublicOperationResult, PublicSandboxOperationResult, SandboxLifecycleState,
    };
    use pplx_space_test_env::TestResult;

    use super::*;
    use crate::public_operations::{ClaimPublicOperations, CompleteClaimedPublicOperation};

    // Long enough to observe a queued row a few times, short enough that a
    // dispatcher-free test stays fast.
    const OBSERVE: Duration = Duration::from_millis(400);

    fn retry_after(error: E2bApiError) -> (StatusCode, Option<HeaderValue>) {
        let response = error.into_response();
        (
            response.status(),
            response
                .headers()
                .get(axum::http::header::RETRY_AFTER)
                .cloned(),
        )
    }

    async fn admit(
        store: &PublicOperationStore,
        sandbox_id: PublicSandboxId,
    ) -> TestResult<pplx_space_public_api::OperationId> {
        let payload = SandboxLifecycleOperationPayload {
            sandbox_id,
            action: PublicSandboxLifecycleAction::Delete,
            rollback: None,
        }
        .into_json();
        match store
            .create_or_join_active_sandbox_delete(
                OrgId::from_uuid(SEED_ORG_ID),
                &sandbox_id.to_string(),
                payload,
                None,
            )
            .await?
        {
            CreatePublicOperationResult::Created(id) => Ok(id),
            other => Err(format!("delete was not admitted: {other:?}").into()),
        }
    }

    #[tokio::test]
    async fn queued_delete_is_accepted_and_completed_delete_is_no_content() -> TestResult {
        let db = pg_test_db().await?;
        let store = PublicOperationStore::new(db.pool.clone());
        let sandbox_id = PublicSandboxId::new();
        let operation_id = admit(&store, sandbox_id).await?;
        let org_id = OrgId::from_uuid(SEED_ORG_ID);

        // No dispatcher runs here, as when the owning worker is draining:
        // the durable operation stays queued past the HTTP wait budget.
        let queued =
            wait_for_delete_operation(&store, org_id, operation_id, sandbox_id, OBSERVE).await;
        assert_eq!(
            queued.map_err(|error| error.status),
            Ok(StatusCode::ACCEPTED)
        );

        let claimed = store
            .claim_operations(ClaimPublicOperations {
                limit: 1,
                claim_stale_after: Duration::from_secs(60),
                claimable_kinds: &[OperationKind::DeleteSandbox],
            })
            .await?;
        let result = PublicOperationResult::Sandbox(PublicSandboxOperationResult {
            sandbox_id,
            state: SandboxLifecycleState::Deleted,
            action: Some(PublicSandboxLifecycleAction::Delete),
            controller_response: None,
        });
        assert!(
            store
                .complete_claimed_operation(CompleteClaimedPublicOperation {
                    claim_token: claimed.claim_token,
                    operation_id,
                    progress_message: "sandbox deleted".to_string(),
                    result_json: serde_json::to_value(&result)?,
                })
                .await?
        );
        let completed =
            wait_for_delete_operation(&store, org_id, operation_id, sandbox_id, OBSERVE).await;
        assert_eq!(
            completed.map_err(|error| error.status),
            Ok(StatusCode::NO_CONTENT)
        );
        Ok(())
    }

    #[tokio::test]
    async fn lost_observation_of_a_durable_delete_is_still_accepted() -> TestResult {
        let db = pg_test_db().await?;
        let store = PublicOperationStore::new(db.pool.clone());
        let sandbox_id = PublicSandboxId::new();
        let operation_id = admit(&store, sandbox_id).await?;
        db.pool.close().await;

        let observed = wait_for_delete_operation(
            &store,
            OrgId::from_uuid(SEED_ORG_ID),
            operation_id,
            sandbox_id,
            OBSERVE,
        )
        .await;
        assert_eq!(
            observed.map_err(|error| error.status),
            Ok(StatusCode::ACCEPTED)
        );
        Ok(())
    }

    #[tokio::test]
    async fn unproven_delete_admission_is_a_paced_retryable_unavailable() -> TestResult {
        // Nothing listens on port 1, so admission cannot reach the database.
        let pool = sqlx::postgres::PgPoolOptions::new()
            .acquire_timeout(Duration::from_millis(200))
            .connect_lazy("postgres://unused@127.0.0.1:1/unused")?;
        let error = PublicOperationStore::new(pool)
            .create_or_join_active_sandbox_delete(
                OrgId::from_uuid(SEED_ORG_ID),
                &PublicSandboxId::new().to_string(),
                serde_json::json!({}),
                None,
            )
            .await
            .expect_err("an unreachable database cannot admit a delete");

        assert_eq!(
            retry_after(delete_admission_error(&error)),
            (
                StatusCode::SERVICE_UNAVAILABLE,
                Some(HeaderValue::from_static("1"))
            )
        );
        assert_eq!(
            retry_after(delete_admission_error(
                &PublicOperationStoreError::InvalidStoredFingerprint("corrupt".into())
            ))
            .0,
            StatusCode::INTERNAL_SERVER_ERROR
        );
        Ok(())
    }

    #[test]
    fn failed_delete_reports_unavailability_as_retryable() {
        assert_eq!(
            retry_after(e2b_operation_failure(
                Some(PublicErrorCode::ServiceUnavailable),
                None,
                "worker unavailable",
            )),
            (
                StatusCode::SERVICE_UNAVAILABLE,
                Some(HeaderValue::from_static("1"))
            )
        );
        assert_eq!(
            retry_after(e2b_operation_failure(
                Some(PublicErrorCode::Conflict),
                None,
                "conflict"
            )),
            (StatusCode::CONFLICT, None)
        );
    }
}
