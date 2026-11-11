use axum::http::HeaderValue;

use super::*;
use crate::federated_identity::FederatedIdentityPolicy;

const UPSTREAM_ORG_ID: &str = "8a3f64c2-1f2b-4c4e-9a90-1f0d4f6f5a01";

fn enabled_config() -> FederatedIdentityConfig {
    FederatedIdentityConfig {
        enabled: true,
        policy: FederatedIdentityPolicy::parse(Some("test-issuer"), None, Some(r#"["0"]"#))
            .unwrap(),
    }
}

fn headers(pairs: &[(&str, &str)]) -> HeaderMap {
    let mut headers = HeaderMap::new();
    for (name, value) in pairs {
        headers.insert(
            axum::http::header::HeaderName::try_from(*name).unwrap(),
            HeaderValue::from_str(value).unwrap(),
        );
    }
    headers
}

#[test]
fn builds_owner_from_valid_headers() {
    let owner = federated_owner_from_headers(
        &enabled_config(),
        &headers(&[
            (FEDERATED_ORG_ID_HEADER, UPSTREAM_ORG_ID),
            (FEDERATED_USER_ID_HEADER, "123"),
        ]),
    )
    .unwrap();

    assert_eq!(owner.issuer.as_str(), "test-issuer");
    assert_eq!(owner.org_id.as_str(), UPSTREAM_ORG_ID);
    assert_eq!(owner.subject_id.as_str(), "123");
}

#[test]
fn tenantless_request_fails_closed() {
    let err = federated_owner_from_headers(
        &enabled_config(),
        &headers(&[(FEDERATED_USER_ID_HEADER, "123")]),
    )
    .unwrap_err();

    assert_eq!(err, FederatedIngressError::MissingOrg);
}

#[test]
fn missing_subject_fails_closed() {
    let err = federated_owner_from_headers(
        &enabled_config(),
        &headers(&[(FEDERATED_ORG_ID_HEADER, UPSTREAM_ORG_ID)]),
    )
    .unwrap_err();

    assert_eq!(err, FederatedIngressError::MissingSubject);
}

#[test]
fn placeholder_subject_is_rejected() {
    let err = federated_owner_from_headers(
        &enabled_config(),
        &headers(&[
            (FEDERATED_ORG_ID_HEADER, UPSTREAM_ORG_ID),
            (FEDERATED_USER_ID_HEADER, "0"),
        ]),
    )
    .unwrap_err();

    assert_eq!(err, FederatedIngressError::PlaceholderSubject);
}

#[test]
fn empty_and_oversized_values_are_rejected() {
    let empty = federated_owner_from_headers(
        &enabled_config(),
        &headers(&[
            (FEDERATED_ORG_ID_HEADER, ""),
            (FEDERATED_USER_ID_HEADER, "123"),
        ]),
    )
    .unwrap_err();
    let oversized = federated_owner_from_headers(
        &enabled_config(),
        &headers(&[
            (FEDERATED_ORG_ID_HEADER, UPSTREAM_ORG_ID),
            (FEDERATED_USER_ID_HEADER, &"9".repeat(129)),
        ]),
    )
    .unwrap_err();

    assert_eq!(empty, FederatedIngressError::InvalidOrg);
    assert_eq!(oversized, FederatedIngressError::InvalidSubject);
}

#[test]
fn detects_owner_header_presence() {
    assert!(!owner_headers_present(&headers(&[])));
    assert!(owner_headers_present(&headers(&[(
        FEDERATED_ORG_ID_HEADER,
        UPSTREAM_ORG_ID
    )])));
    assert!(owner_headers_present(&headers(&[(
        FEDERATED_USER_ID_HEADER,
        "123"
    )])));
}

/// Some absolute instant, well past any interval SPACE used to invent, so a
/// test asserting on it cannot pass by accident under a fallback TTL.
const GRANT_EXPIRES_AT_UNIX: i64 = 1_797_120_000;

fn grant_headers(pairs: &[(&str, &str)]) -> HeaderMap {
    let expiry = GRANT_EXPIRES_AT_UNIX.to_string();
    let mut pairs = pairs.to_vec();
    pairs.push((FEDERATED_SERVICE_GRANT_EXPIRES_AT_HEADER, &expiry));
    headers(&pairs)
}

#[test]
fn extracts_federated_service_grant_bytes_and_provider_expiry() {
    let grant = grant_headers(&[(
        FEDERATED_SERVICE_GRANT_HEADER,
        "opaque-federated-service-grant",
    )]);
    let grant = federated_service_grant_from_headers(&grant).unwrap();

    assert_eq!(grant.as_bytes(), b"opaque-federated-service-grant");
    assert_eq!(grant.expires_at_unix(), GRANT_EXPIRES_AT_UNIX);
    assert_eq!(
        grant.expires_at(),
        time::OffsetDateTime::from_unix_timestamp(GRANT_EXPIRES_AT_UNIX).unwrap()
    );
}

#[test]
fn missing_empty_and_oversized_federated_service_grants_are_rejected() {
    let missing = federated_service_grant_from_headers(&grant_headers(&[])).unwrap_err();
    let empty = federated_service_grant_from_headers(&grant_headers(&[(
        FEDERATED_SERVICE_GRANT_HEADER,
        "",
    )]))
    .unwrap_err();
    let oversized = FederatedServiceGrant::from_unix_expiry(
        vec![b'x'; MAX_FEDERATED_SERVICE_GRANT_BYTES + 1],
        GRANT_EXPIRES_AT_UNIX,
    )
    .unwrap_err();

    assert_eq!(missing, FederatedServiceGrantError::Missing);
    assert_eq!(empty, FederatedServiceGrantError::Empty);
    assert_eq!(oversized, FederatedServiceGrantError::TooLarge);
}

/// A grant with no usable expiry is refused rather than stored under a
/// lifetime SPACE made up: the blob is opaque, so a wrong expiry would either
/// strand a live grant or keep a dead one on file.
#[test]
fn grant_without_a_usable_expiry_is_rejected() {
    let grant = [(FEDERATED_SERVICE_GRANT_HEADER, "opaque-grant")];
    let with_expiry = |expiry: &str| {
        federated_service_grant_from_headers(&headers(&[
            grant[0],
            (FEDERATED_SERVICE_GRANT_EXPIRES_AT_HEADER, expiry),
        ]))
        .unwrap_err()
    };

    assert_eq!(
        federated_service_grant_from_headers(&headers(&grant)).unwrap_err(),
        FederatedServiceGrantError::MissingExpiry
    );
    assert_eq!(with_expiry(""), FederatedServiceGrantError::InvalidExpiry);
    assert_eq!(
        with_expiry("2026-08-14T00:00:00Z"),
        FederatedServiceGrantError::InvalidExpiry
    );
    // Parses as i64 but is not a representable instant.
    assert_eq!(
        with_expiry(&i64::MAX.to_string()),
        FederatedServiceGrantError::InvalidExpiry
    );
}

#[test]
fn zero_is_a_valid_subject_when_the_provider_does_not_reserve_it() {
    let config = FederatedIdentityConfig {
        enabled: true,
        ..FederatedIdentityConfig::disabled()
    };
    let owner = federated_owner_from_headers(
        &config,
        &headers(&[
            (FEDERATED_ORG_ID_HEADER, UPSTREAM_ORG_ID),
            (FEDERATED_USER_ID_HEADER, "0"),
        ]),
    )
    .unwrap();
    assert_eq!(owner.subject_id.as_str(), "0");
}
