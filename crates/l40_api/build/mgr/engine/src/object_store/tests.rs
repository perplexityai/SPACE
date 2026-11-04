use super::*;

use rstest::rstest;

#[test]
fn config_is_absent_when_no_object_store_env_exists() {
    let config = ObjectStoreConfig::from_values(None, None, None, None)
        .expect("missing object-store env should be accepted");

    assert!(config.is_none());
}

#[test]
fn config_rejects_partial_object_store_env() {
    let error = ObjectStoreConfig::from_values(Some("bucket".to_string()), None, None, None)
        .expect_err("partial object-store config should fail");

    assert!(matches!(
        error,
        ObjectStoreConfigError::MissingEnv("SPACE_OBJECT_STORE_REGION")
    ));
}

#[test]
fn config_uses_object_store_env_values() {
    let config = ObjectStoreConfig::from_values(
        Some("bucket".to_string()),
        Some("us-east-1".to_string()),
        Some("prefix".to_string()),
        Some("https://minio.internal".to_string()),
    )
    .expect("object-store config should parse")
    .expect("object store should be configured");

    assert_eq!(config.bucket, "bucket");
    assert_eq!(config.region, "us-east-1");
    assert_eq!(config.key_prefix, "prefix");
    assert_eq!(config.endpoint.as_deref(), Some("https://minio.internal"));
    assert!(config.path_style);
}

#[rstest]
#[case::unavailable(tonic::Code::Unavailable, true)]
#[case::deadline_exceeded(tonic::Code::DeadlineExceeded, true)]
#[case::unauthenticated(tonic::Code::Unauthenticated, false)]
#[case::permission_denied(tonic::Code::PermissionDenied, false)]
fn credential_exchange_classifies_retriable_statuses(
    #[case] code: tonic::Code,
    #[case] expected_retriable: bool,
) {
    let error = CredentialExchangeError::Exchange(tonic::Status::new(code, "test"));

    assert_eq!(error.is_retriable(), expected_retriable);
}

#[test]
fn storage_access_requests_only_the_actions_each_operation_needs() {
    assert_eq!(StorageAccess::Read.scope(), STORAGE_READ_SCOPE);
    assert_eq!(StorageAccess::Write.scope(), STORAGE_WRITE_SCOPE);
    assert_eq!(
        StorageAccess::ReadWrite.scope(),
        format!("{STORAGE_READ_SCOPE} {STORAGE_WRITE_SCOPE}")
    );
}
