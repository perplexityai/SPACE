use super::SharedValkeyConfig;

const URL: &str = "rediss://:test-token@configuration-endpoint.example:6379";

#[test]
fn optional_config_disables_only_unset_or_blank_values() {
    assert!(
        SharedValkeyConfig::from_optional_url(None)
            .unwrap()
            .is_none()
    );
    assert!(
        SharedValkeyConfig::from_optional_url(Some("  "))
            .unwrap()
            .is_none()
    );
    SharedValkeyConfig::from_optional_url(Some(URL)).unwrap();
    let error = SharedValkeyConfig::from_optional_url(Some("https://example.com"))
        .err()
        .expect("non-Redis URL should fail validation");

    assert!(matches!(
        error,
        super::SharedValkeyConfigError::InvalidUrl(_)
    ));
}

#[test]
fn client_construction_is_lazy_and_cluster_aware() {
    let config = SharedValkeyConfig::from_optional_url(Some(URL))
        .unwrap()
        .unwrap();
    config.client().unwrap();
}
