use super::*;

#[test]
fn feature_gate_controls_enabled_mode() {
    let config = FederatedIdentityConfig::from_env().unwrap();

    assert_eq!(config.enabled, cfg!(feature = "federated-identity"));
    assert_eq!(
        config.policy.issuer.as_str(),
        DEFAULT_FEDERATED_IDENTITY_ISSUER
    );
}

#[test]
fn disabled_config_uses_default_issuer() {
    let config = FederatedIdentityConfig::disabled();

    assert!(!config.enabled);
    assert_eq!(
        config.policy.issuer.as_str(),
        DEFAULT_FEDERATED_IDENTITY_ISSUER
    );
}
