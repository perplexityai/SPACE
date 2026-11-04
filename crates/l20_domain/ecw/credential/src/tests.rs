use super::*;

#[test]
fn missing_current_key_is_rejected() {
    let env = pplx_space_test_env::TestEnv::default();

    assert!(matches!(
        EcwCredentialKeyring::load_required_from_environment(&env),
        Err(EcwCredentialError::MissingCurrentKey)
    ));
}

#[test]
fn v2_credentials_are_kind_epoch_and_key_scoped() {
    let sandbox_id = SandboxId::new();
    let epoch = EcwCredentialEpoch::new(3).unwrap();
    let envd = mint_v2(sandbox_id, epoch, EcwCredentialKind::Envd, b"current").unwrap();
    let traffic = mint_v2(sandbox_id, epoch, EcwCredentialKind::Traffic, b"current").unwrap();
    let next_epoch = mint_v2(
        sandbox_id,
        EcwCredentialEpoch::new(4).unwrap(),
        EcwCredentialKind::Envd,
        b"current",
    )
    .unwrap();
    let next_key = mint_v2(sandbox_id, epoch, EcwCredentialKind::Envd, b"next").unwrap();

    assert_ne!(envd, traffic);
    assert_ne!(envd, next_epoch);
    assert_ne!(envd, next_key);
    assert!(envd.secret().starts_with("ecw_v2_3_"));
}

#[test]
fn loaded_keyring_is_not_changed_by_per_request_environment_mutation() {
    let mut env = pplx_space_test_env::TestEnv::default();
    let sandbox_id = SandboxId::new();
    env.set(SPACE_ECW_CREDENTIAL_KEY_ENV, "startup-key");
    env.remove(SPACE_ECW_CREDENTIAL_KEY_PATH_ENV);
    let keyring = EcwCredentialKeyring::load_from_environment(&env).unwrap();
    // Mutate the env after the keyring is loaded: the keyring must
    // keep using the key it captured at load time, not re-read env
    // on every mint.
    env.set(SPACE_ECW_CREDENTIAL_KEY_ENV, "mutated-request-time-key");

    let minted = keyring
        .mint(
            sandbox_id,
            EcwCredentialEpoch::new(9).unwrap(),
            EcwCredentialKind::Traffic,
        )
        .unwrap()
        .unwrap();
    let expected = mint_v2(
        sandbox_id,
        EcwCredentialEpoch::new(9).unwrap(),
        EcwCredentialKind::Traffic,
        b"startup-key",
    )
    .unwrap();
    assert_eq!(minted, expected);
}
