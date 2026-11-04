use super::*;

fn key_id() -> MasterKeyIdentifier {
    MasterKeyIdentifier::parse("test/k1").unwrap()
}

#[tokio::test]
async fn wrap_then_unwrap_round_trips() {
    let wrapper = InMemoryKeyWrapper::new();
    let org = OrgId::new();
    let kid = key_id();
    let aad: &[u8] = b"sandbox=abc/record=42";

    let out = wrapper
        .generate_and_wrap_dek(
            org,
            None,
            KekProvider::InMemory,
            &kid,
            DekKeySpec::Aes256,
            aad,
        )
        .await
        .unwrap();
    assert_eq!(out.plaintext_dek.len(), 32);
    let recovered = wrapper
        .unwrap_dek(
            org,
            None,
            KekProvider::InMemory,
            &kid,
            &out.wrapped_dek,
            aad,
        )
        .await
        .unwrap();
    assert_eq!(recovered, out.plaintext_dek);
}

#[tokio::test]
async fn generated_dek_is_not_all_zero() {
    let wrapper = InMemoryKeyWrapper::new();
    let org = OrgId::new();
    let kid = key_id();
    let out = wrapper
        .generate_and_wrap_dek(
            org,
            None,
            KekProvider::InMemory,
            &kid,
            DekKeySpec::Aes256,
            b"",
        )
        .await
        .unwrap();
    assert_ne!(out.plaintext_dek, vec![0u8; 32]);
}

#[tokio::test]
async fn wrap_emits_fresh_nonce_each_call() {
    let wrapper = InMemoryKeyWrapper::new();
    let org = OrgId::new();
    let kid = key_id();

    let a = wrapper
        .generate_and_wrap_dek(
            org,
            None,
            KekProvider::InMemory,
            &kid,
            DekKeySpec::Aes256,
            b"",
        )
        .await
        .unwrap();
    let b = wrapper
        .generate_and_wrap_dek(
            org,
            None,
            KekProvider::InMemory,
            &kid,
            DekKeySpec::Aes256,
            b"",
        )
        .await
        .unwrap();
    // Two calls in the same (org, kid, aad) — distinct DEKs and
    // distinct ciphertexts (nonce + plaintext both refreshed).
    assert_ne!(a.plaintext_dek, b.plaintext_dek);
    assert_ne!(a.wrapped_dek, b.wrapped_dek);
}

#[tokio::test]
async fn aes_128_returns_16_byte_dek() {
    let wrapper = InMemoryKeyWrapper::new();
    let org = OrgId::new();
    let kid = key_id();
    let out = wrapper
        .generate_and_wrap_dek(
            org,
            None,
            KekProvider::InMemory,
            &kid,
            DekKeySpec::Aes128,
            b"",
        )
        .await
        .unwrap();
    assert_eq!(out.plaintext_dek.len(), 16);
}

#[tokio::test]
async fn unwrap_rejects_wrong_aad() {
    let wrapper = InMemoryKeyWrapper::new();
    let org = OrgId::new();
    let kid = key_id();
    let out = wrapper
        .generate_and_wrap_dek(
            org,
            None,
            KekProvider::InMemory,
            &kid,
            DekKeySpec::Aes256,
            b"original",
        )
        .await
        .unwrap();
    let err = wrapper
        .unwrap_dek(
            org,
            None,
            KekProvider::InMemory,
            &kid,
            &out.wrapped_dek,
            b"different",
        )
        .await
        .unwrap_err();
    assert!(matches!(err, UnwrapError::AuthFailed), "got {err:?}");
}

#[tokio::test]
async fn unwrap_rejects_cross_tenant_request() {
    let wrapper = InMemoryKeyWrapper::new();
    let org_a = OrgId::new();
    let org_b = OrgId::new();
    let kid = key_id();
    let out = wrapper
        .generate_and_wrap_dek(
            org_a,
            None,
            KekProvider::InMemory,
            &kid,
            DekKeySpec::Aes256,
            b"",
        )
        .await
        .unwrap();
    // Caller claims org_b but presents bytes minted for org_a; the
    // lookup at (org_b, kid) misses, so we report UnknownOrgKek.
    let err = wrapper
        .unwrap_dek(
            org_b,
            None,
            KekProvider::InMemory,
            &kid,
            &out.wrapped_dek,
            b"",
        )
        .await
        .unwrap_err();
    assert!(matches!(err, UnwrapError::UnknownOrgKek), "got {err:?}");
}

#[tokio::test]
async fn unwrap_rejects_wrong_master_key_identifier() {
    let wrapper = InMemoryKeyWrapper::new();
    let org = OrgId::new();
    let kid_wrap = MasterKeyIdentifier::parse("k1").unwrap();
    let kid_unwrap = MasterKeyIdentifier::parse("k2").unwrap();
    let out = wrapper
        .generate_and_wrap_dek(
            org,
            None,
            KekProvider::InMemory,
            &kid_wrap,
            DekKeySpec::Aes256,
            b"",
        )
        .await
        .unwrap();
    // Different master_key_identifier resolves to a different (or
    // absent) KEK.
    let err = wrapper
        .unwrap_dek(
            org,
            None,
            KekProvider::InMemory,
            &kid_unwrap,
            &out.wrapped_dek,
            b"",
        )
        .await
        .unwrap_err();
    assert!(matches!(err, UnwrapError::UnknownOrgKek), "got {err:?}");
}

#[tokio::test]
async fn unwrap_rejects_unknown_org() {
    let wrapper = InMemoryKeyWrapper::new();
    let org = OrgId::new();
    let kid = key_id();
    let err = wrapper
        .unwrap_dek(org, None, KekProvider::InMemory, &kid, &[0u8; 64], b"")
        .await
        .unwrap_err();
    assert!(matches!(err, UnwrapError::UnknownOrgKek), "got {err:?}");
}

#[tokio::test]
async fn unwrap_rejects_truncated_wrapped_dek() {
    let wrapper = InMemoryKeyWrapper::new();
    let org = OrgId::new();
    let kid = key_id();
    // Provision the KEK so we get past UnknownOrgKek.
    let _ = wrapper
        .generate_and_wrap_dek(
            org,
            None,
            KekProvider::InMemory,
            &kid,
            DekKeySpec::Aes256,
            b"",
        )
        .await
        .unwrap();
    // wrapped_dek shorter than nonce+tag must fail Malformed.
    let err = wrapper
        .unwrap_dek(
            org,
            None,
            KekProvider::InMemory,
            &kid,
            &[0u8; NONCE_LEN + TAG_LEN - 1],
            b"",
        )
        .await
        .unwrap_err();
    assert!(matches!(err, UnwrapError::Malformed), "got {err:?}");
}

#[tokio::test]
async fn wrap_rejects_unsupported_provider() {
    let wrapper = InMemoryKeyWrapper::new();
    let org = OrgId::new();
    let kid = key_id();
    let err = wrapper
        .generate_and_wrap_dek(
            org,
            None,
            KekProvider::AwsKms,
            &kid,
            DekKeySpec::Aes256,
            b"",
        )
        .await
        .unwrap_err();
    assert!(
        matches!(err, WrapError::UnsupportedProvider(KekProvider::AwsKms)),
        "got {err:?}",
    );
}

#[tokio::test]
async fn unwrap_rejects_unsupported_provider() {
    let wrapper = InMemoryKeyWrapper::new();
    let org = OrgId::new();
    let kid = key_id();
    let err = wrapper
        .unwrap_dek(org, None, KekProvider::AwsKms, &kid, &[0u8; 64], b"")
        .await
        .unwrap_err();
    assert!(
        matches!(err, UnwrapError::UnsupportedProvider(KekProvider::AwsKms)),
        "got {err:?}",
    );
}
