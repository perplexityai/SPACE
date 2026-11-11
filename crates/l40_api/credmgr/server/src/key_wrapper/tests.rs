use super::*;
use crate::in_memory::InMemoryKeyWrapper;

fn key_id() -> MasterKeyIdentifier {
    MasterKeyIdentifier::parse("test/k1").unwrap()
}

#[tokio::test]
async fn composite_routes_to_installed_backend() {
    let composite = CompositeKeyWrapper::new().with_in_memory(InMemoryKeyWrapper::new());
    let org = OrgId::new();
    let kid = key_id();
    let out = composite
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
    let recovered = composite
        .unwrap_dek(
            org,
            None,
            KekProvider::InMemory,
            &kid,
            &out.wrapped_dek,
            b"",
        )
        .await
        .unwrap();
    assert_eq!(recovered, out.plaintext_dek);
}

#[tokio::test]
async fn composite_rejects_uninstalled_provider() {
    let composite = CompositeKeyWrapper::new().with_in_memory(InMemoryKeyWrapper::new());
    let org = OrgId::new();
    let kid = key_id();
    let err = composite
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
