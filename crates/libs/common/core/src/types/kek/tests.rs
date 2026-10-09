use super::*;

#[test]
fn provider_round_trips_persisted_string() {
    for p in [
        KekProvider::AwsKms,
        KekProvider::InMemory,
        KekProvider::IntermediateKeyBroker,
    ] {
        assert_eq!(KekProvider::parse(p.as_str()).unwrap(), p);
        assert_eq!(p.to_string(), p.as_str());
    }
}

#[test]
fn legacy_intermediate_key_broker_identifier_remains_readable() {
    assert_eq!(
        KekProvider::parse("intermediate-key-broker").unwrap(),
        KekProvider::IntermediateKeyBroker
    );
    let provider = KekProvider::parse("external-branching").unwrap();
    assert_eq!(provider, KekProvider::IntermediateKeyBroker);
    assert_eq!(provider.as_str(), "external-branching");
}

#[test]
fn provider_rejects_unknown() {
    let err = KekProvider::parse("vault").unwrap_err();
    assert!(format!("{err}").contains("vault"));
}

#[test]
fn master_key_identifier_accepts_arn_and_alias() {
    let arn = "arn:aws:kms:us-east-1:111122223333:key/1234abcd-12ab-34cd-56ef-1234567890ab";
    assert_eq!(MasterKeyIdentifier::parse(arn).unwrap().as_str(), arn);
    let alias = "alias/example";
    assert_eq!(MasterKeyIdentifier::parse(alias).unwrap().as_str(), alias);
}

#[test]
fn master_key_identifier_rejects_empty_or_nul() {
    MasterKeyIdentifier::parse("").unwrap_err();
    MasterKeyIdentifier::parse("with\0nul").unwrap_err();
}

#[test]
fn authority_owned_providers_provision_lazily() {
    assert!(KekProvider::IntermediateKeyBroker.provisions_lazily());
    assert!(KekProvider::InMemory.provisions_lazily());
    assert!(!KekProvider::AwsKms.provisions_lazily());
}

#[test]
fn master_key_identifier_rejects_oversized() {
    let big = "k".repeat(2049);
    MasterKeyIdentifier::parse(big).unwrap_err();
}

#[test]
fn artifact_wrap_aad_pins_wire_layout_and_differs_from_volume_domain() {
    let org = crate::types::OrgId::from_uuid(uuid::Uuid::from_u128(1));
    let sandbox = crate::types::SandboxId::from_uuid(uuid::Uuid::from_u128(2));
    let aad = artifact_wrap_aad(org, &sandbox);
    let mut expected = b"space-artifact-wrap/v1\0".to_vec();
    expected.extend_from_slice(org.as_uuid().as_bytes());
    expected.extend_from_slice(sandbox.as_uuid().as_bytes());
    assert_eq!(
        aad, expected,
        "persisted artifact wraps depend on this layout"
    );
    assert_ne!(aad, volume_wrap_aad(org, &sandbox));
}
