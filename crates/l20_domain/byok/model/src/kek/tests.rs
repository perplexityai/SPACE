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
