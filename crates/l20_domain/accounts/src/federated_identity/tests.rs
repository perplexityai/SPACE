use super::*;

#[test]
fn upstream_ids_reject_empty_controls_and_oversized_values() {
    FederatedGrantAudience::parse("").unwrap_err();
    FederatedGrantAudience::parse("invalid\n").unwrap_err();
    FederatedGrantAudience::parse("example-provider").unwrap();
    UpstreamIssuer::parse("").unwrap_err();
    UpstreamIssuer::parse(format!("{}x", "a".repeat(MAX_UPSTREAM_ID_BYTES))).unwrap_err();
    UpstreamSubjectId::parse("123\u{202e}").unwrap_err();
    UpstreamOrgId::parse("\t").unwrap_err();

    UpstreamIssuer::parse("example/auth").unwrap();
    UpstreamOrgId::parse("8a3f64c2-1f2b-4c4e-9a90-1f0d4f6f5a01").unwrap();
    // An upstream subject can be an integer user id rendered as opaque text.
    UpstreamSubjectId::parse("123").unwrap();
}

#[test]
fn owner_source_parses_persisted_labels() {
    assert_eq!(
        SandboxOwnerSource::from_str("local").unwrap(),
        SandboxOwnerSource::Local
    );
    assert_eq!(
        SandboxOwnerSource::from_str("federated_identity").unwrap(),
        SandboxOwnerSource::FederatedIdentity
    );
    SandboxOwnerSource::from_str("federated").unwrap_err();
}
