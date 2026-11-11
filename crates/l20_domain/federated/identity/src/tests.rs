use super::*;

#[test]
fn configured_identity_policy_preserves_exact_identifiers() {
    let policy = FederatedIdentityPolicy::parse(
        Some("https://identity.example"),
        Some("example-provider"),
        Some(r#"["anonymous","0"]"#),
    )
    .unwrap();

    assert_eq!(policy.issuer.as_str(), "https://identity.example");
    assert_eq!(policy.grant_audience.as_str(), "example-provider");
    assert!(
        policy
            .rejected_subjects
            .contains(&UpstreamSubjectId::parse("0").unwrap())
    );
    assert!(
        !policy
            .rejected_subjects
            .contains(&UpstreamSubjectId::parse("100").unwrap())
    );
    assert!(
        FederatedIdentityPolicy::default()
            .rejected_subjects
            .is_empty()
    );
}

#[test]
fn malformed_identity_policy_is_rejected() {
    for (issuer, audience, subjects) in [
        (Some(""), None, None),
        (None, Some(""), None),
        (Some("issuer\n"), None, None),
        (None, Some("audience\n"), None),
        (None, None, Some("{}")),
        (None, None, Some(r#"[""]"#)),
        (None, None, Some(r#"[0]"#)),
    ] {
        FederatedIdentityPolicy::parse(issuer, audience, subjects)
            .expect_err("malformed identity policy must be rejected");
    }
}
