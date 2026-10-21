use pplx_space_common_core::types::OrgId;

use super::OrgSandboxQuota;

#[test]
fn unset_or_empty_configuration_caps_no_org() {
    let org = OrgId::new();
    for (default_limit, overrides) in [(None, None), (Some(""), Some("  "))] {
        let quota = OrgSandboxQuota::parse(default_limit, overrides).unwrap();
        assert_eq!(quota.limit_for(org), None);
    }
}

#[test]
fn an_org_override_takes_precedence_over_the_default() {
    let capped = OrgId::new();
    let other = OrgId::new();
    let overrides = format!(r#"{{"{capped}": 3}}"#);

    let quota = OrgSandboxQuota::parse(None, Some(&overrides)).unwrap();
    assert_eq!(quota.limit_for(capped), Some(3));
    assert_eq!(quota.limit_for(other), None);

    let quota = OrgSandboxQuota::parse(Some("100"), Some(&overrides)).unwrap();
    assert_eq!(quota.limit_for(capped), Some(3));
    assert_eq!(quota.limit_for(other), Some(100));
}

#[test]
fn malformed_configuration_is_rejected() {
    for (default_limit, overrides) in [
        (Some("-1"), None),
        (Some("ten"), None),
        (None, Some(r#"{"not-an-org": 3}"#)),
        (
            None,
            Some(r#"{"0190d5a4-2b4e-7c3e-8f00-000000000001": -3}"#),
        ),
        (None, Some("[3]")),
    ] {
        assert!(
            OrgSandboxQuota::parse(default_limit, overrides).is_err(),
            "{default_limit:?} {overrides:?}"
        );
    }
}
