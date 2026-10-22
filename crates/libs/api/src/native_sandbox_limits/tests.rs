use pplx_space_common_core::types::{OrgId, UserId};

use super::NativeSandboxLimits;
use crate::public_operations::{SandboxQuota, SubjectSandboxQuota};

#[test]
fn unset_or_empty_configuration_limits_no_caller() {
    let org = OrgId::new();
    for (default_limit, overrides, subject_limit) in
        [(None, None, None), (Some(""), Some("  "), Some(""))]
    {
        let quota = NativeSandboxLimits::parse(default_limit, overrides, subject_limit).unwrap();
        assert_eq!(quota.for_create(org, None), None);
        assert_eq!(quota.for_create(org, Some(UserId::new())), None);
    }
}

#[test]
fn an_org_override_takes_precedence_over_the_default() {
    let capped = OrgId::new();
    let other = OrgId::new();
    let overrides = format!(r#"{{"{capped}": 3}}"#);

    let quota = NativeSandboxLimits::parse(None, Some(&overrides), None).unwrap();
    assert_eq!(quota.limit_for(capped), Some(3));
    assert_eq!(quota.limit_for(other), None);

    let quota = NativeSandboxLimits::parse(Some("100"), Some(&overrides), None).unwrap();
    assert_eq!(quota.limit_for(capped), Some(3));
    assert_eq!(quota.limit_for(other), Some(100));
}

/// An override of zero is an explicit choice for one org: it refuses every
/// native create of that org.
#[test]
fn an_org_override_of_zero_is_kept() {
    let blocked = OrgId::new();
    let overrides = format!(r#"{{"{blocked}": 0}}"#);
    let quota = NativeSandboxLimits::parse(Some("100"), Some(&overrides), None).unwrap();
    assert_eq!(quota.limit_for(blocked), Some(0));
}

#[test]
fn only_a_federated_caller_has_a_subject_limit() {
    let org = OrgId::new();
    let subject = UserId::new();
    let subject_only = NativeSandboxLimits::parse(None, None, Some("5")).unwrap();
    assert_eq!(subject_only.for_create(org, None), None);
    assert_eq!(
        subject_only.for_create(org, Some(subject)),
        Some(SandboxQuota {
            org_id: org,
            org_limit: None,
            subject: Some(SubjectSandboxQuota {
                user_id: subject,
                limit: 5
            }),
        })
    );

    let both = NativeSandboxLimits::parse(Some("100"), None, Some("5")).unwrap();
    assert_eq!(
        both.for_create(org, None),
        Some(SandboxQuota {
            org_id: org,
            org_limit: Some(100),
            subject: None,
        })
    );
    assert_eq!(
        both.for_create(org, Some(subject))
            .map(|quota| quota.org_limit),
        Some(Some(100))
    );
}

#[test]
fn malformed_configuration_is_rejected() {
    for (default_limit, overrides, subject_limit) in [
        (Some("-1"), None, None),
        (Some("ten"), None, None),
        (None, Some(r#"{"not-an-org": 3}"#), None),
        (
            None,
            Some(r#"{"0190d5a4-2b4e-7c3e-8f00-000000000001": -3}"#),
            None,
        ),
        (None, Some("[3]"), None),
        (None, None, Some("five")),
        (None, None, Some("-5")),
        // Zero is not "no limit": unset means that.
        (Some("0"), None, None),
        (None, None, Some("0")),
        (
            None,
            Some(
                r#"{"0190d5a4-2b4e-7c3e-8f00-000000000001": 3, "0190d5a4-2b4e-7c3e-8f00-000000000001": 5}"#,
            ),
            None,
        ),
    ] {
        assert!(
            NativeSandboxLimits::parse(default_limit, overrides, subject_limit).is_err(),
            "{default_limit:?} {overrides:?} {subject_limit:?}"
        );
    }
}
