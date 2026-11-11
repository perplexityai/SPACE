use super::*;

#[test]
fn email_rejects_empty_parts_controls_and_multiple_at_signs() {
    Email::parse("@").unwrap_err();
    Email::parse("a@").unwrap_err();
    Email::parse("@example.com").unwrap_err();
    Email::parse("a@b@example.com").unwrap_err();
    Email::parse("a\nb@example.com").unwrap_err();
    Email::parse("a@\u{202e}example.com").unwrap_err();
    Email::parse("a b@example.com").unwrap_err();
}

#[test]
fn email_normalizes_ascii_case() {
    let email = Email::parse("User@Example.COM").unwrap();
    assert_eq!(email.expose(), "user@example.com");
}

#[test]
fn auth_ids_use_local_parse_error() {
    assert!(matches!(
        UserId::from_str("not-a-uuid"),
        Err(AuthIdParseError::InvalidUuid(_))
    ));
}

#[test]
fn sandbox_owner_classifies_reserved_local_and_platform_users() {
    assert_eq!(
        TEMPLATE_MANAGER_SANDBOX_OWNER.kind(),
        SandboxOwnerKind::TemplateManager
    );
    assert_eq!(BUILDER_SANDBOX_OWNER.kind(), SandboxOwnerKind::Builder);
    assert_eq!(ROOT_SANDBOX_OWNER.kind(), SandboxOwnerKind::Root);
    assert_eq!(DEPLOY_SANDBOX_OWNER.kind(), SandboxOwnerKind::Deploy);
    assert_eq!(DEV_SANDBOX_OWNER.kind(), SandboxOwnerKind::Dev);
    assert_eq!(ORPHANED_SANDBOX_OWNER.kind(), SandboxOwnerKind::Orphaned);

    let user_id = UserId::new();
    let owner = SandboxOwner::platform_user(user_id);
    assert_eq!(owner.kind(), SandboxOwnerKind::PlatformUser(user_id));

    let owner = SandboxOwner::parse("alice@local").unwrap();
    assert_eq!(owner.kind(), SandboxOwnerKind::Local(owner.clone()));
}

#[test]
fn sandbox_owner_kind_exposes_reserved_owner_labels() {
    assert_eq!(
        LOCAL_OPERATOR_SANDBOX_OWNER_KINDS
            .iter()
            .filter_map(SandboxOwnerKind::reserved_owner_label)
            .collect::<Vec<_>>(),
        vec!["root@local", "deploy@local", "dev@local"]
    );
    assert_eq!(
        format_reserved_sandbox_owner_kind_labels(LOCAL_OPERATOR_SANDBOX_OWNER_KINDS),
        "root@local, deploy@local, or dev@local"
    );

    let user_id = UserId::new();
    assert_eq!(
        SandboxOwnerKind::PlatformUser(user_id).reserved_owner_label(),
        None
    );
    let local = SandboxOwner::parse("alice@local").unwrap();
    assert_eq!(SandboxOwnerKind::Local(local).reserved_owner_label(), None);
}

#[test]
fn sandbox_owner_rejects_empty_controls_whitespace_and_long_values() {
    SandboxOwner::parse("").unwrap_err();
    SandboxOwner::parse("user name").unwrap_err();
    SandboxOwner::parse("user\nname").unwrap_err();
    SandboxOwner::parse("user\u{202e}@example.com").unwrap_err();
    SandboxOwner::parse("x".repeat(MAX_SANDBOX_OWNER_BYTES + 1)).unwrap_err();
}
