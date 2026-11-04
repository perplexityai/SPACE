use super::*;

#[test]
fn relkind_parses_catalog_values() {
    assert_eq!(RelKind::try_from("r").unwrap(), RelKind::Table);
    assert_eq!(RelKind::try_from("p").unwrap().alter_keyword(), "TABLE");
    assert_eq!(RelKind::try_from("S").unwrap().alter_keyword(), "SEQUENCE");
    assert!(RelKind::try_from("S").unwrap().is_sequence());
    assert_eq!(RelKind::try_from("v").unwrap().alter_keyword(), "VIEW");
    assert_eq!(
        RelKind::try_from("m").unwrap().alter_keyword(),
        "MATERIALIZED VIEW"
    );
    RelKind::try_from("i").expect_err("index relkind must be rejected");
}

#[test]
fn quote_literal_escapes_single_quotes() {
    assert_eq!(quote_literal("pa'ss"), "'pa''ss'");
    assert_eq!(quote_literal("plain"), "'plain'");
}

#[test]
fn quote_ident_escapes_double_quotes() {
    assert_eq!(quote_ident("weird\"name"), "\"weird\"\"name\"");
    assert_eq!(quote_ident("space_user"), "\"space_user\"");
}

#[test]
fn missing_password_envs_lists_unset_roles() {
    let passwords = BootstrapPasswords {
        superadmin: Some("a".to_string()),
        user: None,
        ro: None,
        migrator: Some("m".to_string()),
        creds: None,
    };
    assert_eq!(
        passwords.missing_password_envs(),
        vec![
            "SPACE_DB_BOOTSTRAP_USER_PASSWORD",
            "SPACE_DB_BOOTSTRAP_RO_PASSWORD",
            "SPACE_DB_BOOTSTRAP_CREDS_PASSWORD",
        ]
    );
    assert!(BootstrapPasswords::default().missing_password_envs().len() == 5);
}

mod postgres_bootstrap_tests;

/// `SPACE_DB_APP_LOCK_TIMEOUT` is the provisioning wrappers' input: an explicit
/// `0` must reach the bootstrap as `Some("0")` (reset), never as an omission.
#[test]
fn app_lock_timeout_env_zero_is_an_explicit_reset() {
    use clap::Parser as _;
    let zero = std::ffi::OsStr::new("0");
    if !pplx_space_test_env::enter_test_process(&[("SPACE_DB_APP_LOCK_TIMEOUT", Some(zero))]) {
        return;
    }
    let args = BootstrapRolesArgs::try_parse_from([
        "bootstrap-roles",
        "--database-url",
        "postgresql://example.invalid/synthetic",
    ])
    .expect("parse");
    assert_eq!(args.app_lock_timeout.as_deref(), Some("0"));
}

/// Omitting both the flag and the env leaves role lock timeouts untouched.
#[test]
fn app_lock_timeout_omission_is_none() {
    use clap::Parser as _;
    if !pplx_space_test_env::enter_test_process(&[("SPACE_DB_APP_LOCK_TIMEOUT", None)]) {
        return;
    }
    let args = BootstrapRolesArgs::try_parse_from([
        "bootstrap-roles",
        "--database-url",
        "postgresql://example.invalid/synthetic",
    ])
    .expect("parse");
    assert_eq!(args.app_lock_timeout, None);
}
