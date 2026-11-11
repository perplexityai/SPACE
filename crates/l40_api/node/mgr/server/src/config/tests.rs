use super::*;

#[test]
fn deployment_name_does_not_authorize_execution() {
    if !pplx_space_test_env::enter_test_process(&[
        ("SPACE_ENV", Some(std::ffi::OsStr::new("staging"))),
        ("SPACE_ALLOWED_EXECUTION_ENVIRONMENTS", None),
        ("SPACE_DEFAULT_EXECUTION_ENVIRONMENT", None),
    ]) {
        return;
    }
    let settings = ServiceSettings::from_env().unwrap();
    let catalog = settings.execution_policy.catalog();
    if let Some(initial) = catalog.iter().find(|entry| entry.name() == "e0") {
        assert_eq!(
            settings
                .execution_policy
                .allowed_references()
                .collect::<Vec<_>>(),
            vec![initial.definition().reference()]
        );
    } else {
        assert_eq!(settings.execution_policy.default_reference(), None);
    }
}

#[test]
fn malformed_policy_prevents_startup() {
    if !pplx_space_test_env::enter_test_process(&[
        (
            "SPACE_ALLOWED_EXECUTION_ENVIRONMENTS",
            Some(std::ffi::OsStr::new("unknown")),
        ),
        ("SPACE_DEFAULT_EXECUTION_ENVIRONMENT", None),
    ]) {
        return;
    }
    ServiceSettings::from_env().unwrap_err();
}

#[test]
fn candidate_catalog_failure_preserves_all_spellings_of_initial_policy() {
    use pplx_space_common_core::types::Arch;
    use pplx_space_node_runtime_core::catalog::CatalogError;
    let resolve = |allowed, default| {
        execution_policy_with_catalog(
            allowed,
            default,
            Arch::X86_64,
            Err(CatalogError::MissingEnvironmentPins {
                runtime: "1.17.0",
                kernel: "6.18.44",
            }),
        )
    };
    let implicit = resolve(None, None).unwrap();
    for allowed in ["e0,legacy", "legacy,e0"] {
        for default in [None, Some("e0")] {
            let explicit = resolve(Some(allowed), default).unwrap();
            assert_eq!(
                explicit.allowed_references().collect::<Vec<_>>(),
                implicit.allowed_references().collect::<Vec<_>>()
            );
            assert_eq!(explicit.default_reference(), implicit.default_reference());
            assert!(explicit.legacy_unbound_allowed());
        }
    }
    assert!(!resolve(Some("e0"), None).unwrap().legacy_unbound_allowed());
    for allowed in ["e0,fc-1.17.0-linux-6.1.155", "e0,legacy,legacy", "e0,"] {
        resolve(Some(allowed), None).unwrap_err();
    }
}
