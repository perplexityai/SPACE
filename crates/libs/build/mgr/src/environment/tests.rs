use super::*;

#[test]
fn references_resolve_only_operator_exports_under_guest_names() {
    let host = HashMap::from([
        ("SPACE_INTERNAL_TOKEN", "dummy-daemon-only"),
        ("PACKAGE_TOKEN", "dummy-unexported-value"),
        (
            "SPACE_BUILD_EXPORT_PACKAGE_TOKEN",
            "exported ' \" $()\nvalue",
        ),
        ("SPACE_BUILD_EXPORT_EMPTY", ""),
    ]);
    let lookup = |name: &str| {
        host.get(name)
            .map(|v| (*v).into())
            .ok_or(std::env::VarError::NotPresent)
    };

    let exports = resolve_refs_with(&["PACKAGE_TOKEN".into(), "EMPTY".into()], lookup).unwrap();

    assert_eq!(exports["PACKAGE_TOKEN"], "exported ' \" $()\nvalue");
    assert_eq!(exports["EMPTY"], "");
    assert_eq!(exports.len(), 2);
    for name in [
        "SPACE_INTERNAL_TOKEN",
        "MISSING",
        "SPACE_BUILD_EXPORT_PACKAGE_TOKEN",
    ] {
        let error = resolve_refs_with(&[name.into()], lookup).unwrap_err();
        assert!(matches!(error, BuildEnvironmentError::NotExported { .. }));
        assert!(!format!("{error:?}").contains("dummy-daemon-only"));
    }
}

#[test]
fn invalid_names_never_reach_the_environment_lookup() {
    for name in [
        "",
        "1TOKEN",
        "A=B",
        "A B",
        "A'",
        "A\"",
        "A;touch x",
        "$(id)",
        "A\nB",
        "A\0B",
        "é",
    ] {
        let result = resolve_refs_with(&[name.into()], |_| {
            panic!("invalid reference was looked up")
        });
        assert!(matches!(result, Err(BuildEnvironmentError::InvalidName)));
    }
    for name in ["A", "a_b9", "_TOKEN"] {
        validate_name(name).unwrap();
    }
}

#[test]
fn invalid_export_values_are_not_included_in_errors() {
    let result = resolve_refs_with(&["TOKEN".into()], |_| Ok("secret\0value".into()));

    let error = result.unwrap_err();
    assert!(matches!(error, BuildEnvironmentError::InvalidValue { .. }));
    assert!(!format!("{error:?}").contains("secret"));
}

#[cfg(unix)]
#[test]
fn non_unicode_exports_are_rejected_without_their_value() {
    use std::os::unix::ffi::OsStringExt;
    let result = resolve_refs_with(&["TOKEN".into()], |_| {
        Err(std::env::VarError::NotUnicode(
            std::ffi::OsString::from_vec(b"secret\xff".to_vec()),
        ))
    });

    let error = result.unwrap_err();
    assert!(matches!(error, BuildEnvironmentError::InvalidValue { .. }));
    assert!(!format!("{error:?}").contains("secret"));
}
