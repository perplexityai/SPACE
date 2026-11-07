use super::*;

#[test]
fn guest_support_is_directional_and_requires_consistent_identity() {
    for (version, family, supported) in [
        (Some("0.1.0"), Some("0.1"), true),
        (Some("0.1.4"), Some("0.1"), true),
        (Some("0.7.0"), Some("0.7"), true),
        (Some("0.8.0"), Some("0.8"), true),
        (Some("0.8.1"), Some("0.8"), true),
        (Some("0.8.99+build.1"), Some("0.8"), true),
        (Some("0.0.9"), Some("0.0.9"), false),
        (Some("0.1.0-rc.1"), Some("0.1"), false),
        (Some("0.8.1-rc.1"), Some("0.8"), false),
        (Some("0.9.0"), Some("0.9"), false),
        (Some("1.0.0"), Some("1"), false),
        (Some("not-semver"), Some("0.8"), false),
        (Some("0.1"), Some("0.1"), false),
        (Some("0.1.0"), Some("0.8"), false),
        (Some("0.8.0"), Some("0.1"), false),
        (Some("0.8.0"), Some(""), false),
        (Some("0.8.0"), None, false),
        (None, Some("0.1"), false),
        (None, None, false),
    ] {
        let result = validate_spaced_guest_compatibility(version, family);
        assert_eq!(
            result.is_ok(),
            supported,
            "{version:?} / {family:?}: {result:?}"
        );
    }
}
