struct OneVar;

impl super::Environment for OneVar {
    fn var_os(&self, name: &str) -> Option<std::ffi::OsString> {
        (name == "SPACE_TEST_HOOK_PROBE").then(|| "7".into())
    }
}

/// Without the `test-hooks` feature a release binary has no way to read a
/// hook; with it, the override is visible.
#[test]
fn test_hook_is_visible_only_with_the_feature() {
    let value = super::test_hook(&OneVar, "SPACE_TEST_HOOK_PROBE");
    if cfg!(feature = "test-hooks") {
        assert_eq!(value.as_deref(), Some("7"));
    } else {
        assert_eq!(value, None);
    }
    assert_eq!(super::test_hook(&OneVar, "SPACE_OTHER"), None);
}
