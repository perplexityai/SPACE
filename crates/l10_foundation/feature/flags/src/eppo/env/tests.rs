use super::*;

#[test]
fn eppo_sdk_key_from_env_returns_non_empty_sdk_key() {
    {
        let mut vars: Vec<(&str, Option<&std::ffi::OsStr>)> =
            [EPPO_SDK_KEY_ENV].iter().map(|key| (*key, None)).collect();
        vars.extend([(EPPO_SDK_KEY_ENV, Some(std::ffi::OsStr::new("sdk-key")))]);
        if !pplx_space_test_env::enter_test_process(&vars) {
            return;
        }

        assert_eq!(eppo_sdk_key_from_env(), Some("sdk-key".to_string()));
    };
}

#[test]
fn eppo_sdk_key_from_env_ignores_empty_sdk_key() {
    {
        let mut vars: Vec<(&str, Option<&std::ffi::OsStr>)> =
            [EPPO_SDK_KEY_ENV].iter().map(|key| (*key, None)).collect();
        vars.extend([(EPPO_SDK_KEY_ENV, Some(std::ffi::OsStr::new("")))]);
        if !pplx_space_test_env::enter_test_process(&vars) {
            return;
        }

        assert_eq!(eppo_sdk_key_from_env(), None);
    };
}

#[test]
fn eppo_sdk_key_from_env_returns_none_when_unset() {
    {
        let vars: Vec<(&str, Option<&std::ffi::OsStr>)> =
            [EPPO_SDK_KEY_ENV].iter().map(|key| (*key, None)).collect();

        if !pplx_space_test_env::enter_test_process(&vars) {
            return;
        }

        assert_eq!(eppo_sdk_key_from_env(), None);
    };
}
