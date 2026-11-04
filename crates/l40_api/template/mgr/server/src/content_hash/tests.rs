use super::*;
use pplx_space_test_env::TestEnv;

#[test]
fn production_algorithm_defaults_to_blake3_and_rejects_invalid_overrides() {
    use std::os::unix::ffi::OsStringExt;
    for (value, expected) in [
        (None, Some(HashAlg::Blake3)),
        (Some("sha256".into()), Some(HashAlg::Sha256)),
        (Some("blake3".into()), Some(HashAlg::Blake3)),
        (Some("".into()), None),
        (Some("sha265".into()), None),
        (Some("SHA256".into()), None),
        (Some(std::ffi::OsString::from_vec(vec![0xff])), None),
    ] {
        let mut environment = TestEnv::default();
        if let Some(value) = value {
            environment.set(PRODUCTION_ALGORITHM_ENV, value);
        }
        assert_eq!(
            production_algorithm_from_environment(&environment).ok(),
            expected
        );
    }
}
