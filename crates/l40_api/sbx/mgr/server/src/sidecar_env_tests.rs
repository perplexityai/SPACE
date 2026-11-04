use pplx_space_common_svc::space_api_route_ed25519::SPACE_API_ROUTE_ED25519_VERIFICATION_BUNDLE_PATH_ENV;

#[test]
fn net_gateway_route_env_forwards_verification_bundle_path() {
    let mut source = pplx_space_test_env::TestEnv::default();
    source.set(
        SPACE_API_ROUTE_ED25519_VERIFICATION_BUNDLE_PATH_ENV,
        "/run/route/verification-bundle.json",
    );
    let env = super::net_gateway_env_from_environment(&source);

    assert_eq!(
        env.get(SPACE_API_ROUTE_ED25519_VERIFICATION_BUNDLE_PATH_ENV)
            .map(String::as_str),
        Some("/run/route/verification-bundle.json")
    );
}
