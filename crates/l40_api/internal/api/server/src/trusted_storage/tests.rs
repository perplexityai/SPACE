use super::*;
use pplx_space_test_env::TestEnv;
use rstest::rstest;

#[rstest]
#[case(None)]
#[case(Some(""))]
fn unconfigured_policy_preserves_local_storage_endpoints(#[case] buckets: Option<&str>) {
    let mut env = TestEnv::default();
    if let Some(value) = buckets {
        env.set(TRUSTED_BUCKETS_ENV, value);
    }
    let policy = TrustedTemplateStorageBuckets::from_environment(&env).unwrap();
    let local =
        StorageUri::parse("s3://local-templates/x?region=us-east-1&endpoint=http://localhost:9000")
            .unwrap();

    policy
        .check(&local)
        .expect("unconfigured policy permits local endpoints");
}

#[test]
fn configured_buckets_are_trimmed_and_captured_at_startup() {
    let mut env = TestEnv::default();
    env.set(
        TRUSTED_BUCKETS_ENV,
        " trusted-templates, mirror-templates, , trusted-templates ",
    );
    let policy = TrustedTemplateStorageBuckets::from_environment(&env).unwrap();
    env.remove(TRUSTED_BUCKETS_ENV);

    for (bucket, allowed) in [
        ("trusted-templates", true),
        ("mirror-templates", true),
        ("trusted-templates-evil", false),
        ("evil-trusted-templates", false),
    ] {
        let uri = StorageUri::parse(&format!("s3://{bucket}/x?region=us-east-1")).unwrap();
        assert_eq!(policy.check(&uri).is_ok(), allowed);
    }
}
