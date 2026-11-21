use super::*;

fn uri(value: &str) -> StorageUri {
    StorageUri::parse(value).expect("storage uri")
}

#[test]
fn an_unconfigured_policy_admits_local_endpoints() {
    let local = uri("s3://local-templates/x?region=us-east-1&endpoint=http://localhost:9000");
    for policy in [
        TrustedStorageBuckets::default(),
        TrustedStorageBuckets::from_list(" , "),
    ] {
        policy.check(&local).expect("unconfigured");
    }
}

#[test]
fn a_configured_policy_admits_only_exact_buckets_without_endpoints() {
    let policy = TrustedStorageBuckets::from_list(" trusted-templates, mirror-templates, ,");
    for (bucket, allowed) in [
        ("trusted-templates", true),
        ("mirror-templates", true),
        ("trusted-templates-evil", false),
        ("evil-trusted-templates", false),
    ] {
        assert_eq!(
            policy
                .check(&uri(&format!("s3://{bucket}/x?region=us-east-1")))
                .is_ok(),
            allowed,
            "{bucket}"
        );
    }
    assert_eq!(
        policy.check(&uri(
            "s3://trusted-templates/x?region=us-east-1&endpoint=https://elsewhere.example.com"
        )),
        Err(StorageOriginRejection::EndpointOverride)
    );
}
