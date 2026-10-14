use super::*;

fn cli(extra: &[&str]) -> Cli {
    Cli::try_parse_from(
        [
            "space-netgtw-svc",
            "--socket",
            "/tmp/gateway-control.sock",
            "--sandbox-id",
            "019d84bd-280d-7331-99c6-b63377f3a3a2",
        ]
        .into_iter()
        .chain(extra.iter().copied()),
    )
    .unwrap()
}

#[test]
fn route_hints_remain_optional_and_partial_at_startup() {
    let absent = cli(&[]).into_config().unwrap();
    assert!(absent.route_identity.is_none());

    // Warm-pool policy state is independent of a partially enriched execution
    // identity. Parsing it must not require a complete ingress permit.
    let partial = cli(&[
        "--no-identity",
        "--route-generation",
        "7",
        "--pod-uid",
        "test-pod",
    ])
    .into_config()
    .unwrap();
    assert_eq!(partial.initial_policy_state, InitialPolicyState::NoIdentity);
    let identity = partial.route_identity.unwrap();
    assert_eq!(identity.sandbox_id, Some(partial.sandbox_id));
    assert!(identity.substrate_kind.is_none());
    assert_eq!(
        identity.effective_substrate_kind(),
        Some(NodeManagedBy::Kubernetes)
    );
    identity.try_into_complete().unwrap_err();

    // The CLI accepts u64, while the route wire vocabulary uses i64. Reject
    // overflow here instead of truncating it or passing an invalid generation.
    cli(&["--execution-generation", "18446744073709551615"])
        .into_config()
        .unwrap_err();
}
