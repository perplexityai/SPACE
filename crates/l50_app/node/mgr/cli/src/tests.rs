use super::*;

#[test]
fn command_errors_are_fallible_and_preserve_operator_guidance() {
    for (input, expected) in [("tcp", NetworkProtocol::Tcp), ("udp", NetworkProtocol::Udp)] {
        assert_eq!(parse_protocol(input).unwrap(), expected);
    }
    assert_eq!(
        parse_protocol("sctp").unwrap_err(),
        "ERROR: protocol must be tcp or udp, got sctp"
    );
    for (port, all, valid) in [
        (Some(80), false, true),
        (None, true, true),
        (Some(0), false, false),
        (None, false, false),
    ] {
        assert_eq!(resolve_optional_port(port, all, "deny").is_ok(), valid);
    }
    parse_lease_sandbox_id(&NetworkLease {
        sandbox_id: "malformed".into(),
        ..Default::default()
    })
    .unwrap_err();
    let denied = cleanup_error(
        "cleanup",
        tonic::Status::permission_denied("not authorized"),
    );
    assert!(denied.starts_with("ERROR: "));
    assert!(denied.contains("sudo space-nodemgr cleanup"));
    assert!(!cleanup_error("cleanup", tonic::Status::unavailable("retry")).contains("hint:"));
}

#[test]
fn selectors_count_sandboxes_not_retiring_execution_leases() {
    let sandbox = "019d84bd-280d-7331-99c6-b63312345678";
    let other = "019d84bd-280d-7331-99c6-b63412345678";
    let lease = |id: &str| NetworkLease {
        sandbox_id: id.into(),
        ..Default::default()
    };
    for selector in [sandbox, "12345678"] {
        assert_eq!(
            resolve_lease_selector(
                vec![lease(sandbox), lease(sandbox)],
                &selector.parse().unwrap()
            )
            .unwrap(),
            sandbox
        );
    }
    resolve_lease_selector(
        vec![lease(sandbox), lease(other)],
        &"12345678".parse().unwrap(),
    )
    .unwrap_err();
    assert_eq!(
        resolve_lease_selector(vec![], &sandbox.parse().unwrap()).unwrap(),
        sandbox
    );
}
