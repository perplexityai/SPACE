use super::*;

#[test]
fn sandbox_selector_deduplicates_lease_generations_not_sandboxes() {
    let id = "01900000-0000-7000-8000-000012345678";
    let leases = vec![
        NetworkLease {
            sandbox_id: id.into(),
            ..Default::default()
        },
        NetworkLease {
            sandbox_id: id.into(),
            ..Default::default()
        },
        NetworkLease::default(),
    ];
    for selector in [id, "12345678"] {
        assert_eq!(
            resolve_selector_from_leases(&leases, &selector.parse().unwrap()).unwrap(),
            id
        );
    }
    let mut ambiguous = leases;
    ambiguous.push(NetworkLease {
        sandbox_id: "01900000-0000-7000-8001-000012345678".into(),
        ..Default::default()
    });
    assert!(
        resolve_selector_from_leases(&ambiguous, &"12345678".parse().unwrap())
            .unwrap_err()
            .contains("Ambiguous")
    );
}

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
