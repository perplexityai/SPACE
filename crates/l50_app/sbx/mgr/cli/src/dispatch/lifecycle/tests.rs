use super::*;

#[test]
fn local_restore_delegates_direct_identity_to_sandbox_manager() {
    let request = local_restore_request(
        "sandbox-id".to_string(),
        true,
        Some(60),
        Some("s3://bucket/object".to_string()),
    );

    assert_eq!(request.route_identity, None);
}

#[test]
fn synchronous_commands_accept_legacy_blocking_flags() {
    for command in ["pause", "checkpoint", "snapshot"] {
        for blocking in [false, true] {
            let mut args = vec![
                "space-sbxmgr",
                command,
                "00000000-0000-0000-0000-000000000001",
            ];
            if blocking {
                args.push("--blocking");
            }
            let parsed = Cli::try_parse_from(args).expect("legacy scripts remain accepted");
            let parsed_blocking = match parsed.command {
                Commands::Pause { blocking, .. }
                | Commands::Checkpoint { blocking, .. }
                | Commands::Snapshot { blocking, .. } => blocking,
                _ => panic!("unexpected command"),
            };
            assert_eq!(parsed_blocking, blocking, "{command}");
        }
    }
}
