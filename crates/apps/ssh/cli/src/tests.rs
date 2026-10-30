use super::openssh_set_env_value;

#[test]
fn native_ssh_uses_operator_credentials_and_a_sandbox_fenced_proxy() {
    use super::{Cli, native_ssh_command};
    use clap::Parser;
    use pplx_space_common_core::types::{GuestOperatingSystem, SandboxId};
    use std::ffi::OsString;

    let id: SandboxId = "00000000-0000-7000-8000-000000000001".parse().unwrap();
    for (os, explicit_user, expected_user) in [
        (GuestOperatingSystem::Windows, None, Some("Administrator")),
        (
            GuestOperatingSystem::Windows,
            Some("operator"),
            Some("operator"),
        ),
        (GuestOperatingSystem::Macos, None, None),
    ] {
        let mut input = vec!["space-ssh", "--identity-file", "/tmp/key with spaces"];
        if let Some(user) = explicit_user {
            input.extend(["--user", user]);
        }
        input.extend(["00000001", "hostname"]);
        let cli = Cli::try_parse_from(input).unwrap();
        let command = native_ssh_command(&cli, id, os);
        assert_eq!(command.get_program(), "ssh", "SSH must run as the operator");
        let mut expected: Vec<OsString> = [
            "-o",
            "ProxyCommand=sudo -n /space/bin/space-nodemgr guest-channel 00000000-0000-7000-8000-000000000001 --port 22",
            "-o", "StrictHostKeyChecking=yes",
            "-i", "/tmp/key with spaces",
        ].into_iter().map(Into::into).collect();
        if let Some(user) = expected_user {
            expected.extend([OsString::from("-l"), OsString::from(user)]);
        }
        expected.extend([
            OsString::from("space-00000000-0000-7000-8000-000000000001"),
            OsString::from("hostname"),
        ]);
        assert_eq!(command.get_args().collect::<Vec<_>>(), expected);
    }
}

#[test]
fn openssh_set_env_value_leaves_simple_values_unquoted() {
    assert_eq!(
        openssh_set_env_value("abc_123:/bin").as_deref(),
        Some("abc_123:/bin")
    );
}

#[test]
fn openssh_set_env_value_quotes_spaces_and_escapes_specials() {
    assert_eq!(
        openssh_set_env_value("hello world").as_deref(),
        Some("\"hello world\"")
    );
    assert_eq!(
        openssh_set_env_value("a\"b\\c").as_deref(),
        Some("\"a\\\"b\\\\c\"")
    );
}

#[test]
fn openssh_set_env_value_rejects_line_breaks() {
    assert_eq!(openssh_set_env_value("line 1\nline 2"), None);
    assert_eq!(openssh_set_env_value("line 1\r\nline 2"), None);
}

#[test]
fn linux_ssh_preserves_namespace_credentials_and_environment_arguments() {
    use super::{Cli, linux_ssh_command};
    use clap::Parser;
    use std::{collections::BTreeMap, ffi::OsString};
    for explicit in [false, true] {
        let args = if explicit {
            vec![
                "space-ssh",
                "--user",
                "root",
                "--identity-file",
                "/tmp/key with spaces",
                "01234567",
                "printf",
                "hello world",
            ]
        } else {
            vec!["space-ssh", "01234567", "printf", "hello world"]
        };
        let cli = Cli::try_parse_from(args).unwrap();
        let env = BTreeMap::from([
            ("SAFE".into(), "with spaces".into()),
            ("UNSAFE".into(), "line\nbreak".into()),
        ]);
        let command = linux_ssh_command(&cli, "sns-test", &env);
        assert_eq!(command.get_program(), "sudo");
        let expected: Vec<OsString> = [
            "nsenter",
            "--net=/var/run/netns/sns-test",
            "ssh",
            "-i",
            if explicit {
                "/tmp/key with spaces"
            } else {
                "/root/.ssh/sandbox_ed25519"
            },
            "-o",
            "StrictHostKeyChecking=no",
            "-o",
            "UserKnownHostsFile=/dev/null",
            "-o",
            "LogLevel=ERROR",
            "-l",
            if explicit { "root" } else { "user" },
            "-o",
            "SetEnv=SAFE=\"with spaces\"",
            "169.254.0.21",
            "printf",
            "hello world",
        ]
        .into_iter()
        .map(Into::into)
        .collect();
        assert_eq!(command.get_args().collect::<Vec<_>>(), expected);
    }
}
