use super::*;

/// `SshSession::new` insists the builder key exists on disk; argv shape does
/// not depend on that, so build the struct directly.
fn session() -> SshSession {
    SshSession {
        host: "192.0.2.1".to_string(),
        ns: "sns-test".to_string(),
        key_path: "/root/.ssh/sandbox_ed25519".to_string(),
    }
}

fn argv(cmd: &Command) -> Vec<String> {
    cmd.get_args()
        .map(|a| a.to_string_lossy().into_owned())
        .collect()
}

/// The login name reaches this function straight from a tenant's ECW `RUN` or
/// `USER` template step. Anywhere it can occupy an option position, a leading
/// `-` reconfigures the *local* ssh client (`ProxyCommand`) before the guest
/// is ever contacted.
#[test]
fn ssh_cmd_never_lets_the_user_become_a_local_option() {
    let hostile = "-oProxyCommand=touch /tmp/pwned #";
    let args = argv(&session().ssh_cmd(hostile, "echo hi"));

    let user_at = args
        .iter()
        .position(|a| a == hostile)
        .expect("login name must appear in argv");
    assert_eq!(
        args.get(user_at - 1).map(String::as_str),
        Some("-l"),
        "login name must be consumed as the value of -l, got argv {args:?}"
    );

    let terminator = args
        .iter()
        .position(|a| a == "--")
        .expect("option parsing must be terminated before the destination");
    assert!(
        terminator > user_at,
        "-- must follow the login name, got argv {args:?}"
    );
    assert_eq!(
        args.get(terminator + 1).map(String::as_str),
        Some("192.0.2.1"),
        "destination must be the trusted host, got argv {args:?}"
    );
    assert!(
        !args.iter().any(|a| a.starts_with('-') && a.contains('@')),
        "no argv element may be both option-shaped and carry the appended host: {args:?}"
    );
}

#[test]
fn ssh_cmd_still_runs_the_intended_command_for_a_normal_user() {
    let args = argv(&session().ssh_cmd("root", "echo hi"));
    let dashdash = args
        .iter()
        .position(|a| a == "--")
        .expect("option parsing must be terminated before the destination");
    assert_eq!(&args[dashdash + 1..], ["192.0.2.1", "echo hi"]);
    assert!(args.contains(&"-l".to_string()) && args.contains(&"root".to_string()));
}
#[test]
fn copy_stream_preserves_links_without_reading_their_targets() {
    use std::fs;
    use std::os::unix::fs::PermissionsExt;

    let temp = tempfile::tempdir().unwrap();
    let staging = tempfile::tempdir().unwrap();
    let escape_target = format!(
        "up/../{}/canary",
        temp.path().file_name().unwrap().to_str().unwrap()
    );
    fs::write(
        temp.path().join("canary"),
        b"host secret must not be transferred",
    )
    .unwrap();
    let archive = crate::tar_symlink_tests::tar_archive(&[
        (tar::EntryType::Symlink, "prefix/tree/up", ".."),
        (
            tar::EntryType::Symlink,
            "prefix/tree/escape",
            &escape_target,
        ),
        (
            tar::EntryType::Symlink,
            "prefix/tree/relative",
            "nested/file with ' quotes",
        ),
        (tar::EntryType::Symlink, "prefix/tree/dangling", "missing"),
        (tar::EntryType::Symlink, "prefix/tree/cycle", "cycle"),
        (
            tar::EntryType::Regular,
            "prefix/tree/nested/file with ' quotes",
            "payload",
        ),
    ]);
    let materialized = std::env::var("SPACE_COPY_SSH_SMOKE_KEY").ok().map(|_| {
        use sha2::{Digest, Sha256};
        use std::io::Write;

        let bytes = fs::read(archive.path()).unwrap();
        let hash = hex::encode(Sha256::digest(&bytes));
        let path = crate::blob::blob_path(&hash).unwrap();
        // The smoke runner gives this process a private mount of the blob cache.
        let mut blob = fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)
            .unwrap();
        blob.write_all(&bytes).unwrap();
        crate::materialize_copy_payload(&crate::CopyPayload::BlobSha256(hash), Some("prefix/tree"))
            .unwrap()
    });
    let tree = if let Some(payload) = &materialized {
        payload.path.clone()
    } else {
        crate::extract_tar_payload(archive.path(), staging.path(), Some("prefix/tree")).unwrap()
    };
    assert_eq!(
        fs::read(tree.join("escape")).unwrap(),
        b"host secret must not be transferred"
    );

    // Exercise the same producer and consumer as SSH, without needing a VM.
    for source in [
        &tree.join("escape"),
        &tree,
        &tree.join("nested/file with ' quotes"),
    ] {
        let guest = tempfile::Builder::new()
            .prefix("guest ' ")
            .tempdir()
            .unwrap();
        let copied = guest.path().join(source.file_name().unwrap());
        if let Ok(key_path) = std::env::var("SPACE_COPY_SSH_SMOKE_KEY") {
            let session = SshSession {
                host: "127.0.0.1".into(),
                ns: std::env::var("SPACE_COPY_SSH_SMOKE_NETNS").expect("isolated SSH namespace"),
                key_path,
            };
            if source == &tree {
                crate::copy_staged_payload_to_guest(&session, source, copied.to_str().unwrap())
                    .unwrap();
            } else {
                session
                    .copy(
                        source.to_str().unwrap(),
                        guest.path().to_str().unwrap(),
                        "root",
                    )
                    .unwrap();
            }
        } else {
            let mut receiver = Command::new("sh");
            receiver.args(["-c", &copy_unpack_command(guest.path().to_str().unwrap())]);
            send_copy_archive(source, receiver).unwrap();
        }

        if source == &tree {
            for name in ["up", "escape", "relative", "dangling", "cycle"] {
                assert_eq!(
                    fs::read_link(copied.join(name)).unwrap(),
                    fs::read_link(tree.join(name)).unwrap()
                );
            }
            assert_eq!(fs::read(copied.join("relative")).unwrap(), b"payload");
            assert_eq!(
                fs::metadata(copied.join("relative"))
                    .unwrap()
                    .permissions()
                    .mode()
                    & 0o777,
                0o755
            );
        } else if source.is_symlink() {
            assert!(
                copied.is_symlink(),
                "outside host sentinel was copied as a regular file"
            );
            assert_eq!(fs::read_link(copied).unwrap(), Path::new(&escape_target));
        } else {
            assert_eq!(fs::read(copied).unwrap(), b"payload");
        }
    }
}

#[test]
fn copy_stream_propagates_producer_and_consumer_failures() {
    let source = tempfile::NamedTempFile::new().unwrap();
    let mut receiver = Command::new("sh");
    receiver.args(["-c", "cat >/dev/null; exit 42"]);

    let result = send_copy_archive(source.path(), receiver);

    assert!(matches!(result, Err(SshError::CommandStatus(status)) if status.code() == Some(42)));

    let mut receiver = Command::new("sh");
    receiver.args(["-c", "cat >/dev/null"]);

    let result = send_copy_archive(&source.path().join("missing"), receiver);

    assert!(matches!(result, Err(SshError::CopyIo(_))));
}

#[tokio::test]
async fn bounded_exchange_closes_stdin_and_limits_output_and_time() {
    let command = |script: &str| {
        let mut command = tokio::process::Command::new("/bin/sh");
        command.args(["-c", script]);
        command
    };
    let payload = vec![b'x'; 128 * 1024];
    let result = super::exchange_command(
        command("cat"),
        &payload,
        payload.len(),
        Duration::from_secs(2),
    )
    .await
    .unwrap();
    assert_eq!(
        result, payload,
        "cat requires EOF after the complete declaration"
    );
    assert!(matches!(
        super::exchange_command(command("printf too-long"), &[], 2, Duration::from_secs(2)).await,
        Err(SshError::BoundedOperation)
    ));
    assert!(matches!(
        super::exchange_command(command("exec sleep 10"), &[], 0, Duration::from_millis(20)).await,
        Err(SshError::BoundedOperation)
    ));
}

fn env_map(values: &[(&str, &str)]) -> pplx_space_common_core::types::EnvMap {
    pplx_space_common_core::types::EnvMap::try_from(
        values
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect::<std::collections::BTreeMap<_, _>>(),
    )
    .unwrap()
}

#[test]
fn command_execution_preserves_shell_environment() {
    let dir = tempfile::tempdir().unwrap();
    let value = "synthetic ' \" $(touch injected) `touch injected`\nsecond line";
    let startup = dir.path().join("bash-env");
    std::fs::write(&startup, "export STARTUP=loaded\n").unwrap();
    let env = env_map(&[
        ("TOKEN", value),
        ("BASH_ENV", startup.to_str().unwrap()),
        ("UID", "12345"),
        ("EUID", "12345"),
    ]);
    let env = StepEnvironment::from_env(&env);
    let ssh = std::env::var("SPACE_BUILD_SSH_SMOKE_KEY")
        .ok()
        .map(|key_path| SshSession {
            host: "127.0.0.1".into(),
            ns: std::env::var("SPACE_BUILD_SSH_SMOKE_NETNS").expect("isolated SSH namespace"),
            key_path,
        });
    let cases = [
        (
            "extglob",
            "shopt -s extglob\nprintf '%s' @(hello)",
            Some("@(hello)"),
        ),
        (
            "alias",
            "shopt -s expand_aliases\nalias greet='printf hello'\ngreet",
            Some("hello"),
        ),
        ("startup", "printf %s \"$STARTUP\"", Some("loaded")),
        ("readonly", "printf '%s:%s' \"$UID\" \"$EUID\"", None),
        ("quotes", "printf %s \"$TOKEN\"; cat", Some(value)),
    ];
    for (name, command, expected) in cases {
        let output = dir.path().join(name);
        // The old env ... bash -c path is the oracle for Bash's automatic variables.
        let mut oracle = Command::new("bash");
        oracle
            .args(["-c", command])
            .env_clear()
            .env("PATH", "/usr/bin:/bin")
            .stdin(Stdio::null());
        for (key, value) in env.iter() {
            oracle.env(key.as_str(), value.expose());
        }
        let oracle = oracle.output().unwrap();
        assert!(oracle.status.success());
        if let Some(expected) = expected {
            assert_eq!(oracle.stdout, expected.as_bytes(), "{name}");
        }
        let command = format!(
            "cd {}\n{command}",
            shell_quote(dir.path().to_str().unwrap())
        );
        let command = format!(
            "exec > {} 2> {}\n{command}",
            shell_quote(output.to_str().unwrap()),
            shell_quote(dir.path().join("stderr").to_str().unwrap())
        );
        if let Some(ssh) = &ssh {
            ssh.exec(&command, "root", &env).unwrap();
        } else {
            let mut receiver = Command::new("bash");
            receiver.arg("-s");
            send_command_script(receiver, &command_script(&command, &env)).unwrap();
        }
        assert_eq!(std::fs::read(output).unwrap(), oracle.stdout, "{name}");
        assert_eq!(
            std::fs::read(dir.path().join("stderr")).unwrap(),
            oracle.stderr,
            "{name}"
        );
        assert!(!dir.path().join("injected").exists());
    }
    // No stream implementation may swallow the command's failure.
    let failure = if let Some(ssh) = &ssh {
        ssh.exec("exit 42", "root", &env)
    } else {
        let mut receiver = Command::new("bash");
        receiver.arg("-s");
        send_command_script(receiver, &command_script("exit 42", &env))
    };
    assert!(matches!(failure, Err(SshError::CommandStatus(status)) if status.code() == Some(42)));

    if let Some(ssh) = ssh {
        assert_real_ssh_boundary(ssh, dir.path(), &env, value);
    }
}

fn assert_real_ssh_boundary(ssh: SshSession, dir: &Path, env: &StepEnvironment<'_>, secret: &str) {
    // The smoke runner uses a dash login shell and an ambient SendEnv policy.
    let marker = dir.join("direct");
    ssh.exec(&format!("test -z \"${{BASH_VERSION-}}\" && test -z \"${{BUILD_SSH_AMBIENT_SECRET-}}\" && printf direct > {}", shell_quote(marker.to_str().unwrap())), "root", &StepEnvironment::default()).unwrap();
    assert_eq!(std::fs::read(marker).unwrap(), b"direct");
    let ready = dir.join("ready");
    let release = dir.join("release");
    let command = format!(
        "test -z \"${{BUILD_SSH_AMBIENT_SECRET-}}\" && touch {} && while ! test -e {}; do sleep 0.02; done",
        shell_quote(ready.to_str().unwrap()),
        shell_quote(release.to_str().unwrap())
    );
    std::thread::scope(|scope| {
        let running = scope.spawn(|| ssh.exec(&command, "root", env));
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while !ready.exists() && std::time::Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(10));
        }
        // Release even on assertion failure so the child cannot strand the test.
        let observed = std::fs::read_dir("/proc")
            .unwrap()
            .filter_map(Result::ok)
            .filter_map(|entry| std::fs::read(entry.path().join("cmdline")).ok())
            .filter(|args| {
                args.split(|b| *b == 0)
                    .next()
                    .is_some_and(|arg| arg == b"ssh" || arg.ends_with(b"/ssh") || arg == b"nsenter")
            })
            .collect::<Vec<_>>();
        std::fs::write(&release, b"release").unwrap();
        assert!(ready.exists(), "SSH command never became ready");
        assert!(!observed.is_empty(), "must observe a live host SSH process");
        for args in observed {
            assert!(
                !String::from_utf8_lossy(&args).contains(secret),
                "export entered host argv"
            );
        }
        running.join().unwrap().unwrap();
    });
}
