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
