use super::*;
use std::io::Write;
use std::os::unix::fs::{MetadataExt, symlink};
use std::path::PathBuf;

fn receive_fixture() -> Option<PathBuf> {
    const FIXTURE: &str = "SPACE_NODEMGR_RECEIVE_TEST_BTRFS_DIR";
    if let Some(path) = std::env::var_os(FIXTURE).filter(|path| !path.is_empty()) {
        return Some(path.into());
    }
    let required = std::env::var_os(pplx_space_test_env::REQUIRE_TEST_PRIVILEGE)
        .is_some_and(|value| !value.is_empty() && value != "0");
    assert!(
        !required,
        "CI requires receive coverage: set {FIXTURE} through the disposable Btrfs fixture wrapper"
    );
    eprintln!("SKIPPED: set {FIXTURE} to a disposable btrfs directory");
    None
}

#[test]
#[ignore = "requires root and an explicitly supplied disposable btrfs fixture directory"]
fn receive_keeps_destination_identity_through_scrub_and_publication() {
    let Some(fixture) = receive_fixture() else {
        return;
    };
    assert!(nix::unistd::geteuid().is_root());
    let temp = tempfile::tempdir_in(fixture).unwrap();
    let fixture = temp.path().canonicalize().unwrap();
    let parent = trusted_paths::open_directory(&fixture).unwrap();
    let host_tmp = std::fs::metadata("/tmp").unwrap();
    // Use the real incremental stream: receiving must still resolve the
    // template outside the destination, not silently fall back to a full copy.
    btrfs::create(&parent, "template".as_ref()).unwrap();
    let template = directory(&parent, "template".as_ref()).unwrap();
    std::fs::write(fixture.join("template/rootfs.ext4"), b"template").unwrap();
    btrfs::set_readonly(&template, true).unwrap();
    let result = std::panic::catch_unwind(|| {
        for replacement in [
            "unchanged",
            "symlink",
            "directory",
            "during-symlink",
            "during-directory",
            "cancel",
        ] {
            let case = fixture.join(replacement);
            std::fs::create_dir(&case).unwrap();
            let case_fd = trusted_paths::open_directory(&case).unwrap();
            btrfs::snapshot(&template, &case_fd, "source".as_ref(), false).unwrap();
            std::fs::write(case.join("source/rootfs.ext4"), b"restored data").unwrap();
            let source = directory(&case_fd, "source".as_ref()).unwrap();
            btrfs::snapshot(&source, &case_fd, "instance.ro".as_ref(), true).unwrap();
            let snapshot = directory(&case_fd, "instance.ro".as_ref()).unwrap();
            let stream = File::create(case.join("stream")).unwrap();
            btrfs::send(&template, &snapshot, &stream).unwrap();
            drop(snapshot);
            remove_at(&case_fd, "instance.ro".as_ref()).unwrap();
            let destination = case.join("destination");
            std::fs::create_dir(&destination).unwrap();
            std::fs::create_dir(destination.join("instance")).unwrap();
            std::fs::write(destination.join("instance/stale"), b"scrub me").unwrap();
            let receive = ReceiveRoot::open(&destination).unwrap();
            // Cancellation before admission cannot erase the old instance.
            let cancelled = CancellationToken::new();
            cancelled.cancel();
            assert_eq!(
                receive
                    .receive(File::open(case.join("stream")).unwrap(), Some(&cancelled))
                    .unwrap_err()
                    .code(),
                tonic::Code::Cancelled,
            );
            assert!(destination.join("instance/stale").exists());

            // Validation fails before scrub, so a partial stream cannot erase
            // the previous instance or create a receive landing.
            let bytes = std::fs::read(case.join("stream")).unwrap();
            std::fs::write(case.join("partial"), &bytes[..bytes.len() / 2]).unwrap();
            receive
                .receive(File::open(case.join("partial")).unwrap(), None)
                .unwrap_err();
            assert!(destination.join("instance/stale").exists());
            assert!(!destination.join("instance.ro").exists());
            remove_at(&receive.directory, "instance.ro".as_ref()).unwrap();
            let mut original = destination.clone();
            let result = if replacement.starts_with("during-") || replacement == "cancel" {
                // Stop the real receiver after its first SNAPSHOT command,
                // not merely before exec: realpath-based pseudo-pinning can
                // pass the earlier replacement cases and still fail here.
                let (input, output) = nix::unistd::pipe2(nix::fcntl::OFlag::O_CLOEXEC).unwrap();
                let mut output = File::from(output);
                let cancel = CancellationToken::new();
                std::thread::scope(|scope| {
                    // Admission now requires a complete immutable file. Test
                    // destination pinning during exec separately, feeding the
                    // already validated fixture through a controllable pipe.
                    ValidatedStream::prepare(
                        &receive.directory,
                        &File::open(case.join("stream")).unwrap(),
                        None,
                        std::time::Instant::now() + crate::host_command::MUTATION_BUDGET,
                    )
                    .unwrap();
                    let receiver = scope.spawn(|| {
                        receive.receive_validated(ValidatedStream(File::from(input)), Some(&cancel))
                    });
                    // Protocol v1: 17-byte stream header, 10-byte command
                    // header, then the first command's declared payload.
                    let first = 27 + u32::from_le_bytes(bytes[17..21].try_into().unwrap()) as usize;
                    output.write_all(&bytes[..first]).unwrap();
                    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
                    while !destination.join("instance.ro").exists()
                        && !receiver.is_finished()
                        && std::time::Instant::now() < deadline
                    {
                        std::thread::sleep(std::time::Duration::from_millis(5));
                    }
                    let started = destination.join("instance.ro").exists();
                    if started {
                        if replacement == "cancel" {
                            cancel.cancel();
                        } else {
                            original =
                                replace_destination(&destination, replacement.ends_with("symlink"))
                                    .unwrap();
                            output.write_all(&bytes[first..]).unwrap();
                        }
                    }
                    drop(output);
                    let result = receiver.join().unwrap();
                    assert!(started, "receiver did not create its landing: {result:?}");
                    result
                })
            } else {
                if replacement != "unchanged" {
                    original = replace_destination(&destination, replacement == "symlink").unwrap();
                }
                receive.receive(File::open(case.join("stream")).unwrap(), None)
            };
            if replacement.ends_with("symlink") || replacement.ends_with("directory") {
                assert!(
                    std::fs::read_dir(&destination).unwrap().next().is_none(),
                    "{replacement}: receive mutated the replacement directory: {result:?}"
                );
            }
            if replacement == "cancel" {
                result.unwrap_err();
                assert!(!original.join("instance").exists());
                receive
                    .receive(File::open(case.join("stream")).unwrap(), None)
                    .unwrap();
            } else {
                result.unwrap();
            }
            assert_eq!(
                std::fs::read(original.join("instance/rootfs.ext4")).unwrap(),
                b"restored data"
            );
            assert!(!original.join("instance/stale").exists());
            assert!(!original.join("instance.ro").exists());
        }
        let after = std::fs::metadata("/tmp").unwrap();
        assert_eq!((host_tmp.dev(), host_tmp.ino()), (after.dev(), after.ino()));
    });
    // A deliberately failing red run must not strand subvolumes either.
    for name in super::super::entries(&parent).unwrap() {
        remove_at(&parent, &name).unwrap();
    }
    if let Err(panic) = result {
        std::panic::resume_unwind(panic);
    }
}

/// A stream an older node-manager stored after the kernel restarted its send
/// (its partial output, then a complete stream) restores from the complete
/// stream; a valid stream receives as before; a stream with no complete part
/// still fails.
#[test]
#[ignore = "requires root and an explicitly supplied disposable btrfs fixture directory"]
fn a_restarted_send_restores_from_its_complete_stream() {
    let Some(fixture) = receive_fixture() else {
        return;
    };
    assert!(nix::unistd::geteuid().is_root());
    let temp = tempfile::tempdir_in(fixture).unwrap();
    let fixture = temp.path().canonicalize().unwrap();
    let parent = trusted_paths::open_directory(&fixture).unwrap();
    btrfs::create(&parent, "template".as_ref()).unwrap();
    let template = directory(&parent, "template".as_ref()).unwrap();
    std::fs::write(fixture.join("template/rootfs.ext4"), b"template").unwrap();
    btrfs::set_readonly(&template, true).unwrap();
    let result = std::panic::catch_unwind(|| {
        btrfs::snapshot(&template, &parent, "source".as_ref(), false).unwrap();
        std::fs::write(fixture.join("source/rootfs.ext4"), b"restored data").unwrap();
        let source = directory(&parent, "source".as_ref()).unwrap();
        btrfs::snapshot(&source, &parent, "instance.ro".as_ref(), true).unwrap();
        let snapshot = directory(&parent, "instance.ro".as_ref()).unwrap();
        let stream = File::create(fixture.join("stream")).unwrap();
        btrfs::send(&template, &snapshot, &stream).unwrap();
        drop(snapshot);
        remove_at(&parent, "instance.ro".as_ref()).unwrap();
        let bytes = std::fs::read(fixture.join("stream")).unwrap();
        let cut = bytes.len() / 2;

        for (case, staged, expected) in [
            ("valid", bytes.clone(), Some(None)),
            (
                "restarted",
                [&bytes[..cut], &bytes[..]].concat(),
                Some(Some(cut as u64)),
            ),
            ("partial", bytes[..cut].to_vec(), None),
        ] {
            let destination = fixture.join(case);
            std::fs::create_dir_all(destination.join("snapshot")).unwrap();
            std::fs::write(destination.join("snapshot/rootfs.btrfs-send"), &staged).unwrap();
            let receive = ReceiveRoot::open(&destination).unwrap();
            let received = receive.receive_staged(None);
            match expected {
                Some(salvaged) => {
                    assert_eq!(received.unwrap(), salvaged, "{case}");
                    assert_eq!(
                        std::fs::read(destination.join("instance/rootfs.ext4")).unwrap(),
                        b"restored data",
                        "{case}"
                    );
                    assert!(!destination.join("instance.ro").exists(), "{case}");
                }
                None => {
                    received.unwrap_err();
                    assert!(!destination.join("instance").exists(), "{case}");
                }
            }
        }
    });
    // A deliberately failing red run must not strand subvolumes either.
    for case in ["valid", "restarted", "partial"] {
        let Ok(dir) = trusted_paths::open_directory(&fixture.join(case)) else {
            continue;
        };
        for name in super::super::entries(&dir).unwrap() {
            let _ = remove_at(&dir, &name);
        }
    }
    for name in super::super::entries(&parent).unwrap() {
        remove_at(&parent, &name).unwrap();
    }
    if let Err(panic) = result {
        std::panic::resume_unwind(panic);
    }
}

#[test]
#[ignore = "requires an explicitly supplied disposable btrfs fixture directory"]
fn validation_pins_bytes_and_rejects_before_scrubbing() {
    use std::io::Read;

    let Some(fixture) = receive_fixture() else {
        return;
    };
    let temp = tempfile::tempdir_in(fixture).unwrap();
    let root = ReceiveRoot::open(temp.path()).unwrap();
    let bytes = include_bytes!("../../../fixtures/btrfs_send_v1.stream");
    let source_path = temp.path().join("stream");
    std::fs::write(&source_path, bytes).unwrap();
    let deadline = || std::time::Instant::now() + crate::host_command::MUTATION_BUDGET;
    let mut prepared = ValidatedStream::prepare(
        &root.directory,
        &File::open(&source_path).unwrap(),
        None,
        deadline(),
    )
    .unwrap();
    // Mutation through another descriptor must not change receive's input.
    std::fs::write(&source_path, b"replacement").unwrap();
    let mut actual = Vec::new();
    prepared.0.read_to_end(&mut actual).unwrap();
    assert_eq!(actual, bytes);
    assert_eq!(prepared.0.metadata().unwrap().nlink(), 0);
    drop(prepared);

    std::fs::create_dir(temp.path().join("instance")).unwrap();
    std::fs::write(temp.path().join("instance/stale"), b"preserve").unwrap();
    let mut v2 = bytes.to_vec();
    v2[13..17].copy_from_slice(&2_u32.to_le_bytes());
    for input in [
        bytes[..bytes.len() / 2].to_vec(),
        [bytes.as_slice(), bytes].concat(),
        v2,
    ] {
        std::fs::write(&source_path, input).unwrap();
        let error = root
            .receive(File::open(&source_path).unwrap(), None)
            .unwrap_err();
        assert_eq!(error.code(), tonic::Code::InvalidArgument);
        assert_eq!(
            std::fs::read(temp.path().join("instance/stale")).unwrap(),
            b"preserve"
        );
        assert!(!temp.path().join("instance.ro").exists());
        assert_eq!(std::fs::read_dir(temp.path()).unwrap().count(), 2);
    }
    let error = ValidatedStream::prepare(
        &root.directory,
        &File::open(&source_path).unwrap(),
        None,
        std::time::Instant::now(),
    )
    .err()
    .unwrap();
    assert_eq!(error.code(), tonic::Code::DeadlineExceeded);
}

fn replace_destination(destination: &Path, link: bool) -> std::io::Result<PathBuf> {
    let original = destination.with_file_name("original");
    std::fs::rename(destination, &original)?;
    if link {
        let foreign = destination.with_file_name("foreign");
        std::fs::create_dir(&foreign)?;
        symlink(&foreign, destination)?;
    } else {
        std::fs::create_dir(destination)?;
    }
    Ok(original)
}

/// Unmounts on the host side reach a receiver that started while the mount
/// existed: an encrypted volume released during a receive must not stay busy
/// on the receiver's copy. Nothing the receiver mounts reaches the host, and
/// its `/tmp` is still the pinned destination.
#[test]
#[ignore = "requires root and an explicitly supplied disposable btrfs fixture directory"]
fn receiver_namespace_follows_host_unmounts_and_stays_contained() {
    use std::io::BufRead;

    let Some(fixture) = receive_fixture() else {
        return;
    };
    assert!(nix::unistd::geteuid().is_root());
    let temp = tempfile::tempdir_in(fixture).unwrap();
    let root = temp.path().canonicalize().unwrap();
    assert!(
        !root.starts_with("/tmp"),
        "the receiver's /tmp is its destination, so the fixture must live elsewhere"
    );
    // A shared parent, like node-manager's Bidirectional /space/cache.
    let shared = root.join("cache");
    let volume = shared.join("volume");
    let destination = root.join("destination");
    for dir in [&shared, &destination] {
        std::fs::create_dir(dir).unwrap();
    }
    std::fs::write(destination.join("pinned"), b"").unwrap();
    struct Unmount(Vec<PathBuf>);
    impl Drop for Unmount {
        fn drop(&mut self) {
            for path in self.0.iter().rev() {
                let _ = nix::mount::umount2(path, nix::mount::MntFlags::MNT_DETACH);
            }
        }
    }
    let mut mounts = Unmount(Vec::new());
    use nix::mount::{MsFlags, mount};
    mount(
        Some(&shared),
        &shared,
        None::<&str>,
        MsFlags::MS_BIND,
        None::<&str>,
    )
    .unwrap();
    mounts.0.push(shared.clone());
    mount(
        None::<&str>,
        &shared,
        None::<&str>,
        MsFlags::MS_SHARED,
        None::<&str>,
    )
    .unwrap();
    std::fs::create_dir(&volume).unwrap();
    mount(
        Some("tmpfs"),
        &volume,
        Some("tmpfs"),
        MsFlags::empty(),
        None::<&str>,
    )
    .unwrap();
    mounts.0.push(volume.clone());
    std::fs::write(volume.join("memory"), b"vmstate").unwrap();

    // The receiver reports what it sees, mounts a tmpfs of its own under the
    // shared parent, then waits for the host to drop the volume.
    let script = r#"
        test -e /tmp/pinned && echo pinned
        test -e "$1/memory" && echo volume
        mkdir -p "$2" && mount -t tmpfs receiver "$2" && echo mounted
        i=0
        while [ -e "$1/memory" ] && [ $i -lt 100 ]; do sleep 0.05; i=$((i+1)); done
        test -e "$1/memory" && echo still-mounted || echo unmounted
    "#;
    let leaked = shared.join("leaked");
    let mut command = std::process::Command::new("sh");
    command
        .args(["-c", script, "sh"])
        .arg(&volume)
        .arg(&leaked)
        .stdout(std::process::Stdio::piped());
    let destination_fd = trusted_paths::open_directory(&destination).unwrap();
    attach_destination_at_tmp(&mut command, &destination_fd).unwrap();
    let mut child = command.spawn().unwrap();
    let mut lines = std::io::BufReader::new(child.stdout.take().unwrap()).lines();
    let mut next = || lines.next().unwrap().unwrap();
    assert_eq!(next(), "pinned");
    assert_eq!(next(), "volume");
    assert_eq!(next(), "mounted");
    let host_mountinfo = std::fs::read_to_string("/proc/self/mountinfo").unwrap();
    assert!(
        !host_mountinfo.contains(&*leaked.to_string_lossy()),
        "the receiver's mount propagated to the host"
    );
    nix::mount::umount2(&volume, nix::mount::MntFlags::MNT_DETACH).unwrap();
    mounts.0.pop();
    assert_eq!(next(), "unmounted");
    assert!(child.wait().unwrap().success());
    drop(mounts);
}
