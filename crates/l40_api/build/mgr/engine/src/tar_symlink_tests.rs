use super::*;

pub(super) fn tar_archive(entries: &[(tar::EntryType, &str, &str)]) -> tempfile::NamedTempFile {
    let archive = tempfile::NamedTempFile::new().expect("archive temp file");
    let mut builder = tar::Builder::new(archive.reopen().expect("reopen archive"));
    for &(kind, path, contents) in entries {
        let mut header = tar::Header::new_gnu();
        header.set_entry_type(kind);
        header.set_mode(0o755);
        // Preserve leading `./` components that append_data would normalize away.
        assert!(path.len() <= 100);
        header.as_mut_bytes()[..path.len()].copy_from_slice(path.as_bytes());
        let data = if kind.is_symlink() || kind.is_hard_link() {
            header.set_link_name(contents).expect("set link target");
            &[][..]
        } else {
            contents.as_bytes()
        };
        header.set_size(data.len() as u64);
        header.set_cksum();
        builder.append(&header, data).expect("append entry");
    }
    builder.finish().expect("finish archive");
    archive
}

fn pax_record(key: &str, value: &str) -> String {
    let record = format!(" {key}={value}\n");
    let mut length = record.len() + 1;
    loop {
        let encoded = format!("{length}{record}");
        if encoded.len() == length {
            return encoded;
        }
        length = encoded.len();
    }
}

#[test]
fn extract_tar_payload_checks_effective_extended_paths_and_links() {
    for extended in [tar::EntryType::GNULongName, tar::EntryType::XHeader] {
        for path in [
            "../outside/canary",
            "/outside/canary",
            "prefix/tree/../../canary",
        ] {
            let contents = if extended == tar::EntryType::XHeader {
                pax_record("path", path)
            } else {
                format!("{path}\0")
            };
            let archive = tar_archive(&[
                (extended, "././@LongLink", &contents),
                (tar::EntryType::Regular, "prefix/tree/safe", "overwrite"),
            ]);
            let staging = tempfile::tempdir().unwrap();

            let result = extract_tar_payload(archive.path(), staging.path(), Some("prefix/tree"));

            assert!(result.is_err(), "accepted {extended:?} path {path}");
            assert_eq!(fs::read_dir(staging.path()).unwrap().count(), 0);
        }
    }
    for extended in [tar::EntryType::GNULongLink, tar::EntryType::XHeader] {
        for target in ["../../outside", "/outside", "../../../outside"] {
            let contents = if extended == tar::EntryType::XHeader {
                pax_record("linkpath", target)
            } else {
                format!("{target}\0")
            };
            let archive = tar_archive(&[
                (extended, "././@LongLink", &contents),
                (tar::EntryType::Symlink, "prefix/tree/link", "safe"),
            ]);
            let staging = tempfile::tempdir().unwrap();

            let result = extract_tar_payload(archive.path(), staging.path(), Some("prefix/tree"));

            assert!(result.is_err(), "accepted {extended:?} target {target}");
            assert_eq!(fs::read_dir(staging.path()).unwrap().count(), 0);
        }
    }
}

#[test]
fn extract_tar_payload_accepts_extended_names_without_losing_selection() {
    let basename = "long-name".repeat(20);
    let name = format!("prefix/tree/{basename}");
    for extended in [tar::EntryType::GNULongName, tar::EntryType::XHeader] {
        let contents = if extended == tar::EntryType::XHeader {
            pax_record("path", &name)
        } else {
            format!("{name}\0")
        };
        let (link_extension, link_contents) = if extended == tar::EntryType::XHeader {
            (extended, pax_record("linkpath", &basename))
        } else {
            (tar::EntryType::GNULongLink, format!("{basename}\0"))
        };
        let archive = tar_archive(&[
            (extended, "././@LongLink", &contents),
            (tar::EntryType::Regular, "ignored", "payload"),
            (link_extension, "././@LongLink", &link_contents),
            (tar::EntryType::Symlink, "prefix/tree/link", "ignored"),
        ]);
        let staging = tempfile::tempdir().unwrap();

        let tree =
            extract_tar_payload(archive.path(), staging.path(), Some("prefix/tree")).unwrap();

        assert_eq!(
            fs::read(tree.join(Path::new(&name).file_name().unwrap())).unwrap(),
            b"payload"
        );
        assert!(!staging.path().join("ignored").exists());
        assert_eq!(
            fs::read_link(tree.join("link")).unwrap(),
            Path::new(&basename)
        );
        assert_eq!(fs::read(tree.join("link")).unwrap(), b"payload");
    }
}

#[test]
fn extract_tar_payload_never_replaces_existing_entries() {
    let kinds = [
        tar::EntryType::Regular,
        tar::EntryType::Directory,
        tar::EntryType::Symlink,
    ];
    for first in kinds {
        for second in kinds {
            for source in [None, Some("prefix/tree")] {
                let archive = tar_archive(&[
                    (first, "prefix/tree/entry", "first"),
                    (second, "prefix/tree/entry", "second"),
                ]);
                let staging = tempfile::tempdir().unwrap();

                let result = extract_tar_payload(archive.path(), staging.path(), source);

                assert_eq!(
                    result.is_ok(),
                    first.is_dir() && second.is_dir(),
                    "{first:?} then {second:?}"
                );
                let path = staging.path().join(if source.is_some() {
                    "tree/entry"
                } else {
                    "prefix/tree/entry"
                });
                if first.is_file() {
                    assert_eq!(fs::read(path).unwrap(), b"first");
                } else if first.is_symlink() {
                    assert_eq!(fs::read_link(path).unwrap(), Path::new("first"));
                } else {
                    assert!(path.symlink_metadata().unwrap().is_dir());
                }
            }
        }
    }
}

#[test]
fn extract_tar_payload_rejects_corrupt_or_truncated_archives() {
    use std::io::{Seek, Write};

    for length in [100, 512, 513] {
        let archive = tar_archive(&[(tar::EntryType::Regular, "tree/file", &"x".repeat(1024))]);
        archive.as_file().set_len(length).unwrap();
        let staging = tempfile::tempdir().unwrap();

        let result = extract_tar_payload(archive.path(), staging.path(), None);

        assert!(result.is_err(), "accepted truncation at {length}");
    }
    let archive = tar_archive(&[(tar::EntryType::Regular, "tree/file", "data")]);
    let mut file = archive.reopen().unwrap();
    file.rewind().unwrap();
    file.write_all(b"corrupt-checksum").unwrap();
    let staging = tempfile::tempdir().unwrap();

    let result = extract_tar_payload(archive.path(), staging.path(), None);

    result.unwrap_err();
    assert_eq!(fs::read_dir(staging.path()).unwrap().count(), 0);
}

#[test]
fn extract_tar_payload_generated_link_orders_cannot_modify_outside_staging() {
    use std::os::unix::fs::PermissionsExt;

    let targets = [
        "..",
        "../..",
        "../../outside",
        "up/../outside",
        "./up/../../outside",
        "real",
        "link",
        "missing",
    ];
    for source in [None, Some("prefix/tree")] {
        for up in targets {
            for link in targets {
                for order in [
                    [0, 1, 2],
                    [0, 2, 1],
                    [1, 0, 2],
                    [1, 2, 0],
                    [2, 0, 1],
                    [2, 1, 0],
                ] {
                    for kind in [tar::EntryType::Regular, tar::EntryType::Directory] {
                        let temp = tempfile::tempdir().unwrap();
                        let staging = temp.path().join("staging");
                        let outside = temp.path().join("outside");
                        fs::create_dir(&staging).unwrap();
                        fs::create_dir(&outside).unwrap();
                        fs::write(outside.join("canary"), b"untouched").unwrap();
                        fs::set_permissions(
                            outside.join("canary"),
                            fs::Permissions::from_mode(0o600),
                        )
                        .unwrap();
                        let entries = [
                            (tar::EntryType::Symlink, "prefix/tree/up", up),
                            (tar::EntryType::Symlink, "prefix/tree/link", link),
                            (kind, "prefix/tree/link/new", "payload"),
                        ];
                        let archive = tar_archive(&order.map(|index| entries[index]));

                        let _result = extract_tar_payload(archive.path(), &staging, source);

                        assert_eq!(
                            fs::read(outside.join("canary")).unwrap(),
                            b"untouched",
                            "{source:?} {up} {link} {order:?} {kind:?}"
                        );
                        assert_eq!(
                            fs::metadata(outside.join("canary"))
                                .unwrap()
                                .permissions()
                                .mode()
                                & 0o777,
                            0o600
                        );
                        assert_eq!(fs::read_dir(&outside).unwrap().count(), 1);
                    }
                }
            }
        }
    }
}

#[test]
fn extract_tar_payload_rejects_special_entries() {
    for kind in [
        tar::EntryType::Link,
        tar::EntryType::Fifo,
        tar::EntryType::Char,
        tar::EntryType::Block,
        tar::EntryType::Continuous,
        tar::EntryType::new(b'?'),
    ] {
        let archive = tar_archive(&[(kind, "prefix/tree/special", "target")]);
        let staging = tempfile::tempdir().unwrap();

        let result = extract_tar_payload(archive.path(), staging.path(), Some("prefix/tree"));

        assert!(result.is_err(), "accepted {kind:?}");
        assert!(!staging.path().join("tree/special").exists());
    }
}

#[test]
fn extract_tar_payload_rejects_remapped_symlink_escape() {
    let archive = tar_archive(&[
        (tar::EntryType::Symlink, "prefix/tree/link", "../../outside"),
        (
            tar::EntryType::Regular,
            "prefix/tree/link/escaped",
            "payload",
        ),
    ]);
    let temp = tempfile::tempdir().expect("temp dir");
    let staging = temp.path().join("staging");
    let outside = temp.path().join("outside");
    fs::create_dir(&staging).expect("create staging");
    fs::create_dir(&outside).expect("create outside");

    let result = extract_tar_payload(archive.path(), &staging, Some("prefix/tree"));

    assert!(!outside.join("escaped").exists(), "wrote outside staging");
    assert!(matches!(result, Err(SshError::CopyPayload(_))));
    assert_eq!(
        fs::symlink_metadata(staging.join("tree/link"))
            .unwrap_err()
            .kind(),
        std::io::ErrorKind::NotFound
    );
}

#[test]
fn extract_tar_payload_rejects_escape_with_leading_dot() {
    let archive = tar_archive(&[
        (tar::EntryType::Symlink, "./tree/link", "../../outside"),
        (tar::EntryType::Regular, "./tree/link/escaped", "payload"),
    ]);
    let temp = tempfile::tempdir().expect("temp dir");
    let staging = temp.path().join("staging");
    let outside = temp.path().join("outside");
    fs::create_dir(&staging).expect("create staging");
    fs::create_dir(&outside).expect("create outside");

    let result = extract_tar_payload(archive.path(), &staging, None);

    assert!(!outside.join("escaped").exists(), "wrote outside staging");
    assert!(matches!(result, Err(SshError::CopyPayload(_))));
    assert_eq!(
        fs::symlink_metadata(staging.join("tree/link"))
            .unwrap_err()
            .kind(),
        std::io::ErrorKind::NotFound
    );
}

#[test]
fn extract_tar_payload_rejects_symlink_as_directory_or_file_destination() {
    for kind in [tar::EntryType::Regular, tar::EntryType::Directory] {
        let archive = tar_archive(&[
            (tar::EntryType::Directory, "prefix/tree/real", ""),
            (tar::EntryType::Symlink, "prefix/tree/link", "real"),
            (kind, "prefix/tree/link", ""),
        ]);
        let staging = tempfile::tempdir().expect("staging dir");

        let result = extract_tar_payload(archive.path(), staging.path(), Some("prefix/tree"));

        assert!(matches!(result, Err(SshError::CopyPayload(_))));
        assert_eq!(
            fs::read_link(staging.path().join("tree/link")).unwrap(),
            Path::new("real")
        );
        assert!(staging.path().join("tree/real").is_dir());
    }
}

#[test]
fn extract_tar_payload_rejects_writes_through_symlink_ancestors() {
    for source_path in [None, Some("prefix/tree")] {
        for kind in [tar::EntryType::Regular, tar::EntryType::Directory] {
            let archive = tar_archive(&[
                (tar::EntryType::Directory, "prefix/tree/real", ""),
                (tar::EntryType::Symlink, "prefix/tree/link", "real"),
                (kind, "prefix/tree/link/child", ""),
            ]);
            let staging = tempfile::tempdir().expect("staging dir");

            let result = extract_tar_payload(archive.path(), staging.path(), source_path);

            let tree = staging.path().join(if source_path.is_some() {
                "tree"
            } else {
                "prefix/tree"
            });
            assert!(
                !tree.join("real/child").exists(),
                "followed a symlink parent"
            );
            assert!(matches!(result, Err(SshError::CopyPayload(_))));
        }
    }
}

#[test]
fn extract_tar_payload_rejects_symlink_chain_write_escape() {
    let archive = tar_archive(&[
        (tar::EntryType::Symlink, "prefix/tree/up", ".."),
        (tar::EntryType::Symlink, "prefix/tree/link", "up/../outside"),
        (
            tar::EntryType::Regular,
            "prefix/tree/link/escaped",
            "payload",
        ),
    ]);
    let temp = tempfile::tempdir().expect("temp dir");
    let staging = temp.path().join("staging");
    let outside = temp.path().join("outside");
    fs::create_dir(&staging).expect("create staging");
    fs::create_dir(&outside).expect("create outside");

    let result = extract_tar_payload(archive.path(), &staging, Some("prefix/tree"));

    assert!(
        !outside.join("escaped").exists(),
        "wrote through symlink chain"
    );
    assert!(matches!(result, Err(SshError::CopyPayload(_))));
}

#[test]
fn extract_tar_payload_preserves_nested_files_and_relative_links() {
    let archive = tar_archive(&[
        (
            tar::EntryType::Symlink,
            "prefix/tree/README",
            "guide/readme",
        ),
        (
            tar::EntryType::Symlink,
            "prefix/tree/bin/readme",
            "../guide/readme",
        ),
        (
            tar::EntryType::Regular,
            "prefix/tree/guide/readme",
            "readme",
        ),
        (tar::EntryType::Directory, "prefix/tree/docs", ""),
        (tar::EntryType::Regular, "unselected/file", "not selected"),
    ]);
    let staging = tempfile::tempdir().expect("staging dir");

    let tree = extract_tar_payload(archive.path(), staging.path(), Some("prefix/tree/"))
        .expect("extract subtree");

    assert_eq!(tree, staging.path().join("tree"));
    assert_eq!(fs::read(tree.join("guide/readme")).unwrap(), b"readme");
    assert_eq!(fs::read(tree.join("README")).unwrap(), b"readme");
    assert_eq!(fs::read(tree.join("bin/readme")).unwrap(), b"readme");
    assert_eq!(
        fs::read_link(tree.join("README")).unwrap(),
        Path::new("guide/readme")
    );
    assert_eq!(
        fs::read_link(tree.join("bin/readme")).unwrap(),
        Path::new("../guide/readme")
    );
    assert!(!staging.path().join("unselected").exists());
}

fn ok(link: &str, target: &str) {
    validate_tar_symlink_target(Path::new(link), Path::new(target))
        .unwrap_or_else(|err| panic!("expected {link} -> {target} to be safe, got: {err:?}"));
}

fn err(link: &str, target: &str) {
    validate_tar_symlink_target(Path::new(link), Path::new(target))
        .err()
        .unwrap_or_else(|| panic!("expected {link} -> {target} to be rejected"));
}

#[test]
fn safe_sibling_symlink_is_allowed() {
    // app-runtime's `CLAUDE.md -> AGENTS.md`
    ok("app-runtime/CLAUDE.md", "AGENTS.md");
}

#[test]
fn safe_within_subtree_symlink_is_allowed() {
    ok("app-runtime/wrappers/cli.sh", "../utils/wrapper.sh");
    ok("app-runtime/wrappers/cli.sh", "./helper.sh");
}

#[test]
fn absolute_target_is_rejected() {
    err("app-runtime/x", "/etc/passwd");
}

#[test]
fn parent_escape_is_rejected() {
    err("app-runtime/x", "../../../etc/passwd");
    err("app-runtime/x", "../..");
}

#[test]
fn empty_target_is_rejected() {
    err("app-runtime/x", "");
}

#[test]
fn guest_path_join_handles_root_parent() {
    assert_eq!(guest_path_join("/", "payload.whl"), "/payload.whl");
}

#[test]
fn guest_path_join_handles_nested_parent() {
    assert_eq!(
        guest_path_join("/tmp/example_sdk_local/", "payload.whl"),
        "/tmp/example_sdk_local/payload.whl"
    );
}

#[test]
fn guest_copy_finalize_moves_file_to_exact_target() {
    let tempdir = tempfile::tempdir().expect("tempdir");
    let source_dir = tempdir.path().join("stage");
    fs::create_dir(&source_dir).expect("create source dir");
    let source = source_dir.join("example_sdk.whl");
    fs::write(&source, b"wheel bytes").expect("write staged file");
    let target = tempdir
        .path()
        .join("example_sdk_local")
        .join("example_sdk-renamed.whl");

    let command = guest_copy_finalize_command(
        source.to_str().expect("source path utf8"),
        target.to_str().expect("target path utf8"),
    );
    let status = Command::new("bash")
        .args(["-c", &command])
        .status()
        .expect("run finalizer");

    assert!(status.success());
    assert_eq!(fs::read(&target).expect("read target file"), b"wheel bytes");
    assert!(target.is_file());
    assert!(!target.is_dir());
}

#[test]
fn guest_copy_finalize_moves_directory_contents_to_target() {
    let tempdir = tempfile::tempdir().expect("tempdir");
    let source = tempdir.path().join("stage").join("payload");
    fs::create_dir_all(source.join("pkg")).expect("create staged dir");
    fs::write(source.join("pkg").join("__init__.py"), b"").expect("write nested file");
    fs::write(source.join("README.md"), b"readme").expect("write top-level file");
    let target = tempdir.path().join("_read_test_pkg");

    let command = guest_copy_finalize_command(
        source.to_str().expect("source path utf8"),
        target.to_str().expect("target path utf8"),
    );
    let status = Command::new("bash")
        .args(["-c", &command])
        .status()
        .expect("run finalizer");

    assert!(status.success());
    assert_eq!(
        fs::read(target.join("README.md")).expect("read readme"),
        b"readme"
    );
    assert!(target.join("pkg").join("__init__.py").is_file());
    assert!(!target.join("payload").exists());
}

#[test]
fn extract_tar_payload_handles_nested_file_without_directory_entry() {
    let archive = tempfile::NamedTempFile::new().expect("archive temp file");
    {
        let mut builder = tar::Builder::new(archive.reopen().expect("reopen archive"));
        let contents = b"wheel bytes";
        let mut header = tar::Header::new_gnu();
        header
            .set_path("local_wheels/example_sdk.whl")
            .expect("set path");
        header.set_size(contents.len() as u64);
        header.set_cksum();
        builder
            .append(&header, contents.as_slice())
            .expect("append tar entry");
        builder.finish().expect("finish archive");
    }

    let dst = tempfile::tempdir().expect("staging dir");
    let staged =
        extract_tar_payload(archive.path(), dst.path(), None).expect("extract nested payload");

    assert_eq!(
        staged.file_name().and_then(|name| name.to_str()),
        Some("local_wheels")
    );
    let extracted = staged.join("example_sdk.whl");
    assert_eq!(
        std::fs::read(&extracted)
            .unwrap_or_else(|err| panic!("read {}: {err}", extracted.display())),
        b"wheel bytes",
    );
}

#[test]
fn extract_tar_payload_selects_requested_nested_file() {
    let archive = tempfile::NamedTempFile::new().expect("archive temp file");
    {
        let mut builder = tar::Builder::new(archive.reopen().expect("reopen archive"));
        let contents = b"wheel bytes";
        let mut header = tar::Header::new_gnu();
        header
            .set_path("local_wheels/example_sdk.whl")
            .expect("set path");
        header.set_size(contents.len() as u64);
        header.set_cksum();
        builder
            .append(&header, contents.as_slice())
            .expect("append tar entry");
        builder.finish().expect("finish archive");
    }

    let dst = tempfile::tempdir().expect("staging dir");
    let staged = extract_tar_payload(
        archive.path(),
        dst.path(),
        Some("local_wheels/example_sdk.whl"),
    )
    .expect("extract requested source path");

    assert_eq!(
        staged.file_name().and_then(|name| name.to_str()),
        Some("example_sdk.whl")
    );
    assert!(!dst.path().join("local_wheels").exists());
    assert_eq!(
        std::fs::read(&staged).unwrap_or_else(|err| panic!("read {}: {err}", staged.display())),
        b"wheel bytes",
    );
}

#[test]
fn extract_tar_payload_selects_requested_nested_directory_contents() {
    let archive = tempfile::NamedTempFile::new().expect("archive temp file");
    {
        let mut builder = tar::Builder::new(archive.reopen().expect("reopen archive"));
        let contents = b"tool bytes";
        let mut header = tar::Header::new_gnu();
        header.set_path("tools/bin/tool").expect("set path");
        header.set_size(contents.len() as u64);
        header.set_cksum();
        builder
            .append(&header, contents.as_slice())
            .expect("append tar entry");
        builder.finish().expect("finish archive");
    }

    let dst = tempfile::tempdir().expect("staging dir");
    let staged = extract_tar_payload(archive.path(), dst.path(), Some("tools"))
        .expect("extract requested directory source path");

    assert_eq!(
        staged.file_name().and_then(|name| name.to_str()),
        Some("tools")
    );
    assert_eq!(
        std::fs::read(staged.join("bin").join("tool")).expect("read nested tool"),
        b"tool bytes",
    );
}
