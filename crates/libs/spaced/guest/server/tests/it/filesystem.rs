use crate::guest_service::GuestFixture;
use nix::unistd::{Uid, User};
use pplx_space_spaced_common::proto::bridge_guest::filesystem_client::FilesystemClient as BridgeGuestFilesystemClient;
use pplx_space_spaced_common::proto::bridge_guest::{
    FileType, ListRequest, MkdirRequest, ReadRequest, RemoveRequest, RenameRequest, StatRequest,
    WriteOpen, WriteRequest,
};
#[cfg(target_os = "linux")]
use std::os::unix::ffi::OsStringExt;
use std::os::unix::fs::MetadataExt;

#[tokio::test]
async fn bridge_guest_stat_returns_file_metadata() {
    let temp_dir = tempfile::tempdir().expect("temp dir created");
    let file_path = temp_dir.path().join("sample.txt");
    std::fs::write(&file_path, b"hello").expect("sample file written");
    let metadata = std::fs::metadata(&file_path).expect("sample file metadata");
    let fixture = GuestFixture::start(temp_dir.path()).await;
    let mut client = BridgeGuestFilesystemClient::new(fixture.channel.clone());

    let stat = client
        .stat_info(StatRequest {
            path: file_path.display().to_string(),
            user: None,
        })
        .await
        .expect("stat response")
        .into_inner()
        .info
        .expect("file info");
    assert_eq!(stat.file_type(), FileType::File);
    assert_eq!(stat.size_bytes, 5);
    assert!(stat.mode != 0);
    assert!(stat.modified_at.is_some());
    assert_eq!(stat.owner_uid, metadata.uid());
    assert_eq!(stat.owner_gid, metadata.gid());
    assert_eq!(stat.symlink_target, None);

    // SPACED_COMPAT_REMOVE_WHEN_MIN_GUEST_0_1_4: this block also needs MIN_HOST
    // 0.1.4; prove old clients still receive the old shape from the new guest.
    #[allow(deprecated)]
    let legacy = client
        .stat(StatRequest {
            path: file_path.display().to_string(),
            user: None,
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(legacy.file_type, stat.file_type);
    assert_eq!(legacy.size_bytes, stat.size_bytes);
    assert_eq!(legacy.mode, stat.mode);
    assert_eq!(legacy.modified_at, stat.modified_at);
    assert_eq!(
        (legacy.owner_uid, legacy.owner_gid),
        (stat.owner_uid, stat.owner_gid)
    );

    #[allow(deprecated)]
    let mut legacy_list = client
        .list(ListRequest {
            path: temp_dir.path().display().to_string(),
            depth: Some(1),
            user: None,
        })
        .await
        .unwrap()
        .into_inner();
    let mut found = false;
    while let Some(entry) = legacy_list.message().await.unwrap() {
        if entry.name == "sample.txt" {
            assert_eq!(entry.path, stat.path);
            assert_eq!(entry.size_bytes, stat.size_bytes);
            found = true;
        }
    }
    assert!(found);

    fixture.stop().await;
}

#[tokio::test]
async fn bridge_guest_stat_resolves_relative_path_under_user_home() {
    let current = User::from_uid(Uid::current())
        .expect("current user lookup works")
        .expect("current user exists");
    let home_temp = tempfile::Builder::new()
        .prefix("space-relative-stat-")
        .tempdir_in(&current.dir)
        .expect("temp dir created under user home");
    let relative_dir = home_temp
        .path()
        .file_name()
        .expect("temp dir has file name")
        .to_string_lossy()
        .to_string();
    std::fs::write(home_temp.path().join("sample.txt"), b"hello").expect("sample file written");

    let temp_dir = tempfile::tempdir().expect("temp dir created");
    let fixture = GuestFixture::start(temp_dir.path()).await;
    let mut client = BridgeGuestFilesystemClient::new(fixture.channel.clone());

    let stat = client
        .stat_info(StatRequest {
            path: format!("{relative_dir}/sample.txt"),
            user: Some(current.name),
        })
        .await
        .expect("stat response")
        .into_inner()
        .info
        .expect("file info");
    assert_eq!(stat.file_type(), FileType::File);
    assert_eq!(stat.size_bytes, 5);
    assert_eq!(stat.path, format!("{relative_dir}/sample.txt"));

    fixture.stop().await;
}

#[tokio::test]
async fn bridge_guest_stat_maps_missing_path_to_not_found() {
    let temp_dir = tempfile::tempdir().expect("temp dir created");
    let fixture = GuestFixture::start(temp_dir.path()).await;
    let mut client = BridgeGuestFilesystemClient::new(fixture.channel.clone());

    let error = client
        .stat_info(StatRequest {
            path: temp_dir.path().join("missing").display().to_string(),
            user: None,
        })
        .await
        .expect_err("missing path rejected");
    assert_eq!(error.code(), tonic::Code::NotFound);

    fixture.stop().await;
}

#[tokio::test]
async fn bridge_guest_list_returns_directory_entries() {
    let temp_dir = tempfile::tempdir().expect("temp dir created");
    let directory = temp_dir.path().join("dir");
    std::fs::create_dir(&directory).expect("directory created");
    std::fs::write(directory.join("a.txt"), b"hello").expect("sample file written");
    std::fs::create_dir(directory.join("nested")).expect("nested dir created");
    let fixture = GuestFixture::start(temp_dir.path()).await;
    let mut client = BridgeGuestFilesystemClient::new(fixture.channel.clone());

    let mut stream = client
        .list_info(ListRequest {
            path: directory.display().to_string(),
            depth: Some(1),
            user: None,
        })
        .await
        .expect("list response")
        .into_inner();
    let mut names = Vec::new();
    while let Some(entry) = stream.message().await.expect("list item") {
        names.push((entry.name.clone().expect("entry name"), entry.file_type()));
    }
    names.sort_by(|left, right| left.0.cmp(&right.0));
    assert_eq!(
        names,
        vec![
            ("a.txt".to_string(), FileType::File),
            ("nested".to_string(), FileType::Directory),
        ]
    );

    fixture.stop().await;
}

#[tokio::test]
async fn bridge_guest_list_depth_returns_nested_metadata() {
    let temp_dir = tempfile::tempdir().expect("temp dir created");
    let directory = temp_dir.path().join("dir");
    let nested = directory.join("nested");
    std::fs::create_dir(&directory).expect("directory created");
    std::fs::create_dir(&nested).expect("nested dir created");
    std::fs::write(nested.join("child.txt"), b"hello").expect("nested file written");
    let fixture = GuestFixture::start(temp_dir.path()).await;
    let mut client = BridgeGuestFilesystemClient::new(fixture.channel.clone());

    let mut stream = client
        .list_info(ListRequest {
            path: directory.display().to_string(),
            depth: Some(2),
            user: None,
        })
        .await
        .expect("list response")
        .into_inner();
    let mut entries = Vec::new();
    while let Some(entry) = stream.message().await.expect("list item") {
        entries.push(entry);
    }
    let child = entries
        .iter()
        .find(|entry| entry.name.as_deref() == Some("child.txt"))
        .expect("nested child listed");
    assert_eq!(child.file_type(), FileType::File);
    assert_eq!(child.size_bytes, 5);
    assert!(child.modified_at.is_some());

    fixture.stop().await;
}

#[tokio::test]
async fn bridge_guest_list_maps_missing_path_to_not_found() {
    let temp_dir = tempfile::tempdir().expect("temp dir created");
    let fixture = GuestFixture::start(temp_dir.path()).await;
    let mut client = BridgeGuestFilesystemClient::new(fixture.channel.clone());

    let error = client
        .list_info(ListRequest {
            path: temp_dir.path().join("missing").display().to_string(),
            depth: Some(1),
            user: None,
        })
        .await
        .expect_err("missing path rejected");
    assert_eq!(error.code(), tonic::Code::NotFound);

    fixture.stop().await;
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn bridge_guest_list_rejects_non_utf8_entry_name() {
    let temp_dir = tempfile::tempdir().expect("temp dir created");
    let directory = temp_dir.path().join("dir");
    std::fs::create_dir(&directory).expect("directory created");
    std::fs::write(
        directory.join(std::ffi::OsString::from_vec(vec![0xff])),
        b"bad",
    )
    .expect("non-utf8 file written");
    let fixture = GuestFixture::start(temp_dir.path()).await;
    let mut client = BridgeGuestFilesystemClient::new(fixture.channel.clone());

    let mut stream = client
        .list_info(ListRequest {
            path: directory.display().to_string(),
            depth: Some(1),
            user: None,
        })
        .await
        .expect("list response")
        .into_inner();
    let error = stream.message().await.expect_err("non-utf8 entry rejected");
    assert_eq!(error.code(), tonic::Code::InvalidArgument);

    fixture.stop().await;
}

#[tokio::test]
async fn bridge_guest_read_ranges() {
    let temp_dir = tempfile::tempdir().expect("temp dir created");
    let file_path = temp_dir.path().join("sample.txt");
    std::fs::write(&file_path, b"abcdef").expect("sample file written");
    let fixture = GuestFixture::start(temp_dir.path()).await;
    let mut client = BridgeGuestFilesystemClient::new(fixture.channel.clone());

    for (offset, length, expected) in [
        (2, Some(3), b"cde".as_slice()),
        (3, None, b"def"),
        (3, Some(0), b""),
    ] {
        let mut stream = client
            .read(ReadRequest {
                path: file_path.display().to_string(),
                offset,
                length,
                user: None,
            })
            .await
            .expect("read response")
            .into_inner();
        let mut data = Vec::new();
        while let Some(chunk) = stream.message().await.expect("read chunk") {
            assert_ne!(length, Some(0), "zero-length read emits no frames");
            data.extend_from_slice(&chunk.data);
        }
        assert_eq!(data, expected, "offset={offset}, length={length:?}");
    }

    fixture.stop().await;
}

#[tokio::test]
async fn bridge_guest_write_creates_file() {
    let temp_dir = tempfile::tempdir().expect("temp dir created");
    let file_path = temp_dir.path().join("created.txt");
    let fixture = GuestFixture::start(temp_dir.path()).await;
    let mut client = BridgeGuestFilesystemClient::new(fixture.channel.clone());

    let messages = vec![
        WriteRequest {
            message: Some(
                pplx_space_spaced_common::proto::bridge_guest::write_request::Message::Open(
                    WriteOpen {
                        path: file_path.display().to_string(),
                        append: false,
                        idempotency_key: None,
                        user: None,
                    },
                ),
            ),
        },
        WriteRequest {
            message: Some(
                pplx_space_spaced_common::proto::bridge_guest::write_request::Message::Data(
                    b"hello".to_vec(),
                ),
            ),
        },
    ];
    let response = client
        .write(tokio_stream::iter(messages))
        .await
        .expect("write response")
        .into_inner();
    assert_eq!(response.bytes_written, 5);
    assert_eq!(std::fs::read(&file_path).expect("file read"), b"hello");

    fixture.stop().await;
}

#[tokio::test]
async fn bridge_guest_write_rejects_unknown_user() {
    let temp_dir = tempfile::tempdir().expect("temp dir created");
    let file_path = temp_dir.path().join("owned.txt");
    let fixture = GuestFixture::start(temp_dir.path()).await;
    let mut client = BridgeGuestFilesystemClient::new(fixture.channel.clone());

    let messages = vec![
        WriteRequest {
            message: Some(
                pplx_space_spaced_common::proto::bridge_guest::write_request::Message::Open(
                    WriteOpen {
                        path: file_path.display().to_string(),
                        append: false,
                        idempotency_key: None,
                        user: Some("definitely-not-a-real-user-3a7c91".to_string()),
                    },
                ),
            ),
        },
        WriteRequest {
            message: Some(
                pplx_space_spaced_common::proto::bridge_guest::write_request::Message::Data(
                    b"hello".to_vec(),
                ),
            ),
        },
    ];
    let error = client
        .write(tokio_stream::iter(messages))
        .await
        .expect_err("unknown user must fail before any data is written");
    assert_eq!(error.code(), tonic::Code::InvalidArgument);
    assert!(
        !file_path.exists(),
        "file must not be created when user resolution fails"
    );

    fixture.stop().await;
}

#[tokio::test]
async fn bridge_guest_write_rejects_symlink_path() {
    let temp_dir = tempfile::tempdir().expect("temp dir created");
    let target_path = temp_dir.path().join("target.txt");
    let link_path = temp_dir.path().join("link.txt");
    std::fs::write(&target_path, b"original").expect("target file written");
    std::os::unix::fs::symlink(&target_path, &link_path).expect("symlink created");
    let fixture = GuestFixture::start(temp_dir.path()).await;
    let mut client = BridgeGuestFilesystemClient::new(fixture.channel.clone());

    let messages = vec![
        WriteRequest {
            message: Some(
                pplx_space_spaced_common::proto::bridge_guest::write_request::Message::Open(
                    WriteOpen {
                        path: link_path.display().to_string(),
                        append: false,
                        idempotency_key: None,
                        user: None,
                    },
                ),
            ),
        },
        WriteRequest {
            message: Some(
                pplx_space_spaced_common::proto::bridge_guest::write_request::Message::Data(
                    b"replacement".to_vec(),
                ),
            ),
        },
    ];
    let error = client
        .write(tokio_stream::iter(messages))
        .await
        .expect_err("symlink write path rejected");

    assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    assert_eq!(
        std::fs::read(&target_path).expect("target read"),
        b"original"
    );

    fixture.stop().await;
}

#[tokio::test]
async fn bridge_guest_write_requires_open_first() {
    let temp_dir = tempfile::tempdir().expect("temp dir created");
    let fixture = GuestFixture::start(temp_dir.path()).await;
    let mut client = BridgeGuestFilesystemClient::new(fixture.channel.clone());

    let error = client
        .write(tokio_stream::iter(vec![WriteRequest {
            message: Some(
                pplx_space_spaced_common::proto::bridge_guest::write_request::Message::Data(
                    b"hello".to_vec(),
                ),
            ),
        }]))
        .await
        .expect_err("open required");
    assert_eq!(error.code(), tonic::Code::InvalidArgument);

    fixture.stop().await;
}

#[tokio::test]
async fn bridge_guest_mkdir_and_remove_flags_work() {
    let temp_dir = tempfile::tempdir().expect("temp dir created");
    let test_root = temp_dir.path().canonicalize().expect("canonical temp dir");
    let directory = test_root.join("parent").join("child");
    let fixture = GuestFixture::start(temp_dir.path()).await;
    let mut client = BridgeGuestFilesystemClient::new(fixture.channel.clone());

    client
        .mkdir(MkdirRequest {
            path: directory.display().to_string(),
            parents: true,
            exist_ok: false,
            user: None,
        })
        .await
        .expect("mkdir response");
    assert!(directory.is_dir());

    client
        .mkdir(MkdirRequest {
            path: directory.display().to_string(),
            parents: true,
            exist_ok: true,
            user: None,
        })
        .await
        .expect("mkdir existing ok");

    for parents in [false, true] {
        let exists_error = client
            .mkdir(MkdirRequest {
                path: directory.display().to_string(),
                parents,
                exist_ok: false,
                user: None,
            })
            .await
            .expect_err("mkdir existing rejected");
        assert_eq!(exists_error.code(), tonic::Code::AlreadyExists);
    }

    client
        .remove(RemoveRequest {
            path: test_root.join("parent").display().to_string(),
            recursive: true,
            missing_ok: false,
            user: None,
        })
        .await
        .expect("remove recursive response");
    assert!(!directory.exists());

    client
        .remove(RemoveRequest {
            path: test_root.join("parent").display().to_string(),
            recursive: true,
            missing_ok: true,
            user: None,
        })
        .await
        .expect("remove missing ok");

    let regular_file = test_root.join("blob.bin");
    std::fs::write(&regular_file, b"\x00\x01\x02\x03").expect("regular file written");
    client
        .remove(RemoveRequest {
            path: regular_file.display().to_string(),
            recursive: true,
            missing_ok: false,
            user: None,
        })
        .await
        .expect("recursive remove on a regular file should succeed (rm -rf semantics)");
    assert!(!regular_file.exists());

    fixture.stop().await;
}

#[tokio::test]
async fn bridge_guest_mkdir_accepts_current_user() {
    let temp_dir = tempfile::tempdir().expect("temp dir created");
    let test_root = temp_dir.path().canonicalize().expect("canonical temp dir");
    let directory = test_root.join("parent").join("child");
    let user = nix::unistd::User::from_uid(nix::unistd::Uid::current())
        .expect("current user lookup")
        .expect("current user exists");
    let fixture = GuestFixture::start(temp_dir.path()).await;
    let mut client = BridgeGuestFilesystemClient::new(fixture.channel.clone());

    client
        .mkdir(MkdirRequest {
            path: directory.display().to_string(),
            parents: true,
            exist_ok: false,
            user: Some(user.name.clone()),
        })
        .await
        .expect("mkdir response");

    for path in [test_root.join("parent"), directory] {
        let metadata = std::fs::metadata(path).expect("created directory metadata");
        assert_eq!(metadata.uid(), user.uid.as_raw());
        assert_eq!(metadata.gid(), user.gid.as_raw());
    }

    fixture.stop().await;
}

#[tokio::test]
async fn bridge_guest_mkdir_with_unknown_user_does_not_create_dirs() {
    let temp_dir = tempfile::tempdir().expect("temp dir created");
    let directory = temp_dir.path().join("parent").join("child");
    let fixture = GuestFixture::start(temp_dir.path()).await;
    let mut client = BridgeGuestFilesystemClient::new(fixture.channel.clone());

    let error = client
        .mkdir(MkdirRequest {
            path: directory.display().to_string(),
            parents: true,
            exist_ok: false,
            user: Some("__space_missing_user__".to_string()),
        })
        .await
        .expect_err("unknown user rejected");

    assert_eq!(error.code(), tonic::Code::InvalidArgument);
    assert!(!temp_dir.path().join("parent").exists());

    fixture.stop().await;
}

#[tokio::test]
async fn bridge_guest_mkdir_parents_follows_directory_symlink() {
    let temp_dir = tempfile::tempdir().expect("temp dir created");
    let test_root = temp_dir.path().canonicalize().expect("canonical temp dir");
    let target_dir = test_root.join("target");
    let link_dir = test_root.join("link");
    std::fs::create_dir(&target_dir).expect("target dir created");
    std::os::unix::fs::symlink(&target_dir, &link_dir).expect("symlink created");
    let directory = link_dir.join("child");
    let user = nix::unistd::User::from_uid(nix::unistd::Uid::current())
        .expect("current user lookup")
        .expect("current user exists");
    let fixture = GuestFixture::start(temp_dir.path()).await;
    let mut client = BridgeGuestFilesystemClient::new(fixture.channel.clone());

    client
        .mkdir(MkdirRequest {
            path: directory.display().to_string(),
            parents: true,
            exist_ok: false,
            user: Some(user.name),
        })
        .await
        .expect("directory symlink traversed");

    assert!(target_dir.join("child").is_dir());
    assert!(std::fs::symlink_metadata(&link_dir).unwrap().is_symlink());

    fixture.stop().await;
}

#[tokio::test]
async fn bridge_guest_mkdir_exist_ok_accepts_directory_symlink() {
    let temp_dir = tempfile::tempdir().expect("temp dir created");
    let target_dir = temp_dir.path().join("target");
    let link_dir = temp_dir.path().join("link");
    std::fs::create_dir(&target_dir).expect("target dir created");
    std::os::unix::fs::symlink(&target_dir, &link_dir).expect("symlink created");
    let fixture = GuestFixture::start(temp_dir.path()).await;
    let mut client = BridgeGuestFilesystemClient::new(fixture.channel.clone());

    client
        .mkdir(MkdirRequest {
            path: link_dir.display().to_string(),
            parents: false,
            exist_ok: true,
            user: None,
        })
        .await
        .expect("existing directory symlink accepted");

    assert!(std::fs::symlink_metadata(&link_dir).unwrap().is_symlink());
    assert!(target_dir.is_dir());

    fixture.stop().await;
}

#[tokio::test]
async fn bridge_guest_watch_reports_modified_file() {
    let temp_dir = tempfile::tempdir().expect("temp dir created");
    let watched = temp_dir.path().join("watched");
    let file = watched.join("file.txt");
    std::fs::create_dir(&watched).expect("watched dir created");
    std::fs::write(&file, b"hello").expect("file written");
    let fixture = GuestFixture::start(temp_dir.path()).await;
    let mut client = BridgeGuestFilesystemClient::new(fixture.channel.clone());

    let mut stream = client
        .watch(
            pplx_space_spaced_common::proto::bridge_guest::WatchRequest {
                path: watched.display().to_string(),
                recursive: false,
                include_metadata_events: true,
                include_globs: Vec::new(),
                exclude_globs: Vec::new(),
                use_default_excludes: None,
                user: None,
            },
        )
        .await
        .expect("watch response")
        .into_inner();
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    std::fs::write(&file, b"hello world").expect("file modified");

    let event = tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            let event = stream
                .message()
                .await
                .expect("watch message")
                .expect("watch event");
            if event.path == "file.txt"
                && event.kind()
                    == pplx_space_spaced_common::proto::bridge_guest::FileEventKind::Modified
            {
                break event;
            }
        }
    })
    .await
    .expect("watch event timed out");
    assert_eq!(event.path, "file.txt");
    assert_eq!(
        event.kind(),
        pplx_space_spaced_common::proto::bridge_guest::FileEventKind::Modified
    );

    fixture.stop().await;
}

#[tokio::test]
async fn bridge_guest_write_batch_creates_files() {
    let temp_dir = tempfile::tempdir().expect("temp dir created");
    let fixture = GuestFixture::start(temp_dir.path()).await;
    let mut client = BridgeGuestFilesystemClient::new(fixture.channel.clone());

    let first = temp_dir.path().join("a.txt");
    let second = temp_dir.path().join("b.txt");
    let response = client
        .write_batch(
            pplx_space_spaced_common::proto::bridge_guest::WriteBatchRequest {
                entries: vec![
                    pplx_space_spaced_common::proto::bridge_guest::WriteBatchEntry {
                        path: first.display().to_string(),
                        data: b"a".to_vec(),
                        append: false,
                    },
                    pplx_space_spaced_common::proto::bridge_guest::WriteBatchEntry {
                        path: second.display().to_string(),
                        data: b"b".to_vec(),
                        append: false,
                    },
                ],
                idempotency_key: Some("batch-1".into()),
                user: None,
            },
        )
        .await
        .expect("write batch response")
        .into_inner();
    assert_eq!(response.files_written, 2);
    assert_eq!(std::fs::read(first).expect("first read"), b"a");
    assert_eq!(std::fs::read(second).expect("second read"), b"b");

    fixture.stop().await;
}

#[tokio::test]
async fn bridge_guest_write_batch_failure_replays_error_without_rewrite() {
    let temp_dir = tempfile::tempdir().expect("temp dir created");
    let fixture = GuestFixture::start(temp_dir.path()).await;
    let mut client = BridgeGuestFilesystemClient::new(fixture.channel.clone());

    let first = temp_dir.path().join("partial.txt");
    let missing_parent = temp_dir.path().join("missing").join("b.txt");
    let request = pplx_space_spaced_common::proto::bridge_guest::WriteBatchRequest {
        entries: vec![
            pplx_space_spaced_common::proto::bridge_guest::WriteBatchEntry {
                path: first.display().to_string(),
                data: b"a".to_vec(),
                append: false,
            },
            pplx_space_spaced_common::proto::bridge_guest::WriteBatchEntry {
                path: missing_parent.display().to_string(),
                data: b"b".to_vec(),
                append: false,
            },
        ],
        idempotency_key: Some("batch-partial".into()),
        user: None,
    };

    let error = client
        .write_batch(request.clone())
        .await
        .expect_err("partial batch should fail");
    assert_eq!(error.code(), tonic::Code::NotFound);
    assert_eq!(std::fs::read(&first).expect("partial first file"), b"a");

    std::fs::write(&first, b"changed").expect("partial first file changed");
    std::fs::create_dir(missing_parent.parent().expect("parent")).expect("parent created");
    let retry_error = client
        .write_batch(request)
        .await
        .expect_err("retry should replay original failure");
    assert_eq!(retry_error.code(), tonic::Code::NotFound);
    assert_eq!(
        std::fs::read(&first).expect("partial first file"),
        b"changed"
    );
    assert!(!missing_parent.exists());

    fixture.stop().await;
}

#[tokio::test]
async fn bridge_guest_watch_reports_created_file() {
    let temp_dir = tempfile::tempdir().expect("temp dir created");
    let watched = temp_dir.path().join("watched");
    std::fs::create_dir(&watched).expect("watched dir created");
    let fixture = GuestFixture::start(temp_dir.path()).await;
    let mut client = BridgeGuestFilesystemClient::new(fixture.channel.clone());

    let mut stream = client
        .watch(
            pplx_space_spaced_common::proto::bridge_guest::WatchRequest {
                path: watched.display().to_string(),
                recursive: false,
                include_metadata_events: true,
                include_globs: Vec::new(),
                exclude_globs: Vec::new(),
                use_default_excludes: None,
                user: None,
            },
        )
        .await
        .expect("watch response")
        .into_inner();
    let new_file = watched.join("created.txt");
    std::fs::write(&new_file, b"hello").expect("created file written");

    let event = tokio::time::timeout(std::time::Duration::from_secs(2), stream.message())
        .await
        .expect("watch event timed out")
        .expect("watch message")
        .expect("watch event");
    assert_eq!(event.path, "created.txt");
    assert_eq!(
        event.kind(),
        pplx_space_spaced_common::proto::bridge_guest::FileEventKind::Created
    );

    fixture.stop().await;
}

#[tokio::test]
async fn bridge_guest_rename_requires_and_dedups_idempotency_key() {
    let temp_dir = tempfile::tempdir().expect("temp dir created");
    let old_path = temp_dir.path().join("old.txt");
    let new_path = temp_dir.path().join("new.txt");
    std::fs::write(&old_path, b"hello").expect("sample file written");
    let fixture = GuestFixture::start(temp_dir.path()).await;
    let mut client = BridgeGuestFilesystemClient::new(fixture.channel.clone());

    let missing_key = client
        .rename(RenameRequest {
            old_path: old_path.display().to_string(),
            new_path: new_path.display().to_string(),
            idempotency_key: None,
            user: None,
        })
        .await
        .expect_err("rename without key rejected");
    assert_eq!(missing_key.code(), tonic::Code::FailedPrecondition);

    client
        .rename(RenameRequest {
            old_path: old_path.display().to_string(),
            new_path: new_path.display().to_string(),
            idempotency_key: Some("rename-1".into()),
            user: None,
        })
        .await
        .expect("rename response");
    client
        .rename(RenameRequest {
            old_path: old_path.display().to_string(),
            new_path: new_path.display().to_string(),
            idempotency_key: Some("rename-1".into()),
            user: None,
        })
        .await
        .expect("rename dedup response");
    assert!(!old_path.exists());
    assert_eq!(std::fs::read(&new_path).expect("renamed file"), b"hello");

    fixture.stop().await;
}

#[tokio::test]
async fn bridge_guest_reports_hard_link_count() {
    let temp_dir = tempfile::tempdir().expect("temp dir created");
    let directory = temp_dir.path().join("dir");
    std::fs::create_dir(&directory).expect("directory created");
    let linked = directory.join("linked.txt");
    std::fs::write(&linked, b"hello").expect("sample file written");
    std::fs::hard_link(&linked, directory.join("alias.txt")).expect("hard link created");
    std::fs::write(directory.join("lone.txt"), b"hello").expect("sample file written");
    let fixture = GuestFixture::start(temp_dir.path()).await;
    let mut client = BridgeGuestFilesystemClient::new(fixture.channel.clone());

    let stat = client
        .stat_info(StatRequest {
            path: linked.display().to_string(),
            user: None,
        })
        .await
        .expect("stat response")
        .into_inner()
        .info
        .expect("file info");
    assert_eq!(stat.link_count, Some(2));

    let mut stream = client
        .list_info(ListRequest {
            path: directory.display().to_string(),
            depth: Some(1),
            user: None,
        })
        .await
        .expect("list response")
        .into_inner();
    let mut counts = Vec::new();
    while let Some(entry) = stream.message().await.expect("list item") {
        counts.push((entry.name.clone().expect("entry name"), entry.link_count));
    }
    counts.sort_by(|left, right| left.0.cmp(&right.0));
    // Both names of the linked inode report 2, and an ordinary file 1: the
    // count comes from the inode, not from a constant either path assumed.
    assert_eq!(
        counts,
        vec![
            ("alias.txt".to_string(), Some(2)),
            ("linked.txt".to_string(), Some(2)),
            ("lone.txt".to_string(), Some(1)),
        ]
    );

    fixture.stop().await;
}
