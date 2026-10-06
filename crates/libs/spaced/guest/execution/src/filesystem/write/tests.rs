use super::*;
use std::sync::{Arc, Mutex};

#[tokio::test]
async fn only_explicit_finish_completes_a_worker_upload() {
    use pplx_space_spaced_guest_protocol::proto::worker::{Abort, Finish, WriteStart};
    for terminal in [
        None,
        Some(write_frame::Frame::Abort(Abort {})),
        Some(write_frame::Frame::Finish(Finish {})),
    ] {
        let directory = tempfile::tempdir().expect("directory");
        let path = directory.path().join("append");
        let current = nix::unistd::User::from_uid(nix::unistd::Uid::current())
            .expect("lookup")
            .expect("account");
        let user = pplx_space_common_core::types::GuestUserName::parse(current.name).expect("user");
        let account = GuestAccount::resolve(user).expect("account");
        let finished = matches!(terminal, Some(write_frame::Frame::Finish(_)));
        let mut frames = vec![
            Ok(WriteFrame {
                frame: Some(write_frame::Frame::Open(WriteStart {
                    execution_id: uuid::Uuid::now_v7().to_string(),
                    path: path.display().to_string(),
                    append: true,
                })),
            }),
            Ok(WriteFrame {
                frame: Some(write_frame::Frame::Data(b"once".to_vec())),
            }),
        ];
        if let Some(frame) = terminal {
            frames.push(Ok(WriteFrame { frame: Some(frame) }));
        }
        let result = write(
            futures_util::stream::iter(frames),
            &account,
            None,
            tokio_util::sync::CancellationToken::new(),
            Arc::new(UploadBufferPool::new()),
        )
        .await;
        assert_eq!(result.is_ok(), finished);
        if !finished {
            assert!(matches!(result, Err(WriteError::Interrupted)));
        }
        assert_eq!(
            tokio::fs::read(path).await.expect("settled file"),
            b"once",
            "interruption does not imply rollback"
        );
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RecordedFileOperation {
    Flush,
    SyncAll,
}

#[derive(Default)]
struct RecordingDurableFile {
    operations: Arc<Mutex<Vec<RecordedFileOperation>>>,
    fail_on: Option<RecordedFileOperation>,
}

impl RecordingDurableFile {
    fn new() -> Self {
        Self {
            operations: Arc::default(),
            fail_on: None,
        }
    }

    fn failing_on(operation: RecordedFileOperation) -> Self {
        Self {
            operations: Arc::default(),
            fail_on: Some(operation),
        }
    }

    fn operations(&self) -> Vec<RecordedFileOperation> {
        self.operations
            .lock()
            .expect("operation log lock poisoned")
            .clone()
    }
}

impl DurableFile for RecordingDurableFile {
    async fn flush(&mut self) -> io::Result<()> {
        let operations = Arc::clone(&self.operations);
        let fail_on = self.fail_on;
        operations
            .lock()
            .expect("operation log lock poisoned")
            .push(RecordedFileOperation::Flush);
        if fail_on == Some(RecordedFileOperation::Flush) {
            return Err(io::Error::other("flush failed"));
        }
        Ok(())
    }

    async fn sync_all(&self) -> io::Result<()> {
        let operations = Arc::clone(&self.operations);
        let fail_on = self.fail_on;
        operations
            .lock()
            .expect("operation log lock poisoned")
            .push(RecordedFileOperation::SyncAll);
        if fail_on == Some(RecordedFileOperation::SyncAll) {
            return Err(io::Error::other("sync_all failed"));
        }
        Ok(())
    }
}

#[tokio::test]
async fn finalize_written_file_flushes_then_syncs_a_file_written_in_place() {
    let mut file = RecordingDurableFile::new();

    finalize_written_file(&mut file, false)
        .await
        .expect("write completion");

    assert_eq!(
        file.operations(),
        vec![RecordedFileOperation::Flush, RecordedFileOperation::SyncAll]
    );
}

// A replacement's rename publishes it atomically; persistence waits for the
// filesystem's own writeback, or a lifecycle flush.
#[tokio::test]
async fn finalize_written_file_only_flushes_a_replacement() {
    let mut file = RecordingDurableFile::new();

    finalize_written_file(&mut file, true)
        .await
        .expect("write completion");

    assert_eq!(file.operations(), vec![RecordedFileOperation::Flush]);
}

#[tokio::test]
async fn finalize_written_file_stops_before_sync_when_flush_fails() {
    let mut file = RecordingDurableFile::failing_on(RecordedFileOperation::Flush);

    let error = finalize_written_file(&mut file, false)
        .await
        .expect_err("flush failure should fail completion");

    assert_eq!(error.code(), tonic::Code::Internal);
    assert_eq!(file.operations(), vec![RecordedFileOperation::Flush]);
}

#[tokio::test]
async fn finalize_written_file_reports_sync_all_failures() {
    let mut file = RecordingDurableFile::failing_on(RecordedFileOperation::SyncAll);

    let error = finalize_written_file(&mut file, false)
        .await
        .expect_err("sync_all failure should fail completion");

    assert_eq!(error.code(), tonic::Code::Internal);
    assert_eq!(
        file.operations(),
        vec![RecordedFileOperation::Flush, RecordedFileOperation::SyncAll]
    );
}

async fn write_frames(
    path: &std::path::Path,
    append: bool,
    data: &[&[u8]],
    terminal: Option<write_frame::Frame>,
) -> Result<WriteResponse, WriteError> {
    use pplx_space_spaced_guest_protocol::proto::worker::WriteStart;
    let current = nix::unistd::User::from_uid(nix::unistd::Uid::current())
        .expect("lookup")
        .expect("account");
    let user = pplx_space_common_core::types::GuestUserName::parse(current.name).expect("user");
    let account = GuestAccount::resolve(user).expect("account");
    let mut frames = vec![Ok(WriteFrame {
        frame: Some(write_frame::Frame::Open(WriteStart {
            execution_id: uuid::Uuid::now_v7().to_string(),
            path: path.display().to_string(),
            append,
        })),
    })];
    frames.extend(data.iter().map(|data| {
        Ok(WriteFrame {
            frame: Some(write_frame::Frame::Data(data.to_vec())),
        })
    }));
    frames.extend(terminal.map(|frame| Ok(WriteFrame { frame: Some(frame) })));
    write(
        futures_util::stream::iter(frames),
        &account,
        None,
        tokio_util::sync::CancellationToken::new(),
        Arc::new(UploadBufferPool::new()),
    )
    .await
}

fn directory_names(directory: &std::path::Path) -> Vec<String> {
    let mut names: Vec<_> = std::fs::read_dir(directory)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    names.sort();
    names
}

// An overwrite that fails or is interrupted must not leave a truncated or
// partly written destination: readers keep seeing the previous content.
#[tokio::test]
async fn an_unfinished_overwrite_leaves_the_previous_content() {
    use pplx_space_spaced_guest_protocol::proto::worker::Abort;
    for terminal in [None, Some(write_frame::Frame::Abort(Abort {}))] {
        let directory = tempfile::tempdir().expect("directory");
        let path = directory.path().join("file");
        std::fs::write(&path, b"previous").unwrap();
        let result = write_frames(&path, false, &[b"partial"], terminal).await;
        assert!(matches!(result, Err(WriteError::Interrupted)));
        assert_eq!(std::fs::read(&path).unwrap(), b"previous");
        assert_eq!(directory_names(directory.path()), ["file"]);
    }
}

// A completed overwrite replaces the content, keeps the destination's mode,
// and leaves no temporary file behind.
#[tokio::test]
async fn a_completed_overwrite_replaces_the_file_and_keeps_its_mode() {
    use pplx_space_spaced_guest_protocol::proto::worker::Finish;
    use std::os::unix::fs::PermissionsExt;
    let directory = tempfile::tempdir().expect("directory");
    let path = directory.path().join("script");
    std::fs::write(&path, b"#!/bin/sh\necho previous\n").unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o751)).unwrap();
    let response = write_frames(
        &path,
        false,
        &[b"#!/bin/sh\n", b"echo new\n"],
        Some(write_frame::Frame::Finish(Finish {})),
    )
    .await
    .expect("write completes");
    assert_eq!(response.bytes_written, 19);
    assert_eq!(std::fs::read(&path).unwrap(), b"#!/bin/sh\necho new\n");
    assert_eq!(
        std::fs::metadata(&path).unwrap().permissions().mode() & 0o7777,
        0o751
    );
    assert_eq!(directory_names(directory.path()), ["script"]);
}

// Replacing would change what a hard-linked path or a symlink refers to, so
// those keep the in-place behavior: every link sees the new content, and a
// symlink is refused as before.
#[tokio::test]
async fn links_are_written_in_place_as_before() {
    use pplx_space_spaced_guest_protocol::proto::worker::Finish;
    let directory = tempfile::tempdir().expect("directory");
    let path = directory.path().join("file");
    let link = directory.path().join("link");
    std::fs::write(&path, b"previous").unwrap();
    std::fs::hard_link(&path, &link).unwrap();
    write_frames(
        &path,
        false,
        &[b"new"],
        Some(write_frame::Frame::Finish(Finish {})),
    )
    .await
    .expect("write completes");
    assert_eq!(std::fs::read(&link).unwrap(), b"new");

    let symlink = directory.path().join("symlink");
    std::os::unix::fs::symlink(&path, &symlink).unwrap();
    let refused = write_frames(
        &symlink,
        false,
        &[b"other"],
        Some(write_frame::Frame::Finish(Finish {})),
    )
    .await;
    assert!(matches!(refused, Err(WriteError::Failed(_))));
    assert_eq!(std::fs::read(&path).unwrap(), b"new");
    assert_eq!(
        directory_names(directory.path()),
        ["file", "link", "symlink"]
    );
}

// Replacing must not let a caller do what overwriting would not: an owned file
// the caller cannot open for writing is still refused, and keeps its content.
#[tokio::test]
async fn a_file_the_caller_cannot_write_is_not_replaced() {
    use pplx_space_spaced_guest_protocol::proto::worker::Finish;
    use std::os::unix::fs::PermissionsExt;
    if nix::unistd::geteuid().is_root() {
        // Root may write any file, so there is no refusal to preserve.
        return;
    }
    let directory = tempfile::tempdir().expect("directory");
    let path = directory.path().join("readonly");
    std::fs::write(&path, b"previous").unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o444)).unwrap();
    let refused = write_frames(
        &path,
        false,
        &[b"new"],
        Some(write_frame::Frame::Finish(Finish {})),
    )
    .await;
    assert!(
        matches!(&refused, Err(WriteError::Failed(status)) if status.code() == tonic::Code::PermissionDenied),
        "{refused:?}"
    );
    assert_eq!(std::fs::read(&path).unwrap(), b"previous");
    assert_eq!(directory_names(directory.path()), ["readonly"]);
}

// Replacing needs only to create and rename in the directory, not to read
// it, so an overwrite in a directory that can be written but not read is
// still a replacement.
#[tokio::test]
async fn an_unreadable_directory_is_still_overwritten_by_replacement() {
    use pplx_space_spaced_guest_protocol::proto::worker::Finish;
    use std::os::unix::fs::{MetadataExt, PermissionsExt};
    if nix::unistd::geteuid().is_root() {
        return;
    }
    let directory = tempfile::tempdir().expect("directory");
    let path = directory.path().join("file");
    std::fs::write(&path, b"previous").unwrap();
    let previous = std::fs::metadata(&path).unwrap().ino();
    std::fs::set_permissions(directory.path(), std::fs::Permissions::from_mode(0o300)).unwrap();
    let written = write_frames(
        &path,
        false,
        &[b"new"],
        Some(write_frame::Frame::Finish(Finish {})),
    )
    .await;
    std::fs::set_permissions(directory.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    written.expect("the overwrite succeeds");
    assert_eq!(std::fs::read(&path).unwrap(), b"new");
    assert_ne!(
        std::fs::metadata(&path).unwrap().ino(),
        previous,
        "the file was replaced, not written in place"
    );
    assert_eq!(directory_names(directory.path()), ["file"]);
}

#[cfg(target_os = "linux")]
const ACL_UNDEFINED_ID: u32 = u32::MAX;

/// Linux's binary ACL attribute: version 2, then sorted (tag, permissions,
/// id) entries.
#[cfg(target_os = "linux")]
fn posix_acl(entries: &[(u16, u16, u32)]) -> Vec<u8> {
    let mut acl = 2_u32.to_le_bytes().to_vec();
    for (tag, permissions, id) in entries {
        acl.extend_from_slice(&tag.to_le_bytes());
        acl.extend_from_slice(&permissions.to_le_bytes());
        acl.extend_from_slice(&id.to_le_bytes());
    }
    acl
}

/// Owner rw(x), a named user `named`, the owning group nothing, a mask
/// allowing `named`, and others nothing.
#[cfg(target_os = "linux")]
fn acl_with_named_user(owner: u16, named: u16) -> Vec<u8> {
    posix_acl(&[
        (0x01, owner, ACL_UNDEFINED_ID),
        (0x02, named, 12_345),
        (0x04, 0, ACL_UNDEFINED_ID),
        (0x10, named, ACL_UNDEFINED_ID),
        (0x20, 0, ACL_UNDEFINED_ID),
    ])
}

/// Set an ACL attribute; `false` only when the filesystem has no ACLs.
#[cfg(target_os = "linux")]
fn set_acl(path: &std::path::Path, name: &str, acl: &[u8]) -> bool {
    let file = std::fs::File::open(path).unwrap();
    match rustix::fs::fsetxattr(&file, name, acl, rustix::fs::XattrFlags::empty()) {
        Ok(()) => true,
        Err(rustix::io::Errno::NOTSUP) => false,
        Err(error) => panic!("set {name} on {}: {error}", path.display()),
    }
}

#[cfg(target_os = "linux")]
fn access_acl(path: &std::path::Path) -> Option<Vec<u8>> {
    let file = std::fs::File::open(path).unwrap();
    let mut buffer = vec![0_u8; 256];
    match rustix::fs::fgetxattr(&file, "system.posix_acl_access", &mut buffer[..]) {
        Ok(length) => {
            buffer.truncate(length);
            Some(buffer)
        }
        Err(rustix::io::Errno::NODATA) => None,
        Err(error) => panic!("read the access ACL of {}: {error}", path.display()),
    }
}

#[cfg(target_os = "linux")]
async fn overwrite(path: &std::path::Path, data: &[u8]) {
    use pplx_space_spaced_guest_protocol::proto::worker::Finish;
    write_frames(
        path,
        false,
        &[data],
        Some(write_frame::Frame::Finish(Finish {})),
    )
    .await
    .expect("write completes");
}

// A file with an access ACL is written in place: a replacement would carry
// the mode, whose group bits are the ACL's mask, without the ACL that limits
// the owning group.
#[cfg(target_os = "linux")]
#[tokio::test]
async fn a_file_with_an_access_acl_is_written_in_place() {
    let directory = tempfile::tempdir().expect("directory");
    let path = directory.path().join("shared");
    std::fs::write(&path, b"previous").unwrap();
    let acl = acl_with_named_user(6, 6);
    if !set_acl(&path, "system.posix_acl_access", &acl) {
        return;
    }
    overwrite(&path, b"new").await;
    assert_eq!(std::fs::read(&path).unwrap(), b"new");
    assert_eq!(
        access_acl(&path),
        Some(acl),
        "the file kept its ACL, so it was written in place"
    );
    assert_eq!(directory_names(directory.path()), ["shared"]);
}

// A directory's default ACL is inherited by the temporary file. Replacing an
// ACL-free file must not publish it: with the destination's mode applied, its
// named user would gain access the destination never gave.
#[cfg(target_os = "linux")]
#[tokio::test]
async fn a_directory_default_acl_does_not_reach_a_replaced_file() {
    use std::os::unix::fs::PermissionsExt;
    let directory = tempfile::tempdir().expect("directory");
    let path = directory.path().join("private");
    std::fs::write(&path, b"previous").unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o640)).unwrap();
    if !set_acl(
        directory.path(),
        "system.posix_acl_default",
        &acl_with_named_user(7, 4),
    ) {
        return;
    }
    overwrite(&path, b"new").await;
    assert_eq!(std::fs::read(&path).unwrap(), b"new");
    assert_eq!(access_acl(&path), None, "no access ACL was published");
    assert_eq!(
        std::fs::metadata(&path).unwrap().permissions().mode() & 0o7777,
        0o640
    );
    assert_eq!(directory_names(directory.path()), ["private"]);
}

// A new file is created as the in-place write would create it, so it still
// inherits the directory's default ACL.
#[cfg(target_os = "linux")]
#[tokio::test]
async fn a_new_file_inherits_the_directory_default_acl() {
    let directory = tempfile::tempdir().expect("directory");
    if !set_acl(
        directory.path(),
        "system.posix_acl_default",
        &acl_with_named_user(7, 4),
    ) {
        return;
    }
    let path = directory.path().join("created");
    overwrite(&path, b"new").await;
    assert_eq!(std::fs::read(&path).unwrap(), b"new");
    assert!(access_acl(&path).is_some(), "the default ACL was inherited");
}
