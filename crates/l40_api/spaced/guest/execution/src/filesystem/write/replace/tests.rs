use super::*;
use std::io::Write;

// Another user who may write the directory replaces the temporary name during
// the upload, with a file or a symlink to one the writer can read. The
// bind-mount fallback must copy only the uploaded bytes, and leave the
// substitute alone.
#[test]
fn the_fallback_copies_the_uploaded_inode_whatever_its_name_now_names() {
    for substitute_with_symlink in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let temporary = directory.path().join(".destination.spaced-upload");
        let mut upload = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&temporary)
            .unwrap();
        upload.write_all(b"uploaded").unwrap();
        // The upload leaves O_DIRECT on where the filesystem takes it; the
        // copy must still read the unaligned tail.
        #[cfg(target_os = "linux")]
        let _direct = super::super::upload::set_direct_io(&upload, true);

        std::fs::remove_file(&temporary).unwrap();
        let secret = directory.path().join("secret");
        std::fs::write(&secret, b"readable by the writer only").unwrap();
        if substitute_with_symlink {
            std::os::unix::fs::symlink(&secret, &temporary).unwrap();
        } else {
            std::fs::write(&temporary, b"substituted").unwrap();
        }

        let destination = directory.path().join("destination");
        std::fs::write(&destination, b"previous content").unwrap();
        let target = std::fs::OpenOptions::new()
            .write(true)
            .open(&destination)
            .unwrap();
        copy_upload_in_place(&upload, target, &temporary).unwrap();

        assert_eq!(std::fs::read(&destination).unwrap(), b"uploaded");
        assert!(
            std::fs::symlink_metadata(&temporary).is_ok(),
            "the substitute is not removed"
        );
        assert_eq!(
            std::fs::read(&secret).unwrap(),
            b"readable by the writer only"
        );
    }
}

// Without a substitute, the fallback removes the temporary file it copied.
#[test]
fn the_fallback_removes_the_temporary_file_it_copied() {
    let directory = tempfile::tempdir().unwrap();
    let temporary = directory.path().join(".destination.spaced-upload");
    let mut upload = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(&temporary)
        .unwrap();
    upload.write_all(b"uploaded").unwrap();
    let destination = directory.path().join("destination");
    std::fs::write(&destination, b"previous content").unwrap();
    let target = std::fs::OpenOptions::new()
        .write(true)
        .open(&destination)
        .unwrap();
    copy_upload_in_place(&upload, target, &temporary).unwrap();
    assert_eq!(std::fs::read(&destination).unwrap(), b"uploaded");
    std::fs::symlink_metadata(&temporary).unwrap_err();
}
