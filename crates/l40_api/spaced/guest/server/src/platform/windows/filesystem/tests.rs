use super::*;

struct TestDirectory(std::path::PathBuf);
impl Drop for TestDirectory {
    fn drop(&mut self) {
        if let Err(error) = std::fs::remove_dir_all(&self.0) {
            eprintln!("remove test directory: {error}");
        }
    }
}

#[test]
fn removing_a_directory_link_preserves_its_target_for_both_recursion_modes() {
    let root =
        TestDirectory(std::env::temp_dir().join(format!("space-remove-{}", uuid::Uuid::now_v7())));
    std::fs::create_dir(&root.0).unwrap();
    let target = root.0.join("target");
    std::fs::create_dir(&target).unwrap();
    let sentinel = target.join("sentinel");
    std::fs::write(&sentinel, b"retained").unwrap();
    for recursive in [false, true] {
        let link = root.0.join("link");
        std::os::windows::fs::symlink_dir(&target, &link).unwrap();
        remove_entry(&link, recursive).unwrap();
        assert!(
            std::fs::symlink_metadata(&link).is_err_and(|e| e.kind() == io::ErrorKind::NotFound)
        );
        assert_eq!(std::fs::read(&sentinel).unwrap(), b"retained");
    }
}

#[test]
fn filesystem_refusals_are_distinct_from_internal_failures() {
    for kind in [
        io::ErrorKind::NotADirectory,
        io::ErrorKind::DirectoryNotEmpty,
        io::ErrorKind::CrossesDevices,
    ] {
        assert_eq!(
            io_status(io::Error::from(kind)).code(),
            tonic::Code::FailedPrecondition
        );
    }
    for code in [
        windows_sys::Win32::Foundation::ERROR_LOCK_VIOLATION,
        windows_sys::Win32::Foundation::ERROR_SHARING_VIOLATION,
    ] {
        assert_eq!(
            io_status(io::Error::from_raw_os_error(i32::try_from(code).unwrap())).code(),
            tonic::Code::FailedPrecondition
        );
    }
}
