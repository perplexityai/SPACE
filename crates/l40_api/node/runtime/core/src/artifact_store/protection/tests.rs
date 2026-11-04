use super::*;

#[test]
fn directory_io_failures_preserve_their_cause() {
    for errno in [
        libc::EIO,
        libc::EMFILE,
        libc::EINTR,
        libc::ENOENT,
        libc::EACCES,
    ] {
        let error = check_directories_with(
            Path::new("/synthetic/cache"),
            0,
            RootProtection::ExecutionAncestors,
            |_| Err(std::io::Error::from_raw_os_error(errno)),
        )
        .unwrap_err();
        let VerifyError::Io { path, source, .. } = VerifyError::from(error) else {
            panic!("lost I/O cause")
        };
        assert_eq!(source.raw_os_error(), Some(errno));
        assert_eq!(path, Path::new("/synthetic/cache"));
    }
}
