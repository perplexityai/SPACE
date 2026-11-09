use super::*;

#[test]
fn exited_process_errors_do_not_hide_real_fd_scan_failures() {
    for (errno, exited) in [
        (libc::ENOENT, true),
        (libc::ESRCH, true),
        (libc::EACCES, false),
        (libc::EIO, false),
    ] {
        let entries = [Err(io::Error::from_raw_os_error(errno))];
        let result = ensure_fds_unreferenced(entries, &[], &mut 0);
        assert_eq!(result.is_ok(), exited, "errno={errno}");
    }
}
