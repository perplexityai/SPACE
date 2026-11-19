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

#[test]
fn empty_slots_and_large_non_utf8_maps_do_not_block_reuse() {
    let slot = tempfile::tempdir().unwrap();
    ensure_unreferenced(slot.path()).unwrap();
    let line = b"1000-2000 r--p 00000000 08:01 42 /tmp/lib\xe9.so\n";
    let maps = line.repeat(30_000);
    ensure_maps_unreferenced(&maps, &[(libc::makedev(8, 1), 43)]).unwrap();
    assert!(matches!(
        ensure_maps_unreferenced(&maps, &[(libc::makedev(8, 1), 42)]),
        Err(UpdateError::Referenced)
    ));
}
