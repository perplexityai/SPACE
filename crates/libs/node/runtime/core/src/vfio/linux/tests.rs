use super::*;

#[test]
fn pinning_a_device_does_not_open_it_for_io() {
    let file = pin_node(Path::new("/dev/null"), false).unwrap();
    let flags = nix::fcntl::fcntl(&file, nix::fcntl::FcntlArg::F_GETFL).unwrap();
    assert_ne!(flags & libc::O_PATH, 0);
    assert_eq!(
        std::io::Read::read(&mut &file, &mut [0])
            .unwrap_err()
            .raw_os_error(),
        Some(libc::EBADF)
    );
}
