#[test]
fn advise_dontneed_accepts_regular_file() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("rootfs.ext4");
    std::fs::write(&path, b"rootfs").unwrap();
    let file = std::fs::File::open(&path).unwrap();

    super::advise_dontneed(&file, &path, "test page cache");
}

#[cfg(target_os = "linux")]
#[test]
fn range_writeback_and_advice_accept_regular_file() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("streamed-output");
    std::fs::write(&path, vec![0xa5; 64 * 1024]).unwrap();
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .unwrap();

    super::start_writeback_range(&file, 0, 64 * 1024).unwrap();
    super::wait_writeback_range(&file, 0, 64 * 1024).unwrap();
    super::advise_range_dontneed(&file, 0, 64 * 1024).unwrap();
}
