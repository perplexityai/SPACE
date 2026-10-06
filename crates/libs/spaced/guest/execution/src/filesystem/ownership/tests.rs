use super::*;

#[tokio::test]
async fn ownership_requires_a_directory_and_never_follows_a_symlink() {
    let root = tempfile::tempdir().expect("directory");
    let directory = root.path().join("directory");
    std::fs::create_dir(&directory).expect("directory");
    let file = root.path().join("file");
    std::fs::write(&file, b"unchanged").expect("file");
    let link = root.path().join("link");
    std::os::unix::fs::symlink(&directory, &link).expect("symlink");
    let uid = nix::unistd::Uid::effective().as_raw();
    let gid = nix::unistd::Gid::effective().as_raw();
    for path in [&file, &link] {
        assert_eq!(
            chown_directory_no_follow(path, uid, gid)
                .await
                .expect_err("wrong target kind")
                .code(),
            tonic::Code::FailedPrecondition
        );
    }
    chown_directory_no_follow(&directory, uid, gid)
        .await
        .expect("directory ownership");
    assert_eq!(std::fs::read(file).expect("file"), b"unchanged");
    assert!(std::fs::symlink_metadata(link).expect("link").is_symlink());
}
