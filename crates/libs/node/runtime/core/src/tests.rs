#[cfg(target_os = "linux")]
#[test]
fn restored_nocow_file_remains_reflink_compatible() {
    let Some(root) = std::env::var_os("SPACE_NOCOW_TEST_BTRFS_DIR") else {
        assert!(
            std::env::var_os("CI").is_none()
                && std::env::var_os("SPACE_CI_REQUIRE_TEST_DEPS").is_none(),
            "CI must provide the disposable NoCoW btrfs fixture"
        );
        eprintln!("not opted in: set SPACE_NOCOW_TEST_BTRFS_DIR to a disposable btrfs directory");
        return;
    };
    let temporary = tempfile::tempdir_in(root).unwrap();
    let src = temporary.path().join("source");
    std::fs::write(&src, []).unwrap();
    assert!(
        std::process::Command::new("chattr")
            .arg("+C")
            .arg(&src)
            .status()
            .unwrap()
            .success()
    );
    let original = vec![7; 8192];
    std::fs::write(&src, &original).unwrap();
    let dst = temporary.path().join("restored");
    super::reflink_copy(&src, &dst).unwrap();
    // A mandatory clone into an empty, known NoCoW inode is an independent
    // compatibility oracle; flags alone cannot show that reflinking works.
    let check = temporary.path().join("check");
    std::fs::write(&check, []).unwrap();
    assert!(
        std::process::Command::new("chattr")
            .arg("+C")
            .arg(&check)
            .status()
            .unwrap()
            .success()
    );
    assert!(
        std::process::Command::new("cp")
            .arg("--reflink=always")
            .arg(&dst)
            .arg(&check)
            .status()
            .unwrap()
            .success()
    );
    std::fs::write(&src, b"modified").unwrap();
    assert_eq!(std::fs::read(&dst).unwrap(), original);
}
