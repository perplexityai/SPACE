use super::*;

#[test]
fn bind_inputs_are_readonly_without_changing_the_source_mount() {
    if !pplx_space_test_env::root_or_skip("bind mount native guest inputs") {
        return;
    }
    const CHILD_ROOT: &str = "SPACE_TEST_WASI_BIND_ROOT";
    if let Some(root) = std::env::var_os(CHILD_ROOT) {
        nix::sched::unshare(nix::sched::CloneFlags::CLONE_NEWNS).unwrap();
        mount::<str, str, str, str>(None, "/", None, MsFlags::MS_REC | MsFlags::MS_SLAVE, None)
            .unwrap();
        std::env::set_current_dir(root).unwrap();
        let source = File::open("source").unwrap();
        bind_file(&source, "tool").unwrap();
        assert_eq!(std::fs::read("tool").unwrap(), b"trusted runtime");
        assert_eq!(
            std::fs::write("tool", b"replacement")
                .unwrap_err()
                .raw_os_error(),
            Some(libc::EROFS),
        );
        std::fs::write("source", b"host update").unwrap();
        assert_eq!(std::fs::read("tool").unwrap(), b"host update");

        std::fs::create_dir("input").unwrap();
        std::fs::write("input/library", b"trusted library").unwrap();
        std::fs::create_dir("library").unwrap();
        bind_directory(
            &open_directory(Path::new("input")).unwrap(),
            "library",
            true,
        )
        .unwrap();
        assert_eq!(
            std::fs::read("library/library").unwrap(),
            b"trusted library"
        );
        assert_eq!(
            std::fs::write("library/injected", b"guest injection")
                .unwrap_err()
                .raw_os_error(),
            Some(libc::EROFS),
        );
        std::fs::write("input/host-file", b"still writable on host").unwrap();

        std::fs::create_dir("workspace").unwrap();
        bind_directory(
            &open_directory(Path::new("input")).unwrap(),
            "workspace",
            false,
        )
        .unwrap();
        std::fs::write("workspace/artifact", b"guest output").unwrap();
        assert_eq!(std::fs::read("input/artifact").unwrap(), b"guest output");
        let workspace = nix::sys::statvfs::statvfs("workspace").unwrap();
        assert!(
            workspace
                .flags()
                .contains(nix::sys::statvfs::FsFlags::ST_NOEXEC)
        );
        return;
    }
    let root = tempfile::tempdir().unwrap();
    std::fs::write(root.path().join("source"), b"trusted runtime").unwrap();
    let result = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "contained_launcher::wasi_guest::tests::bind_inputs_are_readonly_without_changing_the_source_mount",
            "--nocapture",
        ])
        .env(CHILD_ROOT, root.path())
        .output()
        .unwrap();
    assert!(
        result.status.success(),
        "{}{}",
        String::from_utf8_lossy(&result.stdout),
        String::from_utf8_lossy(&result.stderr),
    );
}
