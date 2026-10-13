use super::*;

#[test]
fn scratch_scripts_remain_pinned_when_staged_paths_are_replaced() {
    let root = tempfile::tempdir().expect("staging root");
    let scripts = write_scratch_scripts(root.path()).expect("stage scripts");
    let bundle_path = scripts._tempdir.path().to_path_buf();
    assert_eq!(
        fs::metadata(&bundle_path)
            .expect("bundle metadata")
            .permissions()
            .mode()
            & 0o777,
        0o700
    );

    for (script, expected) in [
        (&scripts.helper, BUNDLED_BUILD_SEED_SCRIPT),
        (&scripts.rootfs_builder, BUNDLED_ROOTFS_BUILDER_SCRIPT),
    ] {
        fs::rename(script.path(), script.path().with_extension("original"))
            .expect("rename staged script");
        fs::write(script.path(), "echo replacement\n").expect("replace staged path");
        let output = Command::new("/bin/cat")
            .arg(scratch_script_path(script))
            .output()
            .expect("read script from child process");
        assert!(output.status.success());
        assert_eq!(output.stdout, expected.as_bytes());
    }

    drop(scripts);
    assert!(!bundle_path.exists(), "bundle must be removed on drop");
}
