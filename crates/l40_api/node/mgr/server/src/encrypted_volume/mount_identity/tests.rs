use super::*;

#[test]
fn only_one_resolved_block_device_is_ownership_evidence() {
    let root = tempfile::tempdir().unwrap();
    single_device(root.path()).unwrap_err();
    let device = root.path().join("dm-5");
    std::fs::create_dir(&device).unwrap();
    single_device(root.path()).unwrap_err();
    for (raw, expected) in [
        ("253:5\n", Some((253, 5))),
        ("0:42\n", None),
        ("garbage", None),
        ("253:", None),
    ] {
        std::fs::write(device.join("dev"), raw).unwrap();
        assert_eq!(single_device(root.path()).ok(), expected, "{raw}");
    }
    std::fs::write(device.join("dev"), "253:5\n").unwrap();
    std::fs::create_dir(root.path().join("dm-6")).unwrap();
    single_device(root.path()).unwrap_err();
}
