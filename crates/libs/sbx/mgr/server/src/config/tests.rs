use super::*;

#[test]
fn only_an_absent_config_uses_defaults() {
    let directory = tempfile::tempdir().unwrap();
    let missing = directory.path().join("absent.toml");
    let defaults = load_from(&missing).unwrap();
    assert_eq!(defaults.max_memory_mib, None);
    assert!(!defaults.hugepages);
    assert_eq!(
        defaults.disk_io_burst_secs(),
        pplx_space_common_core::types::DEFAULT_DISK_IO_BURST_SECS
    );
    // A directory is an unreadable config even when this test runs as root.
    assert!(matches!(
        load_from(directory.path()),
        Err(ConfigLoadError::Read { .. })
    ));
}

#[test]
fn present_invalid_config_is_never_replaced_with_defaults() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("sandbox.toml");
    for content in [
        "max_memory_mib = [",
        "max_memory_mib = 'large'",
        "max_memory_mib = -1",
        "hugepages = 'true'",
        "disk_io_burst_secs = 0",
    ] {
        std::fs::write(&path, content).unwrap();
        let error = load_from(&path).unwrap_err();
        assert!(
            matches!(error, ConfigLoadError::Parse { .. }),
            "{content}: {error}"
        );
        assert!(error.to_string().contains(path.to_str().unwrap()));
    }
    std::fs::write(&path, "max_memory_mib = 0").unwrap();
    assert!(matches!(
        load_from(&path),
        Err(ConfigLoadError::InvalidMemoryCap { .. })
    ));
}

#[test]
fn loaded_cold_start_config_preserves_restore_options_without_rejecting_startup() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("sandbox.toml");
    for (hugepages, restore_supported) in [(false, true), (true, false)] {
        std::fs::write(
            &path,
            format!("max_memory_mib = 4096\nhugepages = {hugepages}"),
        )
        .unwrap();
        let config = load_from(&path).unwrap();
        assert_eq!(config.max_memory_mib, Some(4096));
        assert_eq!(config.hugepages, hugepages);
        assert_eq!(
            config.validate_snapshot_restore().is_ok(),
            restore_supported
        );
    }
}
