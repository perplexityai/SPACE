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

#[test]
fn verified_launch_cannot_be_decoded_as_an_executable_legacy_request() {
    use prost::Message;
    #[derive(Clone, PartialEq, Message)]
    struct LegacyLaunch {
        #[prost(string, tag = "1")]
        sandbox_id: String,
        #[prost(string, tag = "2")]
        jailer_bin: String,
        #[prost(string, tag = "3")]
        firecracker_bin: String,
    }
    let request = super::proto::LaunchFirecrackerRequest {
        verified_runtime: Some(super::proto::VerifiedRuntimeLaunch {
            sandbox_id: "0198dc20-05be-7000-8000-000000000001".into(),
            environment_reference:
                "sha256:3168c96b5719ce62543ae53e6641a4eb80416a9f01e5585f6f76c3c70111fa0b".into(),
            integrity_requirement:
                super::proto::ArtifactIntegrityRequirement::AuthenticatedImmutableSources.into(),
        }),
        ..Default::default()
    };
    let legacy = LegacyLaunch::decode(request.encode_to_vec().as_slice()).unwrap();
    legacy
        .sandbox_id
        .parse::<pplx_space_common_core::types::SandboxId>()
        .unwrap_err();
    assert!(legacy.jailer_bin.is_empty());
    assert!(legacy.firecracker_bin.is_empty());
    assert_eq!(
        super::proto::LaunchFirecrackerRequest::decode(request.encode_to_vec().as_slice()).unwrap(),
        request
    );
}

/// Sealing tests require an explicitly disposable btrfs filesystem and root
/// for authenticated inode linking. CI must never count an unsupported skip.
pub(crate) fn verity_test_directory() -> Option<tempfile::TempDir> {
    if !pplx_space_test_env::root_or_skip("test fs-verity and authenticated inode links") {
        return None;
    }
    let Some(root) = std::env::var_os("SPACE_NOCOW_TEST_BTRFS_DIR") else {
        let required = std::env::var_os("SPACE_CI_REQUIRE_TEST_PRIVILEGE")
            .is_some_and(|value| !value.is_empty() && value != "0");
        assert!(
            !required,
            "CI must provide the disposable btrfs fixture for fs-verity tests"
        );
        eprintln!("SKIPPED: set SPACE_NOCOW_TEST_BTRFS_DIR to a disposable btrfs directory");
        return None;
    };
    Some(tempfile::tempdir_in(root).unwrap())
}
