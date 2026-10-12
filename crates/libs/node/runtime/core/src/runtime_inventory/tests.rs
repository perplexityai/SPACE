use std::io::Write;
use std::os::unix::fs::{PermissionsExt, symlink};

use super::*;

#[test]
fn a_missing_guest_kernel_is_a_terminal_failed_precondition() {
    let error = RuntimeInventoryError::IncompleteKernelInstallation(PathBuf::from(
        "/space/cache/firecracker/assets/default/kernel",
    ));
    let rendered = error.to_string();

    assert!(
        !error.is_transient(),
        "no amount of retrying installs a guest kernel"
    );
    let status = error.into_status();
    assert_eq!(status.code(), tonic::Code::FailedPrecondition);
    assert_eq!(status.message(), rendered);
}

#[test]
fn an_absent_or_inaccessible_inventory_path_is_terminal() {
    // These describe the installation, not the read: the path is not there,
    // or cannot be opened at all. A retry re-reads the same missing or
    // unreadable path, which is the same condition
    // `IncompleteKernelInstallation` already reports terminally.
    let cases = [
        RuntimeInventoryError::ReadDir {
            path: PathBuf::from("/space/firecracker"),
            source: std::io::Error::from(std::io::ErrorKind::NotFound),
        },
        RuntimeInventoryError::ReadDir {
            path: PathBuf::from("/space/firecracker"),
            source: std::io::Error::from(std::io::ErrorKind::NotADirectory),
        },
        RuntimeInventoryError::InspectKernel {
            path: PathBuf::from("/space/cache/firecracker/assets/default/kernel/vmlinux.bin"),
            source: std::io::Error::from(std::io::ErrorKind::PermissionDenied),
        },
    ];
    for error in cases {
        let rendered = error.to_string();
        assert!(!error.is_transient(), "expected terminal: {rendered}");
        assert_eq!(
            error.into_status().code(),
            tonic::Code::FailedPrecondition,
            "expected FailedPrecondition: {rendered}"
        );
    }
}

#[test]
fn only_version_named_entries_are_kernel_candidates() {
    // The scan disqualifies by name before it probes anything, so an entry
    // ruled out here can never abandon the scan with an unlucky stat. Note
    // `ArtifactVersion` is permissive — `lost+found` is a legal identifier
    // and stays a candidate, screened out later by its missing image.
    for name in [
        "default",
        "legacy-default",
        "",
        "6.1.0 (old)",
        "6.1.0/nested",
    ] {
        assert!(
            kernel_candidate_version(std::ffi::OsStr::new(name)).is_none(),
            "{name:?} must not be treated as a kernel installation"
        );
    }
    for name in ["6.1.0", "lost+found"] {
        assert_eq!(
            kernel_candidate_version(std::ffi::OsStr::new(name)),
            Some(ArtifactVersion::parse(name).unwrap()),
            "{name:?} is a legal artifact version"
        );
    }
}

#[test]
fn a_transiently_unreadable_candidate_fails_the_whole_scan() {
    // Skipping it would publish an inventory missing this kernel, and
    // node-manager caches the first successful scan for the life of the
    // process — so every later resolution asking for it would answer with
    // a terminal NoCompatibleComponent long after the read recovered.
    let error = RuntimeInventoryError::InspectKernel {
        path: PathBuf::from("/space/cache/firecracker/kernels/6.1.0/vmlinux.bin"),
        source: std::io::Error::from(std::io::ErrorKind::ResourceBusy),
    };
    assert!(error.is_transient());

    assert_eq!(unreadable_candidate(&error), UnreadableCandidate::FailScan);
}

#[test]
fn a_permanently_unreadable_candidate_is_skipped() {
    // Permission-denied is a node that needs repairing, not a read to
    // retry: an inventory without this kernel is accurate.
    let error = RuntimeInventoryError::InspectKernel {
        path: PathBuf::from("/space/cache/firecracker/kernels/6.1.0/vmlinux.bin"),
        source: std::io::Error::from(std::io::ErrorKind::PermissionDenied),
    };
    assert!(!error.is_transient());

    assert_eq!(unreadable_candidate(&error), UnreadableCandidate::Skip);
}

#[test]
fn an_absent_kernel_path_probes_as_missing_rather_than_failing() {
    let root = tempfile::tempdir().unwrap();

    assert!(
        probe_inventory_path(&root.path().join("absent"))
            .unwrap()
            .is_none()
    );
    assert!(
        canonicalize_inventory_path(&root.path().join("absent"))
            .unwrap()
            .is_none()
    );
}

#[test]
fn a_malformed_kernel_path_probes_as_missing_rather_than_failing() {
    // Permanently unresolvable is still "not installed" as far as the scan
    // is concerned; only a transient read aborts it.
    let probed = probe_inventory_path(Path::new("/space/cache/firecracker/kernels/6.1\0.0"));
    assert!(probed.unwrap().is_none());
}

#[test]
fn an_unreadable_kernel_image_stays_retryable() {
    let error = RuntimeInventoryError::InspectKernel {
        path: PathBuf::from("/space/cache/firecracker/assets/default/kernel/vmlinux.bin"),
        source: std::io::Error::other("read failed"),
    };
    let rendered = error.to_string();

    assert!(
        error.is_transient(),
        "a failed read is not a verdict about the installation"
    );
    let status = error.into_status();
    assert_eq!(status.code(), tonic::Code::Internal);
    assert_eq!(status.message(), rendered);
}

fn installation(release: &str, state: &str) -> RuntimeInstallation {
    RuntimeInstallation {
        runtime: RuntimeIdentity {
            kind: RuntimeKind::Firecracker,
            version: RuntimeVersion::parse(release).unwrap(),
        },
        guest: GuestPlatform {
            os: GuestOperatingSystem::Linux,
            architecture: host_arch().unwrap(),
        },
        state_format: StateFormatIdentity {
            format: StateFormatName::parse(FIRECRACKER_STATE_FORMAT).unwrap(),
            version: StateFormatVersion::parse(state).unwrap(),
        },
        files: RuntimeInstallationFiles::new([
            (
                RuntimeFileRole::FirecrackerBinary,
                PathBuf::from(format!("/{release}/firecracker")),
            ),
            (
                RuntimeFileRole::JailerBinary,
                PathBuf::from(format!("/{release}/jailer")),
            ),
        ]),
    }
}

fn kernel(
    version: &str,
    digest_byte: u8,
    default_for_new: bool,
    legacy_default: bool,
) -> ArtifactComponentInstallation {
    kernel_installation(
        kernel_metadata(
            ArtifactVersion::parse(version).unwrap(),
            Digest::sha256([digest_byte; 32]),
        )
        .unwrap(),
        PathBuf::from(format!("/kernels/{version}/vmlinux.bin")),
        default_for_new,
        legacy_default,
    )
}

fn requested(
    release: &str,
    state: &str,
    components: Vec<ArtifactComponentMetadata>,
) -> ExecutionMetadata {
    let installation = installation(release, state);
    ExecutionMetadata::new(
        installation.runtime,
        installation.guest,
        Some(installation.state_format),
        components,
    )
    .unwrap()
}

fn inventory(runtimes: Vec<RuntimeInstallation>) -> RuntimeInventory {
    RuntimeInventory {
        runtimes,
        components: vec![kernel("vmlinux-6.1.158", 0x42, true, true)],
        defaults: Vec::new(),
    }
}

fn selected_kernel_path(selected: &ResolvedExecutionEnvironment) -> &Path {
    let boot_role = boot_component_role();
    selected
        .component_path(ArtifactComponentKind::Kernel, Some(&boot_role))
        .unwrap()
}

#[test]
fn exact_release_wins_when_installed() {
    let inventory = inventory(vec![
        installation("1.16.1", "10.0.0"),
        installation("1.16.0", "10.0.0"),
    ]);
    let requested_execution = requested("1.16.0", "10.0.0", vec![]);
    assert_eq!(
        inventory
            .resolve(
                Some(&requested_execution),
                RuntimeKind::Firecracker,
                ExecutionSelectionMode::PersistedStateRestore,
            )
            .unwrap()
            .execution
            .runtime()
            .version
            .as_str(),
        "1.16.0"
    );
}

#[test]
fn compatible_release_is_selected_by_state_generation() {
    // Synthetic releases exercise a forward-compatible minor format change.
    let inventory = inventory(vec![
        installation("1.99.2", "10.1.0"),
        installation("1.99.1", "10.0.0"),
    ]);
    let requested_execution = requested("1.99.0", "10.0.0", vec![]);
    assert_eq!(
        inventory
            .resolve(
                Some(&requested_execution),
                RuntimeKind::Firecracker,
                ExecutionSelectionMode::PersistedStateRestore,
            )
            .unwrap()
            .execution
            .runtime()
            .version
            .as_str(),
        "1.99.2"
    );

    let old_generation = requested("1.98.1", "9.0.0", vec![]);
    assert!(matches!(
        inventory.resolve(
            Some(&old_generation),
            RuntimeKind::Firecracker,
            ExecutionSelectionMode::PersistedStateRestore,
        ),
        Err(RuntimeInventoryError::NoCompatibleRuntime { .. })
    ));
}

/// Without a `default` alias the newest release is used.
#[test]
fn latest_release_and_default_kernel_are_used_for_unpinned_starts() {
    let inventory = inventory(vec![
        installation("1.16.0", "10.0.0"),
        installation("1.15.1", "9.0.0"),
    ]);
    let selected = inventory
        .resolve(
            None,
            RuntimeKind::Firecracker,
            ExecutionSelectionMode::NewColdBoot,
        )
        .unwrap();
    assert_eq!(selected.execution.runtime().version.as_str(), "1.16.0");
    assert_eq!(
        selected_kernel_path(&selected),
        Path::new("/kernels/vmlinux-6.1.158/vmlinux.bin")
    );
}

/// Without a `default` alias the newest release is used.
#[test]
fn new_cold_boot_uses_latest_release_without_snapshot_format_gating() {
    let inventory = inventory(vec![
        installation("1.16.0", "10.0.0"),
        installation("1.15.1", "9.0.0"),
    ]);
    let template_execution = requested("1.15.1", "9.0.0", vec![]);

    let selected = inventory
        .resolve(
            Some(&template_execution),
            RuntimeKind::Firecracker,
            ExecutionSelectionMode::NewColdBoot,
        )
        .unwrap();

    assert_eq!(selected.execution.runtime().version.as_str(), "1.16.0");
    assert_eq!(
        selected.execution.state_format().unwrap().version.as_str(),
        "10.0.0"
    );
}

#[test]
fn existing_cold_boot_requires_the_recorded_runtime_release() {
    let inventory = inventory(vec![installation("1.16.0", "10.0.0")]);
    let existing = requested("1.15.1", "9.0.0", vec![]);

    assert!(matches!(
        inventory.resolve(
            Some(&existing),
            RuntimeKind::Firecracker,
            ExecutionSelectionMode::ExistingColdBoot,
        ),
        Err(RuntimeInventoryError::NoCompatibleRuntime { .. })
    ));
}

#[test]
fn persisted_state_restore_requires_a_state_format() {
    let inventory = inventory(vec![installation("1.15.1", "9.0.0")]);
    let execution = ExecutionMetadata::new(
        installation("1.15.1", "9.0.0").runtime,
        installation("1.15.1", "9.0.0").guest,
        None,
        vec![],
    )
    .unwrap();

    assert!(matches!(
        inventory.resolve(
            Some(&execution),
            RuntimeKind::Firecracker,
            ExecutionSelectionMode::PersistedStateRestore,
        ),
        Err(RuntimeInventoryError::MissingStateFormat)
    ));
}

#[test]
fn persisted_state_restore_reports_invalid_driver_version_metadata() {
    let inventory = inventory(vec![installation("1.15.1", "9.0.0")]);
    let invalid = requested("1.15.1", "opaque-state-version", vec![]);

    assert!(matches!(
        inventory.resolve(
            Some(&invalid),
            RuntimeKind::Firecracker,
            ExecutionSelectionMode::PersistedStateRestore,
        ),
        Err(RuntimeInventoryError::InvalidVersionOutput { .. })
    ));
}

#[test]
fn pinned_starts_select_the_exact_kernel_digest() {
    let inventory = RuntimeInventory {
        runtimes: vec![installation("1.16.0", "10.0.0")],
        components: vec![
            kernel("vmlinux-6.1.158", 0x42, true, true),
            kernel("vmlinux-6.6.92", 0x43, false, false),
        ],
        defaults: Vec::new(),
    };
    let requested_kernel = kernel("vmlinux-6.6.92", 0x43, false, false).metadata;
    let requested = requested("1.16.0", "10.0.0", vec![requested_kernel]);

    let selected = inventory
        .resolve(
            Some(&requested),
            RuntimeKind::Firecracker,
            ExecutionSelectionMode::NewColdBoot,
        )
        .unwrap();
    assert_eq!(
        selected
            .execution
            .component(ArtifactComponentKind::Kernel, Some(&boot_component_role()))
            .unwrap()
            .digest,
        Some(Digest::sha256([0x43; 32]))
    );
    assert_eq!(
        selected_kernel_path(&selected),
        Path::new("/kernels/vmlinux-6.6.92/vmlinux.bin")
    );
}

#[test]
fn digest_matches_even_when_kernel_labels_differ() {
    let inventory = RuntimeInventory {
        runtimes: vec![installation("1.16.0", "10.0.0")],
        components: vec![kernel("legacy-424242424242", 0x42, true, true)],
        defaults: Vec::new(),
    };
    let requested_kernel = kernel("vmlinux-6.1.158", 0x42, false, false).metadata;
    let requested = requested("1.16.0", "10.0.0", vec![requested_kernel]);

    let selected = inventory
        .resolve(
            Some(&requested),
            RuntimeKind::Firecracker,
            ExecutionSelectionMode::ExistingColdBoot,
        )
        .unwrap();

    assert_eq!(
        selected
            .execution
            .component(ArtifactComponentKind::Kernel, Some(&boot_component_role()))
            .unwrap()
            .version
            .as_ref()
            .unwrap()
            .as_str(),
        "legacy-424242424242"
    );
}

#[test]
fn persisted_restore_does_not_require_the_recorded_kernel_to_be_installed() {
    let inventory = RuntimeInventory {
        runtimes: vec![installation("1.16.0", "10.0.0")],
        components: vec![kernel("vmlinux-6.1.158", 0x42, true, true)],
        defaults: Vec::new(),
    };
    let requested_kernel = kernel("vmlinux-6.6.92", 0x43, false, false).metadata;
    let requested = requested("1.16.0", "10.0.0", vec![requested_kernel.clone()]);

    let selected = inventory
        .resolve(
            Some(&requested),
            RuntimeKind::Firecracker,
            ExecutionSelectionMode::PersistedStateRestore,
        )
        .unwrap();

    assert_eq!(
        selected_kernel_path(&selected),
        Path::new("/kernels/vmlinux-6.1.158/vmlinux.bin")
    );
    assert_eq!(
        selected
            .execution
            .component(ArtifactComponentKind::Kernel, Some(&boot_component_role()))
            .unwrap(),
        &requested_kernel
    );
}

#[test]
fn compatible_restore_can_use_an_older_runtime_release() {
    let inventory = inventory(vec![installation("1.16.0", "10.0.0")]);
    let requested = requested("1.16.1", "10.0.0", vec![]);
    assert_eq!(
        inventory
            .resolve(
                Some(&requested),
                RuntimeKind::Firecracker,
                ExecutionSelectionMode::PersistedStateRestore,
            )
            .unwrap()
            .execution
            .runtime()
            .version
            .as_str(),
        "1.16.0"
    );
}

#[test]
fn runtime_and_component_inventories_are_independent() {
    let inventory = RuntimeInventory {
        runtimes: vec![
            installation("1.16.0", "10.0.0"),
            installation("1.15.1", "9.0.0"),
        ],
        components: vec![
            kernel("vmlinux-6.1.158", 0x42, true, true),
            kernel("vmlinux-6.6.92", 0x43, false, false),
        ],
        defaults: Vec::new(),
    };

    let published = inventory.to_proto();

    assert_eq!(published.runtimes.len(), 2);
    assert_eq!(published.components.len(), 2);
    assert_eq!(
        published
            .components
            .iter()
            .filter(|component| component.default_for_new)
            .count(),
        1
    );
}

#[test]
fn legacy_metadata_uses_the_pinned_legacy_kernel() {
    let inventory = RuntimeInventory {
        runtimes: vec![installation("1.15.1", "9.0.0")],
        components: vec![
            kernel("vmlinux-6.1.158", 0x42, false, true),
            kernel("vmlinux-6.6.92", 0x43, true, false),
        ],
        defaults: Vec::new(),
    };
    let legacy = requested("1.15.1", "9.0.0", vec![]);

    let selected = inventory
        .resolve(
            Some(&legacy),
            RuntimeKind::Firecracker,
            ExecutionSelectionMode::ExistingColdBoot,
        )
        .unwrap();

    assert_eq!(
        selected_kernel_path(&selected),
        Path::new("/kernels/vmlinux-6.1.158/vmlinux.bin")
    );
}

#[test]
fn discovers_release_state_and_kernel_from_installation_directory() {
    for (version, format) in [
        ("1.16.0", "10.0.0"),
        ("1.16.1", "10.0.0"),
        ("1.16.2", "11.0.0"),
        ("1.17.0", "12.0.0"),
    ] {
        let temp = tempfile::tempdir().unwrap();
        let assets = tempfile::tempdir().unwrap();
        let kernels = tempfile::tempdir().unwrap();
        let version_dir = temp.path().join(format!("v{version}"));
        std::fs::create_dir_all(version_dir.join("bin")).unwrap();
        let kernel_dir = kernels.path().join("vmlinux-6.1.158");
        let kernel = kernel_dir.join("vmlinux.bin");
        std::fs::create_dir_all(kernel.parent().unwrap()).unwrap();
        std::fs::write(&kernel, b"kernel").unwrap();
        write_executable(&version_dir.join("bin/firecracker"), "#!/bin/sh\nexit 42\n");
        write_executable(&version_dir.join("bin/jailer"), "#!/bin/sh\nexit 42\n");
        symlink(format!("v{version}"), temp.path().join("default")).unwrap();
        symlink("vmlinux-6.1.158", kernels.path().join("default")).unwrap();
        symlink("vmlinux-6.1.158", kernels.path().join("legacy-default")).unwrap();

        let inventory = RuntimeInventory::discover(
            [RuntimeInstallationRoot::new(
                RuntimeKind::Firecracker,
                temp.path(),
            )],
            assets.path(),
            kernels.path(),
        )
        .unwrap();
        let requested_execution = requested(version, format, vec![]);
        let selected = inventory
            .resolve(
                Some(&requested_execution),
                RuntimeKind::Firecracker,
                ExecutionSelectionMode::ExistingColdBoot,
            )
            .unwrap();
        assert_eq!(selected.execution.runtime().version.as_str(), version);
        assert_eq!(
            selected.execution.state_format().unwrap().version.as_str(),
            format
        );
        assert_eq!(
            selected
                .execution
                .component(ArtifactComponentKind::Kernel, Some(&boot_component_role()))
                .unwrap()
                .version
                .as_ref()
                .unwrap()
                .as_str(),
            "vmlinux-6.1.158"
        );
        assert_eq!(selected_kernel_path(&selected), kernel.as_path());
        if format != "10.0.0" {
            let old_snapshot = requested("1.16.0", "10.0.0", vec![]);
            assert!(matches!(
                inventory.resolve(
                    Some(&old_snapshot),
                    RuntimeKind::Firecracker,
                    ExecutionSelectionMode::PersistedStateRestore,
                ),
                Err(RuntimeInventoryError::NoCompatibleRuntime { .. })
            ));
        }
    }
}

#[test]
fn discovery_does_not_execute_firecracker_binaries() {
    let temp = tempfile::tempdir().unwrap();
    let assets = tempfile::tempdir().unwrap();
    let kernels = tempfile::tempdir().unwrap();
    let version_dir = temp.path().join("v1.15.1");
    std::fs::create_dir_all(version_dir.join("bin")).unwrap();
    let firecracker_marker = temp.path().join("firecracker-executed");
    let jailer_marker = temp.path().join("jailer-executed");
    let kernel_dir = kernels.path().join("vmlinux-6.1.158");
    std::fs::create_dir_all(&kernel_dir).unwrap();
    std::fs::write(kernel_dir.join("vmlinux.bin"), b"kernel").unwrap();
    write_executable(
        &version_dir.join("bin/firecracker"),
        &format!(
            "#!/bin/sh\ntouch '{}'\nexit 42\n",
            firecracker_marker.display()
        ),
    );
    write_executable(
        &version_dir.join("bin/jailer"),
        &format!("#!/bin/sh\ntouch '{}'\nexit 42\n", jailer_marker.display()),
    );
    symlink("v1.15.1", temp.path().join("default")).unwrap();
    symlink("vmlinux-6.1.158", kernels.path().join("default")).unwrap();
    symlink("vmlinux-6.1.158", kernels.path().join("legacy-default")).unwrap();

    let inventory = RuntimeInventory::discover(
        [RuntimeInstallationRoot::new(
            RuntimeKind::Firecracker,
            temp.path(),
        )],
        assets.path(),
        kernels.path(),
    )
    .unwrap();

    assert_eq!(inventory.runtimes[0].runtime.version.as_str(), "1.15.1");
    assert_eq!(inventory.runtimes[0].state_format.version.as_str(), "9.0.0");
    assert!(!firecracker_marker.exists());
    assert!(!jailer_marker.exists());
}

#[test]
fn discovery_skips_malformed_runtime_and_kernel_candidates() {
    let temp = tempfile::tempdir().unwrap();
    let assets = tempfile::tempdir().unwrap();
    let kernels = tempfile::tempdir().unwrap();
    let valid_version_dir = temp.path().join("v1.16.0");
    std::fs::create_dir_all(valid_version_dir.join("bin")).unwrap();
    let staging_dir = temp.path().join(".v1.17.0.tmp.123");
    std::fs::create_dir_all(staging_dir.join("bin")).unwrap();
    let incomplete_version_dir = temp.path().join("v1.18.0");
    std::fs::create_dir_all(incomplete_version_dir.join("bin")).unwrap();
    let valid_kernel_dir = kernels.path().join("vmlinux-6.1.158");
    let valid_kernel = valid_kernel_dir.join("vmlinux.bin");
    std::fs::create_dir_all(valid_kernel.parent().unwrap()).unwrap();
    std::fs::write(&valid_kernel, b"kernel").unwrap();
    let incomplete_kernel_dir = kernels.path().join("vmlinux-6.6.92");
    std::fs::create_dir_all(&incomplete_kernel_dir).unwrap();
    write_executable(
        &valid_version_dir.join("bin/firecracker"),
        "#!/bin/sh\nexit 42\n",
    );
    write_executable(
        &valid_version_dir.join("bin/jailer"),
        "#!/bin/sh\nexit 42\n",
    );
    symlink("v1.16.0", temp.path().join("default")).unwrap();
    symlink("vmlinux-6.1.158", kernels.path().join("default")).unwrap();
    symlink("vmlinux-6.1.158", kernels.path().join("legacy-default")).unwrap();

    let inventory = RuntimeInventory::discover(
        [RuntimeInstallationRoot::new(
            RuntimeKind::Firecracker,
            temp.path(),
        )],
        assets.path(),
        kernels.path(),
    )
    .unwrap();

    assert_eq!(inventory.runtimes.len(), 1);
    assert_eq!(inventory.runtimes[0].runtime.version.as_str(), "1.16.0");
    assert_eq!(inventory.components.len(), 1);
    assert_eq!(
        inventory.components[0]
            .metadata
            .version
            .as_ref()
            .unwrap()
            .as_str(),
        "vmlinux-6.1.158"
    );
}

#[test]
fn discovery_errors_on_complete_firecracker_with_unknown_snapshot_format() {
    let temp = tempfile::tempdir().unwrap();
    let assets = tempfile::tempdir().unwrap();
    let kernels = tempfile::tempdir().unwrap();
    let unknown_version_dir = temp.path().join("v9.99.0");
    std::fs::create_dir_all(unknown_version_dir.join("bin")).unwrap();
    write_executable(
        &unknown_version_dir.join("bin/firecracker"),
        "#!/bin/sh\nexit 42\n",
    );
    write_executable(
        &unknown_version_dir.join("bin/jailer"),
        "#!/bin/sh\nexit 42\n",
    );
    let kernel_dir = kernels.path().join("vmlinux-6.1.158");
    std::fs::create_dir_all(&kernel_dir).unwrap();
    std::fs::write(kernel_dir.join("vmlinux.bin"), b"kernel").unwrap();
    symlink("v9.99.0", temp.path().join("default")).unwrap();
    symlink("vmlinux-6.1.158", kernels.path().join("default")).unwrap();
    symlink("vmlinux-6.1.158", kernels.path().join("legacy-default")).unwrap();

    assert!(matches!(
        RuntimeInventory::discover(
            [RuntimeInstallationRoot::new(
                RuntimeKind::Firecracker,
                temp.path(),
            )],
            assets.path(),
            kernels.path(),
        ),
        Err(RuntimeInventoryError::UnknownFirecrackerSnapshotFormat(
            version
        )) if version.as_str() == "9.99.0"
    ));
}

fn write_executable(path: &Path, contents: &str) {
    let mut file = std::fs::File::create(path).unwrap();
    file.write_all(contents.as_bytes()).unwrap();
    let mut permissions = file.metadata().unwrap().permissions();
    permissions.set_mode(0o755);
    file.set_permissions(permissions).unwrap();
}

fn with_default(mut inventory: RuntimeInventory, release: &str) -> RuntimeInventory {
    inventory.defaults = vec![RuntimeIdentity {
        kind: RuntimeKind::Firecracker,
        version: RuntimeVersion::parse(release).unwrap(),
    }];
    inventory
}

#[test]
fn new_cold_boots_use_the_default_release_not_the_newest() {
    let inventory = with_default(
        inventory(vec![
            installation("1.16.0", "10.0.0"),
            installation("1.15.1", "9.0.0"),
        ]),
        "1.15.1",
    );
    let unpinned = inventory
        .resolve(
            None,
            RuntimeKind::Firecracker,
            ExecutionSelectionMode::NewColdBoot,
        )
        .unwrap();
    assert_eq!(unpinned.execution.runtime().version.as_str(), "1.15.1");

    let template_execution = requested("1.16.0", "10.0.0", vec![]);
    let from_template = inventory
        .resolve(
            Some(&template_execution),
            RuntimeKind::Firecracker,
            ExecutionSelectionMode::NewColdBoot,
        )
        .unwrap();
    assert_eq!(from_template.execution.runtime().version.as_str(), "1.15.1");
}

#[test]
fn recorded_releases_still_win_over_the_default() {
    let inventory = with_default(
        inventory(vec![
            installation("1.16.0", "10.0.0"),
            installation("1.16.1", "10.0.0"),
        ]),
        "1.16.1",
    );
    let existing = requested("1.16.0", "10.0.0", vec![]);
    for mode in [
        ExecutionSelectionMode::ExistingColdBoot,
        ExecutionSelectionMode::PersistedStateRestore,
    ] {
        let selected = inventory
            .resolve(Some(&existing), RuntimeKind::Firecracker, mode)
            .unwrap();
        assert_eq!(
            selected.execution.runtime().version.as_str(),
            "1.16.0",
            "{mode:?}"
        );
    }
}

#[test]
fn a_restore_without_its_release_prefers_a_compatible_default() {
    let inventory = with_default(
        inventory(vec![
            installation("1.16.2", "11.0.0"),
            installation("1.16.1", "10.0.0"),
            installation("1.16.0", "10.0.0"),
        ]),
        "1.16.0",
    );
    let snapshot = requested("1.16.9", "10.0.0", vec![]);
    let selected = inventory
        .resolve(
            Some(&snapshot),
            RuntimeKind::Firecracker,
            ExecutionSelectionMode::PersistedStateRestore,
        )
        .unwrap();
    assert_eq!(selected.execution.runtime().version.as_str(), "1.16.0");

    // A default that cannot load the snapshot gives way to the newest that can.
    let inventory = with_default(inventory, "1.16.2");
    let selected = inventory
        .resolve(
            Some(&snapshot),
            RuntimeKind::Firecracker,
            ExecutionSelectionMode::PersistedStateRestore,
        )
        .unwrap();
    assert_eq!(selected.execution.runtime().version.as_str(), "1.16.1");
}

#[test]
fn discovery_records_each_roots_default_release() {
    let temp = tempfile::tempdir().unwrap();
    let assets = tempfile::tempdir().unwrap();
    let kernels = tempfile::tempdir().unwrap();
    for release in ["v1.15.1", "v1.16.0"] {
        let dir = temp.path().join(release).join("bin");
        std::fs::create_dir_all(&dir).unwrap();
        write_executable(&dir.join("firecracker"), "#!/bin/sh\nexit 42\n");
        write_executable(&dir.join("jailer"), "#!/bin/sh\nexit 42\n");
    }
    symlink("v1.15.1", temp.path().join("default")).unwrap();
    let kernel = kernels.path().join("vmlinux-6.1.158/vmlinux.bin");
    std::fs::create_dir_all(kernel.parent().unwrap()).unwrap();
    std::fs::write(&kernel, b"kernel").unwrap();
    symlink("vmlinux-6.1.158", kernels.path().join("default")).unwrap();
    symlink("vmlinux-6.1.158", kernels.path().join("legacy-default")).unwrap();
    let root = || RuntimeInstallationRoot::new(RuntimeKind::Firecracker, temp.path());

    let inventory = RuntimeInventory::discover([root()], assets.path(), kernels.path()).unwrap();
    let selected = inventory
        .resolve(
            None,
            RuntimeKind::Firecracker,
            ExecutionSelectionMode::NewColdBoot,
        )
        .unwrap();
    assert_eq!(selected.execution.runtime().version.as_str(), "1.15.1");

    // Without the alias, the newest release is used, as before.
    std::fs::remove_file(temp.path().join("default")).unwrap();
    let inventory = RuntimeInventory::discover([root()], assets.path(), kernels.path()).unwrap();
    let selected = inventory
        .resolve(
            None,
            RuntimeKind::Firecracker,
            ExecutionSelectionMode::NewColdBoot,
        )
        .unwrap();
    assert_eq!(selected.execution.runtime().version.as_str(), "1.16.0");
}

/// A `default` alias that exists but names no listed release fails
/// discovery rather than falling back to the newest release: installing a
/// release beside a broken alias must not change what new sandboxes boot.
#[test]
fn a_broken_default_alias_fails_discovery() {
    let temp = tempfile::tempdir().unwrap();
    let assets = tempfile::tempdir().unwrap();
    let kernels = tempfile::tempdir().unwrap();
    for release in ["v1.15.1", "v1.16.0"] {
        let dir = temp.path().join(release).join("bin");
        std::fs::create_dir_all(&dir).unwrap();
        write_executable(&dir.join("firecracker"), "#!/bin/sh\nexit 42\n");
        write_executable(&dir.join("jailer"), "#!/bin/sh\nexit 42\n");
    }
    std::fs::create_dir_all(temp.path().join("notes")).unwrap();
    let kernel = kernels.path().join("vmlinux-6.1.158/vmlinux.bin");
    std::fs::create_dir_all(kernel.parent().unwrap()).unwrap();
    std::fs::write(&kernel, b"kernel").unwrap();
    symlink("vmlinux-6.1.158", kernels.path().join("default")).unwrap();
    symlink("vmlinux-6.1.158", kernels.path().join("legacy-default")).unwrap();
    let root = || RuntimeInstallationRoot::new(RuntimeKind::Firecracker, temp.path());

    // Dangling, and naming something that is not a release.
    for target in ["v1.17.0", "notes"] {
        let _ = std::fs::remove_file(temp.path().join("default"));
        symlink(target, temp.path().join("default")).unwrap();
        let error =
            RuntimeInventory::discover([root()], assets.path(), kernels.path()).expect_err(target);
        assert!(
            matches!(error, RuntimeInventoryError::InvalidDefaultRelease { .. }),
            "{target}: {error}"
        );
        assert!(!error.is_transient(), "{target}");
    }
}
