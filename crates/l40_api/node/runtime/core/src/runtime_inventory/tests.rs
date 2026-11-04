use std::io::Write;
use std::os::unix::fs::{PermissionsExt, symlink};

use super::*;

#[test]
fn an_absent_or_inaccessible_inventory_path_is_terminal() {
    // These describe the installation, not the read: the path is not there,
    // or cannot be opened at all. A retry re-reads the same missing or
    // unreadable path; provisioning must repair it.
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
fn external_directory_names_do_not_discard_identity_suffixes() {
    let label = format!("1.15.1--{}", "a".repeat(64));
    assert_eq!(
        version_from_dir(Path::new(&format!("v{label}")))
            .unwrap()
            .as_str(),
        label
    );
    assert_eq!(
        kernel_candidate_version(std::ffi::OsStr::new(&label))
            .unwrap()
            .as_str(),
        label
    );
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
        state_format: Some(StateFormatIdentity {
            format: StateFormatName::parse(FIRECRACKER_STATE_FORMAT).unwrap(),
            version: StateFormatVersion::parse(state).unwrap(),
        }),
        files: RuntimeInstallationFiles::new([
            (
                RuntimeFileRole::RuntimeExecutable,
                PathBuf::from(format!("/{release}/firecracker")),
            ),
            (
                RuntimeFileRole::LauncherExecutable,
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
        installation.state_format,
        components,
    )
    .unwrap()
}

fn inventory(runtimes: Vec<RuntimeInstallation>) -> RuntimeInventory {
    RuntimeInventory {
        runtimes,
        components: vec![kernel("vmlinux-6.1.158", 0x42, true, true)],
        defaults: Vec::new(),
        fingerprints: Vec::new(),
        environments: BTreeMap::new(),
        #[cfg(feature = "artifact-store")]
        unavailable: Vec::new(),
        retry_after: None,
        initial_environment: None,
    }
}

fn selected_kernel_path(selected: &ResolvedExecutionEnvironment) -> &Path {
    let boot_role = boot_component_role();
    selected
        .component(ArtifactComponentKind::Kernel, Some(&boot_role))
        .unwrap()
        .path
        .as_path()
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

/// Discovery alone does not select an installed candidate.
#[test]
fn missing_default_rejects_unpinned_and_template_cold_boots() {
    let inventory = inventory(vec![
        installation("1.16.0", "10.0.0"),
        installation("1.15.1", "9.0.0"),
    ]);
    let template = requested("1.15.1", "9.0.0", vec![]);
    for requested in [None, Some(&template)] {
        inventory
            .resolve(
                requested,
                RuntimeKind::Firecracker,
                ExecutionSelectionMode::NewColdBoot,
            )
            .unwrap_err();
    }
}

/// Cold boot uses the selected release regardless of the template snapshot format.
#[test]
fn new_cold_boot_uses_selected_release_without_snapshot_format_gating() {
    let inventory = with_default(
        inventory(vec![
            installation("1.16.0", "10.0.0"),
            installation("1.15.1", "9.0.0"),
        ]),
        "1.16.0",
    );
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
    let inventory = with_default(
        RuntimeInventory {
            runtimes: vec![installation("1.16.0", "10.0.0")],
            components: vec![
                kernel("vmlinux-6.1.158", 0x42, true, true),
                kernel("vmlinux-6.6.92", 0x43, false, false),
            ],
            defaults: Vec::new(),
            fingerprints: Vec::new(),
            environments: BTreeMap::new(),
            #[cfg(feature = "artifact-store")]
            unavailable: Vec::new(),
            retry_after: None,
            initial_environment: None,
        },
        "1.16.0",
    );
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
        fingerprints: Vec::new(),
        environments: BTreeMap::new(),
        #[cfg(feature = "artifact-store")]
        unavailable: Vec::new(),
        retry_after: None,
        initial_environment: None,
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
        components: vec![kernel("unrelated-default", 0x42, true, true)],
        defaults: Vec::new(),
        fingerprints: Vec::new(),
        environments: BTreeMap::new(),
        #[cfg(feature = "artifact-store")]
        unavailable: Vec::new(),
        retry_after: None,
        initial_environment: None,
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

    assert!(selected.components.is_empty());
    assert_eq!(
        selected
            .execution
            .component(ArtifactComponentKind::Kernel, Some(&boot_component_role()))
            .unwrap(),
        &requested_kernel
    );
}

#[test]
fn binding_availability_depends_on_the_operation() {
    let reference = EnvironmentReference::new(Digest::sha256([7; 32]));
    let component = kernel("6.1.155", 42, false, false);
    let requested =
        requested("1.15.1", "9.0.0", vec![component.metadata.clone()]).with_environment(reference);
    for runtime_available in [false, true] {
        for kernel_available in [false, true] {
            let mut inventory = inventory(Vec::new());
            inventory.environments.insert(
                reference,
                InstalledEnvironment {
                    runtime: if runtime_available {
                        Ok(installation("1.15.1", "9.0.0"))
                    } else {
                        Err(std::sync::Arc::new(
                            RuntimeInventoryError::NoInstalledRuntime(RuntimeKind::Firecracker),
                        ))
                    },
                    components: if kernel_available {
                        Ok(vec![component.clone()])
                    } else {
                        Err(std::sync::Arc::new(
                            RuntimeInventoryError::NoCompatibleComponent(
                                component.metadata.clone(),
                            ),
                        ))
                    },
                },
            );
            for mode in [
                ExecutionSelectionMode::NewColdBoot,
                ExecutionSelectionMode::ExistingColdBoot,
                ExecutionSelectionMode::PersistedStateRestore,
            ] {
                let result = inventory.resolve(Some(&requested), RuntimeKind::Firecracker, mode);
                let restoring = mode == ExecutionSelectionMode::PersistedStateRestore;
                if runtime_available && (restoring || kernel_available) {
                    let resolved = result.unwrap();
                    assert_eq!(resolved.execution.components(), requested.components());
                    assert_eq!(resolved.components.is_empty(), restoring);
                    assert_eq!(resolved.environment, Some(reference));
                } else {
                    assert!(matches!(
                        result,
                        Err(RuntimeInventoryError::UnavailableBinding(_))
                    ));
                }
            }
        }
    }
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
        fingerprints: Vec::new(),
        environments: BTreeMap::new(),
        #[cfg(feature = "artifact-store")]
        unavailable: Vec::new(),
        retry_after: None,
        initial_environment: None,
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
        fingerprints: Vec::new(),
        environments: BTreeMap::new(),
        #[cfg(feature = "artifact-store")]
        unavailable: Vec::new(),
        retry_after: None,
        initial_environment: None,
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
    assert_eq!(
        inventory.runtimes[0]
            .state_format
            .as_ref()
            .unwrap()
            .version
            .as_str(),
        "9.0.0"
    );
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
fn discovery_skips_complete_firecracker_with_unknown_snapshot_format() {
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

    let inventory = RuntimeInventory::discover(
        [RuntimeInstallationRoot::new(
            RuntimeKind::Firecracker,
            temp.path(),
        )],
        assets.path(),
        kernels.path(),
    )
    .unwrap();
    assert!(inventory.to_proto().runtimes.is_empty());
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

    // Removing selection cannot select the newer installed release.
    std::fs::remove_file(temp.path().join("default")).unwrap();
    let inventory = RuntimeInventory::discover([root()], assets.path(), kernels.path()).unwrap();
    inventory
        .resolve(
            None,
            RuntimeKind::Firecracker,
            ExecutionSelectionMode::NewColdBoot,
        )
        .unwrap_err();
}

/// A `default` alias that exists but names no listed release fails
/// discovery rather than falling back to the newest release: installing a
/// release beside a broken alias must not change what new sandboxes boot.
#[test]
fn a_broken_default_alias_cannot_select_another_release() {
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
        let inventory =
            RuntimeInventory::discover([root()], assets.path(), kernels.path()).unwrap();
        assert!(
            inventory
                .resolve(
                    None,
                    RuntimeKind::Firecracker,
                    ExecutionSelectionMode::NewColdBoot
                )
                .is_err(),
            "{target}"
        );
        assert!(!inventory.runtimes.is_empty());
    }
}

#[test]
fn discovery_observes_installations_after_an_empty_scan() {
    #[derive(Clone, Copy, Debug)]
    enum Layout {
        Absent,
        Empty,
        Incomplete,
        SameDirectoryMetadata,
    }
    for layout in [
        Layout::Absent,
        Layout::Empty,
        Layout::Incomplete,
        Layout::SameDirectoryMetadata,
    ] {
        let temp = tempfile::tempdir().unwrap();
        let vendor = temp.path().join("vendor");
        let kernels = temp.path().join("kernels");
        let assets = temp.path().join("assets");
        let binaries = vendor.join("v1.15.1/bin");
        let image = kernels.join("6.1.155/vmlinux.bin");
        match layout {
            Layout::Absent => {}
            Layout::Empty => {
                std::fs::create_dir_all(&vendor).unwrap();
                std::fs::create_dir_all(&kernels).unwrap();
            }
            Layout::SameDirectoryMetadata => {
                std::fs::create_dir_all(kernels.join("6.1.154")).unwrap();
                std::fs::create_dir_all(vendor.join("v1.15.0/bin")).unwrap();
            }
            Layout::Incomplete => {
                std::fs::create_dir_all(&binaries).unwrap();
                std::fs::create_dir_all(image.parent().unwrap()).unwrap();
                symlink("v1.15.1", vendor.join("default")).unwrap();
                for alias in ["default", "legacy-default"] {
                    symlink("6.1.155", kernels.join(alias)).unwrap();
                }
            }
        }
        let discover = || {
            RuntimeInventory::discover(
                [RuntimeInstallationRoot::new(
                    RuntimeKind::Firecracker,
                    &vendor,
                )],
                &assets,
                &kernels,
            )
            .unwrap()
        };
        let empty = discover();
        assert!(empty.runtimes.is_empty());
        assert!(empty.components.is_empty());
        assert!(empty.check_files_unchanged(), "{layout:?}");

        let kernel_root_before = std::fs::metadata(&kernels).ok();
        if matches!(layout, Layout::SameDirectoryMetadata) {
            std::fs::rename(kernels.join("6.1.154"), image.parent().unwrap()).unwrap();
        }
        std::fs::create_dir_all(image.parent().unwrap()).unwrap();
        std::fs::write(&image, b"synthetic guest kernel").unwrap();
        if matches!(layout, Layout::SameDirectoryMetadata) {
            restore_directory_metadata(&kernels, kernel_root_before.as_ref().unwrap());
        }
        assert!(
            !empty.check_files_unchanged(),
            "kernel installation: {layout:?}"
        );
        let with_kernel = discover();
        assert_eq!(with_kernel.components.len(), 1);
        assert!(with_kernel.check_files_unchanged(), "{layout:?}");

        let runtime_root_before = std::fs::metadata(&vendor).ok();
        if matches!(layout, Layout::SameDirectoryMetadata) {
            std::fs::rename(vendor.join("v1.15.0"), vendor.join("v1.15.1")).unwrap();
        }
        std::fs::create_dir_all(&binaries).unwrap();
        for name in ["firecracker", "jailer"] {
            write_executable(&binaries.join(name), "#!/bin/sh\nexit 42\n");
        }
        if matches!(layout, Layout::SameDirectoryMetadata) {
            restore_directory_metadata(&vendor, runtime_root_before.as_ref().unwrap());
        }
        assert!(
            !with_kernel.check_files_unchanged(),
            "runtime installation: {layout:?}"
        );
        if !matches!(layout, Layout::Incomplete) {
            symlink("v1.15.1", vendor.join("default")).unwrap();
            for alias in ["default", "legacy-default"] {
                symlink("6.1.155", kernels.join(alias)).unwrap();
            }
        }
        discover()
            .resolve(
                None,
                RuntimeKind::Firecracker,
                ExecutionSelectionMode::NewColdBoot,
            )
            .unwrap();
    }
}

// Same-length entry renames keep directory size stable. Restoring mtime makes
// the filesystem collision deterministic without sleeps or clock assumptions.
fn restore_directory_metadata(path: &Path, before: &std::fs::Metadata) {
    std::fs::File::open(path)
        .unwrap()
        .set_times(std::fs::FileTimes::new().set_modified(before.modified().unwrap()))
        .unwrap();
    let after = std::fs::metadata(path).unwrap();
    assert_eq!(after.len(), before.len());
    assert_eq!(after.modified().unwrap(), before.modified().unwrap());
}

#[test]
fn bound_cold_boot_uses_the_recorded_runtime_and_never_the_node_default() {
    let component = kernel("6.18.44", 0x61, false, false);
    let candidate = requested("1.17.0", "12.0.0", vec![component.metadata.clone()])
        .with_environment(pplx_space_common_core::types::EnvironmentReference::new(
            Digest::sha256([8; 32]),
        ));
    for candidate_present in [true, false] {
        let mut inventory = inventory(vec![installation("1.15.1", "9.0.0")]);
        inventory.defaults = vec![installation("1.15.1", "9.0.0").runtime];
        inventory.components.push(component.clone());
        if candidate_present {
            inventory.runtimes.push(installation("1.17.0", "12.0.0"));
        }
        for mode in [
            ExecutionSelectionMode::NewColdBoot,
            ExecutionSelectionMode::ExistingColdBoot,
        ] {
            let result = inventory.resolve(Some(&candidate), RuntimeKind::Firecracker, mode);
            assert_eq!(result.is_ok(), candidate_present);
            if let Ok(resolved) = result {
                assert_eq!(resolved.execution.runtime(), candidate.runtime());
                assert_eq!(
                    resolved.components[0].metadata.digest,
                    component.metadata.digest
                );
            }
        }
        assert_eq!(
            inventory
                .resolve(
                    None,
                    RuntimeKind::Firecracker,
                    ExecutionSelectionMode::NewColdBoot
                )
                .unwrap()
                .execution
                .runtime()
                .version
                .as_str(),
            "1.15.1"
        );
    }
}

#[test]
fn compatible_selection_preserves_exact_before_fallback() {
    use crate::catalog::CompiledEnvironment;
    use crate::selection::{self, CompatibleSelection, ExecutionPolicy};
    let mut catalog = CompiledEnvironment::for_architecture(Arch::X86_64).unwrap();
    // These scenarios exercise Firecracker snapshot-format qualification.
    catalog.retain(|entry| entry.release().release.kind == RuntimeKind::Firecracker);
    let source = catalog[0].definition().execution_requirements().unwrap();
    let all = "e0,fc-1.17.0-linux-6.1.155,fc-1.17.0-linux-6.18.44";
    for exact_installed in [true, false] {
        let mut inventory = inventory(Vec::new());
        for entry in &catalog {
            if entry.name() == "e0" && !exact_installed {
                continue;
            }
            let execution = entry.definition().execution_requirements().unwrap();
            let mut runtime = installation(
                execution.runtime().version.as_str(),
                execution.state_format().unwrap().version.as_str(),
            );
            runtime.guest = execution.guest();
            let mut component = kernel(entry.kernel().unwrap().version.as_str(), 0, false, false);
            component.metadata = execution.components()[0].clone();
            inventory.environments.insert(
                entry.definition().reference(),
                InstalledEnvironment {
                    runtime: Ok(runtime),
                    components: Ok(vec![component]),
                },
            );
        }
        for (allowed, default, exact_allowed) in [
            (all, "e0", true),
            (
                "fc-1.17.0-linux-6.1.155,fc-1.17.0-linux-6.18.44",
                "fc-1.17.0-linux-6.18.44",
                false,
            ),
        ] {
            let policy = ExecutionPolicy::parse(Some(allowed), Some(default), &catalog).unwrap();
            let (_, cold) = selection::select(
                &inventory,
                &policy,
                CompatibleSelection::ColdBoot { source: &source },
            )
            .unwrap();
            assert_eq!(
                cold.execution.runtime().version.as_str(),
                if exact_installed && exact_allowed {
                    "1.15.1"
                } else {
                    "1.17.0"
                }
            );
            assert_eq!(cold.execution.components(), source.components());
            let restore = selection::select(
                &inventory,
                &policy,
                CompatibleSelection::Restore { source: &source },
            );
            if exact_installed && exact_allowed {
                assert_eq!(
                    restore.unwrap().1.execution.runtime().version.as_str(),
                    "1.15.1"
                );
            } else {
                restore.unwrap_err();
            }
        }
    }
}

#[test]
fn restore_falls_back_after_revocation_without_boot_files() {
    use crate::catalog::CompiledEnvironment;
    use crate::selection::{self, CompatibleSelection, ExecutionPolicy};
    let mut catalog = CompiledEnvironment::for_architecture(Arch::X86_64).unwrap();
    // These scenarios exercise Firecracker snapshot-format qualification.
    catalog.retain(|entry| entry.release().release.kind == RuntimeKind::Firecracker);
    let source = catalog[0].definition().execution_requirements().unwrap();
    // Synthetic qualification proves a patch release is eligible because its
    // declared profile matches, not because its version happens to be newer.
    let mut release = catalog[0].release().clone();
    release.release.version = "1.15.2".parse().unwrap();
    catalog.push(CompiledEnvironment::for_test(
        release,
        catalog[0].kernel().unwrap().clone(),
        "9.0.0",
        true,
    ));
    let mut inventory = inventory(Vec::new());
    inventory.components.clear();
    for entry in &catalog {
        let execution = entry.definition().execution_requirements().unwrap();
        let mut runtime = installation(
            execution.runtime().version.as_str(),
            execution.state_format().unwrap().version.as_str(),
        );
        runtime.guest = execution.guest();
        inventory.environments.insert(
            entry.definition().reference(),
            InstalledEnvironment {
                runtime: Ok(runtime),
                components: Err(std::sync::Arc::new(
                    RuntimeInventoryError::NoCompatibleComponent(execution.components()[0].clone()),
                )),
            },
        );
    }
    for (allowed, expected) in [("e0,synthetic", "1.15.1"), ("synthetic", "1.15.2")] {
        let policy = ExecutionPolicy::parse(Some(allowed), Some("synthetic"), &catalog).unwrap();
        let (_, selected) = selection::select(
            &inventory,
            &policy,
            CompatibleSelection::Restore { source: &source },
        )
        .unwrap();
        assert_eq!(selected.execution.runtime().version.as_str(), expected);
        assert_eq!(selected.execution.state_format(), source.state_format());
        assert_eq!(selected.execution.components(), source.components());
        assert!(selected.components.is_empty());
        assert_eq!(source.runtime().version.as_str(), "1.15.1");
        selection::select(
            &inventory,
            &policy,
            CompatibleSelection::ColdBoot { source: &source },
        )
        .unwrap_err();
    }
}

#[test]
fn transient_exact_source_failure_never_selects_another_consumer() {
    use crate::catalog::CompiledEnvironment;
    use crate::selection::{self, CompatibleSelection, ExecutionPolicy, SelectionError};
    let mut catalog = CompiledEnvironment::for_architecture(Arch::X86_64).unwrap();
    // These scenarios exercise Firecracker snapshot-format qualification.
    catalog.retain(|entry| entry.release().release.kind == RuntimeKind::Firecracker);
    let source = catalog[0].definition().execution_requirements().unwrap();
    let mut release = catalog[0].release().clone();
    release.release.version = "1.15.2".parse().unwrap();
    catalog.push(CompiledEnvironment::for_test(
        release,
        catalog[0].kernel().unwrap().clone(),
        "9.0.0",
        true,
    ));
    for reverse in [false, true] {
        if reverse {
            catalog.reverse();
        }
        for transient in [false, true] {
            for allowed in ["e0,synthetic", "synthetic"] {
                let policy =
                    ExecutionPolicy::parse(Some(allowed), Some("synthetic"), &catalog).unwrap();
                let mut inventory = inventory(Vec::new());
                for entry in &catalog {
                    let execution = entry.definition().execution_requirements().unwrap();
                    let mut runtime = installation(
                        execution.runtime().version.as_str(),
                        execution.state_format().unwrap().version.as_str(),
                    );
                    runtime.guest = execution.guest();
                    let mut component =
                        kernel(entry.kernel().unwrap().version.as_str(), 0, false, false);
                    component.metadata = execution.components()[0].clone();
                    inventory.environments.insert(
                        entry.definition().reference(),
                        InstalledEnvironment {
                            runtime: if entry.name() == "e0" {
                                Err(std::sync::Arc::new(RuntimeInventoryError::ReadDir {
                                    path: "/synthetic/runtime".into(),
                                    source: std::io::Error::from(if transient {
                                        std::io::ErrorKind::Interrupted
                                    } else {
                                        std::io::ErrorKind::NotFound
                                    }),
                                }))
                            } else {
                                Ok(runtime)
                            },
                            components: Ok(vec![component]),
                        },
                    );
                }
                for intent in [
                    CompatibleSelection::ColdBoot { source: &source },
                    CompatibleSelection::Restore { source: &source },
                ] {
                    let result = selection::select(&inventory, &policy, intent);
                    if transient && allowed == "e0,synthetic" {
                        assert!(
                            matches!(result, Err(SelectionError::Unavailable(ref error)) if error.is_transient())
                        );
                    } else {
                        assert_eq!(
                            result.unwrap().1.execution.runtime().version.as_str(),
                            "1.15.2"
                        );
                    }
                }
            }
        }
    }
}

#[test]
fn compatible_fallback_orders_versions_and_refuses_equal_ranked_identities() {
    use crate::{
        catalog::CompiledEnvironment,
        selection::{self, CompatibleSelection, ExecutionPolicy, SelectionError},
    };
    let initial = CompiledEnvironment::for_architecture(Arch::X86_64)
        .unwrap()
        .remove(0);
    let source = initial.definition().execution_requirements().unwrap();
    for ambiguous in [false, true] {
        let mut catalog = vec![initial.clone()];
        let mut inventory = inventory(Vec::new());
        for (name, version, digest) in [
            ("older", "1.15.2", 21),
            ("newer", if ambiguous { "1.15.2" } else { "1.15.3" }, 22),
        ] {
            let mut release = initial.release().clone();
            release.release.version = version.parse().unwrap();
            release.binaries[0].sha256 = Digest::sha256([digest; 32]);
            let entry = CompiledEnvironment::for_test(
                release,
                initial.kernel().unwrap().clone(),
                "9.0.0",
                true,
            )
            .named_for_test(name);
            let mut runtime = installation(version, "9.0.0");
            runtime.guest = source.guest();
            let mut component = kernel("6.1.155", 0, false, false);
            component.metadata = source.components()[0].clone();
            inventory.environments.insert(
                entry.definition().reference(),
                InstalledEnvironment {
                    runtime: Ok(runtime),
                    components: Ok(vec![component]),
                },
            );
            catalog.push(entry);
        }
        for reverse in [false, true] {
            if reverse {
                catalog.reverse();
            }
            let policy =
                ExecutionPolicy::parse(Some("older,newer"), Some("older"), &catalog).unwrap();
            for intent in [
                CompatibleSelection::ColdBoot { source: &source },
                CompatibleSelection::Restore { source: &source },
            ] {
                let result = selection::select(&inventory, &policy, intent);
                if ambiguous {
                    assert!(matches!(result, Err(SelectionError::Ambiguous)));
                } else {
                    assert_eq!(
                        result.unwrap().1.execution.runtime().version.as_str(),
                        "1.15.3"
                    );
                }
            }
        }
    }
}

#[test]
fn cold_only_runtime_has_no_launcher_or_snapshot_claims() {
    let root = tempfile::tempdir().unwrap();
    std::fs::create_dir(root.path().join("bin")).unwrap();
    std::fs::write(
        root.path().join("bin/cloud-hypervisor"),
        b"synthetic verified payload",
    )
    .unwrap();
    let driver = cloud_hypervisor::CloudHypervisorDriver;
    let installation = driver
        .inspect_installation(
            root.path().to_owned(),
            RuntimeVersion::parse("54.0").unwrap(),
        )
        .unwrap();
    assert!(
        installation
            .files
            .path(RuntimeFileRole::RuntimeExecutable)
            .is_some()
    );
    assert!(
        installation
            .files
            .path(RuntimeFileRole::LauncherExecutable)
            .is_none()
    );
    let source = ExecutionMetadata::new(
        installation.runtime.clone(),
        installation.guest,
        None,
        vec![],
    )
    .unwrap();
    let bound = source
        .clone()
        .with_environment(EnvironmentReference::new(Digest::sha256([31; 32])));
    for (mode, requested, allowed) in [
        (ExecutionSelectionMode::NewColdBoot, &bound, true),
        (ExecutionSelectionMode::ExistingColdBoot, &bound, true),
        (ExecutionSelectionMode::PersistedStateRestore, &bound, false),
        (ExecutionSelectionMode::NewColdBoot, &source, false),
    ] {
        assert_eq!(
            driver.can_execute(&installation, requested, mode).unwrap(),
            allowed
        );
    }
    let mut snapshot_installation = installation.clone();
    snapshot_installation.state_format = Some(StateFormatIdentity {
        format: "cloud-hypervisor-snapshot".parse().unwrap(),
        version: "54.0.1".parse().unwrap(),
    });
    let snapshot = ExecutionMetadata::new(
        snapshot_installation.runtime.clone(),
        snapshot_installation.guest,
        snapshot_installation.state_format.clone(),
        vec![],
    )
    .unwrap()
    .with_environment(EnvironmentReference::new(Digest::sha256([32; 32])));
    for mode in [
        ExecutionSelectionMode::NewColdBoot,
        ExecutionSelectionMode::ExistingColdBoot,
        ExecutionSelectionMode::PersistedStateRestore,
    ] {
        assert!(
            driver
                .can_execute(&snapshot_installation, &snapshot, mode)
                .unwrap()
        );
        assert!(!driver.can_execute(&installation, &snapshot, mode).unwrap());
        assert!(
            !driver
                .can_execute(&snapshot_installation, &bound, mode)
                .unwrap()
        );
    }
    let reported = inventory(vec![installation]).to_proto();
    assert!(reported.runtimes[0].state_formats.is_empty());
}

#[test]
fn cold_boot_resolves_every_authenticated_boot_input() {
    let reference = EnvironmentReference::new(Digest::sha256([9; 32]));
    let mut runtime = installation("54.0", "1");
    runtime.runtime.kind = RuntimeKind::CloudHypervisor;
    runtime.state_format = None;
    let kernel = kernel("6.8.0", 1, false, false);
    let initramfs = ArtifactComponentInstallation {
        metadata: ArtifactComponentMetadata {
            kind: ArtifactComponentKind::Initrd,
            role: Some("initramfs".parse().unwrap()),
            version: Some("6.8.0".parse().unwrap()),
            digest: Some(Digest::sha256([2; 32])),
            format: Some("cpio".parse().unwrap()),
        },
        path: PathBuf::from("/boot-assets/initrd.img"),
        default_for_new: false,
        legacy_default: false,
        integrity: ExecutionSourceIntegrity::CatalogSha256,
    };
    let requested = ExecutionMetadata::new(
        runtime.runtime.clone(),
        runtime.guest,
        None,
        vec![kernel.metadata.clone(), initramfs.metadata.clone()],
    )
    .unwrap()
    .with_environment(reference);
    for initramfs_present in [true, false] {
        let mut inventory = inventory(Vec::new());
        let mut components = vec![kernel.clone()];
        if initramfs_present {
            components.push(initramfs.clone());
        }
        inventory.environments.insert(
            reference,
            InstalledEnvironment {
                runtime: Ok(runtime.clone()),
                components: Ok(components),
            },
        );
        let result = inventory.resolve(
            Some(&requested),
            RuntimeKind::CloudHypervisor,
            ExecutionSelectionMode::ExistingColdBoot,
        );
        if initramfs_present {
            let resolved = result.unwrap();
            assert_eq!(resolved.components.len(), 2);
            let selected = resolved
                .component(
                    ArtifactComponentKind::Initrd,
                    initramfs.metadata.role.as_ref(),
                )
                .unwrap();
            assert_eq!(selected.path, initramfs.path);
            assert_eq!(selected.metadata, initramfs.metadata);
            assert_eq!(selected.integrity, ExecutionSourceIntegrity::CatalogSha256);
        } else {
            assert!(matches!(
                result,
                Err(RuntimeInventoryError::NoCompatibleComponent(_))
            ));
        }
    }
}

#[test]
fn bound_firmware_resolution_never_borrows_a_default_kernel() {
    use crate::catalog::CompiledEnvironment;
    let catalog = CompiledEnvironment::for_architecture(Arch::X86_64).unwrap();
    let entry = catalog
        .iter()
        .find(|e| e.name() == "ch-54.0-windows-uefi")
        .unwrap();
    let requested = entry.definition().execution_requirements().unwrap();
    let mut inventory = inventory(Vec::new());
    let runtime = RuntimeInstallation {
        runtime: requested.runtime().clone(),
        guest: requested.guest(),
        state_format: None,
        files: RuntimeInstallationFiles::new([(
            RuntimeFileRole::RuntimeExecutable,
            "/synthetic/cloud-hypervisor".into(),
        )]),
    };
    inventory.environments.insert(
        entry.definition().reference(),
        InstalledEnvironment {
            runtime: Ok(runtime),
            components: Ok(vec![ArtifactComponentInstallation {
                metadata: requested.components()[0].clone(),
                path: "/synthetic/firmware.fd".into(),
                integrity: ExecutionSourceIntegrity::CatalogSha256,
                default_for_new: false,
                legacy_default: false,
            }]),
        },
    );
    let resolved = inventory
        .resolve(
            Some(&requested),
            RuntimeKind::CloudHypervisor,
            ExecutionSelectionMode::NewColdBoot,
        )
        .unwrap();
    assert_eq!(resolved.environment, requested.environment());
    assert_eq!(
        entry
            .definition()
            .bind_execution(resolved.execution)
            .unwrap(),
        requested
    );
    assert_eq!(resolved.components.len(), 1);
    assert_eq!(
        resolved.components[0].metadata.kind,
        ArtifactComponentKind::Firmware
    );
    inventory
        .environments
        .get_mut(&entry.definition().reference())
        .unwrap()
        .components = Ok(Vec::new());
    inventory
        .resolve(
            Some(&requested),
            RuntimeKind::CloudHypervisor,
            ExecutionSelectionMode::NewColdBoot,
        )
        .unwrap_err();
}

#[test]
fn firecracker_sort_keys_order_as_the_driver_compares() {
    let versions = ["1.9.0", "v1.15.1", "1.15.10", "1.17.0", "1.17.1", "2.0.0"];
    let identity = |version: &str| RuntimeIdentity {
        kind: RuntimeKind::Firecracker,
        version: RuntimeVersion::parse(version).unwrap(),
    };
    for left in versions {
        for right in versions {
            let compared = FirecrackerDriver
                .compare_versions(&identity(left).version, &identity(right).version)
                .unwrap();
            let keyed = version_sort_key(&identity(left))
                .unwrap()
                .cmp(&version_sort_key(&identity(right)).unwrap());
            assert_eq!(keyed, compared, "{left} vs {right}");
        }
    }
    assert_eq!(
        version_sort_key(&identity("v1.15.1")).unwrap().components(),
        [1, 15, 1]
    );
    // Suffixes order by strings, and oversized parts do not fit INT[].
    for unordered in [
        "1.17.0-rc.1",
        "1.17.0+build.5",
        "3000000000.0.0",
        "not-semver",
    ] {
        assert_eq!(version_sort_key(&identity(unordered)), None, "{unordered}");
    }
    assert_eq!(
        version_sort_key(&RuntimeIdentity {
            kind: RuntimeKind::Gvisor,
            version: RuntimeVersion::parse("40.0").unwrap(),
        }),
        None,
        "a runtime without a driver publishes no order"
    );
}

#[test]
fn cloud_hypervisor_sort_keys_order_as_the_driver_compares() {
    let driver = cloud_hypervisor::CloudHypervisorDriver;
    let versions = ["40.0", "54.0", "54.0.1", "54.1", "100.0"];
    let version = |text: &str| RuntimeVersion::parse(text).unwrap();
    for left in versions {
        for right in versions {
            let compared = driver
                .compare_versions(&version(left), &version(right))
                .unwrap();
            let keyed = driver
                .version_sort_key(&version(left))
                .unwrap()
                .cmp(&driver.version_sort_key(&version(right)).unwrap());
            assert_eq!(keyed, compared, "{left} vs {right}");
        }
    }
    assert_eq!(
        driver
            .version_sort_key(&version("54.0"))
            .unwrap()
            .components(),
        [54, 0, 0]
    );
    for unordered in ["54.0-rc.1", "3000000000.0", "not-semver"] {
        assert_eq!(
            driver.version_sort_key(&version(unordered)),
            None,
            "{unordered}"
        );
    }
}

#[test]
fn published_sort_keys_match_the_selection_ordering() {
    // Restore selection ranks by `runtime_ordering_key`; the observation
    // publishes the driver's key. A release bump must move both.
    let mut checked = 0;
    for architecture in [Arch::X86_64, Arch::Aarch64] {
        for entry in crate::catalog::CompiledEnvironment::for_architecture(architecture).unwrap() {
            let published = crate::runtime_inventory::version_sort_key(&entry.release().release)
                .unwrap_or_else(|| panic!("{} has no runtime ordering key", entry.name()));
            let selection = entry.runtime_ordering_key();
            assert!(
                selection.pre.is_empty() && selection.build.is_empty(),
                "{} ranks by a key the observation can't publish",
                entry.name()
            );
            let expected = [selection.major, selection.minor, selection.patch]
                .map(|part| u32::try_from(part).unwrap());
            assert_eq!(published.components(), expected, "{}", entry.name());
            checked += 1;
        }
    }
    assert!(checked > 0, "the compiled catalog has entries to compare");
}
