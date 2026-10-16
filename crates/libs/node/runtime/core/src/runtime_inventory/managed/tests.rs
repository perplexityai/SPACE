use super::*;
use crate::artifact_store::test_support::*;
use crate::catalog::PinnedKernel;
use std::os::unix::fs::{MetadataExt, PermissionsExt};

/// Overwrite `path` in place with same-length `bytes`, as a repair does, and
/// move its mtime past the one any earlier observation recorded. The rewrite
/// keeps the inode and the length, and a filesystem that stamps file times at
/// timer-tick granularity (ext4 on Linux, unlike APFS) often gives two writes
/// in quick succession the same mtime and ctime, so the metadata fingerprint
/// would see nothing to invalidate. A real repair lands long after the
/// observation it invalidates.
fn rewrite_in_place(path: &std::path::Path, bytes: &[u8]) {
    let observed = std::fs::metadata(path).unwrap().modified().unwrap();
    std::fs::write(path, bytes).unwrap();
    std::fs::File::options()
        .write(true)
        .open(path)
        .unwrap()
        .set_modified(observed + std::time::Duration::from_secs(1))
        .unwrap();
}

#[test]
fn managed_e0_survives_cache_loss_without_selecting_other_installs() {
    use crate::runtime_inventory::{ExecutionSelectionMode, RuntimeFileRole, RuntimeInventory};
    let fixture = Fixture::new();
    std::fs::remove_dir_all(fixture.release_path("1.15.1", 1)).unwrap();
    let initial = synthetic_initial_environment();
    let vendor = fixture._dir.path().join("external/firecracker");
    let cache = fixture._dir.path().join("cache/firecracker");
    std::fs::create_dir_all(&cache).unwrap();
    let discover = || {
        RuntimeInventory::discover_with_store(
            &fixture.store,
            &initial,
            &vendor,
            &cache.join("assets"),
            &cache.join("kernels"),
        )
    };
    let resolve = |inventory: &RuntimeInventory| {
        inventory.resolve(
            None,
            RuntimeKind::Firecracker,
            ExecutionSelectionMode::NewColdBoot,
        )
    };
    let empty = discover().unwrap();
    assert!(empty.to_proto().runtimes.is_empty());
    assert!(empty.to_proto().components.is_empty());
    resolve(&empty).unwrap_err();

    fixture.stage_release("runtime", "1.15.1", 1);
    fixture
        .store
        .publish_release(initial.release(), &staged("runtime"))
        .unwrap();
    assert!(
        resolve(&discover().unwrap()).is_err(),
        "a release alone is not a usable environment"
    );
    let source = fixture
        .staging
        .join("firecracker/kernels/kernel/6.1.155/vmlinux.bin");
    std::fs::create_dir_all(source.parent().unwrap()).unwrap();
    std::fs::write(&source, binary(42)).unwrap();
    let kernel = fixture
        .store
        .publish_kernel(initial.kernel(), &staged("kernel"))
        .unwrap();

    let inventory = discover().unwrap();
    let selected = resolve(&inventory).unwrap();
    assert_eq!(selected.execution.runtime(), &initial.release().release);
    let staged_kernel = &selected.components[0].path;
    assert_eq!(
        selected.components[0].metadata.digest,
        Some(initial.kernel().sha256)
    );
    assert!(
        !std::fs::symlink_metadata(staged_kernel)
            .unwrap()
            .file_type()
            .is_symlink()
    );
    assert_ne!(
        std::fs::metadata(staged_kernel).unwrap().ino(),
        std::fs::metadata(kernel.path.join(KERNEL_IMAGE))
            .unwrap()
            .ino()
    );
    let jail = cache.join("jail-kernel");
    std::fs::hard_link(staged_kernel, &jail).unwrap();
    assert!(
        inventory.check_files_unchanged(),
        "linking a kernel into a jail must not invalidate the verified inventory"
    );

    // Coexisting baked bytes remain external. Both sources resolve the same
    // identity, with the managed copy preferred whenever it exists.
    let baked = vendor.join("v1.15.1/bin");
    std::fs::create_dir_all(&baked).unwrap();
    for name in ["firecracker", "jailer"] {
        std::fs::copy(
            fixture.release_path("1.15.1", 1).join("bin").join(name),
            baked.join(name),
        )
        .unwrap();
    }
    fixture
        .store
        .remove_release(&initial.release().release, initial.release().digest())
        .unwrap();
    let baked_kernel = cache.join("kernels/6.1.155/vmlinux.bin");
    std::fs::create_dir_all(baked_kernel.parent().unwrap()).unwrap();
    std::fs::copy(kernel.path.join(KERNEL_IMAGE), &baked_kernel).unwrap();
    fixture
        .store
        .remove_kernel(&initial.kernel().version, initial.kernel().sha256)
        .unwrap();
    let from_baked = resolve(&discover().unwrap()).unwrap();
    assert_eq!(from_baked.execution, selected.execution);
    assert_eq!(
        from_baked.components[0].path,
        baked_kernel.canonicalize().unwrap()
    );
    assert_eq!(
        from_baked
            .runtime_files
            .path(RuntimeFileRole::FirecrackerBinary)
            .unwrap(),
        baked.join("firecracker")
    );
    fixture
        .store
        .publish_release(initial.release(), &staged("runtime"))
        .unwrap();
    fixture
        .store
        .publish_kernel(initial.kernel(), &staged("kernel"))
        .unwrap();
    assert_eq!(
        resolve(&discover().unwrap()).unwrap().runtime_files,
        selected.runtime_files
    );

    // A malformed/unselected candidate cannot enter the execution inventory.
    let before_candidate = discover().unwrap();
    fixture.stage_release("candidate", "1.17.0", 7);
    let candidate = fixture
        .store
        .publish_release(&pin("1.17.0", 7), &staged("candidate"))
        .unwrap();
    assert!(
        before_candidate.check_files_unchanged(),
        "unselected managed candidates must not invalidate E0"
    );
    std::fs::write(candidate.path.join("bin/firecracker"), b"damaged candidate").unwrap();
    std::os::unix::fs::symlink(&candidate.path, vendor.join("v1.17.0")).unwrap();
    std::os::unix::fs::symlink("v1.17.0", vendor.join("default")).unwrap();
    let after_candidate = discover().unwrap();
    assert_eq!(after_candidate.to_proto().runtimes.len(), 1);
    assert_eq!(
        resolve(&after_candidate).unwrap().execution,
        selected.execution
    );

    // Explicit historical kernels retain their identity. Ambiguous legacy
    // defaults must not silently turn an unannotated record into E0.
    let historical = cache.join("kernels/custom/vmlinux.bin");
    std::fs::create_dir_all(historical.parent().unwrap()).unwrap();
    std::fs::write(&historical, binary(99)).unwrap();
    for alias in ["default", "legacy-default"] {
        std::os::unix::fs::symlink("custom", cache.join("kernels").join(alias)).unwrap();
    }
    let inventory = discover().unwrap();
    let legacy = pplx_space_common_core::types::ExecutionMetadata::initial_firecracker(
        selected.execution.guest().architecture,
        Vec::new(),
    );
    // A dangling explicit legacy marker must not inherit a stock new default.
    let new_default = cache.join("kernels/default");
    std::fs::remove_file(&new_default).unwrap();
    std::os::unix::fs::symlink("6.1.155", &new_default).unwrap();
    for (target, allowed) in [("custom", false), ("missing", false), ("6.1.155", true)] {
        let marker = cache.join("kernels/legacy-default");
        std::fs::remove_file(&marker).unwrap();
        std::os::unix::fs::symlink(target, &marker).unwrap();
        let binding = discover().unwrap();
        for mode in [
            ExecutionSelectionMode::ExistingColdBoot,
            ExecutionSelectionMode::PersistedStateRestore,
        ] {
            assert_eq!(
                binding
                    .resolve(Some(&legacy), RuntimeKind::Firecracker, mode)
                    .is_ok(),
                allowed,
                "legacy marker {target} must not silently change kernels"
            );
        }
    }
    let mut component = selected.components[0].metadata.clone();
    component.version = Some("custom".parse().unwrap());
    component.digest = Some(sha256(&binary(99)));
    let explicit = pplx_space_common_core::types::ExecutionMetadata::initial_firecracker(
        selected.execution.guest().architecture,
        vec![component],
    );
    let restored = inventory
        .resolve(
            Some(&explicit),
            RuntimeKind::Firecracker,
            ExecutionSelectionMode::ExistingColdBoot,
        )
        .unwrap();
    assert_eq!(restored.components[0].path, historical);

    // Reboot recovery needs only the durable store, not the download staging.
    std::fs::remove_dir_all(&fixture.staging).unwrap();
    std::fs::remove_dir_all(cache.join("managed")).unwrap();
    assert!(!inventory.check_files_unchanged());
    let recovered = discover().unwrap();
    assert!(recovered.check_files_unchanged());
    assert_eq!(resolve(&recovered).unwrap().execution, selected.execution);
    assert_eq!(std::fs::read(&jail).unwrap(), binary(42));

    // Partial cache loss is recoverable too, without touching existing jails.
    std::fs::remove_file(&resolve(&recovered).unwrap().components[0].path).unwrap();
    let recovered = discover().unwrap();
    assert_eq!(
        std::fs::read(&resolve(&recovered).unwrap().components[0].path).unwrap(),
        binary(42)
    );

    // Cache corruption is not repaired by overwriting a file used by a jail.
    std::fs::write(&resolve(&recovered).unwrap().components[0].path, b"bad").unwrap();
    assert!(!recovered.check_files_unchanged());
    resolve(&discover().unwrap()).unwrap_err();
    assert_eq!(std::fs::read(&jail).unwrap(), binary(42));
    assert_eq!(
        selected
            .runtime_files
            .path(RuntimeFileRole::FirecrackerBinary)
            .unwrap(),
        fixture.release_path("1.15.1", 1).join("bin/firecracker")
    );
}

fn synthetic_initial_environment() -> crate::catalog::InitialEnvironment {
    use crate::catalog::InitialEnvironment;
    InitialEnvironment::for_test(
        pin("1.15.1", 1),
        PinnedKernel {
            version: "6.1.155".parse().unwrap(),
            runtime: RuntimeKind::Firecracker,
            runtime_line: "1.15".parse().unwrap(),
            architecture: Arch::X86_64,
            size: 4096,
            sha256: sha256(&binary(42)),
        },
    )
}

#[tokio::test(start_paused = true)]
async fn e0_failures_preserve_unrelated_external_execution() {
    use crate::runtime_inventory::{
        ExecutionSelectionMode, RuntimeInstallationRoot, RuntimeInventory,
    };
    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    enum Failure {
        Binary,
        KernelMode,
        MissingKernel,
        DirectoryMode,
        ManagedCache,
        CacheIo,
    }
    for failure in [
        Failure::Binary,
        Failure::KernelMode,
        Failure::MissingKernel,
        Failure::DirectoryMode,
        Failure::ManagedCache,
        Failure::CacheIo,
    ] {
        let fixture = Fixture::new();
        let initial = synthetic_initial_environment();
        let vendor = fixture._dir.path().join("external/firecracker");
        let cache = fixture._dir.path().join("cache/firecracker");
        let kernels = cache.join("kernels");
        let baked = vendor.join("v1.15.1/bin");
        let other = vendor.join("v1.17.0/bin");
        for directory in [
            &baked,
            &other,
            &kernels.join("6.1.155"),
            &kernels.join("custom"),
        ] {
            std::fs::create_dir_all(directory).unwrap();
        }
        for name in ["firecracker", "jailer"] {
            let source = fixture.release_path("1.15.1", 1).join("bin").join(name);
            std::fs::copy(&source, baked.join(name)).unwrap();
            std::fs::copy(&source, other.join(name)).unwrap();
        }
        std::fs::remove_dir_all(fixture.release_path("1.15.1", 1)).unwrap();
        let image = kernels.join("6.1.155/vmlinux.bin");
        std::fs::write(&image, binary(42)).unwrap();
        std::fs::write(kernels.join("custom/vmlinux.bin"), binary(99)).unwrap();
        std::os::unix::fs::symlink("v1.17.0", vendor.join("default")).unwrap();
        std::os::unix::fs::symlink("custom", kernels.join("default")).unwrap();
        std::os::unix::fs::symlink("6.1.155", kernels.join("legacy-default")).unwrap();
        let external = RuntimeInventory::discover(
            [RuntimeInstallationRoot::new(
                RuntimeKind::Firecracker,
                &vendor,
            )],
            cache.join("assets"),
            &kernels,
        )
        .unwrap();
        let explicit = external
            .resolve(
                None,
                RuntimeKind::Firecracker,
                ExecutionSelectionMode::NewColdBoot,
            )
            .unwrap()
            .execution;
        match failure {
            Failure::Binary => std::fs::write(baked.join("firecracker"), binary(88)).unwrap(),
            Failure::MissingKernel => std::fs::remove_file(&image).unwrap(),
            Failure::KernelMode => {
                std::fs::set_permissions(&image, std::fs::Permissions::from_mode(0o664)).unwrap()
            }
            Failure::DirectoryMode => std::fs::set_permissions(
                image.parent().unwrap(),
                std::fs::Permissions::from_mode(0o775),
            )
            .unwrap(),
            Failure::ManagedCache | Failure::CacheIo => {
                let source = fixture
                    .staging
                    .join("firecracker/kernels/kernel/6.1.155/vmlinux.bin");
                std::fs::create_dir_all(source.parent().unwrap()).unwrap();
                std::fs::write(&source, binary(42)).unwrap();
                fixture
                    .store
                    .publish_kernel(initial.kernel(), &staged("kernel"))
                    .unwrap();
                if failure == Failure::ManagedCache {
                    std::fs::set_permissions(&cache, std::fs::Permissions::from_mode(0o2775))
                        .unwrap();
                } else {
                    let target = cache.join("managed/kernels").join(managed_kernel_directory(
                        &initial.kernel().version,
                        initial.kernel().sha256,
                    ));
                    std::fs::create_dir_all(&target).unwrap();
                    std::fs::write(
                        target.join("obstruction"),
                        b"cache reconstruction I/O failure",
                    )
                    .unwrap();
                }
            }
        }
        let discover = || {
            RuntimeInventory::discover_with_store(
                &fixture.store,
                &initial,
                &vendor,
                &cache.join("assets"),
                &kernels,
            )
        };
        let inventory = discover().unwrap_or_else(|error| panic!("{failure:?}: {error}"));
        let error = inventory
            .resolve(
                None,
                RuntimeKind::Firecracker,
                ExecutionSelectionMode::NewColdBoot,
            )
            .unwrap_err();
        assert_eq!(
            error.is_transient(),
            failure == Failure::CacheIo,
            "{failure:?}: {error}"
        );
        assert!(
            inventory.check_files_unchanged(),
            "unchanged failures must not cause repeated hashing"
        );
        if failure == Failure::CacheIo {
            tokio::time::advance(std::time::Duration::from_secs(11)).await;
            assert!(
                !inventory.check_files_unchanged(),
                "transient I/O must retry without a filesystem change"
            );
            let blocker = cache
                .join("managed/kernels")
                .join(managed_kernel_directory(
                    &initial.kernel().version,
                    initial.kernel().sha256,
                ))
                .join("obstruction");
            std::fs::remove_file(blocker).unwrap();
        }
        for mode in [
            ExecutionSelectionMode::ExistingColdBoot,
            ExecutionSelectionMode::PersistedStateRestore,
        ] {
            let resolved = inventory
                .resolve(Some(&explicit), RuntimeKind::Firecracker, mode)
                .unwrap_or_else(|error| panic!("{failure:?}: {error}"));
            assert_eq!(resolved.execution, explicit, "{failure:?}");
        }
        // Repair only the failed input: unrelated writes must not be needed
        // to invalidate a cached negative observation.
        match failure {
            Failure::Binary => rewrite_in_place(&baked.join("firecracker"), &binary(1)),
            Failure::MissingKernel => std::fs::write(&image, binary(42)).unwrap(),
            Failure::KernelMode => {
                std::fs::set_permissions(&image, std::fs::Permissions::from_mode(0o644)).unwrap()
            }
            Failure::DirectoryMode => std::fs::set_permissions(
                image.parent().unwrap(),
                std::fs::Permissions::from_mode(0o755),
            )
            .unwrap(),
            Failure::ManagedCache => {
                std::fs::set_permissions(&cache, std::fs::Permissions::from_mode(0o3775)).unwrap()
            }
            Failure::CacheIo => {}
        }
        assert!(!inventory.check_files_unchanged(), "{failure:?}");
        discover()
            .unwrap()
            .resolve(
                None,
                RuntimeKind::Firecracker,
                ExecutionSelectionMode::NewColdBoot,
            )
            .unwrap_or_else(|error| panic!("{failure:?}: {error}"));
    }
}

#[test]
fn legacy_asset_alias_retains_e0_boot_and_restore() {
    let fixture = Fixture::new();
    let initial = synthetic_initial_environment();
    let vendor = fixture._dir.path().join("external/firecracker");
    let binaries = vendor.join("v1.15.1/bin");
    std::fs::create_dir_all(&binaries).unwrap();
    for name in ["firecracker", "jailer"] {
        std::fs::copy(
            fixture.release_path("1.15.1", 1).join("bin").join(name),
            binaries.join(name),
        )
        .unwrap();
    }
    std::os::unix::fs::symlink("v1.15.1", vendor.join("default")).unwrap();
    let cache = fixture._dir.path().join("cache/firecracker");
    let assets = cache.join("assets");
    let image = assets.join("v1.15.1/kernel/vmlinux.bin");
    std::fs::create_dir_all(image.parent().unwrap()).unwrap();
    std::fs::write(&image, binary(42)).unwrap();
    std::os::unix::fs::symlink("v1.15.1", assets.join("default")).unwrap();
    let kernels = cache.join("kernels");
    let external = RuntimeInventory::discover(
        [RuntimeInstallationRoot::new(
            RuntimeKind::Firecracker,
            &vendor,
        )],
        &assets,
        &kernels,
    )
    .unwrap();
    let legacy = external
        .resolve(
            None,
            RuntimeKind::Firecracker,
            ExecutionSelectionMode::NewColdBoot,
        )
        .unwrap()
        .execution;
    fixture
        .store
        .verify_kernel_at(initial.kernel(), &image)
        .unwrap();
    let managed =
        RuntimeInventory::discover_with_store(&fixture.store, &initial, &vendor, &assets, &kernels)
            .unwrap();
    for mode in [
        ExecutionSelectionMode::NewColdBoot,
        ExecutionSelectionMode::ExistingColdBoot,
        ExecutionSelectionMode::PersistedStateRestore,
    ] {
        let requested = if mode == ExecutionSelectionMode::NewColdBoot {
            None
        } else {
            Some(&legacy)
        };
        external
            .resolve(requested, RuntimeKind::Firecracker, mode)
            .unwrap();
        let selected = managed
            .resolve(requested, RuntimeKind::Firecracker, mode)
            .unwrap_or_else(|error| panic!("{mode:?}: {error}"));
        assert_eq!(selected.components[0].path, image.canonicalize().unwrap());
    }
    assert!(managed.check_files_unchanged());
    let other = assets.join("v1.16.0/kernel/vmlinux.bin");
    std::fs::create_dir_all(other.parent().unwrap()).unwrap();
    std::fs::write(&other, binary(99)).unwrap();
    std::fs::remove_file(assets.join("default")).unwrap();
    std::os::unix::fs::symlink("v1.16.0", assets.join("default")).unwrap();
    assert!(
        !managed.check_files_unchanged(),
        "retargeting the legacy alias must invalidate its binding"
    );
    let changed =
        RuntimeInventory::discover_with_store(&fixture.store, &initial, &vendor, &assets, &kernels)
            .unwrap();
    changed
        .resolve(
            None,
            RuntimeKind::Firecracker,
            ExecutionSelectionMode::NewColdBoot,
        )
        .unwrap_err();
}

#[test]
fn nondefault_inventory_preserves_e0_and_recovers_only_the_exact_pair() {
    use crate::catalog::CompiledEnvironment;
    let fixture = Fixture::new();
    let initial = synthetic_initial_environment();
    let environment = CompiledEnvironment::for_test(
        pin("1.17.0", 7),
        PinnedKernel {
            version: "6.18.44".parse().unwrap(),
            runtime: RuntimeKind::Firecracker,
            runtime_line: "1.17".parse().unwrap(),
            architecture: Arch::X86_64,
            size: 4096,
            sha256: sha256(&binary(44)),
        },
        "12.0.0",
        false,
    );
    let cache = fixture._dir.path().join("cache/firecracker");
    std::fs::create_dir_all(&cache).unwrap();
    for (job, kernel, fill) in [
        ("initial", initial.kernel(), 42),
        ("candidate", environment.kernel(), 44),
    ] {
        let source = fixture
            .staging
            .join("firecracker/kernels")
            .join(job)
            .join(kernel.version.as_str())
            .join(KERNEL_IMAGE);
        std::fs::create_dir_all(source.parent().unwrap()).unwrap();
        std::fs::write(source, binary(fill)).unwrap();
        fixture.store.publish_kernel(kernel, &staged(job)).unwrap();
    }
    let vendor = fixture._dir.path().join("vendor");
    let legacy_bin = vendor.join("v1.17.0/bin");
    std::fs::create_dir_all(&legacy_bin).unwrap();
    for file in ["firecracker", "jailer"] {
        let path = legacy_bin.join(file);
        std::fs::write(&path, binary(99)).unwrap();
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o755)).unwrap();
    }
    let legacy_kernel = cache.join("kernels/6.18.44/vmlinux.bin");
    std::fs::create_dir_all(legacy_kernel.parent().unwrap()).unwrap();
    std::fs::write(&legacy_kernel, binary(44)).unwrap();
    let discover = || {
        let mut inventory = RuntimeInventory::discover_with_store(
            &fixture.store,
            &initial,
            &vendor,
            &cache.join("assets"),
            &cache.join("kernels"),
        )
        .unwrap();
        inventory
            .include_environment(&fixture.store, &environment, &cache.join("kernels"))
            .unwrap();
        inventory
    };
    let definition = environment.definition();
    let requirements = definition.execution_requirements().unwrap();
    // Inventory inspection uses the test host architecture. The compiled
    // x86-only descriptor itself is tested independently in catalog tests.
    let requested = ExecutionMetadata::new(
        requirements.runtime().clone(),
        GuestPlatform {
            os: GuestOperatingSystem::Linux,
            architecture: host_arch().unwrap(),
        },
        requirements.state_format().cloned(),
        requirements.components().to_vec(),
    )
    .unwrap()
    .with_environment(definition.reference());
    let resolve = |inventory: &RuntimeInventory| {
        inventory.resolve(
            Some(&requested),
            RuntimeKind::Firecracker,
            ExecutionSelectionMode::NewColdBoot,
        )
    };
    let assert_default = |inventory: &RuntimeInventory| {
        assert_eq!(
            inventory
                .resolve(
                    None,
                    RuntimeKind::Firecracker,
                    ExecutionSelectionMode::NewColdBoot
                )
                .unwrap()
                .execution
                .runtime(),
            &initial.release().release
        );
    };
    let legacy_request = ExecutionMetadata::new(
        requested.runtime().clone(),
        requested.guest(),
        requested.state_format().cloned(),
        requested.components().to_vec(),
    )
    .unwrap();
    let assert_legacy = |inventory: &RuntimeInventory| {
        for mode in [
            ExecutionSelectionMode::ExistingColdBoot,
            ExecutionSelectionMode::PersistedStateRestore,
        ] {
            let legacy = inventory
                .resolve(Some(&legacy_request), RuntimeKind::Firecracker, mode)
                .unwrap();
            assert_eq!(
                legacy
                    .runtime_files
                    .path(RuntimeFileRole::FirecrackerBinary),
                Some(legacy_bin.join("firecracker").as_path())
            );
            assert_eq!(legacy.components[0].path, legacy_kernel);
        }
        let report = inventory.to_proto();
        assert!(report.runtimes.iter().any(|entry| {
            entry
                .runtime
                .as_ref()
                .is_some_and(|runtime| runtime.version == "1.17.0")
        }));
        assert!(report.components.iter().any(|entry| {
            entry.component.as_ref().is_some_and(|kernel| {
                kernel.digest.as_deref() == Some(&environment.kernel().sha256.to_string())
            })
        }));
    };
    let missing = discover();
    assert_default(&missing);
    assert_legacy(&missing);
    assert!(
        resolve(&missing).is_err(),
        "candidate kernel alone cannot produce E0/C hybrid"
    );
    fixture.place_release("1.17.0", 7);
    let ready = discover();
    assert_default(&ready);
    assert_legacy(&ready);
    let resolved = resolve(&ready).unwrap();
    assert_eq!(
        resolved
            .runtime_files
            .path(RuntimeFileRole::FirecrackerBinary),
        Some(
            fixture
                .release_path("1.17.0", 7)
                .join("bin/firecracker")
                .as_path()
        )
    );
    assert_eq!(resolved.execution.runtime(), &environment.release().release);
    assert_eq!(
        resolved.components[0].metadata.digest,
        Some(environment.kernel().sha256)
    );
    // Storage reuse is independent of the guest-boot qualification of a pairing.
    // An additional binding must not clear a shared artifact's default flags.
    let shared_kernel = CompiledEnvironment::for_test(
        environment.release().clone(),
        initial.kernel().clone(),
        "12.0.0",
        false,
    );
    let shared_runtime = CompiledEnvironment::for_test(
        initial.release().clone(),
        environment.kernel().clone(),
        "9.0.0",
        false,
    );
    let mut shared = ready.clone();
    for binding in [&shared_kernel, &shared_runtime] {
        shared
            .include_environment(&fixture.store, binding, &cache.join("kernels"))
            .unwrap();
        assert_default(&shared);
    }
    std::fs::remove_dir_all(cache.join("managed")).unwrap();
    assert!(!ready.check_files_unchanged());
    let recovered = discover();
    assert_eq!(resolve(&recovered).unwrap().execution, resolved.execution);
    let image = resolve(&recovered).unwrap().components[0].path.clone();
    std::fs::write(&image, binary(99)).unwrap();
    let bad_kernel = discover();
    assert_default(&bad_kernel);
    assert_legacy(&bad_kernel);
    resolve(&bad_kernel).unwrap_err();
    assert!(bad_kernel.unavailable.iter().any(|artifact| {
        matches!(artifact, UnavailableArtifact::Kernel { digest, .. } if *digest == environment.kernel().sha256)
    }));
    assert!(!bad_kernel.unavailable.iter().any(|artifact| {
        matches!(artifact, UnavailableArtifact::Runtime { identity, .. } if identity == &environment.release().release)
    }));
    rewrite_in_place(&image, &binary(44));
    assert!(
        !bad_kernel.check_files_unchanged(),
        "repairing cached bytes must invalidate the failure"
    );
    resolve(&discover()).unwrap();
    std::fs::write(
        fixture.release_path("1.17.0", 7).join("bin/firecracker"),
        binary(99),
    )
    .unwrap();
    let invalid = discover();
    assert_default(&invalid);
    assert_legacy(&invalid);
    assert!(
        resolve(&invalid).is_err(),
        "corrupt candidate cannot disable or substitute E0"
    );
}
