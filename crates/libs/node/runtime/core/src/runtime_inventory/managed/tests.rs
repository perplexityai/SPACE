use super::*;
use crate::artifact_store::managed_kernel_directory;
use crate::artifact_store::test_support::*;
use crate::catalog::PinnedKernel;
use std::os::unix::fs::{MetadataExt, PermissionsExt};

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
    // The synthetic executable is discovered as the test host's architecture.
    // Qualify its declared guest explicitly to exercise the x86_64 binding even
    // when these filesystem fixtures run on an ARM development host.
    let mut qualified = inventory.clone();
    qualified.runtimes[0].guest.architecture = Arch::X86_64;
    for (runtime_integrity, kernel_integrity) in [
        (
            ExecutionSourceIntegrity::CatalogSha256,
            ExecutionSourceIntegrity::CatalogSha256,
        ),
        (
            ExecutionSourceIntegrity::FsVerity,
            ExecutionSourceIntegrity::CatalogSha256,
        ),
        (
            ExecutionSourceIntegrity::FsVerity,
            ExecutionSourceIntegrity::FsVerity,
        ),
    ] {
        qualified.runtimes[0].files.integrity = runtime_integrity;
        qualified.components[0].integrity = kernel_integrity;
        let resolved = resolve(&qualified).unwrap();
        assert_eq!(
            resolved.environment,
            Some(initial.definition().unwrap().reference())
        );
        assert_eq!(resolved.runtime_files.integrity(), runtime_integrity);
        assert_eq!(resolved.components[0].integrity, kernel_integrity);
    }
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
            .path(RuntimeFileRole::RuntimeExecutable)
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
                allowed || mode == ExecutionSelectionMode::PersistedStateRestore,
                "only a cold boot depends on the legacy marker {target}"
            );
            if mode == ExecutionSelectionMode::PersistedStateRestore {
                let resolved = binding
                    .resolve(Some(&legacy), RuntimeKind::Firecracker, mode)
                    .unwrap();
                assert!(resolved.components.is_empty());
                assert!(resolved.execution.components().is_empty());
                assert_eq!(
                    resolved.environment,
                    (legacy.guest() == initial.definition().unwrap().descriptor().spec().guest)
                        .then(|| initial.definition().unwrap().reference())
                );
                assert_eq!(resolved.execution.environment(), None);
            }
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
            .path(RuntimeFileRole::RuntimeExecutable)
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
            Failure::Binary => replace_bytes(&baked.join("firecracker"), &binary(1)),
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
        if mode == ExecutionSelectionMode::PersistedStateRestore {
            assert!(selected.components.is_empty());
            assert_eq!(selected.execution.components(), legacy.components());
        } else {
            assert_eq!(selected.components[0].path, image.canonicalize().unwrap());
        }
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
        ("candidate", environment.kernel().unwrap(), 44),
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
    let requested = definition.execution_requirements().unwrap();
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
        GuestPlatform {
            os: GuestOperatingSystem::Linux,
            architecture: host_arch().unwrap(),
        },
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
                    .path(RuntimeFileRole::RuntimeExecutable),
                Some(legacy_bin.join("firecracker").as_path())
            );
            if mode == ExecutionSelectionMode::PersistedStateRestore {
                assert_eq!(legacy.environment, None);
                assert!(legacy.components.is_empty());
                assert_eq!(legacy.execution.components(), legacy_request.components());
            } else {
                assert_eq!(legacy.components[0].path, legacy_kernel);
            }
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
                kernel.digest.as_deref() == Some(&environment.kernel().unwrap().sha256.to_string())
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
    let unreadable = fixture._dir.path().join("blocked-release-root");
    std::fs::write(&unreadable, b"not a directory").unwrap();
    let blocked_store =
        ArtifactStore::new(&unreadable, &fixture.kernels, &fixture.staging, 1 << 20)
            .owned_by(std::fs::metadata(&fixture.kernels).unwrap().uid());
    let mut isolated = ready.clone();
    isolated
        .include_environment(&blocked_store, &environment, &cache.join("kernels"))
        .unwrap();
    assert_default(&isolated);
    assert_legacy(&isolated);
    resolve(&isolated).expect_err("an unreadable candidate must fail only its own binding");
    let resolved = resolve(&ready).unwrap();
    assert_eq!(resolved.environment, Some(definition.reference()));
    assert_eq!(
        resolved
            .runtime_files
            .path(RuntimeFileRole::RuntimeExecutable),
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
        Some(environment.kernel().unwrap().sha256)
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
        environment.kernel().unwrap().clone(),
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
    let different_digest = CompiledEnvironment::for_test(
        pin("1.17.0", 99),
        environment.kernel().unwrap().clone(),
        "12.0.0",
        false,
    );
    shared
        .include_environment(&fixture.store, &different_digest, &cache.join("kernels"))
        .unwrap();
    let request = requested
        .clone()
        .with_environment(different_digest.definition().reference());
    shared
        .resolve(
            Some(&request),
            RuntimeKind::Firecracker,
            ExecutionSelectionMode::NewColdBoot,
        )
        .expect_err("another digest of the same runtime version must not borrow verified bytes");
    std::fs::remove_dir_all(cache.join("managed")).unwrap();
    assert!(!ready.check_files_unchanged());
    let recovered = discover();
    assert_eq!(resolve(&recovered).unwrap().execution, resolved.execution);
    // Retain the old execution inode across a compact-layout upgrade rather
    // than publishing a second copy and leaving the historical one unused.
    let compact = resolve(&recovered).unwrap().components[0].path.clone();
    let old_directory = compact.parent().unwrap().parent().unwrap().join(
        kernel_directory_names(
            &environment.kernel().unwrap().version,
            environment.kernel().unwrap().sha256,
        )[1]
        .clone(),
    );
    let inode = std::fs::metadata(&compact).unwrap().ino();
    std::fs::rename(compact.parent().unwrap(), &old_directory).unwrap();
    let recovered = discover();
    let historical = resolve(&recovered).unwrap().components[0].path.clone();
    assert_eq!(historical, old_directory.join(KERNEL_IMAGE));
    assert_eq!(std::fs::metadata(&historical).unwrap().ino(), inode);
    assert!(!compact.exists());
    let image = resolve(&recovered).unwrap().components[0].path.clone();
    std::fs::write(&image, binary(99)).unwrap();
    let bad_kernel = discover();
    assert_default(&bad_kernel);
    assert_legacy(&bad_kernel);
    resolve(&bad_kernel).unwrap_err();
    assert!(bad_kernel.unavailable.iter().any(|artifact| {
        matches!(artifact, UnavailableArtifact::Kernel { digest, .. } if *digest == environment.kernel().unwrap().sha256)
    }));
    assert!(!bad_kernel.unavailable.iter().any(|artifact| {
        matches!(artifact, UnavailableArtifact::Runtime { identity, .. } if identity == &environment.release().release)
    }));
    replace_bytes(&image, &binary(44));
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

/// Model atomic repair without depending on sub-tick mtime changes for same-size writes.
fn replace_bytes(path: &Path, bytes: &[u8]) {
    let replacement = tempfile::NamedTempFile::new_in(path.parent().unwrap()).unwrap();
    std::fs::write(replacement.path(), bytes).unwrap();
    std::fs::set_permissions(
        replacement.path(),
        std::fs::metadata(path).unwrap().permissions(),
    )
    .unwrap();
    replacement.persist(path).unwrap();
}

#[test]
fn cache_repairs_invalidate_a_failed_binding_observation() {
    enum Repair {
        File,
        Ancestor,
    }
    for repair in [Repair::File, Repair::Ancestor] {
        let fixture = Fixture::new();
        let initial = synthetic_initial_environment();
        let source = fixture
            .staging
            .join("firecracker/kernels/kernel/6.1.155/vmlinux.bin");
        std::fs::create_dir_all(source.parent().unwrap()).unwrap();
        std::fs::write(&source, binary(42)).unwrap();
        fixture
            .store
            .publish_kernel(initial.kernel(), &staged("kernel"))
            .unwrap();
        let cache = fixture._dir.path().join("cache/firecracker");
        let image = ArtifactStore::execution_kernel_path(&cache, initial.kernel()).unwrap();
        std::fs::create_dir_all(image.parent().unwrap()).unwrap();
        std::fs::write(&image, binary(99)).unwrap();
        if matches!(repair, Repair::Ancestor) {
            std::fs::write(&image, binary(42)).unwrap();
            std::fs::set_permissions(&cache, std::fs::Permissions::from_mode(0o777)).unwrap();
        }
        let discover = || {
            RuntimeInventory::discover_with_store(
                &fixture.store,
                &initial,
                &fixture._dir.path().join("vendor"),
                &cache.join("assets"),
                &cache.join("kernels"),
            )
            .unwrap()
        };
        let inventory = discover();
        let [UnavailableArtifact::Kernel { digest, error, .. }] = inventory.unavailable.as_slice()
        else {
            panic!(
                "expected exactly the failed kernel binding: {:?}",
                inventory.unavailable
            );
        };
        assert_eq!(*digest, initial.kernel().sha256);
        match repair {
            Repair::File => assert!(matches!(
                error.as_ref(),
                RuntimeInventoryError::KernelStaging(_)
            )),
            Repair::Ancestor => assert!(matches!(
                error.as_ref(),
                RuntimeInventoryError::ManagedArtifact(
                    crate::artifact_store::PublishError::UnsafeRoot { .. }
                )
            )),
        }
        assert!(!error.is_transient());
        inventory
            .resolve(
                None,
                RuntimeKind::Firecracker,
                ExecutionSelectionMode::NewColdBoot,
            )
            .unwrap_err();
        assert!(inventory.check_files_unchanged());
        match repair {
            Repair::File => replace_bytes(&image, &binary(42)),
            Repair::Ancestor => {
                std::fs::set_permissions(&cache, std::fs::Permissions::from_mode(0o755)).unwrap()
            }
        }
        assert!(!inventory.check_files_unchanged());
        let repaired = discover();
        assert!(repaired.unavailable.is_empty());
        let resolved = repaired
            .resolve(
                None,
                RuntimeKind::Firecracker,
                ExecutionSelectionMode::NewColdBoot,
            )
            .unwrap();
        assert_eq!(
            resolved.components[0].metadata.digest,
            Some(initial.kernel().sha256)
        );
        assert_eq!(
            std::fs::read(&resolved.components[0].path).unwrap(),
            binary(42)
        );
    }
}

#[test]
fn empty_compiled_environment_is_absent_not_corrupt() {
    let fixture = Fixture::new();
    std::fs::remove_dir_all(fixture.release_path("1.15.1", 1)).unwrap();
    let initial = InitialEnvironment::for_architecture(Arch::X86_64).unwrap();
    let vendor = fixture._dir.path().join("external/firecracker");
    let cache = fixture._dir.path().join("cache/firecracker");
    std::fs::create_dir_all(&cache).unwrap();
    let inventory = RuntimeInventory::discover_with_store(
        &fixture.store,
        &initial,
        &vendor,
        &cache.join("assets"),
        &cache.join("kernels"),
    )
    .unwrap();
    assert!(inventory.to_proto().runtimes.is_empty());
    assert!(inventory.to_proto().components.is_empty());
    assert!(matches!(
        inventory.resolve(
            None,
            RuntimeKind::Firecracker,
            ExecutionSelectionMode::NewColdBoot
        ),
        Err(RuntimeInventoryError::NoInstalledRuntime(
            RuntimeKind::Firecracker
        ))
    ));
}

#[test]
fn legacy_cached_kernel_is_verified_without_a_store_source() {
    let fixture = Fixture::new();
    let initial = synthetic_initial_environment();
    std::fs::remove_dir_all(&fixture.kernels).unwrap();
    let cache = fixture._dir.path().join("cache/firecracker");
    let image = ArtifactStore::execution_kernel_path(&cache, initial.kernel()).unwrap();
    std::fs::create_dir_all(image.parent().unwrap()).unwrap();
    let discover = || {
        RuntimeInventory::discover_with_store(
            &fixture.store,
            &initial,
            &fixture._dir.path().join("vendor"),
            &cache.join("assets"),
            &cache.join("kernels"),
        )
        .unwrap()
    };
    for (bytes, accepted) in [(binary(42), true), (binary(99), false)] {
        std::fs::write(&image, bytes).unwrap();
        let inventory = discover();
        let kernel = inventory
            .components
            .iter()
            .find(|component| component.metadata.digest == Some(initial.kernel().sha256));
        assert_eq!(kernel.is_some(), accepted);
        if let Some(kernel) = kernel {
            assert_eq!(kernel.path, image);
            assert_eq!(kernel.integrity, ExecutionSourceIntegrity::CatalogSha256);
        }
    }
}

#[test]
fn managed_runtime_fallback_checks_store_ancestors() {
    let fixture = Fixture::new();
    let pin = pin("1.15.1", 1);
    let source = fixture.release_path("1.15.1", 1);
    let cache = fixture._dir.path().join("cache/firecracker");
    for ancestor in [
        fixture.releases.parent().unwrap(),
        fixture.releases.parent().unwrap().parent().unwrap(),
    ] {
        std::fs::set_permissions(ancestor, std::fs::Permissions::from_mode(0o777)).unwrap();
        assert!(
            runtime_from_source(&fixture.store, &pin, RuntimeSource::Store(&source), &cache)
                .is_err(),
            "managed source must reject writable ancestor {}",
            ancestor.display()
        );
        std::fs::set_permissions(ancestor, std::fs::Permissions::from_mode(0o755)).unwrap();
    }
}

#[test]
fn baked_kernel_aliases_report_one_binding_outcome() {
    for rejected in [
        &[][..],
        &["6.1.155"][..],
        &["v6.1.155"][..],
        &["6.1.155", "v6.1.155"][..],
    ] {
        let fixture = Fixture::new();
        let initial = synthetic_initial_environment();
        let cache = fixture._dir.path().join("cache/firecracker");
        let kernels = cache.join("kernels");
        for label in ["6.1.155", "v6.1.155"] {
            let source = kernels.join(label);
            std::fs::create_dir_all(&source).unwrap();
            let image = source.join(KERNEL_IMAGE);
            std::fs::write(&image, binary(42)).unwrap();
            if rejected.contains(&label) {
                std::fs::set_permissions(&image, std::fs::Permissions::from_mode(0o664)).unwrap();
            }
        }
        for alias in ["default", "legacy-default"] {
            std::os::unix::fs::symlink(kernels.join("6.1.155"), kernels.join(alias)).unwrap();
        }
        let inventory = RuntimeInventory::discover_with_store(
            &fixture.store,
            &initial,
            &fixture._dir.path().join("vendor"),
            &cache.join("assets"),
            &kernels,
        )
        .unwrap();
        let components: Vec<_> = inventory
            .components
            .iter()
            .filter(|component| component.metadata.digest == Some(initial.kernel().sha256))
            .collect();
        let failures = inventory.unavailable.iter().filter(|entry| matches!(entry, UnavailableArtifact::Kernel { digest, .. } if *digest == initial.kernel().sha256)).count();
        let usable = rejected.len() < 2;
        assert_eq!(
            components.len(),
            usize::from(usable),
            "rejected: {rejected:?}"
        );
        assert_eq!(
            failures,
            usize::from(!usable),
            "one identity cannot be both available and unavailable: {rejected:?}"
        );
        if let Some(component) = components.first() {
            assert!(component.default_for_new);
            assert!(component.legacy_default);
        }
    }
}

#[test]
fn discovered_initial_failure_is_not_replaced_by_an_unrelated_runtime_error() {
    use crate::{
        catalog::CompiledEnvironment,
        selection::{self, CompatibleSelection, ExecutionPolicy, SelectionError},
    };
    #[derive(Clone, Copy)]
    enum Failure {
        Absent,
        Corrupt,
        Transient,
    }
    for failure in [Failure::Absent, Failure::Corrupt, Failure::Transient] {
        let fixture = Fixture::new();
        let initial = synthetic_initial_environment();
        let e0 = initial.compiled_environment().unwrap();
        let source = e0.definition().execution_requirements().unwrap();
        let candidate = CompiledEnvironment::for_test(
            pin("1.17.0", 7),
            initial.kernel().clone(),
            "12.0.0",
            true,
        );
        fixture.place_release("1.17.0", 7);
        let kernel = fixture
            .kernels
            .join(crate::artifact_store::managed_kernel_directory(
                &initial.kernel().version,
                initial.kernel().sha256,
            ));
        std::fs::create_dir_all(&kernel).unwrap();
        std::fs::write(kernel.join(KERNEL_IMAGE), binary(42)).unwrap();
        let e0_path = fixture.release_path("1.15.1", 1);
        match failure {
            Failure::Absent | Failure::Transient => std::fs::remove_dir_all(&e0_path).unwrap(),
            Failure::Corrupt => {
                std::fs::write(e0_path.join("bin/firecracker"), binary(99)).unwrap()
            }
        }
        let cache = fixture._dir.path().join("cache/firecracker");
        std::fs::create_dir_all(&cache).unwrap();
        let mut inventory = RuntimeInventory::discover_with_store(
            &fixture.store,
            &initial,
            &fixture._dir.path().join("vendor"),
            &cache.join("assets"),
            &cache.join("kernels"),
        )
        .unwrap();
        assert!(
            !inventory
                .environments
                .contains_key(&e0.definition().reference())
        );
        inventory
            .include_environment(&fixture.store, &candidate, &cache.join("kernels"))
            .unwrap();
        // Synthetic binaries use the test host architecture; the qualification
        // under test is x86_64, including when these filesystem tests run on ARM.
        for environment in inventory.environments.values_mut() {
            if let Ok(runtime) = &mut environment.runtime {
                runtime.guest = source.guest();
            }
        }
        let io_failure = || {
            std::sync::Arc::new(RuntimeInventoryError::ReadDir {
                path: fixture._dir.path().join("injected-read-failure"),
                source: std::io::Error::from(std::io::ErrorKind::Interrupted),
            })
        };
        // Inject the I/O outcome at the discovery boundary; do not put E0 in
        // environments, which would bypass production's unavailable remapping.
        if matches!(failure, Failure::Transient) {
            inventory.unavailable.push(UnavailableArtifact::Runtime {
                identity: initial.release().release.clone(),
                error: io_failure(),
            });
        }
        inventory.unavailable.push(UnavailableArtifact::Runtime {
            identity: firecracker("1.16.0"),
            error: io_failure(),
        });
        let policy = ExecutionPolicy::parse(Some("e0,synthetic"), None, &[e0, candidate]).unwrap();
        let selected = selection::select(
            &inventory,
            &policy,
            CompatibleSelection::ColdBoot { source: &source },
        );
        if matches!(failure, Failure::Transient) {
            assert!(
                matches!(selected, Err(SelectionError::Unavailable(error)) if error.is_transient())
            );
        } else {
            assert_eq!(
                selected.unwrap().1.execution.runtime().version.as_str(),
                "1.17.0"
            );
        }
    }
}
