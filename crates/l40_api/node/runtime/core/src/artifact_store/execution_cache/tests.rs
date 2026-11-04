use super::*;
use crate::artifact_store::test_support::*;
use std::fs::File;

#[test]
fn release_cache_locators_reuse_history_without_hiding_inspection_errors() {
    let fixture = Fixture::new();
    let parent = fixture._dir.path().join("cache");
    let release = pin("1.15.1", 1);
    let root = parent.join("managed/releases");
    let [compact, historical] = release_directory_names(&release.release, release.digest());
    let old = root.join(historical);
    std::fs::create_dir_all(&old).unwrap();
    assert_eq!(
        ArtifactStore::execution_release_path(&parent, &release).unwrap(),
        old
    );
    std::fs::create_dir(root.join(&compact)).unwrap();
    assert_eq!(
        ArtifactStore::execution_release_path(&parent, &release).unwrap(),
        root.join(compact)
    );
    std::fs::remove_dir_all(&root).unwrap();
    std::fs::write(&root, b"not a directory").unwrap();
    ArtifactStore::execution_release_path(&parent, &release).unwrap_err();
}

#[test]
fn sealed_cache_reuses_authenticated_inodes_without_its_source() {
    let Some(directory) = crate::tests::verity_test_directory() else {
        return;
    };
    let fixture = Fixture::in_directory(directory);
    let parent = fixture._dir.path().join("cache/firecracker");
    std::fs::create_dir_all(&parent).unwrap();
    let cache = fixture
        .store
        .prepare_execution_namespace(&parent, "test")
        .unwrap();
    let interrupted = cache.join(".incoming-interrupted");
    std::fs::create_dir(&interrupted).unwrap();
    std::fs::write(interrupted.join(KERNEL_IMAGE), b"partial").unwrap();
    let source = fixture._dir.path().join("source/image");
    std::fs::create_dir_all(source.join("bin")).unwrap();
    let bytes = b"SPACE immutable execution fixture\n";
    std::fs::write(source.join("bin/firecracker"), bytes).unwrap();
    std::fs::write(source.join("bin/jailer"), bytes).unwrap();
    let plan = ["bin/firecracker", "bin/jailer"].map(|path| SealedPlan {
        file: PlannedFile {
            path,
            sha256: sha256(bytes),
            size: Some(bytes.len() as u64),
            mode: 0o644,
        },
        expected: "sha256:01f42ea44ac4d8d5291c536b92a028d6dc7d957b264e7addf3d397bc45d055a3"
            .parse()
            .unwrap(),
    });
    let stage = || {
        fixture.store.stage_sealed_files(
            &cache,
            "image",
            SealedSource::Directory {
                directory: &source,
                protection: RootProtection::Store,
            },
            &plan,
            sha256(bytes),
        )
    };
    let path = stage().expect("privileged btrfs lane must support fs-verity");
    assert!(!interrupted.try_exists().unwrap());
    for name in ["bin/firecracker", "bin/jailer"] {
        std::fs::remove_file(path.join(name)).unwrap();
    }
    stage().unwrap();
    for name in ["bin/firecracker", "bin/jailer"] {
        assert_eq!(std::fs::read(path.join(name)).unwrap(), bytes);
    }
    let first = std::fs::metadata(path.join("bin/firecracker"))
        .unwrap()
        .ino();
    std::fs::remove_dir_all(&source).unwrap();
    let reused = stage().unwrap();
    assert_eq!(
        std::fs::metadata(reused.join("bin/firecracker"))
            .unwrap()
            .ino(),
        first
    );
    assert_eq!(
        std::fs::read(reused.join("bin/firecracker")).unwrap(),
        bytes
    );
    assert_eq!(
        File::options()
            .write(true)
            .open(reused.join("bin/firecracker"))
            .unwrap_err()
            .raw_os_error(),
        Some(libc::EPERM)
    );
    let replacement = path.join("bin/firecracker");
    for corrupt in [b"truncated".to_vec(), vec![b'x'; bytes.len()]] {
        std::fs::remove_file(&replacement).unwrap();
        std::fs::write(&replacement, corrupt).unwrap();
        let error = stage().unwrap_err();
        let unsealed = crate::verity::open_readonly(&replacement).unwrap();
        assert!(matches!(
            crate::verity::measure(&unsealed),
            Err(crate::verity::VerityError::Unsealed)
        ));
        assert!(matches!(
            error,
            StageExecutionError::Verify(
                VerifyError::Mismatch { .. } | VerifyError::InvalidFile { .. }
            )
        ));
        // Refusing corrupt mutable content must leave it repairable in place.
        std::fs::write(&replacement, bytes).unwrap();
        stage().unwrap();
        assert_eq!(crate::verity::measure(&unsealed).unwrap(), plan[0].expected);
    }
    // A wrong immutable measurement still fails closed; it is never downgraded.
    std::fs::remove_file(&replacement).unwrap();
    std::fs::write(&replacement, b"wrong sealed bytes").unwrap();
    let wrong = crate::verity::open_readonly(&replacement).unwrap();
    crate::verity::enable(&wrong).unwrap();
    assert!(matches!(
        stage().unwrap_err(),
        StageExecutionError::Verity {
            source: crate::verity::VerityError::Mismatch { .. },
            ..
        }
    ));
}

#[test]
fn baked_kernel_sources_and_empty_targets_recover_without_weakening_store_protection() {
    let Some(directory) = crate::tests::verity_test_directory() else {
        return;
    };
    let fixture = Fixture::in_directory(directory);
    let parent = fixture._dir.path().join("cache/firecracker");
    let source = parent.join("kernels/6.1.155");
    std::fs::create_dir_all(&source).unwrap();
    std::fs::set_permissions(&parent, std::fs::Permissions::from_mode(0o1775)).unwrap();
    let cache = fixture
        .store
        .prepare_execution_namespace(&parent, "kernels")
        .unwrap();
    let target = cache.join("image");
    std::fs::create_dir(&target).unwrap();
    let bytes = b"SPACE immutable execution fixture\n";
    std::fs::write(source.join(KERNEL_IMAGE), bytes).unwrap();
    let plans = [SealedPlan {
        file: PlannedFile {
            path: KERNEL_IMAGE,
            sha256: sha256(bytes),
            size: Some(bytes.len() as u64),
            mode: 0o644,
        },
        expected: "sha256:01f42ea44ac4d8d5291c536b92a028d6dc7d957b264e7addf3d397bc45d055a3"
            .parse()
            .unwrap(),
    }];
    let stage = |source| {
        fixture.store.stage_sealed_files(
            &cache,
            "image",
            SealedSource::Kernel(source),
            &plans,
            sha256(bytes),
        )
    };
    let image = source.join(KERNEL_IMAGE);
    stage(KernelSource::ExecutionCache(&image))
        .expect("privileged btrfs lane must support fs-verity");
    assert_eq!(std::fs::read(target.join(KERNEL_IMAGE)).unwrap(), bytes);
    // A complete authenticated cache needs neither the source nor its layout.
    let unused = source.join("unused-image-name");
    stage(KernelSource::ExecutionCache(&unused)).unwrap();
    // A lost kernel with its directory retained is republished on rediscovery.
    std::fs::remove_file(target.join(KERNEL_IMAGE)).unwrap();
    assert!(matches!(
        stage(KernelSource::ExecutionCache(&unused)),
        Err(StageExecutionError::Verify(
            VerifyError::KernelImageName { .. }
        ))
    ));
    stage(KernelSource::ExecutionCache(&image)).unwrap();
    assert_eq!(std::fs::read(target.join(KERNEL_IMAGE)).unwrap(), bytes);
    std::fs::remove_dir_all(&target).unwrap();
    assert!(matches!(
        stage(KernelSource::Store(&image)),
        Err(StageExecutionError::Publish(
            PublishError::UnsafeRoot { .. }
        ))
    ));
}

#[test]
fn protected_cache_rejects_unsafe_ancestors_above_the_local_layout() {
    let fixture = Fixture::new();
    let ancestor = fixture._dir.path().join("unsafe");
    let cache = ancestor.join("deep/cache/managed/kernels");
    let target = cache.join("image");
    std::fs::create_dir_all(&target).unwrap();
    std::fs::set_permissions(&ancestor, std::fs::Permissions::from_mode(0o777)).unwrap();
    std::fs::write(target.join(KERNEL_IMAGE), b"unsealed").unwrap();
    fixture
        .store
        .verity_support
        .lock()
        .unwrap()
        .insert(std::fs::metadata(&cache).unwrap().dev(), true);
    let result = fixture.store.stage_sealed_files(
        &cache,
        "image",
        SealedSource::Directory {
            directory: &fixture.kernels,
            protection: RootProtection::Store,
        },
        &[SealedPlan {
            file: PlannedFile {
                path: KERNEL_IMAGE,
                sha256: sha256(b"unsealed"),
                size: None,
                mode: 0o644,
            },
            expected: "sha256:01f42ea44ac4d8d5291c536b92a028d6dc7d957b264e7addf3d397bc45d055a3"
                .parse()
                .unwrap(),
        }],
        sha256(b"unsealed"),
    );
    assert!(
        matches!(result, Err(StageExecutionError::Verify(VerifyError::UnsafeRoot { path, .. })) if path == ancestor)
    );
}

#[test]
fn empty_release_skeleton_is_reclaimed_without_touching_other_entries() {
    enum Contents {
        Empty,
        File,
        Link,
        Unexpected,
    }
    for contents in [
        Contents::Empty,
        Contents::File,
        Contents::Link,
        Contents::Unexpected,
    ] {
        let fixture = Fixture::new();
        let target = fixture._dir.path().join("cache/releases/image");
        let bin = target.join("bin");
        std::fs::create_dir_all(&bin).unwrap();
        let outside = fixture._dir.path().join("outside");
        std::fs::write(&outside, b"retained").unwrap();
        match contents {
            Contents::Empty => {}
            Contents::File => std::fs::write(bin.join("firecracker"), b"retained").unwrap(),
            Contents::Link => {
                std::fs::remove_dir(&bin).unwrap();
                std::os::unix::fs::symlink(&outside, &bin).unwrap();
            }
            Contents::Unexpected => std::fs::create_dir(target.join("unrecognized")).unwrap(),
        }
        let plans = ["bin/firecracker", "bin/jailer"].map(|path| PlannedFile {
            path,
            sha256: sha256(b"retained"),
            size: None,
            mode: 0o755,
        });
        reclaim_empty_execution_target(&target, &plans, fixture.store.owner).unwrap();
        assert_eq!(target.exists(), !matches!(contents, Contents::Empty));
        assert_eq!(std::fs::read(&outside).unwrap(), b"retained");
        if matches!(contents, Contents::File) {
            assert_eq!(std::fs::read(bin.join("firecracker")).unwrap(), b"retained");
        }
    }
}

#[test]
fn unsupported_filesystem_does_not_copy_execution_sources() {
    let fixture = Fixture::new();
    let parent = fixture._dir.path().join("cache/firecracker");
    std::fs::create_dir_all(&parent).unwrap();
    let cache = fixture
        .store
        .prepare_execution_namespace(&parent, "test")
        .unwrap();
    fixture
        .store
        .verity_support
        .lock()
        .unwrap()
        .insert(std::fs::metadata(&cache).unwrap().dev(), false);
    let bytes = b"not copied";
    let source = fixture.kernels.join("source");
    std::fs::create_dir(&source).unwrap();
    std::fs::write(source.join(KERNEL_IMAGE), bytes).unwrap();
    let error = fixture
        .store
        .stage_sealed_files(
            &cache,
            "image",
            SealedSource::Directory {
                directory: &source,
                protection: RootProtection::Store,
            },
            &[SealedPlan {
                file: PlannedFile {
                    path: KERNEL_IMAGE,
                    sha256: sha256(bytes),
                    size: None,
                    mode: 0o644,
                },
                expected: crate::verity::VerityDigest::new(sha256(bytes)),
            }],
            sha256(bytes),
        )
        .unwrap_err();
    assert!(error.is_unsupported());
    assert!(!cache.join("image").exists());
    assert_eq!(std::fs::read(source.join(KERNEL_IMAGE)).unwrap(), bytes);
}

#[test]
fn absent_sources_skip_probing_and_supported_sources_require_protection() {
    enum Source {
        Missing,
        Unsafe,
    }
    for source_state in [Source::Missing, Source::Unsafe] {
        for kernel in [false, true] {
            let fixture = Fixture::new();
            let parent = fixture._dir.path().join("cache/firecracker");
            std::fs::create_dir_all(&parent).unwrap();
            let source = fixture.releases.join("unavailable");
            if matches!(source_state, Source::Unsafe) {
                std::fs::create_dir(&source).unwrap();
                std::fs::set_permissions(&source, std::fs::Permissions::from_mode(0o777)).unwrap();
                fixture
                    .store
                    .verity_support
                    .lock()
                    .unwrap()
                    .insert(std::fs::metadata(&parent).unwrap().dev(), true);
            }
            let initial = crate::catalog::InitialEnvironment::for_architecture(
                pplx_space_common_core::types::Arch::X86_64,
            )
            .unwrap();
            let error = if kernel {
                fixture.store.stage_sealed_kernel_at(
                    initial.kernel(),
                    KernelSource::Store(&source.join(KERNEL_IMAGE)),
                    &parent,
                )
            } else {
                fixture
                    .store
                    .stage_release_at(initial.release(), &source, &parent)
            }
            .unwrap_err();
            assert!(
                !error.is_unsupported(),
                "missing or unsafe supported sources must not authorize fallback"
            );
            assert!(!error.is_transient());
            assert!(!parent.join("managed").try_exists().unwrap());
            assert_eq!(
                fixture.store.verity_support.lock().unwrap().len(),
                usize::from(matches!(source_state, Source::Unsafe))
            );
        }
    }
}

#[test]
fn unsupported_backend_does_not_apply_sealed_layout_requirements() {
    enum Layout {
        ExtraSourceAncestor,
        CachePermissions,
        MissingCache,
        LinkedCache,
    }
    for layout in [
        Layout::ExtraSourceAncestor,
        Layout::CachePermissions,
        Layout::MissingCache,
        Layout::LinkedCache,
    ] {
        let fixture = Fixture::new();
        let parent = fixture._dir.path().join("cache/firecracker");
        std::fs::create_dir_all(&parent).unwrap();
        let release = fixture._dir.path().join("vendor/runtime/v1.15.1");
        std::fs::create_dir_all(release.join("bin")).unwrap();
        let pin = pin("1.15.1", 1);
        for binary in &pin.binaries {
            std::fs::copy(
                fixture.release_path("1.15.1", 1).join(binary.path),
                release.join(binary.path),
            )
            .unwrap();
        }
        let image = fixture
            ._dir
            .path()
            .join("assets/version/kernel/vmlinux.bin");
        std::fs::create_dir_all(image.parent().unwrap()).unwrap();
        std::fs::write(&image, binary(42)).unwrap();
        let initial = crate::catalog::InitialEnvironment::for_architecture(
            pplx_space_common_core::types::Arch::X86_64,
        )
        .unwrap();
        fixture
            .store
            .verity_support
            .lock()
            .unwrap()
            .insert(std::fs::metadata(&parent).unwrap().dev(), false);
        match layout {
            Layout::ExtraSourceAncestor => std::fs::set_permissions(
                release.parent().unwrap().parent().unwrap(),
                std::fs::Permissions::from_mode(0o777),
            )
            .unwrap(),
            Layout::CachePermissions => {
                std::fs::set_permissions(&parent, std::fs::Permissions::from_mode(0o777)).unwrap()
            }
            Layout::MissingCache => std::fs::remove_dir_all(parent.parent().unwrap()).unwrap(),
            Layout::LinkedCache => {
                let destination = fixture._dir.path().join("other-cache");
                std::fs::rename(&parent, &destination).unwrap();
                std::os::unix::fs::symlink(destination, &parent).unwrap();
            }
        }
        fixture.store.verify_release_at(&pin, &release).unwrap();
        let mut kernel = initial.kernel().clone();
        kernel.sha256 = sha256(&binary(42));
        kernel.size = binary(42).len() as u64;
        fixture.store.verify_kernel_at(&kernel, &image).unwrap();
        for error in [
            fixture
                .store
                .stage_release_at(initial.release(), &release, &parent)
                .unwrap_err(),
            fixture
                .store
                .stage_sealed_kernel_at(
                    initial.kernel(),
                    KernelSource::ExecutionCache(&image),
                    &parent,
                )
                .unwrap_err(),
        ] {
            assert!(
                error.is_unsupported(),
                "legacy-valid source must reach unsupported fallback: {error}"
            );
        }
        assert!(!parent.join("managed").exists());
    }
}

#[test]
fn artifact_refusals_do_not_authorize_legacy_fallback() {
    use crate::verity::VerityError;
    for source in [
        VerityError::Unsupported,
        VerityError::Unsealed,
        VerityError::Io {
            operation: "enable",
            source: std::io::Error::from_raw_os_error(libc::EINVAL),
        },
        VerityError::Io {
            operation: "enable",
            source: std::io::Error::from_raw_os_error(libc::EPERM),
        },
        VerityError::Io {
            operation: "measure",
            source: std::io::Error::from_raw_os_error(libc::EIO),
        },
    ] {
        let error = StageExecutionError::Verity {
            path: PathBuf::from("/synthetic/cache/image"),
            source,
        };
        assert!(
            !error.is_unsupported(),
            "artifact failure must not be mistaken for unavailable filesystem capability: {error}"
        );
    }
}

#[test]
fn kernel_source_name_never_substitutes_a_different_file() {
    for supported in [false, true] {
        let fixture = Fixture::new();
        let parent = fixture._dir.path().join("cache/firecracker");
        let image = fixture._dir.path().join("assets/kernel/image.bin");
        std::fs::create_dir_all(&parent).unwrap();
        std::fs::create_dir_all(image.parent().unwrap()).unwrap();
        let canonical = image.with_file_name(KERNEL_IMAGE);
        std::fs::write(&canonical, binary(42)).unwrap();
        std::fs::write(&image, b"not the pinned kernel").unwrap();
        let mut pin = crate::catalog::InitialEnvironment::for_architecture(
            pplx_space_common_core::types::Arch::X86_64,
        )
        .unwrap()
        .kernel()
        .clone();
        pin.sha256 = sha256(&binary(42));
        pin.size = binary(42).len() as u64;
        // The valid sibling must never authenticate a different input path.
        fixture.store.verify_kernel_at(&pin, &canonical).unwrap();
        assert!(matches!(
            fixture.store.verify_kernel_at(&pin, &image),
            Err(VerifyError::KernelImageName { path }) if path == image
        ));
        fixture
            .store
            .verity_support
            .lock()
            .unwrap()
            .insert(std::fs::metadata(&parent).unwrap().dev(), supported);
        let error = fixture
            .store
            .stage_sealed_kernel_at(&pin, KernelSource::ExecutionCache(&image), &parent)
            .unwrap_err();
        assert!(
            matches!(
                error,
                StageExecutionError::Verify(VerifyError::KernelImageName { ref path }) if *path == image
            ),
            "{error}"
        );
        assert!(!parent.join("managed").exists());
        assert_eq!(std::fs::read(&image).unwrap(), b"not the pinned kernel");
    }
}

#[test]
fn cached_kernel_validation_precedes_unused_source_validation() {
    for empty in [false, true] {
        let fixture = Fixture::new();
        let initial = crate::catalog::InitialEnvironment::for_architecture(
            pplx_space_common_core::types::Arch::X86_64,
        )
        .unwrap();
        let pin = initial.kernel();
        let parent = fixture._dir.path().join("cache/firecracker");
        let cached = ArtifactStore::execution_kernel_path(&parent, pin).unwrap();
        let target = cached.parent().unwrap();
        std::fs::create_dir_all(target).unwrap();
        if !empty {
            std::fs::write(&cached, b"must not be accepted").unwrap();
            std::fs::set_permissions(target, std::fs::Permissions::from_mode(0o777)).unwrap();
        }
        fixture
            .store
            .verity_support
            .lock()
            .unwrap()
            .insert(std::fs::metadata(&parent).unwrap().dev(), true);
        let source = fixture._dir.path().join("missing/noncanonical-image");
        let error = fixture
            .store
            .stage_sealed_kernel_at(pin, KernelSource::ExecutionCache(&source), &parent)
            .unwrap_err();
        if empty {
            assert!(
                matches!(error, StageExecutionError::Verify(VerifyError::KernelImageName { path }) if path == source)
            );
            assert!(target.exists(), "invalid sources must not mutate the cache");
        } else {
            assert!(
                matches!(error, StageExecutionError::Publish(PublishError::UnsafeRoot { path, .. }) if path == target)
            );
            assert_eq!(std::fs::read(&cached).unwrap(), b"must not be accepted");
        }
    }
}

#[test]
fn catalog_fallback_copies_authenticated_inodes_only_to_unsupported_filesystems() {
    let fixture = Fixture::new();
    let parent = fixture._dir.path().join("cache/firecracker");
    std::fs::create_dir_all(&parent).unwrap();
    let device = std::fs::metadata(&parent).unwrap().dev();
    let pin = pin("1.15.1", 1);
    let source = fixture.release_path("1.15.1", 1);
    fixture
        .store
        .verity_support
        .lock()
        .unwrap()
        .insert(device, true);
    assert!(matches!(
        fixture
            .store
            .stage_catalog_release_at(&pin, &source, &parent),
        Err(StageExecutionError::FallbackOnCapableBackend(_))
    ));
    fixture
        .store
        .verity_support
        .lock()
        .unwrap()
        .insert(device, false);
    let target = fixture
        .store
        .stage_catalog_release_at(&pin, &source, &parent)
        .unwrap();
    assert!(target.starts_with(&parent));
    assert_ne!(
        std::fs::metadata(target.join("bin/firecracker"))
            .unwrap()
            .ino(),
        std::fs::metadata(source.join("bin/firecracker"))
            .unwrap()
            .ino()
    );
    std::fs::write(source.join("bin/firecracker"), b"changed source").unwrap();
    assert_eq!(
        std::fs::read(target.join("bin/firecracker")).unwrap(),
        binary(1)
    );
    // Retry verifies the published bytes; a damaged destination is never adopted.
    std::fs::write(target.join("bin/firecracker"), b"damaged cache").unwrap();
    fixture
        .store
        .stage_catalog_release_at(&pin, &source, &parent)
        .unwrap_err();
}
