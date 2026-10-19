use super::*;
use crate::artifact_store::test_support::*;
use std::fs::File;

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
            SealedSource {
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
    std::fs::remove_file(reused.join("bin/firecracker")).unwrap();
    std::fs::write(reused.join("bin/firecracker"), b"untrusted replacement").unwrap();
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
    let stage = |protection| {
        fixture.store.stage_sealed_files(
            &cache,
            "image",
            SealedSource {
                directory: &source,
                protection,
            },
            &plans,
            sha256(bytes),
        )
    };
    stage(RootProtection::ExecutionCache).expect("privileged btrfs lane must support fs-verity");
    assert_eq!(std::fs::read(target.join(KERNEL_IMAGE)).unwrap(), bytes);
    // A lost kernel with its directory retained is republished on rediscovery.
    std::fs::remove_file(target.join(KERNEL_IMAGE)).unwrap();
    stage(RootProtection::ExecutionCache).unwrap();
    assert_eq!(std::fs::read(target.join(KERNEL_IMAGE)).unwrap(), bytes);
    std::fs::remove_dir_all(&target).unwrap();
    assert!(matches!(
        stage(RootProtection::Store),
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
    let result = fixture.store.stage_sealed_files(
        &cache,
        "image",
        SealedSource {
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
        let plans = ["bin/firecracker", "bin/jailer"].map(|path| SealedPlan {
            file: PlannedFile {
                path,
                sha256: sha256(b"retained"),
                size: None,
                mode: 0o755,
            },
            expected: crate::verity::VerityDigest::new(sha256(b"retained")),
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
    let source = fixture._dir.path().join("source");
    std::fs::create_dir(&source).unwrap();
    std::fs::write(source.join(KERNEL_IMAGE), bytes).unwrap();
    let error = fixture
        .store
        .stage_sealed_files(
            &cache,
            "image",
            SealedSource {
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
fn unavailable_sources_do_not_prepare_or_probe_execution_cache() {
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
                "source refusal must precede probing"
            );
            assert!(!error.is_transient());
            assert!(!parent.join("managed").try_exists().unwrap());
            assert!(fixture.store.verity_support.lock().unwrap().is_empty());
        }
    }
}
