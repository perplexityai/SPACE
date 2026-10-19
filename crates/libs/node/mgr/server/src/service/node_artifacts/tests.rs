use super::*;
use crate::proto::{ComponentArtifact, RuntimeReleaseArtifact};

fn release(runtime_kind: i32, version: &str) -> NodeArtifact {
    NodeArtifact {
        artifact: Some(node_artifact::Artifact::RuntimeRelease(
            RuntimeReleaseArtifact {
                runtime_kind,
                version: version.to_string(),
                digest: pplx_space_common_core::types::Digest::sha256([7; 32]).to_string(),
            },
        )),
    }
}

#[test]
fn an_artifact_is_named_by_what_it_is() {
    let firecracker = pplx_space_execution_common::proto::RuntimeKind::Firecracker as i32;
    assert!(matches!(
        Artifact::parse(Some(&release(firecracker, "1.16.2"))),
        Ok(Artifact::Release(release, _)) if release.version.as_str() == "1.16.2"
    ));
    let kernel = NodeArtifact {
        artifact: Some(node_artifact::Artifact::Component(ComponentArtifact {
            version: "6.1.158".to_string(),
            kind: pplx_space_execution_common::proto::ArtifactComponentKind::Kernel as i32,
            digest: pplx_space_common_core::types::Digest::sha256([7; 32]).to_string(),
        })),
    };
    assert!(matches!(
        Artifact::parse(Some(&kernel)),
        Ok(Artifact::Kernel(version, _)) if version.as_str() == "6.1.158"
    ));
    for invalid in [
        None,
        Some(NodeArtifact { artifact: None }),
        Some(release(0, "1.16.2")),
        Some(release(firecracker, "")),
    ] {
        let status = Artifact::parse(invalid.as_ref()).unwrap_err();
        assert_eq!(status.code(), tonic::Code::InvalidArgument, "{invalid:?}");
    }
}

/// Bytes other than pinned are the caller's fault and final; a store that
/// cannot be trusted or holds something else needs an operator; only I/O
/// is worth retrying.
#[test]
fn publication_failures_map_to_what_a_caller_can_do() {
    let path = std::path::PathBuf::from("/staged");
    let digest = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        .parse()
        .unwrap();
    let cases = [
        (
            PublishError::Mismatch {
                path: path.clone(),
                found: digest,
                pinned: digest,
            },
            tonic::Code::InvalidArgument,
        ),
        (
            PublishError::ExistingArtifact(
                pplx_space_node_runtime_core::artifact_store::VerifyError::InvalidFile {
                    path: path.clone(),
                    problem: "size differs from pin",
                },
            ),
            tonic::Code::FailedPrecondition,
        ),
        (
            PublishError::ExistingArtifact(
                pplx_space_node_runtime_core::artifact_store::VerifyError::Io {
                    action: "read",
                    path: path.clone(),
                    source: std::io::ErrorKind::Other.into(),
                },
            ),
            tonic::Code::Internal,
        ),
        (
            PublishError::Io {
                action: "write",
                path,
                source: std::io::ErrorKind::Other.into(),
            },
            tonic::Code::Internal,
        ),
    ];
    for (error, code) in cases {
        assert_eq!(publish_status(error).code(), code);
    }
    assert_eq!(
        remove_status(RemoveError::IdentityChanged {
            path: "/managed/v1.15.1".into()
        })
        .code(),
        tonic::Code::FailedPrecondition
    );
}

/// While one store change runs, another is answered at once with a status
/// the runtime manager retries, rather than queued on a blocking thread.
#[tokio::test]
async fn a_second_store_change_is_turned_away_while_one_runs() {
    let service = crate::service::tests::service_tests::service_for_unit_tests().await;
    let running = service.artifact_slot().unwrap();
    let status = service.artifact_slot().unwrap_err();
    assert_eq!(status.code(), tonic::Code::ResourceExhausted);
    drop(running);
    let _again = service.artifact_slot().unwrap();
}

#[test]
fn execution_roots_must_not_alias_the_managed_store() {
    let dir = tempfile::tempdir().unwrap();
    let mut paths = crate::config::RuntimePaths {
        vendor: dir.path().join("execution/releases"),
        assets: dir.path().join("execution/assets"),
        kernels: dir.path().join("execution/kernels"),
        store: crate::config::ArtifactStorePaths {
            releases: dir.path().join("managed/releases"),
            kernels: dir.path().join("managed/kernels"),
            staging: dir.path().join("staging"),
            catalog: dir.path().join("catalog"),
        },
    };
    std::fs::create_dir_all(&paths.store.releases).unwrap();
    std::fs::create_dir_all(paths.vendor.parent().unwrap()).unwrap();
    separate_store(&paths).unwrap();
    std::os::unix::fs::symlink(&paths.store.releases, &paths.vendor).unwrap();
    assert!(separate_store(&paths).is_err());
    std::fs::remove_file(&paths.vendor).unwrap();
    paths.kernels = paths.store.kernels.clone();
    assert!(separate_store(&paths).is_err());
}

#[tokio::test]
async fn compiled_environment_removal_is_refused_even_without_inventory() {
    use crate::service::tests::service_tests::{root_request, service_for_unit_tests};
    let initial = InitialEnvironment::for_architecture(host_arch().unwrap()).unwrap();
    let runtime = NodeArtifact {
        artifact: Some(node_artifact::Artifact::RuntimeRelease(
            RuntimeReleaseArtifact {
                runtime_kind: pplx_space_execution_common::proto::RuntimeKind::Firecracker as i32,
                version: initial.release().release.version.to_string(),
                digest: initial.release().digest().to_string(),
            },
        )),
    };
    let kernel = NodeArtifact {
        artifact: Some(node_artifact::Artifact::Component(ComponentArtifact {
            kind: pplx_space_execution_common::proto::ArtifactComponentKind::Kernel as i32,
            version: initial.kernel().version.to_string(),
            digest: initial.kernel().sha256.to_string(),
        })),
    };
    // Independent service instances represent restart; neither has a discovered
    // inventory or any artifact files. The binding still protects removal.
    for artifact in [runtime, kernel] {
        let service = service_for_unit_tests().await;
        let status = service
            .remove_artifact(root_request(RemoveArtifactRequest {
                artifact: Some(artifact),
            }))
            .await
            .unwrap_err();
        assert_eq!(status.code(), tonic::Code::FailedPrecondition);
        assert!(status.message().contains("compiled E0"));
    }
    let candidate = RuntimeCatalog::compiled()
        .unwrap()
        .releases(
            pplx_space_common_core::types::RuntimeKind::Firecracker,
            host_arch().unwrap(),
        )
        .find(|pin| pin.release.version.as_str() == "1.17.0")
        .unwrap()
        .clone();
    let result =
        Artifact::Release(candidate.release.clone(), candidate.digest()).check_removal(&initial);
    if host_arch().unwrap() == Arch::X86_64 {
        result.unwrap_err();
    } else {
        result.unwrap();
    }
}

#[test]
fn prototype_overlap_keeps_external_execution_but_refuses_mutations() {
    use crate::service::tests::service_tests::installed_runtime;
    use pplx_space_node_runtime_core::runtime_inventory::ExecutionSelectionMode;
    let (_fixture, mut paths) = installed_runtime();
    paths.store.releases = paths.vendor.clone();
    assert!(separate_store(&paths).is_err());
    let inventory = execution_inventory(&paths, true).unwrap();
    let selected = inventory
        .resolve(
            None,
            pplx_space_common_core::types::RuntimeKind::Firecracker,
            ExecutionSelectionMode::NewColdBoot,
        )
        .unwrap();
    assert_eq!(selected.execution.runtime().version.as_str(), "1.16.0");
}

#[tokio::test]
async fn publication_preserves_available_inventory_until_replacement_is_ready() {
    use crate::service::tests::service_tests::{installed_runtime, service_for_unit_tests};
    use pplx_space_common_core::types::RuntimeKind;
    use pplx_space_node_runtime_core::runtime_inventory::{
        RuntimeInstallationRoot, RuntimeInventory,
    };
    use std::sync::Arc;
    use std::time::Duration;
    let initial = InitialEnvironment::for_architecture(host_arch().unwrap()).unwrap();
    let candidate = RuntimeCatalog::compiled()
        .unwrap()
        .releases(RuntimeKind::Firecracker, host_arch().unwrap())
        .find(|pin| pin.release.version.as_str() == "1.17.0")
        .unwrap()
        .clone();
    for pin in [initial.release().clone(), candidate] {
        let is_candidate = pin.release.version.as_str() == "1.17.0";
        let (_fixture, paths) = installed_runtime();
        let mut service = service_for_unit_tests().await;
        service.runtime_paths = paths;
        let inventory = Arc::new(
            RuntimeInventory::discover(
                [RuntimeInstallationRoot::new(
                    RuntimeKind::Firecracker,
                    &service.runtime_paths.vendor,
                )],
                &service.runtime_paths.assets,
                &service.runtime_paths.kernels,
            )
            .unwrap(),
        );
        service.runtime_inventory.store(Some(inventory.clone()));
        let (started, entered) = tokio::sync::oneshot::channel();
        let (finish, release) = std::sync::mpsc::channel();
        let publisher = service.clone();
        let task = tokio::spawn(async move {
            publisher
                .publish_artifact_work(
                    Artifact::Release(pin.release.clone(), pin.digest()),
                    move || {
                        started.send(()).unwrap();
                        release.recv_timeout(Duration::from_secs(5)).unwrap();
                        Ok(Publication {
                            outcome: PublishOutcome::Published,
                            path: "synthetic-publication".into(),
                            digest: pin.digest(),
                        })
                    },
                )
                .await
        });
        entered.await.unwrap();
        let concurrent =
            tokio::time::timeout(Duration::from_secs(1), service.runtime_inventory()).await;
        finish.send(()).unwrap();
        task.await.unwrap().unwrap();
        assert!(Arc::ptr_eq(
            &concurrent
                .expect("publication must not stall usable execution")
                .unwrap(),
            &inventory
        ));
        let after = service.runtime_inventory.load_full().unwrap();
        assert_eq!(
            Arc::ptr_eq(&after, &inventory),
            is_candidate,
            "only an E0 publication replaces executable inventory"
        );
    }
}

#[tokio::test]
async fn environment_resolution_requires_authorization_and_verified_inventory() {
    use crate::service::tests::service_tests::{root_request, service_for_unit_tests};
    let service = service_for_unit_tests().await;
    let request = || crate::proto::ResolveExecutionEnvironmentRequest { reference: None };
    let error = service
        .resolve_execution_environment(Request::new(request()))
        .await
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::Unauthenticated);
    // No installed E0 is never reported as a usable binding, even for root.
    let error = service
        .resolve_execution_environment(root_request(request()))
        .await
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::FailedPrecondition);
}

#[test]
fn production_admission_restricts_selection_without_changing_defaults() {
    use pplx_space_common_core::types::{Digest, EnvironmentReference};
    use pplx_space_node_runtime_core::catalog::CompiledEnvironment;
    let initial = InitialEnvironment::for_architecture(Arch::X86_64).unwrap();
    let environments = CompiledEnvironment::for_architecture(Arch::X86_64).unwrap();
    let candidate = &environments[1];
    let e0 = initial.definition().unwrap().reference();
    let c = candidate.definition().reference();
    let same_kernel = environments[2].definition().reference();
    for production in [false, true] {
        for (reference, mode, accepted) in [
            (None, ExecutionSelectionMode::NewColdBoot, true),
            (Some(e0), ExecutionSelectionMode::NewColdBoot, true),
            (Some(c), ExecutionSelectionMode::NewColdBoot, !production),
            (
                Some(same_kernel),
                ExecutionSelectionMode::NewColdBoot,
                !production,
            ),
            (
                Some(same_kernel),
                ExecutionSelectionMode::ExistingColdBoot,
                !production,
            ),
            (
                Some(same_kernel),
                ExecutionSelectionMode::PersistedStateRestore,
                !production,
            ),
            (
                Some(c),
                ExecutionSelectionMode::ExistingColdBoot,
                !production,
            ),
            (
                Some(c),
                ExecutionSelectionMode::PersistedStateRestore,
                !production,
            ),
            (
                Some(EnvironmentReference::new(Digest::sha256([1; 32]))),
                ExecutionSelectionMode::NewColdBoot,
                false,
            ),
        ] {
            let result =
                select_definition(&initial, || Ok(&environments), production, reference, mode);
            assert_eq!(
                result.is_ok(),
                accepted,
                "{production} {reference:?} {mode:?}"
            );
            if let Ok(definition) = result {
                assert_eq!(definition.reference(), reference.unwrap_or(e0));
            }
        }
    }
}

#[test]
fn candidate_catalog_failure_does_not_change_initial_selection() {
    let initial = InitialEnvironment::for_architecture(Arch::X86_64).unwrap();
    let e0 = initial.definition().unwrap().reference();
    for reference in [None, Some(e0)] {
        let selected = select_definition(
            &initial,
            || Err(Status::failed_precondition("candidate pins missing")),
            true,
            reference,
            ExecutionSelectionMode::NewColdBoot,
        )
        .unwrap();
        assert_eq!(selected.reference(), e0);
    }
    select_definition(
        &initial,
        || Err(Status::failed_precondition("candidate pins missing")),
        true,
        Some(pplx_space_common_core::types::EnvironmentReference::new(
            pplx_space_common_core::types::Digest::sha256([8; 32]),
        )),
        ExecutionSelectionMode::NewColdBoot,
    )
    .unwrap_err();
}

#[tokio::test]
async fn legacy_custom_kernels_survive_managed_runtime_migration() {
    use crate::service::tests::service_tests::{installed_runtime, service_for_unit_tests};
    use pplx_space_common_core::types::{Digest, ExecutionMetadata, RuntimeKind};
    use pplx_space_node_runtime_core::runtime_inventory::{
        RuntimeInstallationRoot, RuntimeInventory,
    };
    use std::os::unix::fs::symlink;
    use std::sync::Arc;

    for managed in [true, false] {
        let (_fixture, paths) = installed_runtime();
        let runtime_root = if managed {
            &paths.store.releases
        } else {
            &paths.vendor
        };
        std::fs::create_dir_all(runtime_root).unwrap();
        std::fs::rename(paths.vendor.join("v1.16.0"), runtime_root.join("v1.15.1")).unwrap();
        std::fs::remove_file(paths.vendor.join("default")).unwrap();
        symlink("v1.15.1", runtime_root.join("default")).unwrap();
        // Inject a discovered snapshot to isolate service admission. Core
        // store tests cover byte verification.
        for (root, version) in [(&paths.vendor, "1.16.0"), (&paths.store.releases, "1.17.0")] {
            let bin = root.join(format!("v{version}/bin"));
            std::fs::create_dir_all(&bin).unwrap();
            for name in ["firecracker", "jailer"] {
                std::fs::copy(runtime_root.join("v1.15.1/bin").join(name), bin.join(name)).unwrap();
            }
        }
        let inventory = Arc::new(
            RuntimeInventory::discover(
                [
                    RuntimeInstallationRoot::new(RuntimeKind::Firecracker, &paths.vendor),
                    RuntimeInstallationRoot::new(RuntimeKind::Firecracker, &paths.store.releases),
                ],
                &paths.assets,
                &paths.kernels,
            )
            .unwrap(),
        );
        let legacy = inventory
            .resolve(
                None,
                RuntimeKind::Firecracker,
                ExecutionSelectionMode::NewColdBoot,
            )
            .unwrap()
            .execution;
        assert_eq!(legacy.runtime().version.as_str(), "1.15.1");
        assert_ne!(
            legacy.components()[0].digest,
            Some(initial_environment().unwrap().kernel().sha256)
        );
        let mut service = service_for_unit_tests().await;
        service.runtime_paths = paths;
        service.runtime_inventory.store(Some(inventory));
        for mode in [
            ExecutionSelectionMode::NewColdBoot,
            ExecutionSelectionMode::ExistingColdBoot,
            ExecutionSelectionMode::PersistedStateRestore,
        ] {
            let resolved = service
                .resolve_bound_execution(Some(&legacy), RuntimeKind::Firecracker, mode)
                .await
                .unwrap_or_else(|error| panic!("managed={managed} {mode:?}: {error}"));
            assert_eq!(resolved.execution, legacy);
        }
        // A restore retains its recorded boot-kernel evidence even when those
        // bytes are absent; only the compatible snapshot loader is required.
        let mut absent_components = legacy.components().to_vec();
        absent_components[0].digest = Some(Digest::sha256([99; 32]));
        let absent = ExecutionMetadata::new(
            legacy.runtime().clone(),
            legacy.guest(),
            legacy.state_format().cloned(),
            absent_components,
        )
        .unwrap();
        let restored = service
            .resolve_bound_execution(
                Some(&absent),
                RuntimeKind::Firecracker,
                ExecutionSelectionMode::PersistedStateRestore,
            )
            .await
            .unwrap();
        assert_eq!(restored.execution.components(), absent.components());
        service
            .resolve_bound_execution(
                Some(&absent),
                RuntimeKind::Firecracker,
                ExecutionSelectionMode::ExistingColdBoot,
            )
            .await
            .unwrap_err();

        for version in ["1.16.0", "1.17.0"] {
            let requested = ExecutionMetadata::new(
                pplx_space_common_core::types::RuntimeIdentity {
                    kind: RuntimeKind::Firecracker,
                    version: version.parse().unwrap(),
                },
                legacy.guest(),
                Some(pplx_space_common_core::types::StateFormatIdentity {
                    format: "firecracker-snapshot".parse().unwrap(),
                    version: if version == "1.16.0" {
                        "10.0.0"
                    } else {
                        "12.0.0"
                    }
                    .parse()
                    .unwrap(),
                }),
                legacy.components().to_vec(),
            )
            .unwrap();
            for mode in [
                ExecutionSelectionMode::NewColdBoot,
                ExecutionSelectionMode::ExistingColdBoot,
                ExecutionSelectionMode::PersistedStateRestore,
            ] {
                let result = service
                    .resolve_bound_execution(Some(&requested), RuntimeKind::Firecracker, mode)
                    .await;
                if version == "1.16.0" {
                    result.unwrap();
                } else {
                    assert_eq!(result.unwrap_err().code(), tonic::Code::FailedPrecondition);
                }
            }
        }
    }
}

#[cfg(target_arch = "x86_64")]
#[tokio::test]
async fn legacy_runtime_placement_is_not_hidden_by_a_compiled_environment() {
    use crate::service::tests::service_tests::{
        installed_runtime, root_request, service_for_unit_tests,
    };
    use pplx_space_common_core::types::RuntimeKind;
    use pplx_space_node_runtime_core::runtime_inventory::{
        RuntimeInstallationRoot, RuntimeInventory,
    };
    use std::sync::Arc;
    let (_fixture, paths) = installed_runtime();
    std::fs::rename(paths.vendor.join("v1.16.0"), paths.vendor.join("v1.17.0")).unwrap();
    std::fs::remove_file(paths.vendor.join("default")).unwrap();
    std::os::unix::fs::symlink("v1.17.0", paths.vendor.join("default")).unwrap();
    let inventory = RuntimeInventory::discover(
        [RuntimeInstallationRoot::new(
            RuntimeKind::Firecracker,
            &paths.vendor,
        )],
        &paths.assets,
        &paths.kernels,
    )
    .unwrap();
    let mut service = service_for_unit_tests().await;
    service.production = false;
    service.runtime_paths = paths;
    service.runtime_inventory.store(Some(Arc::new(inventory)));
    let report = service
        .get_execution_inventory(root_request(crate::proto::GetExecutionInventoryRequest {}))
        .await
        .unwrap()
        .into_inner();
    assert!(report.runtimes.iter().any(|entry| {
        entry
            .runtime
            .as_ref()
            .is_some_and(|runtime| runtime.version == "1.17.0")
    }));
}

#[test]
fn artifact_rpc_status_preserves_missing_paths_and_transient_io() {
    for (errno, code) in [
        (libc::ENOENT, tonic::Code::FailedPrecondition),
        (libc::EACCES, tonic::Code::FailedPrecondition),
        (libc::EIO, tonic::Code::Internal),
        (libc::EBUSY, tonic::Code::Internal),
    ] {
        assert_eq!(
            publish_status(PublishError::Io {
                action: "inspect directory",
                path: "/synthetic/artifacts".into(),
                source: std::io::Error::from_raw_os_error(errno)
            })
            .code(),
            code
        );
        assert_eq!(
            remove_status(RemoveError::Io {
                action: "inspect directory",
                path: "/synthetic/artifacts".into(),
                source: std::io::Error::from_raw_os_error(errno)
            })
            .code(),
            code
        );
    }
}
