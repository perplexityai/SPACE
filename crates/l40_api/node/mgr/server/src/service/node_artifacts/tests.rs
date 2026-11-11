use super::*;
use crate::proto::{ComponentArtifact, RuntimeReleaseArtifact};
use pplx_space_common_core::types::RuntimeKind;

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
            releases: [(
                RuntimeKind::Firecracker,
                dir.path().join("managed/releases"),
            )]
            .into(),
            kernels: dir.path().join("managed/kernels"),
            staging: dir.path().join("staging"),
            catalog: dir.path().join("catalog"),
        },
    };
    std::fs::create_dir_all(&paths.store.releases[&RuntimeKind::Firecracker]).unwrap();
    std::fs::create_dir_all(paths.vendor.parent().unwrap()).unwrap();
    separate_store(&paths, &paths.store.releases[&RuntimeKind::Firecracker]).unwrap();
    std::os::unix::fs::symlink(
        &paths.store.releases[&RuntimeKind::Firecracker],
        &paths.vendor,
    )
    .unwrap();
    assert!(separate_store(&paths, &paths.store.releases[&RuntimeKind::Firecracker]).is_err());
    std::fs::remove_file(&paths.vendor).unwrap();
    paths.kernels = paths.store.kernels.clone();
    assert!(separate_store(&paths, &paths.store.releases[&RuntimeKind::Firecracker]).is_err());
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
    paths
        .store
        .releases
        .insert(RuntimeKind::Firecracker, paths.vendor.clone());
    assert!(separate_store(&paths, &paths.store.releases[&RuntimeKind::Firecracker]).is_err());
    let inventory = execution_inventory(&paths, &initial_policy()).unwrap();
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
        .releases(RuntimeKind::Firecracker, Arch::X86_64)
        .find(|pin| pin.release.version.as_str() == "1.17.0")
        .unwrap()
        .clone();
    let catalog =
        pplx_space_node_runtime_core::catalog::CompiledEnvironment::for_architecture(Arch::X86_64)
            .unwrap();
    for (pin, allowed, replaces_inventory) in [
        (initial.release().clone(), "e0", true),
        (candidate.clone(), "e0", false),
        (candidate, "e0,fc-1.17.0-linux-6.1.155", true),
    ] {
        let (_fixture, paths) = installed_runtime();
        let mut service = service_for_unit_tests().await;
        service.runtime_paths = paths;
        service.set_startup_execution_policy_for_test(
            pplx_space_node_runtime_core::selection::ExecutionPolicy::parse(
                Some(allowed),
                None,
                &catalog,
            )
            .unwrap(),
        );
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
            !replaces_inventory,
            "only E0 or an allowed environment publication replaces executable inventory: {allowed}"
        );
    }
}

#[tokio::test]
async fn environment_resolution_requires_authorization_schema_and_verified_inventory() {
    use crate::service::tests::service_tests::{root_request, service_for_unit_tests};
    let service = service_for_unit_tests().await;
    for definition_schema_version in [0, 1, 2] {
        let request = || crate::proto::ResolveExecutionEnvironmentRequest {
            definition_schema_version,
            reference: None,
            compatible: None,
        };
        let error = service
            .resolve_execution_environment(Request::new(request()))
            .await
            .unwrap_err();
        assert_eq!(error.code(), tonic::Code::Unauthenticated);
        let error = service
            .resolve_execution_environment(root_request(request()))
            .await
            .unwrap_err();
        assert_eq!(error.code(), tonic::Code::FailedPrecondition);
        let schema_refusal = "generic environment schema 2 is required";
        if definition_schema_version < 2 {
            assert_eq!(error.message(), schema_refusal);
        } else {
            // Schema 2 reaches host/inventory validation; missing E0 still
            // cannot produce a usable binding, even for root.
            assert_ne!(error.message(), schema_refusal);
        }
    }
}

fn initial_policy() -> pplx_space_node_runtime_core::selection::ExecutionPolicy {
    pplx_space_node_runtime_core::selection::ExecutionPolicy::parse(
        None,
        None,
        &pplx_space_node_runtime_core::catalog::CompiledEnvironment::for_architecture(Arch::X86_64)
            .unwrap(),
    )
    .unwrap()
}

#[test]
fn policy_checks_exact_admission_and_configured_default() {
    use pplx_space_node_runtime_core::{catalog::CompiledEnvironment, selection::ExecutionPolicy};
    let catalog = CompiledEnvironment::for_architecture(Arch::X86_64).unwrap();
    for (allowed, default) in [
        ("e0", None),
        ("e0,fc-1.17.0-linux-6.1.155", None),
        (
            "e0,fc-1.17.0-linux-6.1.155",
            Some("fc-1.17.0-linux-6.1.155"),
        ),
    ] {
        let policy = ExecutionPolicy::parse(Some(allowed), default, &catalog).unwrap();
        for entry in &catalog {
            for mode in [
                ExecutionSelectionMode::NewColdBoot,
                ExecutionSelectionMode::ExistingColdBoot,
                ExecutionSelectionMode::PersistedStateRestore,
            ] {
                let result = select_definition(&policy, Some(entry.definition().reference()), mode);
                if policy.allows(entry.definition().reference()) {
                    result.unwrap();
                } else {
                    result.unwrap_err();
                }
            }
        }
        assert_eq!(
            select_definition(&policy, None, ExecutionSelectionMode::NewColdBoot)
                .unwrap()
                .reference(),
            policy.default_reference().unwrap()
        );
    }
}

#[tokio::test]
async fn legacy_custom_inputs_survive_unless_legacy_permission_is_removed() {
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
            &paths.store.releases[&RuntimeKind::Firecracker]
        } else {
            &paths.vendor
        };
        std::fs::create_dir_all(runtime_root).unwrap();
        std::fs::rename(paths.vendor.join("v1.16.0"), runtime_root.join("v1.15.1")).unwrap();
        std::fs::remove_file(paths.vendor.join("default")).unwrap();
        symlink("v1.15.1", runtime_root.join("default")).unwrap();
        // Inject a discovered snapshot to isolate service admission. Core
        // store tests cover byte verification.
        for (root, version) in [
            (&paths.vendor, "1.16.0"),
            (&paths.store.releases[&RuntimeKind::Firecracker], "1.17.0"),
        ] {
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
                    RuntimeInstallationRoot::new(
                        RuntimeKind::Firecracker,
                        &paths.store.releases[&RuntimeKind::Firecracker],
                    ),
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
                .unwrap();
            assert_eq!(resolved.execution, legacy);
        }
        // Historical restores retain custom kernel provenance without requiring
        // a boot image; cold starts still require that exact image.
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
                if version == "1.17.0" {
                    // Legacy permission never activates managed candidates.
                    assert_eq!(result.unwrap_err().code(), tonic::Code::FailedPrecondition);
                } else {
                    result.unwrap();
                }
            }
        }
        service.set_startup_execution_policy_for_test(
            pplx_space_node_runtime_core::selection::ExecutionPolicy::parse(
                Some("e0"),
                None,
                &pplx_space_node_runtime_core::catalog::CompiledEnvironment::for_architecture(
                    pplx_space_common_core::types::Arch::X86_64,
                )
                .unwrap(),
            )
            .unwrap(),
        );
        for mode in [
            ExecutionSelectionMode::NewColdBoot,
            ExecutionSelectionMode::ExistingColdBoot,
            ExecutionSelectionMode::PersistedStateRestore,
        ] {
            assert_eq!(
                service
                    .resolve_bound_execution(Some(&legacy), RuntimeKind::Firecracker, mode)
                    .await
                    .unwrap_err()
                    .code(),
                tonic::Code::FailedPrecondition
            );
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
    service.set_startup_execution_policy_for_test(
        pplx_space_node_runtime_core::selection::ExecutionPolicy::parse(
            Some("e0,fc-1.17.0-linux-6.1.155"),
            None,
            &pplx_space_node_runtime_core::catalog::CompiledEnvironment::for_architecture(
                Arch::X86_64,
            )
            .unwrap(),
        )
        .unwrap(),
    );
    service.runtime_paths = paths;
    service.runtime_inventory.store(Some(Arc::new(inventory)));
    let report = service
        .get_execution_inventory(root_request(crate::proto::GetExecutionInventoryRequest {}))
        .await
        .unwrap()
        .into_inner();
    assert!(report.runtimes.iter().any(|entry| {
        entry.runtime.as_ref().is_some_and(|runtime| {
            runtime.version == "1.17.0" && entry.version_sort_key == [1, 17, 0]
        })
    }));
    // The legacy copy is placement evidence only. The observation still names
    // the whole compiled catalog, and no binding it never verified as usable.
    let observation =
        pplx_space_execution_common::environment::EnvironmentObservation::from_inventory(&report)
            .unwrap()
            .expect("current nodes publish an environment observation");
    let catalog =
        pplx_space_node_runtime_core::catalog::CompiledEnvironment::for_architecture(Arch::X86_64)
            .unwrap();
    assert_eq!(observation.known().len(), catalog.len());
    for entry in &catalog {
        assert!(observation.get(entry.definition().reference()).is_some());
    }
    assert!(observation.usable().is_empty());
    // Published definitions use the generic representation only.
    for known in &report.environments.as_ref().unwrap().known {
        assert_eq!(
            known.definition.as_ref().unwrap().schema_version,
            pplx_space_execution_common::environment::RUNTIME_NEUTRAL_ENVIRONMENT_SCHEMA_VERSION
        );
    }
    // A policy validated against E0 alone, as when its catalog failed to load,
    // doesn't shrink what the build reports as known.
    let initial =
        pplx_space_node_runtime_core::catalog::InitialEnvironment::for_architecture(Arch::X86_64)
            .unwrap()
            .compiled_environment()
            .unwrap();
    service.set_startup_execution_policy_for_test(
        pplx_space_node_runtime_core::selection::ExecutionPolicy::parse(None, None, &[initial])
            .unwrap(),
    );
    let report = service
        .get_execution_inventory(root_request(crate::proto::GetExecutionInventoryRequest {}))
        .await
        .unwrap()
        .into_inner();
    let observation =
        pplx_space_execution_common::environment::EnvironmentObservation::from_inventory(&report)
            .unwrap()
            .unwrap();
    assert_eq!(observation.known().len(), catalog.len());
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

#[test]
fn managed_release_routing_is_exact_and_an_unrelated_overlap_does_not_block_it() {
    use crate::service::tests::service_tests::installed_runtime;
    let (fixture, mut paths) = installed_runtime();
    let ch_root = fixture.path().join("store/cloud-hypervisor");
    paths
        .store
        .releases
        .insert(RuntimeKind::CloudHypervisor, ch_root.clone());
    let fc = artifact_store(&paths.store, RuntimeKind::Firecracker).unwrap();
    let ch = artifact_store(&paths.store, RuntimeKind::CloudHypervisor).unwrap();
    assert_ne!(fc.releases_root(), ch.releases_root());
    assert_eq!(ch.releases_root(), ch_root);
    assert_eq!(
        artifact_store(&paths.store, RuntimeKind::Gvisor)
            .unwrap_err()
            .code(),
        tonic::Code::FailedPrecondition,
    );
    paths
        .store
        .releases
        .insert(RuntimeKind::CloudHypervisor, paths.vendor.clone());
    let ch = artifact_store(&paths.store, RuntimeKind::CloudHypervisor).unwrap();
    assert!(separate_store(&paths, ch.releases_root()).is_err());
    separate_store(&paths, fc.releases_root()).unwrap();
}
