use pplx_space_common_svc::PeerIdentity;
use pplx_space_node_runtime_core::runtime_inventory::RuntimeInstallationRoot;
use pplx_space_node_runtime_manager_client::execution::RuntimeKind;
use pplx_space_node_runtime_manager_client::proto::{
    ArtifactLocation, ArtifactManagement, ArtifactProvenance, ArtifactProvider,
    ExecutionArtifactStatus, ScanIssueKind, ScanRootPresence,
};

use super::*;
use std::os::unix::fs::MetadataExt as _;

const LIMITS: ScanLimits = ScanLimits {
    max_entries_per_root: 16,
    max_digest_bytes: 1 << 20,
    deadline: Duration::from_secs(30),
};

fn roots(base: &std::path::Path) -> ScanRoots {
    ScanRoots {
        runtime_releases: vec![RuntimeInstallationRoot::new(
            pplx_space_common_core::types::RuntimeKind::Firecracker,
            base.join("vendor/firecracker"),
        )],
        vendor_kernels: base.join("vendor/kernels"),
        managed_kernels: None,
        cache_kernels: base.join("cache/firecracker/kernels"),
        cache_legacy_assets: base.join("cache/firecracker/assets"),
        runtime_aliases: Vec::new(),
    }
}

fn service(base: &std::path::Path) -> RuntimeManagerService {
    RuntimeManagerService::new(
        roots(base),
        base.join("vendor/runtime-manager/state"),
        LIMITS,
        Duration::from_millis(50),
    )
}

fn request(kinds: Vec<i32>, uid: u32) -> Request<ListExecutionArtifactsRequest> {
    peer_request(
        ListExecutionArtifactsRequest {
            kinds,
            component_kinds: Vec::new(),
        },
        uid,
    )
}

fn peer_request<T>(body: T, uid: u32) -> Request<T> {
    let mut request = Request::new(body);
    request
        .extensions_mut()
        .insert(PeerIdentity::new(uid, uid, None));
    request
}

fn write(path: &std::path::Path, contents: &[u8]) {
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, contents).unwrap();
}

/// An executable holding just an ELF header for an ARM32 machine, which the
/// scan recognizes as ELF but does not name.
fn unnamed_machine_executable(path: &std::path::Path) {
    use std::os::unix::fs::PermissionsExt as _;
    let mut header = vec![0u8; 64];
    header[..4].copy_from_slice(b"\x7fELF");
    header[4] = 1;
    header[5] = 1;
    header[6] = 1;
    header[16..18].copy_from_slice(&2u16.to_le_bytes());
    header[20..24].copy_from_slice(&1u32.to_le_bytes());
    header[40..42].copy_from_slice(&52u16.to_le_bytes());
    header[18..20].copy_from_slice(&40u16.to_le_bytes());
    write(path, &header);
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o755)).unwrap();
}

#[tokio::test]
async fn an_empty_node_answers_with_an_empty_complete_listing() {
    let dir = tempfile::tempdir().unwrap();

    let response = service(dir.path())
        .list_execution_artifacts(request(Vec::new(), 0))
        .await
        .unwrap()
        .into_inner();

    assert!(response.artifacts.is_empty());
    assert!(response.issues.is_empty());
    assert!(response.complete);
    assert_eq!(response.roots.len(), 4);
    assert!(
        response
            .roots
            .iter()
            .all(|root| root.presence == ScanRootPresence::Absent as i32)
    );
}

#[tokio::test]
async fn the_listing_reports_each_entry_with_its_status() {
    let dir = tempfile::tempdir().unwrap();
    let roots = roots(dir.path());
    unnamed_machine_executable(
        &roots.runtime_releases[0]
            .path
            .join("v1.15.1/bin/firecracker"),
    );
    unnamed_machine_executable(&roots.runtime_releases[0].path.join("v1.15.1/bin/jailer"));
    unnamed_machine_executable(
        &roots.runtime_releases[0]
            .path
            .join("v1.16.0/bin/firecracker"),
    );
    write(
        &roots.runtime_releases[0]
            .path
            .join("v1.17.0/bin/firecracker"),
        b"fc",
    );
    write(
        &roots.runtime_releases[0].path.join("v1.17.0/bin/jailer"),
        b"jailer",
    );
    write(&roots.cache_kernels.join("6.1.155/vmlinux.bin"), b"kernel");

    let response = service(dir.path())
        .list_execution_artifacts(request(vec![ExecutionArtifactKind::Runtime as i32], 0))
        .await
        .unwrap()
        .into_inner();

    let statuses = response
        .artifacts
        .iter()
        .map(|artifact| (artifact.name.as_str(), artifact.status))
        .collect::<Vec<_>>();
    assert_eq!(
        statuses,
        [
            ("v1.15.1", ExecutionArtifactStatus::Valid as i32),
            ("v1.16.0", ExecutionArtifactStatus::Incomplete as i32),
            ("v1.17.0", ExecutionArtifactStatus::Invalid as i32),
        ]
    );
    assert!(response.artifacts.iter().all(|artifact| {
        artifact.kind == ExecutionArtifactKind::Runtime as i32
            && artifact.runtime_kind == RuntimeKind::Firecracker as i32
            && artifact.component_kind == ArtifactComponentKind::Unspecified as i32
            && artifact.role.is_none()
            && artifact.location == ArtifactLocation::Vendor as i32
            && artifact.provenance == ArtifactProvenance::VersionDirectory as i32
            // The runtime manager installs nothing yet.
            && artifact.management == ArtifactManagement::External as i32
            && artifact.provider == ArtifactProvider::Unknown as i32
    }));
    // An ELF machine this build does not name stays unknown, not assumed.
    assert_eq!(response.artifacts[0].architecture, None);
    assert_eq!(
        response.artifacts[0]
            .state_format
            .as_ref()
            .map(|state| state.version.as_str()),
        Some("9.0.0")
    );
}

#[tokio::test]
async fn a_limit_hit_is_an_incomplete_listing() {
    let dir = tempfile::tempdir().unwrap();
    let roots = roots(dir.path());
    for index in 0..=LIMITS.max_entries_per_root {
        std::fs::create_dir_all(roots.cache_kernels.join(format!("6.1.{index}"))).unwrap();
    }

    let response = service(dir.path())
        .list_execution_artifacts(request(Vec::new(), 0))
        .await
        .unwrap()
        .into_inner();

    assert!(!response.complete);
    assert_eq!(response.issues.len(), 1);
    assert_eq!(response.issues[0].kind, ScanIssueKind::EntryLimit as i32);
}

#[tokio::test]
async fn a_kernel_over_the_digest_limit_makes_the_listing_incomplete() {
    let dir = tempfile::tempdir().unwrap();
    let roots = roots(dir.path());
    let image = vec![0u8; usize::try_from(LIMITS.max_digest_bytes).unwrap() + 1];
    write(&roots.cache_kernels.join("6.1.155/vmlinux.bin"), &image);

    let response = service(dir.path())
        .list_execution_artifacts(request(vec![ExecutionArtifactKind::Component as i32], 0))
        .await
        .unwrap()
        .into_inner();

    assert!(!response.complete);
    assert_eq!(response.issues.len(), 1);
    assert_eq!(response.issues[0].kind, ScanIssueKind::DigestLimit as i32);
    assert_eq!(response.artifacts[0].digest, None);
}

#[tokio::test]
async fn unknown_kinds_are_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let service = service(dir.path());

    for kind in [ExecutionArtifactKind::Unspecified as i32, 42] {
        let status = service
            .list_execution_artifacts(request(vec![kind], 0))
            .await
            .unwrap_err();
        assert_eq!(status.code(), tonic::Code::InvalidArgument, "{kind}");
    }
}

#[tokio::test]
async fn component_kinds_narrow_the_components_listed() {
    let dir = tempfile::tempdir().unwrap();
    let roots = roots(dir.path());
    unnamed_machine_executable(
        &roots.runtime_releases[0]
            .path
            .join("v1.15.1/bin/firecracker"),
    );
    unnamed_machine_executable(&roots.runtime_releases[0].path.join("v1.15.1/bin/jailer"));
    write(&roots.cache_kernels.join("6.1.155/vmlinux.bin"), b"kernel");
    let service = service(dir.path());
    let list = |kinds: Vec<i32>, component_kinds: Vec<i32>| {
        service.list_execution_artifacts(peer_request(
            ListExecutionArtifactsRequest {
                kinds,
                component_kinds,
            },
            0,
        ))
    };

    let kernels = list(
        vec![ExecutionArtifactKind::Component as i32],
        vec![ArtifactComponentKind::Kernel as i32],
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(kernels.artifacts.len(), 1);
    let kernel = &kernels.artifacts[0];
    assert_eq!(kernel.component_kind, ArtifactComponentKind::Kernel as i32);
    assert_eq!(kernel.runtime_kind, RuntimeKind::Unspecified as i32);
    assert_eq!(kernel.role.as_deref(), Some("boot"));
    assert_eq!(kernel.location, ArtifactLocation::Cache as i32);

    // No initrd layout is scanned, so asking only for initrds lists nothing,
    // while the runtimes still come through.
    let initrds = list(Vec::new(), vec![ArtifactComponentKind::Initrd as i32])
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        initrds
            .artifacts
            .iter()
            .map(|artifact| artifact.kind)
            .collect::<Vec<_>>(),
        [ExecutionArtifactKind::Runtime as i32]
    );

    for kind in [ArtifactComponentKind::Unspecified as i32, 42] {
        let status = list(Vec::new(), vec![kind]).await.unwrap_err();
        assert_eq!(status.code(), tonic::Code::InvalidArgument, "{kind}");
    }
}

#[tokio::test]
async fn unknown_peers_are_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let service = service(dir.path());

    let status = service
        .list_execution_artifacts(request(Vec::new(), 4_000_000_000))
        .await
        .unwrap_err();
    assert_eq!(status.code(), tonic::Code::PermissionDenied);

    let status = service
        .list_execution_artifacts(Request::new(ListExecutionArtifactsRequest::default()))
        .await
        .unwrap_err();
    assert_eq!(status.code(), tonic::Code::Unauthenticated);
}

#[tokio::test]
async fn a_listing_waits_boundedly_for_a_running_one() {
    let dir = tempfile::tempdir().unwrap();
    let service = service(dir.path());
    let running = service.scan_slot.clone().acquire_owned().await.unwrap();

    let status = service
        .list_execution_artifacts(request(Vec::new(), 0))
        .await
        .unwrap_err();
    assert_eq!(status.code(), tonic::Code::ResourceExhausted);

    drop(running);
    service
        .list_execution_artifacts(request(Vec::new(), 0))
        .await
        .unwrap();
}

/// A service whose protection audit trusts the test user in place of root and
/// starts at `base`, since tests cannot create root-owned directories.
fn audited_service(base: &std::path::Path) -> RuntimeManagerService {
    let metadata = std::fs::metadata(base).unwrap();
    let mut service = service(base);
    service.audit_identity = AuditIdentity {
        trusted_uid: metadata.uid(),
        service_uid: metadata.uid(),
        service_gid: metadata.gid(),
        audit_from: base.to_path_buf(),
    };
    service
}

fn mkdir(path: &std::path::Path, mode: u32) {
    use std::os::unix::fs::PermissionsExt as _;
    std::fs::create_dir_all(path).unwrap();
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode)).unwrap();
}

fn statuses(response: &GetStatusResponse) -> Vec<(String, i32, i32)> {
    response
        .protected_paths
        .iter()
        .map(|path| (path.path.clone(), path.role, path.protection))
        .collect()
}

#[tokio::test]
async fn status_reports_every_protected_path_of_a_provisioned_node() {
    use pplx_space_node_runtime_manager_client::proto::{PathProtection, ProtectedPathRole};

    let dir = tempfile::tempdir().unwrap();
    let base = dir.path();
    mkdir(&base.join("vendor"), 0o755);
    mkdir(&base.join("vendor/firecracker"), 0o1775);
    mkdir(&base.join("vendor/kernels"), 0o1775);
    mkdir(&base.join("vendor/runtime-manager"), 0o755);
    mkdir(&base.join("vendor/runtime-manager/state"), 0o700);
    mkdir(&base.join("cache"), 0o1775);
    mkdir(&base.join("cache/firecracker"), 0o1775);
    mkdir(&base.join("cache/firecracker/kernels"), 0o755);
    mkdir(&base.join("cache/firecracker/assets"), 0o755);

    let response = audited_service(base)
        .get_status(peer_request(GetStatusRequest {}, 0))
        .await
        .unwrap()
        .into_inner();

    let shown = |relative: &str| base.join(relative).to_string_lossy().into_owned();
    let protected = PathProtection::Protected as i32;
    assert_eq!(
        statuses(&response),
        [
            (
                shown("vendor/firecracker"),
                ProtectedPathRole::InstallRoot as i32,
                protected
            ),
            (
                shown("vendor/kernels"),
                ProtectedPathRole::InstallRoot as i32,
                protected
            ),
            (
                shown("cache/firecracker/kernels"),
                ProtectedPathRole::RootManagedCache as i32,
                protected
            ),
            (
                shown("cache/firecracker/assets"),
                ProtectedPathRole::RootManagedCache as i32,
                protected
            ),
            (
                shown("vendor/runtime-manager/state"),
                ProtectedPathRole::StateDirectory as i32,
                protected
            ),
        ]
    );
    assert!(
        response
            .protected_paths
            .iter()
            .all(|path| path.problems.is_empty())
    );
}

#[tokio::test]
async fn status_reports_an_unprovisioned_node_without_failing() {
    use pplx_space_node_runtime_manager_client::proto::PathProtection;

    let dir = tempfile::tempdir().unwrap();
    let base = dir.path();
    // An image that predates the runtime manager: a root-only vendor root,
    // no state directory, and shared cache parents without the sticky bit.
    mkdir(&base.join("vendor/firecracker"), 0o755);
    mkdir(&base.join("cache/firecracker/kernels"), 0o755);
    mkdir(&base.join("cache"), 0o775);
    mkdir(&base.join("cache/firecracker"), 0o775);

    let response = audited_service(base)
        .get_status(peer_request(GetStatusRequest {}, 0))
        .await
        .unwrap()
        .into_inner();

    let protection = |suffix: &str| {
        response
            .protected_paths
            .iter()
            .find(|path| path.path.ends_with(suffix))
            .unwrap()
    };
    // The image's own root, writable by root alone, is protected.
    let firecracker = protection("vendor/firecracker");
    assert_eq!(firecracker.protection, PathProtection::Protected as i32);
    assert!(firecracker.problems.is_empty());
    assert_eq!(
        protection("vendor/kernels").protection,
        PathProtection::Absent as i32
    );
    assert_eq!(
        protection("cache/firecracker/kernels").protection,
        PathProtection::Unprotected as i32
    );
    assert_eq!(
        protection("runtime-manager/state").protection,
        PathProtection::Absent as i32
    );
}

#[tokio::test]
async fn status_requires_an_authorized_peer() {
    let dir = tempfile::tempdir().unwrap();
    let status = service(dir.path())
        .get_status(peer_request(GetStatusRequest {}, 4_000_000_000))
        .await
        .unwrap_err();
    assert_eq!(status.code(), tonic::Code::PermissionDenied);
}

#[tokio::test]
async fn a_status_audit_waits_boundedly_for_a_running_scan() {
    let dir = tempfile::tempdir().unwrap();
    let service = audited_service(dir.path());
    let running = service.scan_slot.clone().acquire_owned().await.unwrap();

    let status = service
        .get_status(peer_request(GetStatusRequest {}, 0))
        .await
        .unwrap_err();
    assert_eq!(status.code(), tonic::Code::ResourceExhausted);

    drop(running);
    service
        .get_status(peer_request(GetStatusRequest {}, 0))
        .await
        .unwrap();
}

mod kernels {
    use std::sync::Arc;

    use pplx_space_node_runtime_manager_client::proto::{
        ArtifactManagement, InstallKernelRequest, KernelPresence, ListAvailableKernelsRequest,
        OperationOutcome, OperationState, UninstallKernelRequest,
    };

    use super::*;
    use crate::installs::test_support::{FakeSource, boot_kernel};
    use crate::installs::{InstallLimits, Installer};

    const INSTALL_LIMITS: InstallLimits = InstallLimits {
        max_bytes: 1 << 20,
        download_idle_timeout: Duration::from_secs(30),
        max_attempts: 1,
    };

    /// A service whose kernel install root is the vendor kernel root it
    /// lists, as a node handed over to the runtime manager would have.
    async fn installing_service(
        base: &std::path::Path,
    ) -> (RuntimeManagerService, Arc<FakeSource>) {
        let source = Arc::new(FakeSource::default());
        std::fs::create_dir_all(base.join("vendor/kernels")).unwrap();
        std::fs::create_dir_all(base.join("vendor/runtime-manager/state")).unwrap();
        let installer = Installer::open(
            base.join("vendor/kernels"),
            &base.join("vendor/runtime-manager/state"),
            INSTALL_LIMITS,
            source.clone(),
        )
        .await
        .unwrap();
        let service = service(base)
            .with_kernel_source(source.clone())
            .with_installer(installer);
        (service, source)
    }

    fn install_request(version: &str, uid: u32) -> Request<InstallKernelRequest> {
        peer_request(
            InstallKernelRequest {
                runtime_kind: RuntimeKind::Firecracker as i32,
                runtime_version: "1.15.1".to_string(),
                version: version.to_string(),
            },
            uid,
        )
    }

    async fn settle(service: &RuntimeManagerService, operation_id: &str) -> i32 {
        for _ in 0..500 {
            let operation = service
                .get_operation(peer_request(
                    GetOperationRequest {
                        operation_id: operation_id.to_string(),
                    },
                    0,
                ))
                .await
                .unwrap()
                .into_inner()
                .operation
                .unwrap();
            if matches!(
                OperationState::try_from(operation.state),
                Ok(OperationState::Succeeded | OperationState::Failed | OperationState::Cancelled)
            ) {
                return operation.state;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("operation {operation_id} did not settle");
    }

    #[tokio::test]
    async fn kernels_install_list_as_space_managed_and_uninstall() {
        let dir = tempfile::tempdir().unwrap();
        let (service, source) = installing_service(dir.path()).await;
        source.offer("1.15", "6.1.155", &boot_kernel(4096, 7));
        source.offer("1.15", "5.10.245", &boot_kernel(4096, 8));
        // An external kernel of another version stays external.
        write(
            &dir.path().join("vendor/kernels/5.10.245/vmlinux.bin"),
            b"baked",
        );

        let available = |service: RuntimeManagerService| async move {
            service
                .list_available_kernels(peer_request(
                    ListAvailableKernelsRequest {
                        runtime_kind: RuntimeKind::Firecracker as i32,
                        runtime_version: Some("1.15".to_string()),
                    },
                    0,
                ))
                .await
        };
        let listed = available(service.clone()).await.unwrap().into_inner();
        let presence = |listed: &ListAvailableKernelsResponse, version: &str| {
            listed
                .kernels
                .iter()
                .find(|kernel| kernel.version == version)
                .map(|kernel| kernel.presence)
        };
        assert_eq!(listed.runtime_kind, RuntimeKind::Firecracker as i32);
        assert_eq!(listed.runtime_lines, vec!["1.15".to_string()]);
        assert_eq!(
            presence(&listed, "6.1.155"),
            Some(KernelPresence::NotInstalled as i32)
        );
        assert_eq!(
            presence(&listed, "5.10.245"),
            Some(KernelPresence::VersionPresent as i32)
        );

        let accepted = service
            .install_kernel(install_request("6.1.155", 0))
            .await
            .unwrap()
            .into_inner()
            .operation
            .unwrap();
        assert_eq!(
            settle(&service, &accepted.operation_id).await,
            OperationState::Succeeded as i32
        );

        let listed = available(service.clone()).await.unwrap().into_inner();
        assert_eq!(
            presence(&listed, "6.1.155"),
            Some(KernelPresence::Installed as i32)
        );
        let inventory = service
            .list_execution_artifacts(request(Vec::new(), 0))
            .await
            .unwrap()
            .into_inner();
        let management = |version: &str| {
            inventory
                .artifacts
                .iter()
                .find(|artifact| artifact.version.as_deref() == Some(version))
                .map(|artifact| {
                    (
                        artifact.management,
                        artifact.for_runtime_kind,
                        artifact.for_runtime_version.as_deref(),
                    )
                })
        };
        assert_eq!(
            management("6.1.155"),
            Some((
                ArtifactManagement::SpaceManaged as i32,
                RuntimeKind::Firecracker as i32,
                Some("1.15")
            ))
        );
        assert_eq!(
            management("5.10.245"),
            Some((
                ArtifactManagement::External as i32,
                RuntimeKind::Unspecified as i32,
                None
            ))
        );

        let removed = service
            .uninstall_kernel(peer_request(
                UninstallKernelRequest {
                    version: "6.1.155".to_string(),
                },
                0,
            ))
            .await
            .unwrap()
            .into_inner()
            .operation
            .unwrap();
        assert_eq!(
            settle(&service, &removed.operation_id).await,
            OperationState::Succeeded as i32
        );
        let removed = service
            .get_operation(peer_request(
                GetOperationRequest {
                    operation_id: removed.operation_id,
                },
                0,
            ))
            .await
            .unwrap()
            .into_inner()
            .operation
            .unwrap();
        assert_eq!(removed.outcome, OperationOutcome::Uninstalled as i32);
        let status = service
            .uninstall_kernel(peer_request(
                UninstallKernelRequest {
                    version: "5.10.245".to_string(),
                },
                0,
            ))
            .await
            .unwrap_err();
        assert_eq!(status.code(), tonic::Code::FailedPrecondition);
    }

    /// Uninstalling a version removes the runtime manager's own install of
    /// it and leaves other copies; with no install of it, the refusal says
    /// whether the version is absent or only present read-only.
    #[tokio::test]
    async fn uninstall_by_version_removes_only_the_installed_copy() {
        let dir = tempfile::tempdir().unwrap();
        let (service, source) = installing_service(dir.path()).await;
        let image = crate::installs::test_support::banner_kernel(4096, "6.1.155+");
        source.offer("1.15", "6.1.155", &image);
        let bundled = dir
            .path()
            .join("vendor/firecracker/v1.15.1/kernel/vmlinux.bin");
        write(&bundled, &image);
        let uninstall = |version: &str| {
            service.uninstall_kernel(peer_request(
                UninstallKernelRequest {
                    version: version.to_string(),
                },
                0,
            ))
        };

        let installed = service
            .install_kernel(install_request("6.1.155", 0))
            .await
            .unwrap()
            .into_inner()
            .operation
            .unwrap();
        assert_eq!(
            settle(&service, &installed.operation_id).await,
            OperationState::Succeeded as i32
        );
        let removed = uninstall("6.1.155")
            .await
            .unwrap()
            .into_inner()
            .operation
            .unwrap();
        assert_eq!(
            settle(&service, &removed.operation_id).await,
            OperationState::Succeeded as i32
        );
        assert!(!dir.path().join("vendor/kernels/6.1.155").exists());
        assert!(bundled.exists());

        let read_only = uninstall("6.1.155").await.unwrap_err();
        assert_eq!(read_only.code(), tonic::Code::FailedPrecondition);
        assert!(
            read_only.message().contains(&bundled.display().to_string()),
            "{read_only:?}"
        );
        let absent = uninstall("6.1.1").await.unwrap_err();
        assert_eq!(absent.code(), tonic::Code::NotFound, "{absent:?}");
    }

    /// Kernels are listed and installed only for the runtime the source
    /// publishes for, and an install names its runtime.
    #[tokio::test]
    async fn kernels_are_only_for_the_sources_runtime() {
        let dir = tempfile::tempdir().unwrap();
        let (service, _) = installing_service(dir.path()).await;
        let status = service
            .list_available_kernels(peer_request(
                ListAvailableKernelsRequest {
                    runtime_kind: RuntimeKind::CloudHypervisor as i32,
                    runtime_version: None,
                },
                0,
            ))
            .await
            .unwrap_err();
        assert_eq!(status.code(), tonic::Code::FailedPrecondition, "{status:?}");
        assert!(status.message().contains("cloud-hypervisor"), "{status:?}");

        let install = |runtime_kind: RuntimeKind| {
            service.install_kernel(peer_request(
                InstallKernelRequest {
                    runtime_kind: runtime_kind as i32,
                    runtime_version: "48.0".to_string(),
                    version: "6.1.155".to_string(),
                },
                0,
            ))
        };
        let status = install(RuntimeKind::CloudHypervisor).await.unwrap_err();
        assert_eq!(status.code(), tonic::Code::FailedPrecondition, "{status:?}");
        let status = install(RuntimeKind::Unspecified).await.unwrap_err();
        assert_eq!(status.code(), tonic::Code::InvalidArgument, "{status:?}");
    }

    #[tokio::test]
    async fn only_privileged_peers_mutate_and_only_with_an_install_root() {
        let dir = tempfile::tempdir().unwrap();
        let (installing, _) = installing_service(dir.path()).await;
        let status = installing
            .install_kernel(install_request("6.1.155", 4_000_000_001))
            .await
            .unwrap_err();
        assert_eq!(status.code(), tonic::Code::PermissionDenied);

        let disabled = service(dir.path()).with_kernel_source(Arc::new(FakeSource::default()));
        let status = disabled
            .install_kernel(install_request("6.1.155", 0))
            .await
            .unwrap_err();
        assert_eq!(status.code(), tonic::Code::FailedPrecondition);

        let status = installing
            .install_kernel(install_request("not a version", 0))
            .await
            .unwrap_err();
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn available_kernels_default_to_the_lines_the_node_has() {
        let dir = tempfile::tempdir().unwrap();
        let source = Arc::new(FakeSource::default());
        source.offer("1.15", "6.1.155", &boot_kernel(4096, 7));
        source.offer("1.14", "6.1.141", &boot_kernel(4096, 7));
        source.offer("1.13", "6.1.130", &boot_kernel(4096, 7));
        for release in ["v1.15.1", "v1.15.0", "v1.14.2", "default"] {
            std::fs::create_dir_all(dir.path().join("vendor/firecracker").join(release)).unwrap();
        }
        let listed = service(dir.path())
            .with_kernel_source(source)
            .list_available_kernels(peer_request(
                // No runtime named: the source's.
                ListAvailableKernelsRequest {
                    runtime_kind: RuntimeKind::Unspecified as i32,
                    runtime_version: None,
                },
                0,
            ))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(
            listed.runtime_lines,
            vec!["1.14".to_string(), "1.15".to_string()]
        );
        let mut versions: Vec<_> = listed
            .kernels
            .iter()
            .map(|kernel| kernel.version.clone())
            .collect();
        versions.sort();
        assert_eq!(versions, vec!["6.1.141".to_string(), "6.1.155".to_string()]);
    }
}
