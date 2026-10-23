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
        managed_releases: None,
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
    write(&roots.cache_kernels.join("6.1.158/vmlinux.bin"), b"kernel");

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
    write(&roots.cache_kernels.join("6.1.158/vmlinux.bin"), &image);

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
    write(&roots.cache_kernels.join("6.1.158/vmlinux.bin"), b"kernel");
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
    // Resolved, as the audit resolves the links into the store.
    let base = &dir.path().canonicalize().unwrap();
    mkdir(&base.join("vendor"), 0o755);
    // The node's store, root-only, behind the links the image keeps.
    mkdir(&base.join("vendor/node/firecracker"), 0o755);
    mkdir(&base.join("vendor/node/kernels"), 0o755);
    std::os::unix::fs::symlink("node/firecracker", base.join("vendor/firecracker")).unwrap();
    std::os::unix::fs::symlink("node/kernels", base.join("vendor/kernels")).unwrap();
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

#[tokio::test]
async fn artifact_mutations_require_an_operator_and_an_enabled_installer() {
    let dir = tempfile::tempdir().unwrap();
    let mut service = service(dir.path());
    service.peer_authorizer = UdsPeerAuthorizer::new(
        "runtime-manager-test",
        [
            UdsPeerAccount::with_linux_user(users::SPACE_NODECTL),
            pplx_space_common_svc::UdsPeerAccount::role_with_fallback_uid(
                "rtmgr-test-observer",
                LocalRole::Observer,
                40_001,
            ),
        ],
    );
    let identity =
        crate::convert::component_identity(pplx_space_common_core::types::ComponentIdentity {
            kind: pplx_space_common_core::types::ArtifactComponentKind::Kernel,
            version: "6.1.158".parse().unwrap(),
            digest: Digest::sha256([7; 32]),
        });
    for uid in [0, 40_001, users::SPACE_NODECTL.uid] {
        let expected = if uid == 0 || uid == users::SPACE_NODECTL.uid {
            tonic::Code::FailedPrecondition
        } else {
            tonic::Code::PermissionDenied
        };
        let error = service
            .install_artifact(peer_request(
                InstallArtifactRequest {
                    identity: Some(identity.clone()),
                },
                uid,
            ))
            .await
            .unwrap_err();
        assert_eq!(error.code(), expected);
        let expected = if uid == 0 {
            tonic::Code::FailedPrecondition
        } else {
            tonic::Code::PermissionDenied
        };
        let error = service
            .uninstall_artifact(peer_request(
                UninstallArtifactRequest {
                    identity: Some(identity.clone()),
                },
                uid,
            ))
            .await
            .unwrap_err();
        assert_eq!(error.code(), expected);
        let error = service
            .cancel_operation(peer_request(
                CancelOperationRequest {
                    operation_id: "019e6370-cfc9-7a43-9337-399c7b51a1d9".into(),
                },
                uid,
            ))
            .await
            .unwrap_err();
        assert_eq!(error.code(), expected);
    }
    service
        .list_execution_artifacts(request(Vec::new(), users::SPACE_NODECTL.uid))
        .await
        .unwrap();
}

#[tokio::test]
async fn catalog_listing_does_not_require_a_mutation_worker() {
    use crate::installs::test_support::{FakeReleaseSource, FakeSource, TestPins, release_pin};
    let dir = tempfile::tempdir().unwrap();
    let source = Arc::new(FakeSource::default());
    source.offer("1.16", "6.1.158", b"synthetic kernel");
    let pins = TestPins::new(source.clone());
    pins.pin_release(release_pin("1.16.2", 1));
    let service = service(dir.path()).with_kernel_source(source).with_catalog(
        pins,
        Some(crate::installs::ReleaseContext {
            root: dir.path().join("managed/releases"),
            source: Arc::new(FakeReleaseSource::default()),
        }),
    );
    let response = service
        .list_available_artifacts(peer_request(ListAvailableArtifactsRequest::default(), 0))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(response.artifacts.len(), 2);
    assert!(!dir.path().join("vendor/runtime-manager/state").exists());
    assert!(service.installer().is_err());
}
