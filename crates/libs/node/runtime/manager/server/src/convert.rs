//! Runtime-core scan results to the wire shape.

use std::path::Path;

use pplx_space_execution_common::proto as execution;
use pplx_space_node_runtime_core::runtime_inventory::diagnostic::{
    ArtifactKind, ArtifactLocation, ArtifactManagement, ArtifactOrigin, ArtifactProvenance,
    ArtifactProvider, ArtifactScan, ArtifactStatus, ObservedAlias, RootPresence, ScanIssue,
    ScanIssueKind, ScannedArtifact, ScannedRoot, VersionSource,
};
use pplx_space_node_runtime_manager_client::proto;

use crate::installs::{InstallOperation, JobOutcome, Operation, OperationState};
use crate::protection::{AuditIdentity, PathReport, PathRole, Protection};

pub(crate) fn mark_managed_store(
    scan: &mut ArtifactScan,
    roots: &pplx_space_node_runtime_core::runtime_inventory::diagnostic::ScanRoots,
) {
    for artifact in &mut scan.artifacts {
        let in_store = match artifact.origin.kind {
            ArtifactKind::Runtime(runtime) => roots.managed_releases.iter().any(|root| {
                root.kind == runtime && artifact.path.parent() == Some(root.path.as_path())
            }),
            ArtifactKind::Component(kind) => roots.managed_kernels.as_ref().is_some_and(|root| {
                let root = match kind {
                    pplx_space_common_core::types::ArtifactComponentKind::Firmware => {
                        pplx_space_node_runtime_core::artifact_layout::boot_artifacts_root(root)
                    }
                    _ => root.clone(),
                };
                artifact.path.parent().and_then(Path::parent) == Some(root.as_path())
            }),
        };
        if in_store {
            artifact.management = ArtifactManagement::SpaceManaged;
        }
    }
}

pub(crate) fn runtime_identity(
    runtime: &pplx_space_common_core::types::RuntimeIdentity,
    digest: pplx_space_common_core::types::Digest,
) -> proto::ArtifactIdentity {
    proto::ArtifactIdentity {
        artifact: Some(proto::artifact_identity::Artifact::Runtime(
            proto::RuntimeArtifactIdentity {
                kind: execution::RuntimeKind::from(runtime.kind) as i32,
                version: runtime.version.to_string(),
                digest: digest.to_string(),
            },
        )),
    }
}
pub(crate) fn component_identity(
    identity: pplx_space_common_core::types::ComponentIdentity,
) -> proto::ArtifactIdentity {
    proto::ArtifactIdentity {
        artifact: Some(proto::artifact_identity::Artifact::Component(component(
            identity,
        ))),
    }
}

pub(crate) fn component(
    identity: pplx_space_common_core::types::ComponentIdentity,
) -> proto::ComponentIdentity {
    proto::ComponentIdentity {
        kind: execution::ArtifactComponentKind::from(identity.kind) as i32,
        digest: identity.digest.to_string(),
        version: identity.version.to_string(),
    }
}

pub(crate) fn operation(operation: &Operation) -> proto::Operation {
    let identity = match &operation.request {
        InstallOperation::InstallBootArtifact { pin } => component_identity(pin.identity().clone()),
        InstallOperation::UninstallBootArtifact { identity } => {
            component_identity(identity.clone())
        }
        InstallOperation::InstallKernel { kernel } => {
            component_identity(pplx_space_common_core::types::ComponentIdentity {
                kind: pplx_space_common_core::types::ArtifactComponentKind::Kernel,
                version: kernel.version.artifact_version().clone(),
                digest: kernel.digest,
            })
        }
        InstallOperation::UninstallKernel { version, digest } => {
            component_identity(pplx_space_common_core::types::ComponentIdentity {
                kind: pplx_space_common_core::types::ArtifactComponentKind::Kernel,
                version: version.artifact_version().clone(),
                digest: *digest,
            })
        }
        InstallOperation::InstallRelease { release, digest } => {
            runtime_identity(&release.release, *digest)
        }
        InstallOperation::UninstallRelease { release, digest } => {
            runtime_identity(release, *digest)
        }
    };
    let (state, outcome) = match operation.state {
        OperationState::Queued => (proto::OperationState::Queued, None),
        OperationState::Downloading => (proto::OperationState::Downloading, None),
        OperationState::Publishing => (proto::OperationState::Publishing, None),
        OperationState::Removing => (proto::OperationState::Removing, None),
        OperationState::Cancelling => (proto::OperationState::Cancelling, None),
        OperationState::Succeeded(outcome) => (proto::OperationState::Succeeded, Some(outcome)),
        OperationState::Failed => (proto::OperationState::Failed, None),
        OperationState::Cancelled => (proto::OperationState::Cancelled, None),
    };
    proto::Operation {
        operation_id: operation.job_id.to_string(),
        identity: Some(identity),
        action: if operation.request.is_removal() {
            proto::ArtifactAction::Uninstall
        } else {
            proto::ArtifactAction::Install
        } as i32,
        state: state as i32,
        outcome: match outcome {
            None => proto::OperationOutcome::Unspecified,
            Some(JobOutcome::Installed) => proto::OperationOutcome::Installed,
            Some(JobOutcome::AlreadyInstalled) => proto::OperationOutcome::AlreadyInstalled,
            Some(JobOutcome::Uninstalled) => proto::OperationOutcome::Uninstalled,
        } as i32,
        error: operation.error.clone(),
        on_disk: operation.on_disk.as_ref().map(|on_disk| proto::OnDisk {
            path: path_string(&on_disk.path),
            digest: on_disk.digest.to_string(),
        }),
        created_at_unix_ms: operation
            .created_at
            .map_or(0, jiff::Timestamp::as_millisecond),
        updated_at_unix_ms: operation
            .updated_at
            .map_or(0, jiff::Timestamp::as_millisecond),
    }
}

pub(crate) fn list_response(scan: ArtifactScan) -> proto::ListExecutionArtifactsResponse {
    let complete = scan.is_complete();
    proto::ListExecutionArtifactsResponse {
        artifacts: scan.artifacts.into_iter().map(artifact).collect(),
        aliases: scan.aliases.into_iter().map(alias).collect(),
        roots: scan.roots.into_iter().map(root).collect(),
        issues: scan.issues.into_iter().map(issue).collect(),
        complete,
    }
}

fn path_string(path: &Path) -> String {
    path.to_string_lossy().into_owned()
}

/// The wire form of an origin: the artifact kind with whichever of the
/// runtime or component kind applies, then location and provenance.
struct OriginFields {
    kind: i32,
    runtime_kind: i32,
    component_kind: i32,
    location: i32,
    provenance: i32,
}

fn origin(origin: ArtifactOrigin) -> OriginFields {
    let (kind, runtime_kind, component_kind) = match origin.kind {
        ArtifactKind::Runtime(runtime) => (
            proto::ExecutionArtifactKind::Runtime,
            execution::RuntimeKind::from(runtime),
            execution::ArtifactComponentKind::Unspecified,
        ),
        ArtifactKind::Component(component) => (
            proto::ExecutionArtifactKind::Component,
            execution::RuntimeKind::Unspecified,
            execution::ArtifactComponentKind::from(component),
        ),
    };
    OriginFields {
        kind: kind as i32,
        runtime_kind: runtime_kind as i32,
        component_kind: component_kind as i32,
        location: match origin.location {
            ArtifactLocation::Vendor => proto::ArtifactLocation::Vendor,
            ArtifactLocation::Cache => proto::ArtifactLocation::Cache,
        } as i32,
        provenance: match origin.provenance {
            ArtifactProvenance::VersionDirectory => proto::ArtifactProvenance::VersionDirectory,
            ArtifactProvenance::RuntimeReleaseBundle => {
                proto::ArtifactProvenance::RuntimeReleaseBundle
            }
            ArtifactProvenance::LegacyFlat => proto::ArtifactProvenance::LegacyFlat,
        } as i32,
    }
}

fn artifact(artifact: ScannedArtifact) -> proto::ExecutionArtifact {
    let origin = origin(artifact.origin);
    proto::ExecutionArtifact {
        kind: origin.kind,
        runtime_kind: origin.runtime_kind,
        component_kind: origin.component_kind,
        role: artifact.role.map(|role| role.to_string()),
        location: origin.location,
        provenance: origin.provenance,
        name: artifact.name,
        path: path_string(&artifact.path),
        version: artifact.version.map(|version| version.to_string()),
        version_source: artifact.version_source.map_or(
            proto::ExecutionArtifactVersionSource::Unspecified,
            version_source,
        ) as i32,
        architecture: artifact
            .architecture
            .map(|architecture| architecture.as_str().to_string()),
        state_format: artifact.state_format.map(|state| proto::StateFormat {
            format: state.format.to_string(),
            version: state.version.to_string(),
        }),
        digest: artifact.digest.map(|digest| digest.to_string()),
        status: artifact_status(artifact.status) as i32,
        detail: artifact.detail,
        management: match artifact.management {
            ArtifactManagement::SpaceManaged => proto::ArtifactManagement::SpaceManaged,
            ArtifactManagement::External(_) => proto::ArtifactManagement::External,
        } as i32,
        for_runtime_kind: artifact
            .for_runtime
            .as_ref()
            .map_or(execution::RuntimeKind::Unspecified, |runtime| {
                execution::RuntimeKind::from(runtime.kind)
            }) as i32,
        for_runtime_version: artifact
            .for_runtime
            .map(|runtime| runtime.version.to_string()),
        provider: match artifact.management {
            ArtifactManagement::SpaceManaged => proto::ArtifactProvider::Unspecified,
            ArtifactManagement::External(ArtifactProvider::NodeImage) => {
                proto::ArtifactProvider::NodeImage
            }
            ArtifactManagement::External(ArtifactProvider::HostOs) => {
                proto::ArtifactProvider::HostOs
            }
            ArtifactManagement::External(ArtifactProvider::Unknown) => {
                proto::ArtifactProvider::Unknown
            }
        } as i32,
    }
}

fn alias(alias: ObservedAlias) -> proto::ObservedAlias {
    proto::ObservedAlias {
        path: path_string(&alias.path),
        target: alias.target.as_deref().map(path_string),
        resolved: alias.resolved.as_deref().map(path_string),
        detail: alias.detail,
    }
}

fn root(root: ScannedRoot) -> proto::ScannedRoot {
    let origin = origin(root.origin);
    proto::ScannedRoot {
        kind: origin.kind,
        runtime_kind: origin.runtime_kind,
        component_kind: origin.component_kind,
        location: origin.location,
        provenance: origin.provenance,
        path: path_string(&root.path),
        presence: match root.presence {
            RootPresence::Present => proto::ScanRootPresence::Present,
            RootPresence::Absent => proto::ScanRootPresence::Absent,
            RootPresence::Unreadable => proto::ScanRootPresence::Unreadable,
        } as i32,
    }
}

fn issue(issue: ScanIssue) -> proto::ScanIssue {
    proto::ScanIssue {
        kind: match issue.kind {
            ScanIssueKind::EntryLimit => proto::ScanIssueKind::EntryLimit,
            ScanIssueKind::Deadline => proto::ScanIssueKind::Deadline,
            ScanIssueKind::RootUnreadable => proto::ScanIssueKind::RootUnreadable,
            ScanIssueKind::DigestLimit => proto::ScanIssueKind::DigestLimit,
        } as i32,
        path: path_string(&issue.path),
        detail: issue.detail,
    }
}

fn version_source(source: VersionSource) -> proto::ExecutionArtifactVersionSource {
    match source {
        VersionSource::DirectoryName => proto::ExecutionArtifactVersionSource::DirectoryName,
        VersionSource::ImageBanner => proto::ExecutionArtifactVersionSource::ImageBanner,
    }
}

fn artifact_status(status: ArtifactStatus) -> proto::ExecutionArtifactStatus {
    match status {
        ArtifactStatus::Valid => proto::ExecutionArtifactStatus::Valid,
        ArtifactStatus::Incomplete => proto::ExecutionArtifactStatus::Incomplete,
        ArtifactStatus::Unreadable => proto::ExecutionArtifactStatus::Unreadable,
        ArtifactStatus::Invalid => proto::ExecutionArtifactStatus::Invalid,
        ArtifactStatus::Unrecognized => proto::ExecutionArtifactStatus::Unrecognized,
        ArtifactStatus::Unsupported => proto::ExecutionArtifactStatus::Unsupported,
    }
}

pub(crate) fn status_response(
    identity: &AuditIdentity,
    reports: Vec<PathReport>,
) -> proto::GetStatusResponse {
    proto::GetStatusResponse {
        service_uid: identity.service_uid,
        service_gid: identity.service_gid,
        protected_paths: reports.into_iter().map(protected_path).collect(),
    }
}

fn protected_path(report: PathReport) -> proto::ProtectedPath {
    let (protection, problems) = match report.protection {
        Protection::Protected => (proto::PathProtection::Protected, Vec::new()),
        Protection::Absent => (proto::PathProtection::Absent, Vec::new()),
        Protection::Unprotected(problems) => (proto::PathProtection::Unprotected, problems),
    };
    proto::ProtectedPath {
        path: path_string(&report.path),
        role: match report.role {
            PathRole::InstallRoot => proto::ProtectedPathRole::InstallRoot,
            PathRole::RootManagedCache => proto::ProtectedPathRole::RootManagedCache,
            PathRole::StateDirectory => proto::ProtectedPathRole::StateDirectory,
        } as i32,
        protection: protection as i32,
        problems,
    }
}
