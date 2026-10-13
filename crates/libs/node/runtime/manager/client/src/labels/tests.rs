use super::*;
use crate::{execution, proto};

#[test]
fn labels_drop_the_enum_prefix_and_use_kebab_case() {
    assert_eq!(
        label::<proto::ArtifactProvenance>(proto::ArtifactProvenance::RuntimeReleaseBundle as i32),
        "runtime-release-bundle"
    );
    assert_eq!(
        label::<execution::RuntimeKind>(execution::RuntimeKind::CloudHypervisor as i32),
        "cloud-hypervisor"
    );
    assert_eq!(label::<proto::ScanIssueKind>(99), "unknown-99");
}

#[test]
fn listing_json_names_enum_values() {
    let response = proto::ListExecutionArtifactsResponse {
        artifacts: vec![proto::ExecutionArtifact {
            kind: proto::ExecutionArtifactKind::Component as i32,
            runtime_kind: execution::RuntimeKind::Unspecified as i32,
            component_kind: execution::ArtifactComponentKind::Kernel as i32,
            role: Some("boot".to_string()),
            location: proto::ArtifactLocation::Cache as i32,
            provenance: proto::ArtifactProvenance::VersionDirectory as i32,
            name: "6.1.155".to_string(),
            path: "/space/cache/firecracker/kernels/6.1.155/vmlinux.bin".to_string(),
            version: Some("6.1.155".to_string()),
            version_source: proto::ExecutionArtifactVersionSource::DirectoryName as i32,
            architecture: Some("x86_64".to_string()),
            state_format: None,
            digest: None,
            status: proto::ExecutionArtifactStatus::Valid as i32,
            detail: None,
            management: proto::ArtifactManagement::External as i32,
            provider: proto::ArtifactProvider::NodeImage as i32,
            for_runtime_kind: execution::RuntimeKind::Firecracker as i32,
            for_runtime_version: Some("1.15".to_string()),
        }],
        aliases: Vec::new(),
        roots: vec![proto::ScannedRoot {
            kind: proto::ExecutionArtifactKind::Runtime as i32,
            runtime_kind: execution::RuntimeKind::Firecracker as i32,
            component_kind: execution::ArtifactComponentKind::Unspecified as i32,
            location: proto::ArtifactLocation::Vendor as i32,
            provenance: proto::ArtifactProvenance::VersionDirectory as i32,
            path: "/space/vendor/firecracker".to_string(),
            presence: proto::ScanRootPresence::Present as i32,
        }],
        issues: Vec::new(),
        complete: true,
    };

    let json = serde_json::to_value(&response).unwrap();

    let artifact = &json["artifacts"][0];
    assert_eq!(artifact["kind"], "component");
    assert_eq!(artifact["component_kind"], "kernel");
    assert_eq!(artifact["runtime_kind"], "unspecified");
    assert_eq!(artifact["for_runtime_kind"], "firecracker");
    assert_eq!(artifact["for_runtime_version"], "1.15");
    assert_eq!(artifact["role"], "boot");
    assert_eq!(artifact["location"], "cache");
    assert_eq!(artifact["provenance"], "version-directory");
    assert_eq!(artifact["version_source"], "directory-name");
    assert_eq!(artifact["status"], "valid");
    assert_eq!(artifact["management"], "external");
    assert_eq!(artifact["provider"], "node-image");
    assert_eq!(artifact["digest"], serde_json::Value::Null);
    assert_eq!(json["roots"][0]["runtime_kind"], "firecracker");
    assert_eq!(json["roots"][0]["presence"], "present");
    assert_eq!(json["complete"], true);
}

#[test]
fn operation_json_names_its_request_and_enum_values() {
    let operation = proto::Operation {
        operation_id: "01a0f799-a8d6-70df-bdc9-be151f12112a".to_string(),
        identity: Some(proto::ArtifactIdentity {
            artifact: Some(proto::artifact_identity::Artifact::Runtime(
                proto::RuntimeArtifactIdentity {
                    kind: execution::RuntimeKind::Firecracker as i32,
                    version: "1.16.2".into(),
                    digest: "sha256:4a".into(),
                },
            )),
        }),
        action: proto::ArtifactAction::Install as i32,
        state: proto::OperationState::Succeeded as i32,
        outcome: proto::OperationOutcome::Installed as i32,
        error: None,
        on_disk: Some(proto::OnDisk {
            path: "/space/vendor/artifacts/firecracker/v1.16.2".to_string(),
            digest: "sha256:4a".to_string(),
        }),
        created_at_unix_ms: 0,
        updated_at_unix_ms: 0,
    };
    let json = serde_json::to_value(&operation).unwrap();

    let request = &json["identity"]["artifact"]["runtime"];
    assert_eq!(request["kind"], "firecracker");
    assert_eq!(request["version"], "1.16.2");
    assert_eq!(json["state"], "succeeded");
    assert_eq!(json["outcome"], "installed");
    assert_eq!(
        json["on_disk"]["path"],
        "/space/vendor/artifacts/firecracker/v1.16.2"
    );
}
