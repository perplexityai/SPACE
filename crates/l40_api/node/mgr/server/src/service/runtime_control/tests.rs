use super::*;
use pplx_space_common_core::types::{Arch, RuntimeKind, SandboxId, TemplateName};

#[test]
fn runtime_dispatch_preserves_absence_and_refuses_corrupt_or_foreign_metadata() {
    let cache = tempfile::tempdir().unwrap();
    let id = SandboxId::new();
    let directory = cache.path().join(id.to_string());
    std::fs::create_dir(&directory).unwrap();
    assert!(
        recorded_execution_in(cache.path(), &id.to_string())
            .unwrap()
            .is_none()
    );
    let path = directory.join("metadata.json");
    let metadata = SandboxArtifactMetadata::new(
        id,
        ExecutionMetadata::initial_firecracker(Arch::X86_64, vec![]),
        TemplateName::parse("seed").unwrap(),
        None,
    );
    let mut document = serde_json::to_value(metadata).unwrap();
    std::fs::write(&path, serde_json::to_vec(&document).unwrap()).unwrap();
    assert_eq!(
        recorded_execution_in(cache.path(), &id.to_string())
            .unwrap()
            .unwrap()
            .runtime()
            .kind,
        RuntimeKind::Firecracker
    );
    // Use the real execution serializer; mutating the runtime name alone would
    // create an internally contradictory descriptor rather than a CH fixture.
    let environment =
        pplx_space_node_runtime_core::catalog::CompiledEnvironment::for_architecture(Arch::X86_64)
            .unwrap()
            .into_iter()
            .find(|e| e.release().release.kind == RuntimeKind::CloudHypervisor)
            .unwrap();
    document["execution"] =
        serde_json::to_value(environment.definition().execution_requirements().unwrap()).unwrap();
    std::fs::write(&path, serde_json::to_vec(&document).unwrap()).unwrap();
    assert_eq!(
        recorded_execution_in(cache.path(), &id.to_string())
            .unwrap()
            .unwrap()
            .runtime()
            .kind,
        RuntimeKind::CloudHypervisor
    );
    document["sandbox_id"] = serde_json::json!(SandboxId::new());
    std::fs::write(&path, serde_json::to_vec(&document).unwrap()).unwrap();
    recorded_execution_in(cache.path(), &id.to_string()).unwrap_err();
    std::fs::write(&path, b"{").unwrap();
    recorded_execution_in(cache.path(), &id.to_string()).unwrap_err();
}

#[test]
fn gateway_network_intent_does_not_require_a_retained_environment_catalog_entry() {
    use pplx_space_common_core::types::guest_image::GuestNetwork;
    let directory = tempfile::tempdir().unwrap();
    let cache = directory.path().canonicalize().unwrap();
    let id = SandboxId::new();
    let dir = cache.join(id.to_string());
    std::fs::create_dir(&dir).unwrap();
    assert_eq!(
        recorded_guest_network_in(&cache, &id.to_string()).unwrap(),
        GuestNetwork::KernelSlotIpv4V1
    );
    let metadata = SandboxArtifactMetadata::new(
        id,
        ExecutionMetadata::initial_firecracker(Arch::X86_64, vec![]),
        TemplateName::parse("seed").unwrap(),
        None,
    );
    let mut document = serde_json::to_value(metadata).unwrap();
    // A structurally valid unbound non-Linux execution has no qualified local
    // boot environment. Network recovery still uses its persisted image intent.
    document["execution"]["guest"]["os"] = serde_json::json!("windows");
    std::fs::write(
        dir.join("metadata.json"),
        serde_json::to_vec(&document).unwrap(),
    )
    .unwrap();
    let parsed = recorded_artifact_in(&cache, &id.to_string())
        .unwrap()
        .unwrap();
    pplx_space_node_runtime_core::boot::GuestProfile::resolve(
        &parsed.execution,
        parsed.guest_image(),
    )
    .unwrap_err();
    assert_eq!(
        recorded_guest_network_in(&cache, &id.to_string()).unwrap(),
        GuestNetwork::KernelSlotIpv4V1
    );
    document["sandbox_id"] = serde_json::json!(SandboxId::new());
    std::fs::write(
        dir.join("metadata.json"),
        serde_json::to_vec(&document).unwrap(),
    )
    .unwrap();
    recorded_guest_network_in(&cache, &id.to_string()).unwrap_err();
    std::fs::write(dir.join("metadata.json"), b"{").unwrap();
    recorded_guest_network_in(&cache, &id.to_string()).unwrap_err();
}
