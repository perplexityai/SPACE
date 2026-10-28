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
