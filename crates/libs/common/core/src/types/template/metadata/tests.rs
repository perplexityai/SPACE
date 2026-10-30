use super::*;

#[test]
fn parses_build_manager_shape() {
    // Mirrors the on-disk layout that
    // `crates/libs/build/mgr/src/lib.rs`'s recipe-build path writes.
    let raw = r#"{
            "id": "py-3.12",
            "name": "py-3.12",
            "description": "Python 3.12",
            "base": "base:latest",
            "base_resolved": "sha256:abc",
            "built_at": "2026-05-19T12:34:56Z",
            "spaced_version": "0.1.0",
            "spaced_compatibility_family": "0.1",
            "recipe": "py-3.12",
            "steps_count": 5,
            "vcpu": 2,
            "memory_mib": 1024,
            "disk_size_mb": 4096
        }"#;
    let m: TemplateMetadata = serde_json::from_str(raw).expect("build-mgr shape must parse");
    assert_eq!(m.id, "py-3.12");
    assert_eq!(m.recipe, "py-3.12");
    assert_eq!(m.disk_size_mb, Some(4096));
    let (vcpu, mem, disk) = m.runtime_defaults();
    assert_eq!(vcpu, Some(2));
    assert_eq!(mem, Some(1024));
    assert_eq!(disk, Some(4096));
}

#[test]
fn parses_seed_shape_with_arch_only() {
    // The seed builder writes metadata with `arch` and `built_at`
    // but typically not the recipe/steps_count/vcpu/etc fields.
    let raw = r#"{
            "id": "seed",
            "name": "seed",
            "built_at": "2026-05-19T12:34:56Z",
            "arch": "aarch64"
        }"#;
    let m: TemplateMetadata = serde_json::from_str(raw).expect("seed shape must parse");
    assert_eq!(m.arch.as_deref(), Some("aarch64"));
    assert_eq!(m.recipe, "");
    let (vcpu, mem, disk) = m.runtime_defaults();
    assert_eq!(vcpu, None);
    assert_eq!(mem, None);
    assert_eq!(disk, None);
    assert_eq!(m.execution_metadata().unwrap(), None);
}

#[test]
fn parses_versioned_execution_metadata() {
    let raw = r#"{
            "schema_version": 2,
            "execution": {
                "runtime": { "kind": "firecracker", "version": "1.15.1" },
                "guest": { "os": "linux", "architecture": "aarch64" },
                "state_format": {
                    "format": "firecracker-snapshot",
                    "version": "9.0.0"
                },
                "components": []
            }
        }"#;
    let metadata: TemplateMetadata = serde_json::from_str(raw).unwrap();
    let execution = metadata.execution_metadata().unwrap().unwrap();
    assert_eq!(
        execution.runtime().kind,
        crate::types::RuntimeKind::Firecracker
    );
    assert_eq!(execution.runtime().version.to_string(), "1.15.1");
}

#[test]
fn rejects_partial_or_unknown_versioned_metadata() {
    let missing: TemplateMetadata = serde_json::from_str(r#"{"schema_version":2}"#).unwrap();
    assert_eq!(
        missing.execution_metadata().unwrap_err(),
        TemplateMetadataSchemaError::MissingExecution
    );

    let unknown: TemplateMetadata = serde_json::from_str(r#"{"schema_version":99}"#).unwrap();
    assert_eq!(
        unknown.execution_metadata().unwrap_err(),
        TemplateMetadataSchemaError::UnsupportedVersion(99)
    );
}

#[test]
fn zero_valued_runtime_defaults_collapse_to_none() {
    // A writer that emits an explicit `0` (the integer default)
    // means "no opinion", not "default to zero CPUs". Pin this.
    let raw = r#"{ "vcpu": 0, "memory_mib": 0, "disk_size_mb": 0 }"#;
    let m: TemplateMetadata = serde_json::from_str(raw).unwrap();
    assert_eq!(m.vcpu, Some(0));
    let (vcpu, mem, disk) = m.runtime_defaults();
    assert_eq!(vcpu, None);
    assert_eq!(mem, None);
    assert_eq!(disk, None);
}

#[test]
fn parses_empty_metadata_via_serde_defaults() {
    let m: TemplateMetadata =
        serde_json::from_str("{}").expect("empty metadata must parse via defaults");
    assert!(m.id.is_empty());
    assert!(m.arch.is_none());
    assert_eq!(m.runtime_defaults(), (None, None, None));
}

#[test]
fn description_none_serializes_as_null_for_wire_parity() {
    // The pre-typing build-mgr struct serialized `description:
    // Option<String>` without `skip_serializing_if`, so `None`
    // rendered as JSON `null`. Some external tools could
    // theoretically grep for the literal `"description"` key in
    // metadata.json; preserve null-on-None to keep wire output
    // bit-identical with the old build-mgr writer.
    let m = TemplateMetadata {
        id: "x".to_string(),
        description: None,
        ..TemplateMetadata::default()
    };
    let json = serde_json::to_string(&m).unwrap();
    assert!(
        json.contains("\"description\":null"),
        "description=None must render as null, not be absent: {json}"
    );
}

#[test]
fn always_serialized_string_fields_preserve_pre_typing_wire() {
    // The pre-typing build-mgr struct had `id`, `name`, `base`,
    // `built_at`, `recipe` as plain `String` with no
    // `skip_serializing_if` — they always appeared on the wire
    // even when empty (build-mgr never wrote them empty, but the
    // shape was unconditional). Preserve that.
    let m = TemplateMetadata::default();
    let json = serde_json::to_string(&m).unwrap();
    for required in [
        "\"id\":\"\"",
        "\"name\":\"\"",
        "\"base\":\"\"",
        "\"built_at\":\"\"",
        "\"recipe\":\"\"",
        "\"steps_count\":0",
    ] {
        assert!(
            json.contains(required),
            "wire-parity field {required} missing from default-metadata serialization: {json}"
        );
    }
}

#[test]
fn bound_template_documents_cannot_be_downgraded_or_lose_the_reference() {
    use crate::types::{Arch, Digest, EnvironmentReference, ExecutionMetadata, TemplateManifest};
    let reference = EnvironmentReference::new(Digest::sha256([1; 32]));
    for bound in [false, true] {
        let execution = ExecutionMetadata::initial_firecracker(Arch::X86_64, vec![]);
        let execution = if bound {
            execution.with_environment(reference)
        } else {
            execution
        };
        let mut metadata = TemplateMetadata::default();
        let mut manifest = TemplateManifest::default();
        metadata.set_execution_metadata(execution.clone());
        manifest.set_execution_metadata(execution.clone());
        let metadata: TemplateMetadata =
            serde_json::from_str(&serde_json::to_string(&metadata).unwrap()).unwrap();
        let manifest: TemplateManifest =
            serde_json::from_str(&serde_json::to_string(&manifest).unwrap()).unwrap();
        assert_eq!(metadata.execution_metadata().unwrap(), Some(&execution));
        assert_eq!(manifest.execution_metadata().unwrap(), Some(&execution));
        for schema in [2, 3] {
            let mut metadata = metadata.clone();
            let mut manifest = manifest.clone();
            metadata.schema_version = Some(schema);
            manifest.schema_version = Some(schema);
            assert_eq!(
                metadata.execution_metadata().is_ok(),
                bound == (schema == 3)
            );
            assert_eq!(
                manifest.execution_metadata().is_ok(),
                bound == (schema == 3)
            );
        }
    }
}

#[test]
fn balloon_is_omitted_when_absent_and_rejected_when_invalid() {
    let json = serde_json::to_string(&TemplateMetadata::default()).unwrap();
    assert!(!json.contains("balloon"), "{json}");

    let m: TemplateMetadata =
        serde_json::from_str(r#"{"memory_mib":8192,"balloon":{"mode":"report"}}"#).unwrap();
    assert_eq!(
        m.balloon,
        Some(crate::types::BalloonConfig::new(
            crate::types::BalloonDevice::FreePageReporting,
            0
        ))
    );

    serde_json::from_str::<TemplateMetadata>(r#"{"balloon":{"mode":"shrink"}}"#)
        .expect_err("an unknown balloon mode must not parse");
}

#[test]
fn guest_image_contract_requires_a_new_schema_and_an_exact_environment() {
    use crate::types::guest_image::{GuestDiskLayout, GuestImageContract, GuestReadiness};
    let mut metadata = TemplateMetadata {
        guest_image: Some(
            GuestImageContract::new(
                GuestDiskLayout::OpaqueRawDisk,
                GuestReadiness::TcpService {
                    port: std::num::NonZeroU16::new(3389).unwrap(),
                },
                pplx_space_execution_spec::guest_image::GuestNetwork::SlotDhcpV1,
                pplx_space_execution_spec::guest_image::GuestAgent::Absent,
            )
            .unwrap(),
        ),
        ..Default::default()
    };
    metadata.execution_metadata().unwrap_err();
    let execution = crate::types::ExecutionMetadata::new(
        crate::types::RuntimeIdentity {
            kind: crate::types::RuntimeKind::CloudHypervisor,
            version: "54.0".parse().unwrap(),
        },
        crate::types::GuestPlatform {
            os: crate::types::GuestOperatingSystem::Windows,
            architecture: crate::types::Arch::X86_64,
        },
        None,
        Vec::new(),
    )
    .unwrap();
    metadata.set_execution_metadata(execution.clone());
    metadata.execution_metadata().unwrap_err();
    metadata.set_execution_metadata(
        execution.with_environment(format!("sha256:{}", "ab".repeat(32)).parse().unwrap()),
    );
    assert_eq!(
        metadata.schema_version,
        Some(GUEST_TEMPLATE_METADATA_SCHEMA_VERSION)
    );
    metadata.execution_metadata().unwrap();
    let bytes = serde_json::to_vec(&metadata).unwrap();
    let roundtrip: TemplateMetadata = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(roundtrip, metadata);
    for version in [None, Some(2), Some(3)] {
        metadata.schema_version = version;
        metadata.execution_metadata().unwrap_err();
    }
}
