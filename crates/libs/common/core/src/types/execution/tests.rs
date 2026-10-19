use super::*;

fn runtime() -> RuntimeIdentity {
    RuntimeIdentity {
        kind: RuntimeKind::Firecracker,
        version: RuntimeVersion::parse("1.15.1").unwrap(),
    }
}

fn linux_guest() -> GuestPlatform {
    GuestPlatform {
        os: GuestOperatingSystem::Linux,
        architecture: Arch::Aarch64,
    }
}

#[test]
fn components_are_canonical_and_duplicate_free() {
    let kernel = ArtifactComponentMetadata {
        kind: ArtifactComponentKind::Kernel,
        role: Some(ArtifactComponentRole::parse("boot").unwrap()),
        version: Some(ArtifactVersion::parse("6.10.14").unwrap()),
        digest: None,
        format: Some(ArtifactFormat::parse("vmlinux").unwrap()),
    };
    let rootfs = ArtifactComponentMetadata {
        kind: ArtifactComponentKind::Rootfs,
        role: None,
        version: None,
        digest: None,
        format: Some(ArtifactFormat::parse("ext4").unwrap()),
    };

    let metadata = ExecutionMetadata::new(
        runtime(),
        linux_guest(),
        None,
        vec![rootfs.clone(), kernel.clone()],
    )
    .unwrap();
    assert_eq!(metadata.components(), &[kernel.clone(), rootfs]);

    assert!(matches!(
        ExecutionMetadata::new(runtime(), linux_guest(), None, vec![kernel.clone(), kernel]),
        Err(ExecutionMetadataError::DuplicateComponent { .. })
    ));
}

#[test]
fn snapshots_require_state_format() {
    let metadata = ExecutionMetadata::new(runtime(), linux_guest(), None, vec![]).unwrap();
    assert_eq!(
        SnapshotExecutionMetadata::new(metadata).unwrap_err(),
        ExecutionMetadataError::MissingSnapshotStateFormat
    );
}

#[cfg(feature = "serde")]
#[test]
fn snapshot_cpu_budget_round_trips_without_changing_legacy_shape() {
    let legacy = serde_json::to_value(ExecutionMetadata::initial_firecracker(
        Arch::Aarch64,
        vec![],
    ))
    .unwrap();
    for budget in [None, Some(8), Some(0)] {
        let mut value = legacy.clone();
        if let Some(budget) = budget {
            value["cpu_budget"] = budget.into();
        }
        let parsed = serde_json::from_value::<SnapshotExecutionMetadata>(value.clone());
        if budget == Some(0) {
            assert!(parsed.is_err(), "zero is not a CPU allocation");
        } else {
            assert_eq!(serde_json::to_value(parsed.unwrap()).unwrap(), value);
        }
        value.as_object_mut().unwrap().remove("state_format");
        serde_json::from_value::<SnapshotExecutionMetadata>(value).unwrap_err();
    }
}

#[cfg(feature = "serde")]
#[test]
fn snapshot_host_cpu_round_trips_without_changing_legacy_shape() {
    let legacy =
        serde_json::to_value(ExecutionMetadata::initial_firecracker(Arch::X86_64, vec![])).unwrap();

    // Absent stays absent on the wire, so a legacy row re-serializes to the
    // same JSON and an older reader never sees the field.
    let parsed = serde_json::from_value::<SnapshotExecutionMetadata>(legacy.clone()).unwrap();
    assert_eq!(parsed.host_cpu(), None);
    assert_eq!(serde_json::to_value(&parsed).unwrap(), legacy);

    let mut recorded = legacy.clone();
    recorded["host_cpu"] = "genuineintel-6-85-7".into();
    let parsed = serde_json::from_value::<SnapshotExecutionMetadata>(recorded.clone()).unwrap();
    assert_eq!(
        parsed.host_cpu().map(HostCpu::as_str),
        Some("genuineintel-6-85-7")
    );
    assert_eq!(serde_json::to_value(&parsed).unwrap(), recorded);

    // A value this release doesn't accept reads as unknown rather than
    // failing the whole metadata.
    for unaccepted in [
        serde_json::Value::from("Intel(R) Xeon(R)"),
        serde_json::Value::from(85),
        serde_json::json!({"vendor": "genuineintel", "model": 85}),
    ] {
        let mut invalid = legacy.clone();
        invalid["host_cpu"] = unaccepted;
        let parsed = serde_json::from_value::<SnapshotExecutionMetadata>(invalid).unwrap();
        assert_eq!(parsed.host_cpu(), None);
        assert_eq!(serde_json::to_value(&parsed).unwrap(), legacy);
    }
}

#[test]
fn snapshot_artifact_identity_ignores_only_the_host_cpu() {
    let metadata = SnapshotExecutionMetadata::new(ExecutionMetadata::initial_firecracker(
        Arch::X86_64,
        vec![],
    ))
    .unwrap();
    let with_host_cpu = metadata
        .clone()
        .with_host_cpu(Some(HostCpu::parse("genuineintel-6-85-7").unwrap()));
    assert!(metadata.same_artifact_identity(&with_host_cpu));
    assert!(with_host_cpu.same_artifact_identity(&metadata));

    let with_cpu_budget = metadata
        .clone()
        .with_cpu_budget(Some(VcpuCount::new(std::num::NonZeroU32::new(2).unwrap())));
    assert!(!metadata.same_artifact_identity(&with_cpu_budget));

    let other_execution = SnapshotExecutionMetadata::new(ExecutionMetadata::initial_firecracker(
        Arch::Aarch64,
        vec![],
    ))
    .unwrap()
    .with_host_cpu(with_host_cpu.host_cpu().cloned());
    assert!(!with_host_cpu.same_artifact_identity(&other_execution));
}

#[cfg(feature = "serde")]
#[test]
fn execution_metadata_round_trips_canonical_json() {
    let state_format = StateFormatIdentity {
        format: StateFormatName::parse(FIRECRACKER_STATE_FORMAT).unwrap(),
        version: StateFormatVersion::parse("9.0.0").unwrap(),
    };
    let metadata = ExecutionMetadata::new(
        runtime(),
        linux_guest(),
        Some(state_format),
        vec![ArtifactComponentMetadata {
            kind: ArtifactComponentKind::Kernel,
            role: Some(ArtifactComponentRole::parse("boot").unwrap()),
            version: Some(ArtifactVersion::parse("6.10.14").unwrap()),
            digest: None,
            format: Some(ArtifactFormat::parse("vmlinux").unwrap()),
        }],
    )
    .unwrap();
    let value = serde_json::to_value(&metadata).unwrap();

    assert_eq!(value["runtime"]["kind"], "firecracker");
    assert_eq!(value["runtime"]["version"], "1.15.1");
    assert_eq!(value["guest"]["os"], "linux");
    assert_eq!(value["guest"]["architecture"], "aarch64");
    assert_eq!(value["state_format"]["format"], FIRECRACKER_STATE_FORMAT);
    assert_eq!(
        serde_json::from_value::<ExecutionMetadata>(value).unwrap(),
        metadata
    );
}

#[cfg(feature = "serde")]
#[test]
fn component_json_matches_the_direct_restore_component_probe() {
    // The direct-substrate restore-candidate SQL reads compatibility
    // requirements from `snapshots.execution_metadata->'components'`:
    // object keys kind/role/version/digest/format, optional fields
    // omitted entirely (never JSON null), kinds spelled as `as_str`,
    // and digest in its `sha256:<hex>` string form. That query and this
    // serialization must stay in lockstep.
    let digest = Digest::sha256([0xab; 32]);
    let full = ArtifactComponentMetadata {
        kind: ArtifactComponentKind::Rootfs,
        role: Some(ArtifactComponentRole::parse("system").unwrap()),
        version: Some(ArtifactVersion::parse("2.1.0").unwrap()),
        digest: Some(digest),
        format: Some(ArtifactFormat::parse("erofs").unwrap()),
    };
    let value = serde_json::to_value(&full).unwrap();
    let mut keys: Vec<_> = value
        .as_object()
        .unwrap()
        .keys()
        .map(String::as_str)
        .collect();
    keys.sort_unstable();
    assert_eq!(keys, ["digest", "format", "kind", "role", "version"]);
    assert_eq!(value["kind"], "rootfs");
    assert_eq!(value["role"], "system");
    assert_eq!(value["version"], "2.1.0");
    assert_eq!(value["digest"], digest.to_string());
    assert_eq!(value["format"], "erofs");

    let bare = ArtifactComponentMetadata {
        kind: ArtifactComponentKind::Kernel,
        role: None,
        version: None,
        digest: None,
        format: None,
    };
    let value = serde_json::to_value(&bare).unwrap();
    let keys: Vec<_> = value
        .as_object()
        .unwrap()
        .keys()
        .map(String::as_str)
        .collect();
    assert_eq!(keys, ["kind"]);

    for kind in [
        ArtifactComponentKind::Kernel,
        ArtifactComponentKind::Initrd,
        ArtifactComponentKind::Firmware,
        ArtifactComponentKind::Rootfs,
        ArtifactComponentKind::BootDisk,
        ArtifactComponentKind::GuestTools,
    ] {
        assert_eq!(serde_json::to_value(kind).unwrap(), kind.as_str());
    }
}

#[cfg(feature = "serde")]
#[test]
fn windows_execution_metadata_does_not_require_kernel_or_state_format() {
    let metadata = ExecutionMetadata::new(
        RuntimeIdentity {
            kind: RuntimeKind::CloudHypervisor,
            version: RuntimeVersion::parse("42.0.0").unwrap(),
        },
        GuestPlatform {
            os: GuestOperatingSystem::Windows,
            architecture: Arch::X86_64,
        },
        None,
        vec![
            ArtifactComponentMetadata {
                kind: ArtifactComponentKind::Firmware,
                role: Some(ArtifactComponentRole::parse("boot").unwrap()),
                version: Some(ArtifactVersion::parse("edk2-2026.05").unwrap()),
                digest: Some(Digest::sha256([0x11; 32])),
                format: Some(ArtifactFormat::parse("uefi").unwrap()),
            },
            ArtifactComponentMetadata {
                kind: ArtifactComponentKind::BootDisk,
                role: Some(ArtifactComponentRole::parse("system").unwrap()),
                version: Some(ArtifactVersion::parse("windows-server-2025").unwrap()),
                digest: Some(Digest::sha256([0x22; 32])),
                format: Some(ArtifactFormat::parse("qcow2").unwrap()),
            },
        ],
    )
    .unwrap();

    let value = serde_json::to_value(&metadata).unwrap();

    assert_eq!(value["runtime"]["kind"], "cloud-hypervisor");
    assert_eq!(value["guest"]["os"], "windows");
    assert!(value.get("state_format").is_none());
    assert_eq!(
        serde_json::from_value::<ExecutionMetadata>(value).unwrap(),
        metadata
    );
}
