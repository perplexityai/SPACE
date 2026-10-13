use super::*;

#[test]
fn kind_flags_map_onto_artifact_and_component_kinds() {
    assert_eq!(list_request(&[]), ListExecutionArtifactsRequest::default());

    let runtime = list_request(&[KindArg::Runtime]);
    assert_eq!(runtime.kinds, [ExecutionArtifactKind::Runtime as i32]);
    assert!(runtime.component_kinds.is_empty());

    // Kernels are components narrowed to the kernel kind; runtimes are
    // unaffected by that narrowing.
    let both = list_request(&[KindArg::Runtime, KindArg::Kernel]);
    assert_eq!(
        both.kinds,
        [
            ExecutionArtifactKind::Runtime as i32,
            ExecutionArtifactKind::Component as i32
        ]
    );
    assert_eq!(both.component_kinds, [ArtifactComponentKind::Kernel as i32]);
}

#[test]
fn artifacts_are_named_by_runtime_or_component_kind() {
    assert_eq!(
        what(
            ExecutionArtifactKind::Runtime as i32,
            RuntimeKind::Firecracker as i32,
            ArtifactComponentKind::Unspecified as i32
        ),
        "firecracker"
    );
    assert_eq!(
        what(
            ExecutionArtifactKind::Component as i32,
            RuntimeKind::Unspecified as i32,
            ArtifactComponentKind::Kernel as i32
        ),
        "kernel"
    );
    assert_eq!(
        found_in(
            ArtifactLocation::Vendor as i32,
            ArtifactProvenance::RuntimeReleaseBundle as i32
        ),
        "vendor/runtime-release-bundle"
    );
    let external = ArtifactManagement::External as i32;
    let unknown = ArtifactProvider::Unknown as i32;
    assert_eq!(managed_by(external, unknown, false), "external");
    assert_eq!(managed_by(external, unknown, true), "external/unknown");
    assert_eq!(
        managed_by(
            ArtifactManagement::SpaceManaged as i32,
            ArtifactProvider::Unspecified as i32,
            true
        ),
        "space"
    );
}

/// The listing a flat-layout node produces: its Firecracker release and the
/// kernel versioned by its banner, in the vendor bundle and the legacy cache
/// path.
fn flat_node_listing() -> ListExecutionArtifactsResponse {
    use pplx_space_node_runtime_manager_client::proto::{
        ExecutionArtifact, ObservedAlias, ScannedRoot, StateFormat,
    };
    let external = ArtifactManagement::External as i32;
    let unknown = ArtifactProvider::Unknown as i32;
    let digest = "sha256:e20e46d0c36c55c0d1014eb20576171b3f3d922260d9f792017aeff53af3d4f2";
    let kernel = |location: ArtifactLocation,
                  provenance: ArtifactProvenance,
                  path: &str,
                  for_runtime: Option<&str>| {
        ExecutionArtifact {
            kind: ExecutionArtifactKind::Component as i32,
            component_kind: ArtifactComponentKind::Kernel as i32,
            role: Some("boot".to_string()),
            location: location as i32,
            provenance: provenance as i32,
            name: "v1.15.1".to_string(),
            path: path.to_string(),
            version: Some("6.1.155".to_string()),
            version_source: ExecutionArtifactVersionSource::ImageBanner as i32,
            for_runtime_kind: if for_runtime.is_some() {
                RuntimeKind::Firecracker as i32
            } else {
                RuntimeKind::Unspecified as i32
            },
            for_runtime_version: for_runtime.map(str::to_string),
            architecture: Some("x86_64".to_string()),
            digest: Some(digest.to_string()),
            status: ExecutionArtifactStatus::Valid as i32,
            management: external,
            provider: unknown,
            ..Default::default()
        }
    };
    ListExecutionArtifactsResponse {
        artifacts: vec![
            ExecutionArtifact {
                kind: ExecutionArtifactKind::Runtime as i32,
                runtime_kind: RuntimeKind::Firecracker as i32,
                location: ArtifactLocation::Vendor as i32,
                provenance: ArtifactProvenance::VersionDirectory as i32,
                name: "v1.15.1".to_string(),
                path: "/space/vendor/firecracker/v1.15.1".to_string(),
                version: Some("1.15.1".to_string()),
                version_source: ExecutionArtifactVersionSource::DirectoryName as i32,
                architecture: Some("x86_64".to_string()),
                state_format: Some(StateFormat {
                    format: "firecracker-snapshot".to_string(),
                    version: "9.0.0".to_string(),
                }),
                status: ExecutionArtifactStatus::Valid as i32,
                management: external,
                provider: unknown,
                ..Default::default()
            },
            kernel(
                ArtifactLocation::Vendor,
                ArtifactProvenance::RuntimeReleaseBundle,
                "/space/vendor/firecracker/v1.15.1/kernel/vmlinux.bin",
                Some("1.15.1"),
            ),
            ExecutionArtifact {
                status: ExecutionArtifactStatus::Invalid as i32,
                detail: Some("cache vmlinux.bin is a symlink".to_string()),
                ..kernel(
                    ArtifactLocation::Cache,
                    ArtifactProvenance::LegacyFlat,
                    "/space/cache/firecracker/assets/default/kernel/vmlinux.bin",
                    None,
                )
            },
        ],
        aliases: vec![ObservedAlias {
            path: "/space/vendor/firecracker/default".to_string(),
            target: Some("v1.15.1".to_string()),
            resolved: Some("/space/vendor/firecracker/v1.15.1".to_string()),
            detail: None,
        }],
        roots: vec![ScannedRoot {
            kind: ExecutionArtifactKind::Runtime as i32,
            runtime_kind: RuntimeKind::Firecracker as i32,
            location: ArtifactLocation::Vendor as i32,
            provenance: ArtifactProvenance::VersionDirectory as i32,
            path: "/space/vendor/firecracker".to_string(),
            presence: ScanRootPresence::Present as i32,
            ..Default::default()
        }],
        issues: Vec::new(),
        complete: true,
    }
}

#[test]
fn the_default_listing_fits_a_terminal() {
    let rendered = render_listing(&flat_node_listing(), false);
    let lines: Vec<&str> = rendered.lines().collect();
    let header: Vec<&str> = lines[0].split_whitespace().collect();
    assert_eq!(header, ["KIND", "VERSION", "STATUS", "OWNER", "PATH"]);
    let widest = lines.iter().map(|line| line.chars().count()).max().unwrap();
    assert!(widest <= 120, "{widest} columns:\n{rendered}");
    for absent in [
        "sha256:",
        "firecracker-snapshot",
        "x86_64",
        "Scanned roots",
        "unknown",
    ] {
        assert!(!rendered.contains(absent), "{absent} in:\n{rendered}");
    }
    assert!(rendered.contains("6.1.155 [*]"), "{rendered}");
    assert!(
        rendered.contains("[*] version read from the kernel image"),
        "{rendered}"
    );
    let bundled = lines
        .iter()
        .find(|line| line.trim_end().ends_with("/v1.15.1/kernel/vmlinux.bin"))
        .unwrap();
    let columns: Vec<&str> = bundled.split_whitespace().collect();
    assert_eq!(
        columns[..4],
        ["kernel", "6.1.155", "[*]", "valid"],
        "{rendered}"
    );
    // A problem is listed below the table rather than widening its row.
    assert!(
        rendered.contains(
            "  /space/cache/firecracker/assets/default/kernel/vmlinux.bin: invalid: cache vmlinux.bin is a symlink"
        ),
        "{rendered}"
    );
    assert!(
        rendered.contains("  /space/vendor/firecracker/default -> v1.15.1\n"),
        "{rendered}"
    );
}

#[test]
fn the_wide_listing_adds_the_evidence() {
    let rendered = render_listing(&flat_node_listing(), true);
    let header: Vec<&str> = rendered
        .lines()
        .next()
        .unwrap()
        .split_whitespace()
        .collect();
    assert_eq!(
        header,
        [
            "KIND", "VERSION", "STATUS", "OWNER", "ARCH", "STATE", "DIGEST", "FOUND", "IN", "PATH"
        ]
    );
    for present in [
        "sha256:e20e46d0c36c",
        "firecracker-snapshot 9.0.0",
        "external/unknown",
        "vendor/runtime-release-bundle",
        "Scanned roots:",
        "v1.15.1 (/space/vendor/firecracker/v1.15.1)",
    ] {
        assert!(
            rendered.contains(present),
            "{present} missing from:\n{rendered}"
        );
    }
}

#[test]
fn status_names_absent_and_unprotected_paths_separately() {
    use pplx_space_node_runtime_manager_client::proto::ProtectedPath;
    let path = |protection: PathProtection| ProtectedPath {
        protection: protection as i32,
        ..Default::default()
    };
    let response = |paths| GetStatusResponse {
        protected_paths: paths,
        ..Default::default()
    };
    assert_eq!(
        status_outcome(&response(vec![path(PathProtection::Protected)])),
        Ok(())
    );
    assert_eq!(
        status_outcome(&response(vec![
            path(PathProtection::Protected),
            path(PathProtection::Absent),
            path(PathProtection::Absent),
            path(PathProtection::Unprotected),
        ])),
        Err(
            "ERROR: 1 of 4 runtime-manager paths are protected; 2 absent, 1 unprotected"
                .to_string()
        )
    );
}

#[test]
fn version_shorthand_refuses_ambiguous_bytes() {
    let first = component_identity(
        ComponentKind::Kernel,
        "6.1.158".parse().unwrap(),
        Digest::sha256([1; 32]),
    );
    let second = component_identity(
        ComponentKind::Kernel,
        "6.1.158".parse().unwrap(),
        Digest::sha256([2; 32]),
    );
    assert_eq!(
        unique_artifact([first.clone(), first.clone()]).unwrap(),
        first
    );
    unique_artifact([first, second]).unwrap_err();
}

fn operation(state: OperationState, outcome: OperationOutcome) -> Operation {
    Operation {
        operation_id: "operation-id".into(),
        identity: Some(component_identity(
            ComponentKind::Kernel,
            "6.1.158".parse().unwrap(),
            Digest::sha256([7; 32]),
        )),
        action: ArtifactAction::Install as i32,
        state: state as i32,
        outcome: outcome as i32,
        on_disk: None,
        error: None,
        created_at_unix_ms: 0,
        updated_at_unix_ms: 0,
    }
}

#[test]
fn a_release_argument_names_a_version() {
    assert_eq!(
        parse_release("fc-1.16.2"),
        Ok(ReleaseArg {
            kind: Runtime::Firecracker,
            version: RuntimeVersion::parse("1.16.2").unwrap(),
        })
    );
    assert_eq!(
        parse_release("fc-v1.16.2").unwrap(),
        ReleaseArg {
            kind: Runtime::Firecracker,
            version: RuntimeVersion::parse("1.16.2").unwrap()
        }
    );
    parse_release("fc").expect_err("a runtime alone");
}

#[test]
fn operations_render_identity_state_and_outcome() {
    let rendered = render_operation(&operation(
        OperationState::Succeeded,
        OperationOutcome::Installed,
    ));
    assert!(rendered.contains("succeeded (installed)"), "{rendered}");
    assert!(rendered.contains("kernel 6.1.158 sha256:"), "{rendered}");
    assert!(!rendered.contains("Error:"), "{rendered}");
}

#[test]
fn only_a_failed_or_cancelled_operation_fails_the_command() {
    operation_outcome(&operation(
        OperationState::Succeeded,
        OperationOutcome::Installed,
    ))
    .unwrap();
    operation_outcome(&operation(
        OperationState::Downloading,
        OperationOutcome::Unspecified,
    ))
    .unwrap();
    let mut failed = operation(OperationState::Failed, OperationOutcome::Unspecified);
    failed.error = Some("the download's MD5 does not match".to_string());
    assert_eq!(
        operation_outcome(&failed).unwrap_err(),
        "ERROR: install failed: the download's MD5 does not match"
    );
    operation_outcome(&operation(
        OperationState::Cancelled,
        OperationOutcome::Unspecified,
    ))
    .unwrap_err();
}

#[test]
fn sizes_read_in_mebibytes() {
    assert_eq!(megabytes(44_279_576), "42.2 MiB");
    assert_eq!(megabytes(0), "0.0 MiB");
    assert_eq!(megabytes(1_048_576), "1.0 MiB");
}

#[test]
fn a_refused_mutation_says_it_needs_root() {
    let denied = mutation_error(tonic::Status::permission_denied(
        "local peer operator is not authorized for UninstallComponent",
    ));
    assert!(denied.starts_with("ERROR: local peer operator"), "{denied}");
    assert!(
        denied.contains("run the command as root or the local operator account"),
        "{denied}"
    );
    let refused = mutation_error(tonic::Status::not_found("no kernel 6.1.1 on this node"));
    assert_eq!(refused, "ERROR: no kernel 6.1.1 on this node");
}

#[test]
fn runtimes_are_named_by_alias_or_full_name_with_an_optional_version() {
    let runtime = |kind, version: Option<&str>| RuntimeArg {
        kind,
        version: version.map(str::to_string),
    };
    for (value, expected) in [
        ("fc", runtime(Runtime::Firecracker, None)),
        ("fc-1.15", runtime(Runtime::Firecracker, Some("1.15"))),
        (
            "firecracker-1.15.1",
            runtime(Runtime::Firecracker, Some("1.15.1")),
        ),
        ("cloud-hypervisor", runtime(Runtime::CloudHypervisor, None)),
        (
            "cloud-hypervisor-48.0",
            runtime(Runtime::CloudHypervisor, Some("48.0")),
        ),
        ("ch-48.0", runtime(Runtime::CloudHypervisor, Some("48.0"))),
        ("gv", runtime(Runtime::Gvisor, None)),
    ] {
        assert_eq!(parse_runtime(value), Ok(expected), "{value}");
    }
    for value in ["", "fc-", "fc1.15", "qemu-9.0", "1.15", "-1.15"] {
        parse_runtime(value).expect_err(value);
    }
    assert_eq!(
        runtime(Runtime::Firecracker, None).kind(),
        RuntimeKind::Firecracker as i32
    );

    assert_eq!(
        runtime_label(RuntimeKind::Firecracker as i32, "1.15.1"),
        "fc-1.15.1"
    );
    assert_eq!(
        runtime_label(RuntimeKind::CloudHypervisor as i32, "48.0"),
        "ch-48.0"
    );
    assert_eq!(runtime_label(99, "1.0"), "unknown-99-1.0");
}

#[test]
fn uninstall_resolves_measured_managed_identity_despite_unrelated_scan_issues() {
    let release = parse_release("fc-9.9.9").unwrap();
    let version: ArtifactVersion = "9.9.9".parse().unwrap();
    let digest = "sha256:0000000000000000000000000000000000000000000000000000000000000001";
    for runtime in [false, true] {
        let requested = || {
            if runtime {
                InstalledArtifact::Runtime(&release)
            } else {
                InstalledArtifact::Kernel(&version)
            }
        };
        let entry = ExecutionArtifact {
            kind: if runtime {
                ExecutionArtifactKind::Runtime
            } else {
                ExecutionArtifactKind::Component
            } as i32,
            runtime_kind: if runtime {
                RuntimeKind::Firecracker
            } else {
                RuntimeKind::Unspecified
            } as i32,
            component_kind: if runtime {
                ArtifactComponentKind::Unspecified
            } else {
                ArtifactComponentKind::Kernel
            } as i32,
            management: ArtifactManagement::SpaceManaged as i32,
            version: Some("9.9.9".into()),
            digest: Some(digest.into()),
            ..Default::default()
        };
        let listing = |complete, artifacts| ListExecutionArtifactsResponse {
            complete,
            artifacts,
            ..Default::default()
        };
        for complete in [false, true] {
            let identity =
                installed_identity(listing(complete, vec![entry.clone()]), requested()).unwrap();
            let found = match identity.artifact.unwrap() {
                artifact_identity::Artifact::Runtime(identity) => identity.digest,
                artifact_identity::Artifact::Component(identity) => identity.digest,
            };
            assert_eq!(found, digest);
            let error = installed_identity(listing(complete, vec![]), requested()).unwrap_err();
            assert!(
                error.contains(if complete {
                    "no matching"
                } else {
                    "cannot establish"
                }),
                "{error}"
            );
            let external = ExecutionArtifact {
                management: ArtifactManagement::External as i32,
                ..entry.clone()
            };
            installed_identity(listing(complete, vec![external]), requested()).unwrap_err();
            let unmeasured = ExecutionArtifact {
                digest: None,
                detail: Some("image changed".into()),
                ..entry.clone()
            };
            let error =
                installed_identity(listing(complete, vec![unmeasured]), requested()).unwrap_err();
            assert!(
                error.contains("present but its digest is unavailable")
                    && error.contains("image changed"),
                "{error}"
            );
            let conflicting = ExecutionArtifact {
                digest: Some(
                    "sha256:0000000000000000000000000000000000000000000000000000000000000002"
                        .into(),
                ),
                ..entry.clone()
            };
            let error = installed_identity(
                listing(complete, vec![entry.clone(), conflicting]),
                requested(),
            )
            .unwrap_err();
            assert!(error.contains("ambiguous"), "{error}");
        }
    }
}
