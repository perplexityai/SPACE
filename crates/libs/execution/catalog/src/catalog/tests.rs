use super::*;

fn owner(dir: &Path) -> u32 {
    std::fs::metadata(dir).unwrap().uid()
}

fn firecracker(version: &str) -> RuntimeIdentity {
    RuntimeIdentity {
        kind: RuntimeKind::Firecracker,
        version: RuntimeVersion::parse(version).unwrap(),
    }
}

/// A catalog directory private to this test's account.
fn catalog_dir() -> tempfile::TempDir {
    let dir = tempfile::tempdir().unwrap();
    std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o755)).unwrap();
    dir
}

fn write(dir: &Path, name: &str, file: &serde_json::Value) -> PathBuf {
    let path = dir.join(name);
    std::fs::write(&path, serde_json::to_vec(file).unwrap()).unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
    path
}

const HEX_A: &str = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const HEX_B: &str = "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

fn release_entry(version: &str, architecture: &str) -> serde_json::Value {
    serde_json::json!({
        "runtime": "firecracker",
        "version": version,
        "architecture": architecture,
        "archive_sha256": HEX_A,
        "binaries": { "bin/firecracker": HEX_A, "bin/jailer": HEX_B },
    })
}

#[test]
fn compiled_release_layouts_and_initial_pins_are_complete() {
    let catalog = RuntimeCatalog::compiled().unwrap();
    let known = (ReleaseLayout::of(RuntimeKind::Firecracker)
        .unwrap()
        .known_releases)();
    for version in known {
        for architecture in [Arch::X86_64, Arch::Aarch64] {
            let pinned = catalog
                .release(&firecracker(version), architecture)
                .unwrap_or_else(|| panic!("{version} {architecture} is not pinned"));
            assert_eq!(
                pinned
                    .binaries
                    .iter()
                    .map(|binary| binary.path)
                    .collect::<Vec<_>>(),
                ["bin/firecracker", "bin/jailer"]
            );
        }
    }
    // Raw CH downloads are the installed executable: both pins must describe
    // the same bytes, with no synthetic jailer or archive member.
    let known = (ReleaseLayout::of(RuntimeKind::CloudHypervisor)
        .unwrap()
        .known_releases)();
    assert!(!known.is_empty());
    for version in known {
        let release = RuntimeIdentity {
            kind: RuntimeKind::CloudHypervisor,
            version: version.parse().unwrap(),
        };
        for architecture in [Arch::X86_64, Arch::Aarch64] {
            let pin = catalog.release(&release, architecture).unwrap();
            let [binary] = pin.binaries.as_slice() else {
                panic!("static release must have one executable");
            };
            assert_eq!(binary.path, "bin/cloud-hypervisor");
            assert_eq!(pin.archive_sha256, binary.sha256);
            assert!(compiled_verity_digest(binary.sha256).unwrap().is_some());
        }
    }
    // The image's kernel, pinned as the image build pins it.
    let kernel = catalog
        .kernel(&ArtifactVersion::parse("6.1.155").unwrap(), Arch::X86_64)
        .unwrap();
    assert_eq!(
        kernel.sha256.to_string(),
        "sha256:e20e46d0c36c55c0d1014eb20576171b3f3d922260d9f792017aeff53af3d4f2"
    );
    // E0 is a durable legacy binding, not an alias to the current catalog tip.
    let initial = InitialEnvironment::for_architecture(Arch::X86_64).unwrap();
    assert_eq!(initial.release.release.version.as_str(), "1.15.1");
    assert_eq!(
        initial.release.digest().to_string(),
        "sha256:7225701e6d651bb351c6858e3587994fb75a9932fb16d744dfec30fb8bfdd027"
    );
    assert_eq!(initial.kernel, *kernel);
}

#[test]
fn a_release_digest_names_each_binary_by_file_name() {
    let digest = release_digest([
        ("bin/firecracker", HEX_A.parse().unwrap()),
        ("bin/jailer", HEX_B.parse().unwrap()),
    ]);
    let text = format!(
        "firecracker {}\njailer {}\n",
        "a".repeat(64),
        "b".repeat(64)
    );
    assert_eq!(
        digest,
        Digest::sha256(Sha256::digest(text.as_bytes()).into())
    );
}

#[test]
fn a_missing_directory_adds_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let catalog = RuntimeCatalog::load_owned_by(&dir.path().join("absent"), 0).unwrap();
    assert_eq!(catalog, RuntimeCatalog::compiled().unwrap());
}

#[test]
fn a_file_adds_kernels_and_pins_for_known_releases() {
    let dir = catalog_dir();
    write(
        dir.path(),
        "extra.json",
        &serde_json::json!({
            "schema": CATALOG_SCHEMA,
            "kernels": [{
                "version": "6.1.158",
                "runtime": "firecracker",
                "runtime_line": "1.16",
                "architecture": "x86_64",
                "size": 4096,
                "sha256": HEX_A,
            }],
        }),
    );
    let catalog = RuntimeCatalog::load_owned_by(dir.path(), owner(dir.path())).unwrap();
    let line = RuntimeVersion::parse("1.16").unwrap();
    let kernels: Vec<_> = catalog
        .kernels(RuntimeKind::Firecracker, &line, Arch::X86_64)
        .collect();
    let extra = kernels
        .iter()
        .find(|kernel| kernel.version.as_str() == "6.1.158");
    assert_eq!(extra.map(|kernel| kernel.size), Some(4096));
    // Files that are not JSON are not catalog files.
    std::fs::write(dir.path().join("README"), b"notes").unwrap();
    RuntimeCatalog::load_owned_by(dir.path(), owner(dir.path())).unwrap();
}

#[test]
fn a_file_cannot_add_a_release_this_build_cannot_run_or_contradict_a_pin() {
    let cases = [
        (
            "unknown",
            serde_json::json!({ "schema": 1, "releases": [release_entry("9.9.9", "x86_64")] }),
        ),
        (
            "conflict",
            serde_json::json!({ "schema": 1, "releases": [release_entry("1.16.2", "x86_64")] }),
        ),
        (
            "layout",
            serde_json::json!({ "schema": 1, "releases": [{
                "runtime": "firecracker",
                "version": "1.16.2",
                "architecture": "x86_64",
                "archive_sha256": HEX_A,
                "binaries": { "bin/firecracker": HEX_A },
            }] }),
        ),
        ("schema", serde_json::json!({ "schema": 2 })),
        (
            "unknown field",
            serde_json::json!({ "schema": 1, "extra": [] }),
        ),
    ];
    for (name, file) in cases {
        let dir = catalog_dir();
        write(dir.path(), "extra.json", &file);
        let error = RuntimeCatalog::load_owned_by(dir.path(), owner(dir.path())).unwrap_err();
        let expected = match name {
            "unknown" => matches!(error, CatalogError::UnknownRelease { .. }),
            "conflict" => matches!(error, CatalogError::Conflict { .. }),
            "layout" => matches!(error, CatalogError::Layout { .. }),
            "schema" => matches!(error, CatalogError::Schema { .. }),
            _ => matches!(error, CatalogError::Parse { .. }),
        };
        assert!(expected, "{name}: {error}");
    }
}

#[test]
fn a_catalog_another_account_could_write_is_refused() {
    let dir = catalog_dir();
    let file = write(
        dir.path(),
        "extra.json",
        &serde_json::json!({ "schema": 1 }),
    );
    let mine = owner(dir.path());
    RuntimeCatalog::load_owned_by(dir.path(), mine).unwrap();

    // Owned by someone else.
    let error = RuntimeCatalog::load_owned_by(dir.path(), mine.wrapping_add(1)).unwrap_err();
    assert!(matches!(error, CatalogError::Unsafe { .. }), "{error}");
    // Group-writable file, then directory.
    std::fs::set_permissions(&file, std::fs::Permissions::from_mode(0o664)).unwrap();
    let error = RuntimeCatalog::load_owned_by(dir.path(), mine).unwrap_err();
    assert!(matches!(error, CatalogError::Unsafe { .. }), "{error}");
    std::fs::set_permissions(&file, std::fs::Permissions::from_mode(0o644)).unwrap();
    std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o775)).unwrap();
    let error = RuntimeCatalog::load_owned_by(dir.path(), mine).unwrap_err();
    assert!(matches!(error, CatalogError::Unsafe { .. }), "{error}");
    // A linked file.
    std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o755)).unwrap();
    std::fs::remove_file(&file).unwrap();
    let elsewhere = tempfile::NamedTempFile::new().unwrap();
    std::os::unix::fs::symlink(elsewhere.path(), &file).unwrap();
    let error = RuntimeCatalog::load_owned_by(dir.path(), mine).unwrap_err();
    assert!(matches!(error, CatalogError::Unsafe { .. }), "{error}");
}

#[test]
fn managed_directory_names_retain_labels_and_digest_prefixes() {
    let digest = Digest::sha256([0xab; 32]);
    for label in ["v1.15.1", "6.1.155", "custom--build"] {
        for suffix in ["abababababab".to_owned(), digest.hex_string()] {
            let text = format!("{label}--{suffix}");
            let name: ManagedDirectoryName = text.parse().unwrap();
            assert_eq!(name.label(), label);
            assert!(name.matches_digest(digest));
            let mut different = [0xab; 32];
            different[31] = 0xcd;
            assert_eq!(
                name.matches_digest(Digest::sha256(different)),
                suffix.len() == 12
            );
            assert_eq!(name.digest_prefix().hex_prefix(), "abababababab");
            assert_eq!(name.to_string(), text);
        }
        assert_eq!(
            ManagedDirectoryName::kernel(&ArtifactVersion::parse(label).unwrap(), digest)
                .to_string(),
            format!("{label}--abababababab")
        );
    }
    for name in [
        "1.15.1",
        "--",
        "../v1--",
        "v1--not-a-digest",
        "v1--ABABABABABAB",
        "v1--abababababa",
        "v1--ababababababa",
    ] {
        name.parse::<ManagedDirectoryName>().unwrap_err();
    }
}

#[test]
fn every_qualified_environment_has_pinned_verity_sources() {
    for environment in [Arch::X86_64, Arch::Aarch64]
        .into_iter()
        .flat_map(|arch| CompiledEnvironment::for_architecture(arch).unwrap())
    {
        for binary in &environment.release().binaries {
            assert!(compiled_verity_digest(binary.sha256).unwrap().is_some());
        }
        if let Some(kernel) = environment.kernel() {
            assert!(compiled_verity_digest(kernel.sha256).unwrap().is_some());
        }
        for (_, identity, _) in environment
            .definition()
            .descriptor()
            .spec()
            .boot
            .components()
        {
            assert!(compiled_verity_digest(identity.digest).unwrap().is_some());
            if identity.kind != pplx_space_common_core::types::ArtifactComponentKind::Kernel {
                assert!(environment.boot_artifact(identity).is_some());
            }
        }
    }
}

#[test]
fn auxiliary_boot_pins_are_typed_and_cannot_conflict() {
    use pplx_space_common_core::types::{ArtifactComponentKind, ComponentIdentity};
    let dir = catalog_dir();
    let identity = ComponentIdentity {
        kind: ArtifactComponentKind::Firmware,
        version: "test-bundle".parse().unwrap(),
        digest: HEX_A.parse().unwrap(),
    };
    let pin = PinnedBootArtifact::new(identity.clone(), Arch::X86_64, 4096).unwrap();
    write(
        dir.path(),
        "boot.json",
        &serde_json::json!({"schema": 1, "boot_artifacts": [pin]}),
    );
    let catalog = RuntimeCatalog::load_owned_by(dir.path(), owner(dir.path())).unwrap();
    assert_eq!(catalog.boot_artifact(&identity, Arch::X86_64), Some(&pin));
    assert!(catalog.boot_artifact(&identity, Arch::Aarch64).is_none());
    for (kind, size, digest, conflict) in [
        ("firmware", 4096, HEX_B, true),
        ("firmware", 0, HEX_A, false),
        ("initrd", 4096, HEX_A, false),
        ("rootfs", 4096, HEX_A, false),
    ] {
        write(
            dir.path(),
            "other.json",
            &serde_json::json!({"schema": 1, "boot_artifacts": [{
                "identity": { "kind": kind, "version": "test-bundle", "digest": digest },
                "architecture": "x86_64", "size": size,
            }]}),
        );
        let error = RuntimeCatalog::load_owned_by(dir.path(), owner(dir.path())).unwrap_err();
        if conflict {
            assert!(matches!(error, CatalogError::Conflict { .. }));
        } else {
            assert!(matches!(error, CatalogError::Parse { .. }));
        }
    }
    // ARM64 is installable for qualification, even while execution is gated.
    for kernel in RuntimeCatalog::compiled()
        .unwrap()
        .all_kernels(Arch::Aarch64)
        .filter(|pin| pin.runtime == RuntimeKind::CloudHypervisor)
    {
        assert!(compiled_verity_digest(kernel.sha256).unwrap().is_some());
    }
}
