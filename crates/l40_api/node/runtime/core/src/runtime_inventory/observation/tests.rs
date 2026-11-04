use super::*;

fn compiled(name: &str) -> CompiledEnvironment {
    CompiledEnvironment::for_architecture(Arch::X86_64)
        .unwrap()
        .into_iter()
        .find(|entry| entry.name() == name)
        .unwrap()
}

fn external_runtime(version: &str, state: &str) -> RuntimeInstallation {
    RuntimeInstallation {
        runtime: RuntimeIdentity {
            kind: RuntimeKind::Firecracker,
            version: version.parse().unwrap(),
        },
        guest: GuestPlatform {
            os: GuestOperatingSystem::Linux,
            architecture: Arch::X86_64,
        },
        state_format: Some(StateFormatIdentity {
            format: FIRECRACKER_STATE_FORMAT.parse().unwrap(),
            version: state.parse().unwrap(),
        }),
        files: RuntimeInstallationFiles::new([
            (
                RuntimeFileRole::RuntimeExecutable,
                PathBuf::from(format!("/vendor/v{version}/bin/firecracker")),
            ),
            (
                RuntimeFileRole::LauncherExecutable,
                PathBuf::from(format!("/vendor/v{version}/bin/jailer")),
            ),
        ]),
    }
}

fn startup(allowed: &str, default: &str) -> ExecutionPolicy {
    ExecutionPolicy::parse(
        Some(allowed),
        Some(default),
        &CompiledEnvironment::for_architecture(Arch::X86_64).unwrap(),
    )
    .unwrap()
}

/// An AMI can carry unverified copies with the same version and digest a
/// binding names. They run unbound legacy work, never the binding, so even an
/// allowed binding isn't usable through them.
#[test]
fn unverified_external_copies_are_not_usable_bindings() {
    let e0 = compiled("e0");
    let candidate = compiled("fc-1.17.0-linux-6.18.44");
    let kernel = candidate
        .kernel()
        .expect("the Firecracker fixture has a kernel");
    let inventory = RuntimeInventory {
        runtimes: vec![external_runtime("1.17.0", "12.0.0")],
        components: vec![kernel_installation(
            kernel_metadata(kernel.version.clone(), kernel.sha256).unwrap(),
            PathBuf::from("/vendor/kernels/6.18.44/vmlinux.bin"),
            false,
            false,
        )],
        defaults: Vec::new(),
        fingerprints: Vec::new(),
        environments: BTreeMap::new(),
        #[cfg(feature = "artifact-store")]
        unavailable: Vec::new(),
        retry_after: None,
        initial_environment: Some(e0.definition().clone()),
    };
    let requested = candidate.definition().execution_requirements().unwrap();
    if host_arch().unwrap() == Arch::X86_64 {
        let resolved = inventory
            .resolve(
                Some(&requested),
                RuntimeKind::Firecracker,
                ExecutionSelectionMode::NewColdBoot,
            )
            .expect("the external copies satisfy the binding's artifact identities");
        assert_eq!(resolved.environment, None);
    }
    let policy = startup("e0,fc-1.17.0-linux-6.18.44", "e0");
    assert!(!inventory.is_cold_boot_usable(&candidate, &policy));
    assert!(
        !inventory.is_cold_boot_usable(&e0, &policy),
        "E0 has no installed artifacts here"
    );
}

#[test]
fn observation_reports_the_whole_catalog_with_keys_and_edges() {
    let catalog = CompiledEnvironment::for_architecture(Arch::X86_64).unwrap();
    let e0 = compiled("e0").definition().reference();
    let e1 = compiled("fc-1.17.0-linux-6.1.155").definition().reference();
    let inventory = RuntimeInventory::discover(
        std::iter::empty(),
        "/nonexistent/assets",
        "/nonexistent/kernels",
    )
    .unwrap();
    let observation = inventory
        .environment_observation(&catalog, &startup("e0", "e0"))
        .expect("the compiled catalog always yields an observation");
    assert!(
        observation.usable().is_empty(),
        "discovery without verification proves no binding"
    );
    assert_eq!(observation.known().len(), catalog.len());
    for entry in &catalog {
        let known = observation.get(entry.definition().reference()).unwrap();
        assert_eq!(known.definition(), entry.definition());
        // CH labels omit the patch, which keys as zero.
        let mut expected: Vec<u32> = entry
            .release()
            .release
            .version
            .as_str()
            .split('.')
            .map(|part| part.parse().unwrap())
            .collect();
        expected.resize(3, 0);
        assert_eq!(
            known.runtime_version_sort_key().unwrap().components(),
            expected
        );
        assert_eq!(
            known.upgrade_edges().iter().copied().collect::<Vec<_>>(),
            entry.upgrade_targets()
        );
        assert!(known.memory_edges().is_empty());
    }
    // The certified cold-boot edges, E0 to E1 and back, carry no memory.
    assert_eq!(
        observation
            .get(e0)
            .unwrap()
            .upgrade_edges()
            .iter()
            .collect::<Vec<_>>(),
        [&e1]
    );
    assert_eq!(
        observation
            .get(e1)
            .unwrap()
            .upgrade_edges()
            .iter()
            .collect::<Vec<_>>(),
        [&e0]
    );
}

// E0 and its candidates are qualified for x86_64 only, so resolution of
// their bindings is only meaningful on an x86_64 host.
#[cfg(all(feature = "artifact-store", target_arch = "x86_64"))]
mod managed {
    use super::*;
    use crate::artifact_store::test_support::*;
    use crate::artifact_store::{ArtifactStore, KERNEL_IMAGE};
    use crate::catalog::{InitialEnvironment, PinnedKernel};

    fn kernel_pin(version: &str, line: &str, fill: u8) -> PinnedKernel {
        PinnedKernel {
            version: version.parse().unwrap(),
            runtime: RuntimeKind::Firecracker,
            runtime_line: line.parse().unwrap(),
            architecture: Arch::X86_64,
            size: binary(fill).len() as u64,
            sha256: sha256(&binary(fill)),
        }
    }

    fn publish_kernel(fixture: &Fixture, job: &str, pin: &PinnedKernel, fill: u8) {
        let source = fixture
            .staging
            .join("firecracker/kernels")
            .join(job)
            .join(pin.version.as_str())
            .join(KERNEL_IMAGE);
        std::fs::create_dir_all(source.parent().unwrap()).unwrap();
        std::fs::write(source, binary(fill)).unwrap();
        fixture.store.publish_kernel(pin, &staged(job)).unwrap();
    }

    /// Model an atomic rewrite that same-size writes in one tick would hide.
    fn replace_bytes(path: &Path, bytes: &[u8]) {
        let replacement = tempfile::NamedTempFile::new_in(path.parent().unwrap()).unwrap();
        std::fs::write(replacement.path(), bytes).unwrap();
        std::fs::set_permissions(
            replacement.path(),
            std::fs::metadata(path).unwrap().permissions(),
        )
        .unwrap();
        replacement.persist(path).unwrap();
    }

    /// Usable is cold-boot evidence within the startup ceiling: allowed by the
    /// startup policy, and resolved as itself from verified artifacts.
    #[test]
    fn usable_requires_the_startup_ceiling_and_a_verified_cold_boot() {
        let fixture = Fixture::new();
        let initial =
            InitialEnvironment::for_test(pin("1.15.1", 1), kernel_pin("6.1.155", "1.15", 42));
        publish_kernel(&fixture, "initial", initial.kernel(), 42);
        let e0 = initial.compiled_environment().unwrap();
        // A new runtime and kernel, and a new runtime over E0's kernel (as E1 is).
        let candidate = CompiledEnvironment::for_test(
            pin("1.17.0", 7),
            kernel_pin("6.18.44", "1.17", 44),
            "12.0.0",
            true,
        )
        .named_for_test("candidate");
        let shared = CompiledEnvironment::for_test(
            pin("1.17.0", 7),
            initial.kernel().clone(),
            "12.0.0",
            true,
        )
        .named_for_test("shared-kernel");
        let catalog = [e0.clone(), candidate.clone(), shared.clone()];
        let cache = fixture._dir.path().join("cache/firecracker");
        std::fs::create_dir_all(&cache).unwrap();
        let kernels = cache.join("kernels");
        // Discover as node-manager does: E0, then every non-E0 binding the
        // startup policy allows.
        let discover = |policy: &ExecutionPolicy| {
            let mut inventory = RuntimeInventory::discover_with_store(
                &fixture.store,
                &initial,
                &fixture._dir.path().join("vendor"),
                &cache.join("assets"),
                &kernels,
            )
            .unwrap();
            for entry in catalog.iter().filter(|entry| {
                !entry.is_initial() && policy.allows(entry.definition().reference())
            }) {
                inventory
                    .include_environment(&fixture.store, entry, &kernels)
                    .unwrap();
            }
            inventory
        };
        let policy = |allowed: &str, default: &str| {
            ExecutionPolicy::parse(Some(allowed), Some(default), &catalog).unwrap()
        };
        let usable =
            |inventory: &RuntimeInventory, startup: &ExecutionPolicy| -> Vec<&'static str> {
                catalog
                    .iter()
                    .filter(|entry| inventory.is_cold_boot_usable(entry, startup))
                    .map(CompiledEnvironment::name)
                    .collect()
            };
        let only_e0 = policy("e0", "e0");
        let all = policy("e0,candidate,shared-kernel", "e0");
        let without_e0 = policy("candidate,shared-kernel", "candidate");

        // Allowed but not installed: only E0 is usable.
        assert_eq!(usable(&discover(&all), &all), ["e0"]);

        fixture.place_release("1.17.0", 7);
        let candidate_kernel = candidate
            .kernel()
            .expect("the Firecracker fixture has a kernel");
        publish_kernel(&fixture, "candidate", candidate_kernel, 44);
        // Installed outside the startup ceiling: never usable, and discovery
        // never includes it, so it doesn't resolve as itself under any policy.
        let ceiling = discover(&only_e0);
        assert_eq!(usable(&ceiling, &only_e0), ["e0"]);
        assert_eq!(usable(&ceiling, &all), ["e0"]);
        let installed = discover(&all);
        assert_eq!(
            usable(&installed, &all),
            ["e0", "candidate", "shared-kernel"]
        );
        let observation = installed.environment_observation(&catalog, &all).unwrap();
        assert_eq!(observation.usable().len(), 3);

        // An installed E0 the startup allowlist excludes isn't usable either.
        assert_eq!(
            usable(&discover(&without_e0), &without_e0),
            ["candidate", "shared-kernel"]
        );

        // A corrupt execution-cache copy fails cold boot, so it isn't usable,
        // even though the managed store copy still verifies.
        let image = ArtifactStore::execution_kernel_path(&cache, candidate_kernel).unwrap();
        assert!(image.exists(), "selection staged the candidate kernel");
        replace_bytes(&image, &binary(98));
        assert!(!installed.check_files_unchanged());
        assert_eq!(usable(&discover(&all), &all), ["e0", "shared-kernel"]);
    }

    #[test]
    fn published_runtimes_carry_their_sort_keys() {
        let fixture = Fixture::new();
        let initial =
            InitialEnvironment::for_test(pin("1.15.1", 1), kernel_pin("6.1.155", "1.15", 42));
        publish_kernel(&fixture, "initial", initial.kernel(), 42);
        let cache = fixture._dir.path().join("cache/firecracker");
        std::fs::create_dir_all(&cache).unwrap();
        let inventory = RuntimeInventory::discover_with_store(
            &fixture.store,
            &initial,
            &fixture._dir.path().join("vendor"),
            &cache.join("assets"),
            &cache.join("kernels"),
        )
        .unwrap();
        let report = inventory.to_proto();
        let [runtime] = report.runtimes.as_slice() else {
            panic!("expected only E0: {:?}", report.runtimes);
        };
        assert_eq!(runtime.version_sort_key, [1, 15, 1]);
    }
}
