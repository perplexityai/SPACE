use std::cell::{Cell, RefCell};
use std::io::{Seek as _, Write as _};
use std::os::unix::fs::{PermissionsExt as _, symlink};

use super::*;

const LIMITS: ScanLimits = ScanLimits {
    max_entries_per_root: 64,
    max_digest_bytes: 1 << 20,
    deadline: Duration::from_secs(60),
};

/// A clock that advances by `step` on every read, so a scan's deadline
/// expires after a predictable number of checks.
struct SteppingClock {
    now: Cell<Instant>,
    step: Duration,
}

impl SteppingClock {
    fn new(step: Duration) -> Self {
        Self {
            now: Cell::new(Instant::now()),
            step,
        }
    }
}

impl ScanClock for SteppingClock {
    fn now(&self) -> Instant {
        let now = self.now.get();
        self.now.set(now + self.step);
        now
    }
}

fn elf_header(machine: u16) -> Vec<u8> {
    let mut header = vec![0u8; 64];
    header[..4].copy_from_slice(b"\x7fELF");
    header[4] = 2; // ELFCLASS64
    header[5] = 1; // little-endian
    header[6] = 1; // EV_CURRENT
    header[16..18].copy_from_slice(&2u16.to_le_bytes()); // ET_EXEC
    header[20..24].copy_from_slice(&1u32.to_le_bytes()); // EV_CURRENT
    header[52..54].copy_from_slice(&64u16.to_le_bytes()); // e_ehsize
    header[18..20].copy_from_slice(&machine.to_le_bytes());
    header
}

/// A clock that runs `action` once, on its `trigger`-th reading. A kernel-only
/// scan of one cache kernel reads it once at the start, once for the entry,
/// and once before each hash chunk, so reading 4 falls between the first and
/// second chunk.
struct TriggerClock<F> {
    readings: Cell<usize>,
    trigger: usize,
    action: RefCell<Option<F>>,
}

impl<F: FnOnce()> TriggerClock<F> {
    fn new(trigger: usize, action: F) -> Self {
        Self {
            readings: Cell::new(0),
            trigger,
            action: RefCell::new(Some(action)),
        }
    }
}

impl<F: FnOnce()> ScanClock for TriggerClock<F> {
    fn now(&self) -> Instant {
        let reading = self.readings.get() + 1;
        self.readings.set(reading);
        if reading == self.trigger
            && let Some(action) = self.action.borrow_mut().take()
        {
            action();
        }
        Instant::now()
    }
}

/// Real time, but reporting every file change as long past, so digests of
/// files a test just wrote are cacheable.
struct SettledClock;

impl ScanClock for SettledClock {
    fn now(&self) -> Instant {
        Instant::now()
    }

    fn wall_time(&self) -> SystemTime {
        SystemTime::now() + Duration::from_secs(3600)
    }
}

const KERNELS_ONLY: ArtifactKinds = ArtifactKinds {
    firmware: false,
    runtimes: false,
    kernels: true,
};

fn roots(base: &Path) -> ScanRoots {
    ScanRoots {
        runtime_releases: vec![RuntimeInstallationRoot::new(
            RuntimeKind::Firecracker,
            base.join("vendor/firecracker"),
        )],
        vendor_kernels: base.join("vendor/kernels"),
        managed_kernels: None,
        managed_releases: Vec::new(),
        cache_kernels: base.join("cache/firecracker/kernels"),
        cache_legacy_assets: base.join("cache/firecracker/assets"),
        runtime_aliases: vec![base.join("bin/firecracker"), base.join("bin/jailer")],
    }
}

/// Releases of Firecracker, the one runtime with a release layout.
const FC_RELEASE: ArtifactOrigin = ArtifactOrigin::runtime_release(RuntimeKind::Firecracker);

/// The Firecracker release root of `roots`.
fn fc_releases(roots: &ScanRoots) -> PathBuf {
    roots.runtime_releases[0].path.clone()
}

fn write(path: &Path, contents: &[u8]) {
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, contents).unwrap();
}

fn write_mode(path: &Path, contents: &[u8], mode: u32) {
    write(path, contents);
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode)).unwrap();
}

fn release(roots: &ScanRoots, version: &str) {
    let dir = fc_releases(roots).join(version).join("bin");
    write_mode(
        &dir.join("firecracker"),
        &elf_header(ELF_MACHINE_X86_64),
        0o755,
    );
    write_mode(&dir.join("jailer"), &elf_header(ELF_MACHINE_X86_64), 0o755);
}

fn kernel_image(contents: &[u8]) -> Vec<u8> {
    let mut image = elf_header(ELF_MACHINE_X86_64);
    image.extend_from_slice(contents);
    image
}

fn sha256(bytes: &[u8]) -> Digest {
    Digest::sha256(Sha256::digest(bytes).into())
}

fn run(roots: &ScanRoots) -> ArtifactScan {
    scan(
        roots,
        LIMITS,
        ArtifactKinds::ALL,
        &SystemScanClock,
        &mut DigestCache::default(),
    )
}

fn find<'a>(scan: &'a ArtifactScan, origin: ArtifactOrigin, name: &str) -> &'a ScannedArtifact {
    scan.artifacts
        .iter()
        .find(|artifact| artifact.origin == origin && artifact.name == name)
        .unwrap_or_else(|| panic!("no {origin:?} artifact named {name}: {:#?}", scan.artifacts))
}

#[test]
fn an_empty_node_lists_nothing_and_is_complete() {
    let dir = tempfile::tempdir().unwrap();
    let roots = roots(dir.path());

    let scan = run(&roots);

    assert!(scan.artifacts.is_empty());
    assert!(scan.aliases.is_empty());
    assert!(scan.is_complete(), "{:?}", scan.issues);
    assert_eq!(scan.roots.len(), 4);
    assert!(
        scan.roots
            .iter()
            .all(|root| root.presence == RootPresence::Absent),
        "{:?}",
        scan.roots
    );
}

#[test]
fn damaged_and_unknown_entries_are_listed_beside_valid_ones() {
    let dir = tempfile::tempdir().unwrap();
    let roots = roots(dir.path());
    release(&roots, "v1.15.1");
    // Missing jailer.
    write(
        &fc_releases(&roots).join("v1.16.0/bin/firecracker"),
        &elf_header(ELF_MACHINE_X86_64),
    );
    // Well-formed but absent from the state-format map. The strict
    // inventory fails its whole scan on this; the listing must not.
    release(&roots, "v9.9.9");
    std::fs::create_dir_all(fc_releases(&roots).join("not a version")).unwrap();
    std::fs::create_dir_all(fc_releases(&roots).join(".staging")).unwrap();
    write(&fc_releases(&roots).join("README"), b"notes");

    let vendor_image = kernel_image(b"vendor kernel");
    write(
        &roots.vendor_kernels.join("6.1.155/vmlinux.bin"),
        &vendor_image,
    );
    std::fs::create_dir_all(roots.cache_kernels.join("6.1.100")).unwrap();
    write(
        &roots.cache_kernels.join("6.1.155/vmlinux.bin"),
        &vendor_image,
    );

    let scan = run(&roots);

    let valid = find(&scan, FC_RELEASE, "v1.15.1");
    assert_eq!(valid.status, ArtifactStatus::Valid, "{valid:?}");
    assert_eq!(
        valid.version,
        Some(ScannedVersion::Runtime(
            RuntimeVersion::parse("1.15.1").unwrap()
        ))
    );
    assert_eq!(valid.version_source, Some(VersionSource::DirectoryName));
    assert_eq!(valid.architecture, Some(Arch::X86_64));
    assert_eq!(
        valid
            .state_format
            .as_ref()
            .map(|state| state.version.as_str()),
        Some("9.0.0")
    );
    assert_eq!(valid.digest, None, "runtimes are not hashed");

    let incomplete = find(&scan, FC_RELEASE, "v1.16.0");
    assert_eq!(incomplete.status, ArtifactStatus::Incomplete);
    assert!(incomplete.detail.as_deref().unwrap().contains("jailer"));

    let unsupported = find(&scan, FC_RELEASE, "v9.9.9");
    assert_eq!(unsupported.status, ArtifactStatus::Unsupported);
    assert_eq!(unsupported.state_format, None);
    assert_eq!(
        unsupported.version,
        Some(ScannedVersion::Runtime(
            RuntimeVersion::parse("9.9.9").unwrap()
        ))
    );

    let unrecognized = find(&scan, FC_RELEASE, "not a version");
    assert_eq!(unrecognized.status, ArtifactStatus::Unrecognized);
    assert_eq!(unrecognized.version, None);

    let vendor = find(&scan, ArtifactOrigin::VENDOR_KERNEL, "6.1.155");
    assert_eq!(vendor.status, ArtifactStatus::Valid, "{vendor:?}");
    assert_eq!(vendor.digest, Some(sha256(&vendor_image)));
    assert_eq!(vendor.architecture, Some(Arch::X86_64));

    let cache = find(&scan, ArtifactOrigin::CACHE_KERNEL, "6.1.155");
    assert_eq!(cache.status, ArtifactStatus::Valid);
    let missing = find(&scan, ArtifactOrigin::CACHE_KERNEL, "6.1.100");
    assert_eq!(missing.status, ArtifactStatus::Incomplete);

    assert!(
        scan.artifacts.iter().all(|artifact| artifact.management
            == ArtifactManagement::External(ArtifactProvider::Unknown)),
        "the runtime manager has installed nothing, so owns nothing it lists"
    );
    assert!(
        scan.artifacts
            .iter()
            .all(|artifact| !artifact.name.starts_with('.') && artifact.name != "README"),
        "staging entries and plain files are not artifacts: {:?}",
        scan.artifacts
    );
    assert!(scan.is_complete(), "{:?}", scan.issues);
}

#[test]
fn compatibility_aliases_are_reported_as_observed_links() {
    let dir = tempfile::tempdir().unwrap();
    let roots = roots(dir.path());
    release(&roots, "v1.15.1");
    symlink("v1.15.1", fc_releases(&roots).join("default")).unwrap();
    std::fs::create_dir_all(dir.path().join("bin")).unwrap();
    symlink(
        fc_releases(&roots).join("default/bin/firecracker"),
        dir.path().join("bin/firecracker"),
    )
    .unwrap();
    write(&dir.path().join("bin/jailer"), b"not a link");
    write(
        &roots.cache_kernels.join("6.1.155/vmlinux.bin"),
        &kernel_image(b"k"),
    );
    symlink("6.1.155", roots.cache_kernels.join("default")).unwrap();
    symlink("6.1.999", roots.cache_kernels.join("legacy-default")).unwrap();

    let scan = run(&roots);
    let alias = |path: PathBuf| {
        scan.aliases
            .iter()
            .find(|alias| alias.path == path)
            .unwrap_or_else(|| panic!("no alias {}: {:#?}", path.display(), scan.aliases))
    };

    let release_dir = std::fs::canonicalize(fc_releases(&roots).join("v1.15.1")).unwrap();
    let runtime_default = alias(fc_releases(&roots).join("default"));
    assert_eq!(runtime_default.target, Some(PathBuf::from("v1.15.1")));
    assert_eq!(runtime_default.resolved.as_ref(), Some(&release_dir));
    assert_eq!(
        alias(dir.path().join("bin/firecracker")).resolved,
        Some(release_dir.join("bin/firecracker"))
    );
    let not_a_link = alias(dir.path().join("bin/jailer"));
    assert_eq!(not_a_link.target, None);
    assert!(
        not_a_link
            .detail
            .as_deref()
            .unwrap()
            .contains("not a symlink")
    );

    let dangling = alias(roots.cache_kernels.join("legacy-default"));
    assert_eq!(dangling.target, Some(PathBuf::from("6.1.999")));
    assert_eq!(dangling.resolved, None);
    assert!(dangling.detail.is_some());

    assert!(
        scan.artifacts
            .iter()
            .all(|artifact| artifact.name != "default"
                || artifact.origin == ArtifactOrigin::LEGACY_CACHE_KERNEL),
        "markers are aliases, not artifacts: {:?}",
        scan.artifacts
    );
}

#[test]
fn flat_kernels_read_their_version_from_the_image_banner() {
    let dir = tempfile::tempdir().unwrap();
    let roots = roots(dir.path());
    release(&roots, "v1.15.1");
    let image = kernel_image(b"\0Linux version 6.1.155+ (builder@host) (gcc) #1 SMP\n\0");
    write(
        &roots.cache_legacy_assets.join("default/kernel/vmlinux.bin"),
        &image,
    );
    write(
        &fc_releases(&roots).join("v1.15.1/kernel/vmlinux.bin"),
        &image,
    );
    let firecracker = |version: &str| RuntimeIdentity {
        kind: RuntimeKind::Firecracker,
        version: RuntimeVersion::parse(version).unwrap(),
    };
    let mut cache = DigestCache::default();

    // A repeat scan reports the same, from the cache once the files settle.
    for pass in ["read", "cached"] {
        let scan = scan(
            &roots,
            LIMITS,
            ArtifactKinds::ALL,
            &SystemScanClock,
            &mut cache,
        );
        for (origin, name, for_runtime) in [
            (ArtifactOrigin::LEGACY_CACHE_KERNEL, "default", None),
            (
                ArtifactOrigin::RELEASE_BUNDLED_KERNEL,
                "v1.15.1",
                Some(firecracker("1.15.1")),
            ),
        ] {
            let flat = find(&scan, origin, name);
            assert_eq!(flat.status, ArtifactStatus::Valid, "{flat:?}");
            assert_eq!(
                flat.version,
                Some(ScannedVersion::Component(
                    ArtifactVersion::parse("6.1.155").unwrap()
                )),
                "{pass} {origin:?}"
            );
            assert_eq!(flat.version_source, Some(VersionSource::ImageBanner));
            assert_eq!(flat.digest, Some(sha256(&image)));
            assert_eq!(flat.for_runtime, for_runtime, "{origin:?}");
        }
    }
}

#[test]
fn a_flat_kernel_without_a_banner_has_no_version() {
    let dir = tempfile::tempdir().unwrap();
    let roots = roots(dir.path());
    let image = kernel_image(b"flat kernel");
    write(
        &roots.cache_legacy_assets.join("default/kernel/vmlinux.bin"),
        &image,
    );

    let scan = run(&roots);

    let flat = find(&scan, ArtifactOrigin::LEGACY_CACHE_KERNEL, "default");
    assert_eq!(flat.status, ArtifactStatus::Valid, "{flat:?}");
    assert_eq!(flat.version, None);
    assert_eq!(flat.version_source, None);
    assert_eq!(flat.digest, Some(sha256(&image)));
}

#[test]
fn a_directory_version_wins_over_the_banner() {
    let dir = tempfile::tempdir().unwrap();
    let roots = roots(dir.path());
    write(
        &roots.vendor_kernels.join("5.10.245-no-acpi/vmlinux.bin"),
        &kernel_image(b"\0Linux version 5.10.245 (b@h) #1\n"),
    );

    let scan = run(&roots);

    let kernel = find(&scan, ArtifactOrigin::VENDOR_KERNEL, "5.10.245-no-acpi");
    assert_eq!(
        kernel.version,
        Some(ScannedVersion::Component(
            ArtifactVersion::parse("5.10.245-no-acpi").unwrap()
        ))
    );
    assert_eq!(kernel.version_source, Some(VersionSource::DirectoryName));
    assert_eq!(kernel.for_runtime, None);
}

#[test]
fn damaged_runtime_binaries_are_invalid_but_unknown_machines_are_not() {
    let dir = tempfile::tempdir().unwrap();
    let roots = roots(dir.path());
    let elf = elf_header(ELF_MACHINE_X86_64);
    let changed_byte = |index: usize, value: u8| {
        let mut header = elf.clone();
        header[index] = value;
        header
    };
    let mut elf32 = elf_header(40);
    elf32[4] = 1;
    elf32[40..42].copy_from_slice(&52u16.to_le_bytes());
    elf32.truncate(52);
    let cases = [
        ("empty", Vec::new(), 0o755, ArtifactStatus::Invalid),
        (
            "not ELF",
            b"#!/bin/sh\n".to_vec(),
            0o755,
            ArtifactStatus::Invalid,
        ),
        (
            "not executable",
            elf.clone(),
            0o644,
            ArtifactStatus::Invalid,
        ),
        (
            "truncated after machine",
            elf[..20].to_vec(),
            0o755,
            ArtifactStatus::Invalid,
        ),
        (
            "truncated header",
            elf[..63].to_vec(),
            0o755,
            ArtifactStatus::Invalid,
        ),
        (
            "invalid class",
            changed_byte(4, 0),
            0o755,
            ArtifactStatus::Invalid,
        ),
        (
            "invalid identification version",
            changed_byte(6, 0),
            0o755,
            ArtifactStatus::Invalid,
        ),
        (
            "not an executable object",
            changed_byte(16, 1),
            0o755,
            ArtifactStatus::Invalid,
        ),
        (
            "invalid ELF version",
            changed_byte(20, 0),
            0o755,
            ArtifactStatus::Invalid,
        ),
        (
            "invalid header size",
            changed_byte(52, 0),
            0o755,
            ArtifactStatus::Invalid,
        ),
        (
            "unknown ELF64 machine",
            elf_header(40),
            0o755,
            ArtifactStatus::Valid,
        ),
        ("unknown ELF32 machine", elf32, 0o755, ArtifactStatus::Valid),
        (
            "PIE executable",
            changed_byte(16, 3),
            0o755,
            ArtifactStatus::Valid,
        ),
    ];
    for binary in ["firecracker", "jailer"] {
        for (case, bytes, mode, expected) in &cases {
            release(&roots, "v1.15.1");
            write_mode(
                &fc_releases(&roots).join("v1.15.1/bin").join(binary),
                bytes,
                *mode,
            );
            let scan = run(&roots);
            let artifact = find(&scan, FC_RELEASE, "v1.15.1");
            assert_eq!(artifact.status, *expected, "{binary}: {case}: {artifact:?}");
            if *expected == ArtifactStatus::Invalid {
                assert!(artifact.detail.as_deref().unwrap().contains(binary));
                assert_eq!(
                    artifact.state_format, None,
                    "no evidence for a broken release"
                );
            } else if binary == "firecracker" && case.starts_with("unknown") {
                assert_eq!(artifact.architecture, None);
            }
        }
    }
}

#[test]
fn a_symlinked_cache_kernel_is_invalid_for_jail_hard_links() {
    let dir = tempfile::tempdir().unwrap();
    let roots = roots(dir.path());
    let vendor_image = roots.vendor_kernels.join("6.1.155/vmlinux.bin");
    write(&vendor_image, &kernel_image(b"k"));
    std::fs::create_dir_all(roots.cache_kernels.join("6.1.155")).unwrap();
    symlink(
        &vendor_image,
        roots.cache_kernels.join("6.1.155/vmlinux.bin"),
    )
    .unwrap();

    let scan = run(&roots);

    let cache = find(&scan, ArtifactOrigin::CACHE_KERNEL, "6.1.155");
    assert_eq!(cache.status, ArtifactStatus::Invalid);
    assert!(cache.digest.is_some(), "the bytes are still identified");
    assert_eq!(
        find(&scan, ArtifactOrigin::VENDOR_KERNEL, "6.1.155").status,
        ArtifactStatus::Valid
    );
}

#[test]
fn a_fifo_in_place_of_a_kernel_image_does_not_block_the_scan() {
    let dir = tempfile::tempdir().unwrap();
    let roots = roots(dir.path());
    std::fs::create_dir_all(roots.cache_kernels.join("6.1.155")).unwrap();
    let status = std::process::Command::new("mkfifo")
        .arg(roots.cache_kernels.join("6.1.155/vmlinux.bin"))
        .status()
        .unwrap();
    assert!(status.success());

    // A blocking open of a FIFO with no writer never returns, so reaching the
    // assertions at all is the property under test.
    let scan = run(&roots);

    let kernel = find(&scan, ArtifactOrigin::CACHE_KERNEL, "6.1.155");
    assert_eq!(kernel.status, ArtifactStatus::Invalid, "{kernel:?}");
    assert!(scan.is_complete());
}

#[test]
fn an_entry_limit_hit_is_reported_as_incomplete() {
    let dir = tempfile::tempdir().unwrap();
    let roots = roots(dir.path());
    for version in ["v1.15.1", "v1.16.0", "v1.16.1"] {
        release(&roots, version);
    }
    let limits = ScanLimits {
        max_entries_per_root: 2,
        ..LIMITS
    };

    let scan = scan(
        &roots,
        limits,
        ArtifactKinds::ALL,
        &SystemScanClock,
        &mut DigestCache::default(),
    );

    assert_eq!(scan.artifacts.len(), 2);
    assert!(!scan.is_complete());
    assert_eq!(scan.issues.len(), 1);
    assert_eq!(scan.issues[0].kind, ScanIssueKind::EntryLimit);
    assert_eq!(scan.issues[0].path, fc_releases(&roots));
}

#[test]
fn an_expired_deadline_stops_the_scan_and_is_reported() {
    let dir = tempfile::tempdir().unwrap();
    let roots = roots(dir.path());
    for version in ["v1.15.1", "v1.16.0", "v1.16.1"] {
        release(&roots, version);
    }
    write(
        &roots.cache_kernels.join("6.1.155/vmlinux.bin"),
        &kernel_image(b"k"),
    );
    // The start reading plus one check per entry: two entries fit.
    let clock = SteppingClock::new(Duration::from_secs(1));
    let limits = ScanLimits {
        deadline: Duration::from_millis(2500),
        ..LIMITS
    };

    let scan = scan(
        &roots,
        limits,
        ArtifactKinds::ALL,
        &clock,
        &mut DigestCache::default(),
    );

    assert_eq!(scan.artifacts.len(), 2, "{:#?}", scan.artifacts);
    assert!(!scan.is_complete());
    assert_eq!(
        scan.issues
            .iter()
            .map(|issue| issue.kind)
            .collect::<Vec<_>>(),
        [ScanIssueKind::Deadline],
        "the deadline is reported once, however many checks follow"
    );
}

#[test]
fn a_deadline_during_hashing_leaves_the_digest_unknown() {
    let dir = tempfile::tempdir().unwrap();
    let roots = roots(dir.path());
    write(
        &roots.cache_kernels.join("6.1.155/vmlinux.bin"),
        &kernel_image(&vec![7u8; 3 * HASH_CHUNK_BYTES]),
    );
    let clock = SteppingClock::new(Duration::from_secs(1));
    let limits = ScanLimits {
        deadline: Duration::from_millis(3500),
        max_digest_bytes: u64::MAX,
        ..LIMITS
    };

    let scan = scan(
        &roots,
        limits,
        ArtifactKinds {
            firmware: false,
            runtimes: false,
            kernels: true,
        },
        &clock,
        &mut DigestCache::default(),
    );

    let kernel = find(&scan, ArtifactOrigin::CACHE_KERNEL, "6.1.155");
    assert_eq!(kernel.digest, None);
    assert!(kernel.detail.as_deref().unwrap().contains("deadline"));
    assert!(!scan.is_complete());
}

#[test]
fn digests_are_reused_until_the_image_changes() {
    let dir = tempfile::tempdir().unwrap();
    let roots = roots(dir.path());
    let image_path = roots.cache_kernels.join("6.1.155/vmlinux.bin");
    let image = kernel_image(b"first");
    write(&image_path, &image);
    let mut cache = DigestCache::default();
    let kernels_only = ArtifactKinds {
        firmware: false,
        runtimes: false,
        kernels: true,
    };

    let first = scan(&roots, LIMITS, kernels_only, &SettledClock, &mut cache);
    assert_eq!(
        find(&first, ArtifactOrigin::CACHE_KERNEL, "6.1.155").digest,
        Some(sha256(&image))
    );
    assert_eq!(cache.len(), 1);

    // A zero digest budget can only be answered from the cache.
    let no_hashing = ScanLimits {
        max_digest_bytes: 0,
        ..LIMITS
    };
    let cached = scan(&roots, no_hashing, kernels_only, &SettledClock, &mut cache);
    assert_eq!(
        find(&cached, ArtifactOrigin::CACHE_KERNEL, "6.1.155").digest,
        Some(sha256(&image))
    );

    // A runtime-only scan leaves kernel digests alone.
    let runtimes_only = ArtifactKinds {
        firmware: false,
        runtimes: true,
        kernels: false,
    };
    scan(&roots, LIMITS, runtimes_only, &SettledClock, &mut cache);
    assert_eq!(cache.len(), 1);

    write(&image_path, &kernel_image(b"rewritten"));
    let changed = scan(&roots, no_hashing, kernels_only, &SettledClock, &mut cache);
    let kernel = find(&changed, ArtifactOrigin::CACHE_KERNEL, "6.1.155");
    assert_eq!(kernel.digest, None);
    assert!(kernel.detail.as_deref().unwrap().contains("digest limit"));
    assert!(cache.is_empty(), "entries without a digest are not cached");
    assert!(!changed.is_complete(), "a digest-limit hit is a limit hit");
    assert_eq!(changed.issues[0].kind, ScanIssueKind::DigestLimit);
    assert_eq!(changed.issues[0].path, image_path);
}

#[test]
fn a_just_changed_image_is_reported_but_not_cached() {
    let dir = tempfile::tempdir().unwrap();
    let roots = roots(dir.path());
    let image = kernel_image(b"fresh");
    write(&roots.cache_kernels.join("6.1.155/vmlinux.bin"), &image);
    let mut cache = DigestCache::default();

    let scan = scan(&roots, LIMITS, KERNELS_ONLY, &SystemScanClock, &mut cache);

    assert_eq!(
        find(&scan, ArtifactOrigin::CACHE_KERNEL, "6.1.155").digest,
        Some(sha256(&image))
    );
    assert!(
        cache.is_empty(),
        "a second write in the same timestamp tick would keep this fingerprint"
    );
}

#[test]
fn an_image_that_grows_past_the_limit_while_hashed_gets_no_digest() {
    let dir = tempfile::tempdir().unwrap();
    let roots = roots(dir.path());
    let image_path = roots.cache_kernels.join("6.1.155/vmlinux.bin");
    write(
        &image_path,
        &kernel_image(&vec![1u8; 3 * HASH_CHUNK_BYTES / 2]),
    );
    let limits = ScanLimits {
        max_digest_bytes: 2 * HASH_CHUNK_BYTES as u64,
        ..LIMITS
    };
    // Under the limit when stat'ed; well over it by the second chunk.
    let clock = TriggerClock::new(4, || {
        let mut file = std::fs::OpenOptions::new()
            .append(true)
            .open(&image_path)
            .unwrap();
        file.write_all(&vec![2u8; 3 * HASH_CHUNK_BYTES]).unwrap();
    });
    let mut cache = DigestCache::default();

    let scan = scan(&roots, limits, KERNELS_ONLY, &clock, &mut cache);

    let kernel = find(&scan, ArtifactOrigin::CACHE_KERNEL, "6.1.155");
    assert_eq!(kernel.digest, None);
    assert!(kernel.detail.as_deref().unwrap().contains("digest limit"));
    assert_eq!(
        scan.issues
            .iter()
            .map(|issue| issue.kind)
            .collect::<Vec<_>>(),
        [ScanIssueKind::DigestLimit]
    );
    assert!(cache.is_empty());
}

#[test]
fn an_image_rewritten_while_hashed_gets_no_digest_or_identity() {
    let dir = tempfile::tempdir().unwrap();
    let roots = roots(dir.path());
    let image_path = roots.cache_legacy_assets.join("default/kernel/vmlinux.bin");
    let mut contents = b"Linux version 6.1.155 (b@h) #1\n".to_vec();
    contents.resize(2 * HASH_CHUNK_BYTES, 1);
    let original = kernel_image(&contents);
    write(&image_path, &original);
    // Backdate the mtime so the rewrite moves it even when both writes fall
    // in one coarse filesystem timestamp tick.
    std::fs::File::options()
        .write(true)
        .open(&image_path)
        .unwrap()
        .set_modified(SystemTime::now() - Duration::from_secs(3600))
        .unwrap();
    // Same size, different bytes, written in place after the first chunk:
    // the hash would cover old bytes then new ones.
    let clock = TriggerClock::new(4, || {
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .open(&image_path)
            .unwrap();
        file.rewind().unwrap();
        file.write_all(&kernel_image(&vec![2u8; 2 * HASH_CHUNK_BYTES]))
            .unwrap();
    });
    let mut cache = DigestCache::default();

    let scan = scan(
        &roots,
        ScanLimits {
            max_digest_bytes: u64::MAX,
            ..LIMITS
        },
        KERNELS_ONLY,
        &clock,
        &mut cache,
    );

    let kernel = find(&scan, ArtifactOrigin::LEGACY_CACHE_KERNEL, "default");
    assert_eq!(kernel.digest, None);
    assert_eq!(kernel.version, None, "no version from a torn read");
    assert_eq!(kernel.version_source, None);
    assert!(kernel.detail.as_deref().unwrap().contains("changed"));
    assert!(cache.is_empty(), "a torn digest is never cached");
}

#[test]
fn unreadable_release_entries_do_not_hide_bundled_kernels() {
    let dir = tempfile::tempdir().unwrap();
    let roots = roots(dir.path());
    std::fs::create_dir_all(fc_releases(&roots)).unwrap();
    let entry = fc_releases(&roots).join("v1.15.1");
    symlink("v1.15.1", &entry).unwrap();
    for kinds in [KERNELS_ONLY, ArtifactKinds::ALL] {
        let result = scan(
            &roots,
            LIMITS,
            kinds,
            &SystemScanClock,
            &mut DigestCache::default(),
        );
        assert!(
            !result.is_complete(),
            "unexamined bundled kernels must not look absent"
        );
        assert!(
            result
                .issues
                .iter()
                .any(|issue| issue.path == entry && issue.kind == ScanIssueKind::RootUnreadable)
        );
    }
}

#[test]
fn no_supported_kinds_skips_even_damaged_roots() {
    let dir = tempfile::tempdir().unwrap();
    let roots = roots(dir.path());
    write(&fc_releases(&roots), b"not a directory");
    let result = scan(
        &roots,
        LIMITS,
        ArtifactKinds {
            firmware: false,
            runtimes: false,
            kernels: false,
        },
        &SystemScanClock,
        &mut DigestCache::default(),
    );
    assert!(result.is_complete(), "{:?}", result.issues);
    assert!(result.artifacts.is_empty());
    assert!(result.roots.is_empty());
    assert!(result.aliases.is_empty());
}

#[test]
fn the_kind_filter_skips_other_roots() {
    let dir = tempfile::tempdir().unwrap();
    let roots = roots(dir.path());
    release(&roots, "v1.15.1");
    write(
        &fc_releases(&roots).join("v1.15.1/kernel/vmlinux.bin"),
        &kernel_image(b"k"),
    );
    write(
        &roots.cache_kernels.join("6.1.155/vmlinux.bin"),
        &kernel_image(b"k"),
    );

    let runtimes = scan(
        &roots,
        LIMITS,
        ArtifactKinds {
            firmware: false,
            runtimes: true,
            kernels: false,
        },
        &SystemScanClock,
        &mut DigestCache::default(),
    );
    assert!(
        runtimes
            .artifacts
            .iter()
            .all(|artifact| artifact.origin == FC_RELEASE)
    );
    assert_eq!(
        runtimes
            .roots
            .iter()
            .map(|root| root.origin)
            .collect::<Vec<_>>(),
        [FC_RELEASE]
    );

    let kernels = scan(
        &roots,
        LIMITS,
        KERNELS_ONLY,
        &SystemScanClock,
        &mut DigestCache::default(),
    );
    assert!(kernels.artifacts.iter().all(|artifact| artifact.origin.kind
        == ArtifactKind::Component(ArtifactComponentKind::Kernel)
        && artifact.role == Some(boot_component_role())));
    assert_eq!(kernels.artifacts.len(), 2, "{:#?}", kernels.artifacts);
    assert_eq!(
        kernels
            .roots
            .iter()
            .map(|root| root.origin)
            .collect::<Vec<_>>(),
        [
            // Read for release-bundled kernels.
            FC_RELEASE,
            ArtifactOrigin::VENDOR_KERNEL,
            ArtifactOrigin::CACHE_KERNEL,
            ArtifactOrigin::LEGACY_CACHE_KERNEL,
        ]
    );
}

#[test]
fn listing_leaves_the_tree_untouched() {
    let dir = tempfile::tempdir().unwrap();
    let roots = roots(dir.path());
    release(&roots, "v1.15.1");
    symlink("v1.15.1", fc_releases(&roots).join("default")).unwrap();
    write(
        &roots.cache_kernels.join("6.1.155/vmlinux.bin"),
        &kernel_image(b"k"),
    );
    symlink("6.1.155", roots.cache_kernels.join("default")).unwrap();
    symlink("missing", roots.cache_kernels.join("legacy-default")).unwrap();

    let before = snapshot(dir.path());
    run(&roots);
    run(&roots);

    assert_eq!(before, snapshot(dir.path()));
}

/// Everything a listing could disturb about one path.
#[derive(Debug, Eq, Ord, PartialEq, PartialOrd)]
struct PathState {
    path: PathBuf,
    mode: u32,
    link: Option<PathBuf>,
    contents: Vec<u8>,
    mtime: (i64, i64),
}

fn snapshot(root: &Path) -> Vec<PathState> {
    let mut entries = Vec::new();
    let mut pending = vec![root.to_path_buf()];
    while let Some(dir) = pending.pop() {
        for entry in std::fs::read_dir(&dir).unwrap() {
            let path = entry.unwrap().path();
            let metadata = std::fs::symlink_metadata(&path).unwrap();
            let link = metadata
                .file_type()
                .is_symlink()
                .then(|| std::fs::read_link(&path).unwrap());
            let contents = if metadata.is_file() {
                std::fs::read(&path).unwrap()
            } else {
                Vec::new()
            };
            if metadata.is_dir() {
                pending.push(path.clone());
            }
            entries.push(PathState {
                path,
                mode: metadata.mode(),
                link,
                contents,
                mtime: (metadata.mtime(), metadata.mtime_nsec()),
            });
        }
    }
    entries.sort();
    entries
}

#[test]
fn elf_headers_separate_unknown_machines_from_non_elf_files() {
    let elf = |architecture| ElfProbe::Elf { architecture };
    assert_eq!(
        parse_elf_header(&elf_header(ELF_MACHINE_X86_64)),
        elf(Some(Arch::X86_64))
    );
    assert_eq!(
        parse_elf_header(&elf_header(ELF_MACHINE_AARCH64)),
        elf(Some(Arch::Aarch64))
    );
    let mut big_endian = elf_header(0);
    big_endian[5] = 2;
    big_endian[16..18].copy_from_slice(&2u16.to_be_bytes());
    big_endian[20..24].copy_from_slice(&1u32.to_be_bytes());
    big_endian[52..54].copy_from_slice(&64u16.to_be_bytes());
    big_endian[18..20].copy_from_slice(&ELF_MACHINE_AARCH64.to_be_bytes());
    assert_eq!(parse_elf_header(&big_endian), elf(Some(Arch::Aarch64)));

    assert_eq!(parse_elf_header(&elf_header(40)), elf(None), "ARM32");
    assert_eq!(parse_elf_header(b"MZ not an ELF image"), ElfProbe::NotElf);
    assert_eq!(parse_elf_header(b"\x7fELF"), ElfProbe::NotElf, "truncated");
    assert_eq!(parse_elf_header(b""), ElfProbe::NotElf, "empty");
}

#[test]
fn each_runtime_root_is_scanned_with_its_own_layout() {
    let dir = tempfile::tempdir().unwrap();
    let mut roots = roots(dir.path());
    release(&roots, "v1.15.1");
    let other = dir.path().join("vendor/cloud-hypervisor");
    write_mode(
        &other.join("v54.0/bin/cloud-hypervisor"),
        &elf_header(ELF_MACHINE_AARCH64),
        0o755,
    );
    roots.runtime_releases.push(RuntimeInstallationRoot::new(
        RuntimeKind::CloudHypervisor,
        &other,
    ));

    let scan = run(&roots);

    assert_eq!(
        find(&scan, FC_RELEASE, "v1.15.1").status,
        ArtifactStatus::Valid
    );
    let origin = ArtifactOrigin::runtime_release(RuntimeKind::CloudHypervisor);
    let installed = find(&scan, origin, "v54.0");
    assert_eq!(installed.status, ArtifactStatus::Valid, "{installed:?}");
    assert_eq!(installed.architecture, Some(Arch::Aarch64));
    assert_eq!(installed.state_format, None);
    assert_eq!(
        installed.version,
        Some(ScannedVersion::Runtime(
            RuntimeVersion::parse("54.0").unwrap()
        ))
    );
    assert!(
        scan.roots
            .iter()
            .any(|root| root.origin == origin && root.presence == RootPresence::Present)
    );
}

#[test]
fn the_runtime_managers_kernel_root_is_scanned_once_beside_the_images() {
    let dir = tempfile::tempdir().unwrap();
    let mut roots = roots(dir.path());
    let managed = dir.path().join("vendor/node/kernels");
    write(
        &roots.vendor_kernels.join("5.10.245/vmlinux.bin"),
        &kernel_image(b"baked"),
    );
    write(
        &managed.join("6.1.155/vmlinux.bin"),
        &kernel_image(b"installed"),
    );
    roots.managed_kernels = Some(managed.clone());

    // Separate roots: both are listed.
    let scan = run(&roots);
    let mut listed: Vec<_> = scan
        .artifacts
        .iter()
        .filter(|artifact| artifact.origin == ArtifactOrigin::VENDOR_KERNEL)
        .map(|artifact| artifact.path.clone())
        .collect();
    listed.sort();
    assert_eq!(
        listed,
        [
            roots.vendor_kernels.join("5.10.245/vmlinux.bin"),
            managed.join("6.1.155/vmlinux.bin"),
        ]
    );

    // The image root a link to the runtime manager's: listed once, under
    // the runtime manager's path.
    std::fs::remove_dir_all(&roots.vendor_kernels).unwrap();
    symlink(&managed, &roots.vendor_kernels).unwrap();
    let scan = run(&roots);
    let listed: Vec<_> = scan
        .artifacts
        .iter()
        .filter(|artifact| artifact.origin == ArtifactOrigin::VENDOR_KERNEL)
        .map(|artifact| artifact.path.clone())
        .collect();
    assert_eq!(listed, [managed.join("6.1.155/vmlinux.bin")]);
}

#[test]
fn the_runtime_managers_release_root_is_scanned_once_beside_the_images() {
    let dir = tempfile::tempdir().unwrap();
    let mut roots = roots(dir.path());
    release(&roots, "v1.15.1");
    let managed = dir.path().join("vendor/node/firecracker");
    let bin = managed.join("v1.16.2/bin");
    write_mode(
        &bin.join("firecracker"),
        &elf_header(ELF_MACHINE_X86_64),
        0o755,
    );
    write_mode(&bin.join("jailer"), &elf_header(ELF_MACHINE_X86_64), 0o755);
    roots.managed_releases = vec![RuntimeInstallationRoot::new(
        RuntimeKind::Firecracker,
        &managed,
    )];

    let listed = |scan: &ArtifactScan| {
        let mut paths: Vec<_> = scan
            .artifacts
            .iter()
            .filter(|artifact| artifact.origin == FC_RELEASE)
            .map(|artifact| artifact.path.clone())
            .collect();
        paths.sort();
        paths
    };
    // Separate roots: both are listed.
    assert_eq!(
        listed(&run(&roots)),
        [fc_releases(&roots).join("v1.15.1"), managed.join("v1.16.2")]
    );

    // The image root a link to the runtime manager's: listed once, under
    // the path the runtime manager's records name.
    std::fs::remove_dir_all(fc_releases(&roots)).unwrap();
    symlink(&managed, fc_releases(&roots)).unwrap();
    assert_eq!(listed(&run(&roots)), [managed.join("v1.16.2")]);
}

#[test]
fn the_known_releases_are_the_ones_with_a_state_format() {
    let layout = ReleaseLayout::of(RuntimeKind::Firecracker).unwrap();
    let known = (layout.known_releases)();
    assert!(known.contains(&"1.15.1"), "{known:?}");
    for release in known {
        (layout.state_format.unwrap())(&RuntimeVersion::parse(release).unwrap()).unwrap();
    }
}

#[test]
fn managed_release_identity_is_measured_without_catalog_pins_and_respects_limits() {
    let dir = tempfile::tempdir().unwrap();
    let mut roots = roots(dir.path());
    release(&roots, "v9.9.9");
    let external = run(&roots);
    assert_eq!(find(&external, FC_RELEASE, "v9.9.9").digest, None);
    roots.managed_releases = vec![roots.runtime_releases[0].clone()];
    let expected = crate::catalog::release_digest([
        ("bin/firecracker", sha256(&elf_header(ELF_MACHINE_X86_64))),
        ("bin/jailer", sha256(&elf_header(ELF_MACHINE_X86_64))),
    ]);
    let listing = run(&roots);
    assert_eq!(find(&listing, FC_RELEASE, "v9.9.9").digest, Some(expected));
    assert!(listing.is_complete());
    for digest in [expected, Digest::sha256([0; 32])] {
        let directory = ManagedDirectoryName::release(
            &RuntimeIdentity {
                kind: RuntimeKind::Firecracker,
                version: "9.9.9".parse().unwrap(),
            },
            digest,
        )
        .to_string();
        std::fs::rename(
            fc_releases(&roots).join("v9.9.9"),
            fc_releases(&roots).join(&directory),
        )
        .unwrap();
        let listing = run(&roots);
        let artifact = find(&listing, FC_RELEASE, &directory);
        assert_eq!(artifact.digest, Some(expected));
        assert_eq!(
            artifact.status,
            if digest == expected {
                ArtifactStatus::Unsupported
            } else {
                ArtifactStatus::Invalid
            }
        );
        std::fs::rename(
            fc_releases(&roots).join(&directory),
            fc_releases(&roots).join("v9.9.9"),
        )
        .unwrap();
    }
    let limited = scan(
        &roots,
        ScanLimits {
            max_digest_bytes: 1,
            ..LIMITS
        },
        ArtifactKinds::ALL,
        &SystemScanClock,
        &mut DigestCache::default(),
    );
    assert!(!limited.is_complete());
    assert_eq!(find(&limited, FC_RELEASE, "v9.9.9").digest, None);
    std::fs::remove_file(fc_releases(&roots).join("v9.9.9/bin/jailer")).unwrap();
    assert_eq!(find(&run(&roots), FC_RELEASE, "v9.9.9").digest, None);
}

#[test]
fn filtered_listings_retain_the_other_kinds_bounded_digest_cache() {
    let dir = tempfile::tempdir().unwrap();
    let mut roots = roots(dir.path());
    release(&roots, "v1.16.2");
    roots.managed_releases = vec![roots.runtime_releases[0].clone()];
    write(
        &roots.cache_kernels.join("6.1.155/vmlinux.bin"),
        &kernel_image(b"kernel"),
    );
    let mut cache = DigestCache::default();
    scan(
        &roots,
        LIMITS,
        ArtifactKinds::ALL,
        &SettledClock,
        &mut cache,
    );
    assert_eq!(cache.len(), 3);
    let cached_only = ScanLimits {
        max_digest_bytes: 0,
        ..LIMITS
    };
    for kinds in [
        ArtifactKinds {
            firmware: false,
            runtimes: true,
            kernels: false,
        },
        KERNELS_ONLY,
        ArtifactKinds::ALL,
    ] {
        let listing = scan(&roots, cached_only, kinds, &SettledClock, &mut cache);
        assert!(listing.is_complete(), "{:?}", listing.issues);
        assert!(
            listing
                .artifacts
                .iter()
                .all(|artifact| artifact.digest.is_some())
        );
        assert_eq!(cache.len(), 3);
    }
}

#[test]
fn managed_runtime_roots_keep_same_version_releases_and_failures_separate() {
    let dir = tempfile::tempdir().unwrap();
    let mut roots = roots(dir.path());
    release(&roots, "v1.15.1");
    let fc = roots.runtime_releases[0].clone();
    let ch =
        RuntimeInstallationRoot::new(RuntimeKind::CloudHypervisor, dir.path().join("managed/ch"));
    let binary = ch.path.join("v1.15.1/bin/cloud-hypervisor");
    write_mode(&binary, &elf_header(ELF_MACHINE_X86_64), 0o755);
    // A duplicate external root must not create duplicate managed entries.
    roots.runtime_releases.push(ch.clone());
    roots.managed_releases = vec![fc, ch];
    for damaged in [false, true] {
        if damaged {
            std::fs::remove_file(&binary).unwrap();
        }
        let scan = run(&roots);
        let runtimes: Vec<_> = scan
            .artifacts
            .iter()
            .filter(|artifact| matches!(artifact.origin.kind, ArtifactKind::Runtime(_)))
            .collect();
        assert_eq!(runtimes.len(), 2);
        let fc = runtimes
            .iter()
            .find(|artifact| {
                artifact.origin.kind == ArtifactKind::Runtime(RuntimeKind::Firecracker)
            })
            .unwrap();
        let ch = runtimes
            .iter()
            .find(|artifact| {
                artifact.origin.kind == ArtifactKind::Runtime(RuntimeKind::CloudHypervisor)
            })
            .unwrap();
        assert_eq!(fc.status, ArtifactStatus::Valid);
        assert!(fc.digest.is_some());
        assert_eq!(
            ch.status,
            if damaged {
                ArtifactStatus::Incomplete
            } else {
                ArtifactStatus::Valid
            }
        );
        if !damaged {
            assert!(ch.digest.is_some());
            assert_ne!(fc.digest, ch.digest);
        }
    }
}

#[test]
fn managed_auxiliary_files_are_filtered_hashed_and_never_inferred_as_kernels() {
    let dir = tempfile::tempdir().unwrap();
    let mut roots = roots(dir.path());
    roots.managed_kernels = Some(dir.path().join("managed/kernels"));
    let store = dir.path().join("managed/boot-artifacts");
    let bytes = b"opaque boot payload";
    let digest = Digest::sha256(Sha256::digest(bytes).into());
    let version: ArtifactVersion = "driver-bundle-1".parse().unwrap();
    let directory = ManagedDirectoryName::kernel(&version, digest);
    for kind in [
        ArtifactComponentKind::Initrd,
        ArtifactComponentKind::Firmware,
    ] {
        let path = store.join(format!("{kind}-{directory}"));
        std::fs::create_dir_all(&path).unwrap();
        std::fs::write(path.join(crate::catalog::BOOT_ARTIFACT_IMAGE), bytes).unwrap();
    }
    let mut cache = DigestCache::default();
    for firmware in [false, true] {
        let scan = scan(
            &roots,
            LIMITS,
            ArtifactKinds {
                runtimes: false,
                kernels: false,
                firmware,
            },
            &SettledClock,
            &mut cache,
        );
        assert!(scan.is_complete());
        assert_eq!(scan.artifacts.len(), usize::from(firmware));
        if !firmware {
            continue;
        }
        let artifact = &scan.artifacts[0];
        assert_eq!(
            artifact.origin.kind,
            ArtifactKind::Component(ArtifactComponentKind::Firmware)
        );
        assert_eq!(artifact.digest, Some(digest));
        assert_eq!(
            artifact.version,
            Some(ScannedVersion::Component(version.clone()))
        );
        assert_eq!(artifact.status, ArtifactStatus::Valid);
        assert!(artifact.architecture.is_none());
        assert!(artifact.role.is_none());
    }
    let firmware = store
        .join(format!("firmware-{directory}"))
        .join(crate::catalog::BOOT_ARTIFACT_IMAGE);
    std::fs::write(&firmware, b"tampered payload").unwrap();
    let scan = scan(
        &roots,
        LIMITS,
        ArtifactKinds::ALL,
        &SettledClock,
        &mut cache,
    );
    assert_eq!(
        scan.artifacts
            .iter()
            .find(|artifact| artifact.path == firmware)
            .unwrap()
            .status,
        ArtifactStatus::Invalid
    );
}
