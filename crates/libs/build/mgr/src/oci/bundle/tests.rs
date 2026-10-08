use std::collections::BTreeSet;
use std::fs;
use std::os::unix::fs::PermissionsExt as _;

use rstest::{fixture, rstest};

use super::*;

struct BundleFixture {
    _tempdir: tempfile::TempDir,
    seed: PathBuf,
    target: PathBuf,
    guest_bins: PathBuf,
    cache: PathBuf,
}

#[fixture]
fn bundle_fixture() -> BundleFixture {
    let tempdir = tempfile::tempdir().expect("create bundle fixture");
    let seed = tempdir.path().join("seed");
    let target = tempdir.path().join("target");
    let guest_bins = tempdir.path().join("guest-bins");
    let cache = tempdir.path().join("cache");
    for directory in [&seed, &target, &guest_bins, &cache] {
        fs::create_dir_all(directory).expect("create fixture directory");
    }
    for required in REQUIRED_PATHS {
        let path = seed.join(required);
        if Path::new(required).extension().is_none()
            && [
                "etc/ssh",
                "etc/systemd/system",
                "home/user",
                "root/.ssh",
                "usr/lib/systemd",
            ]
            .contains(required)
        {
            fs::create_dir_all(&path).expect("create required fixture directory");
            fs::write(path.join("fixture"), required).expect("write fixture directory content");
        } else {
            fs::create_dir_all(path.parent().expect("required path has parent"))
                .expect("create required fixture parent");
            fs::write(&path, format!("seed:{required}")).expect("write required fixture file");
        }
    }
    fs::remove_file(seed.join("bin/bash")).expect("remove split seed bash fixture");
    fs::remove_file(seed.join("bin/sh")).expect("remove split seed sh fixture");
    fs::remove_dir(seed.join("bin")).expect("remove split seed bin directory");
    fs::create_dir_all(seed.join("usr/bin")).expect("create merged seed bin directory");
    fs::write(seed.join("usr/bin/bash"), "seed:bin/bash").expect("write merged seed bash");
    fs::write(seed.join("usr/bin/dash"), "seed:bin/sh").expect("write merged seed sh target");
    fs::remove_file(seed.join("usr/bin/sh")).expect("remove regular seed sh fixture");
    std::os::unix::fs::symlink("dash", seed.join("usr/bin/sh"))
        .expect("create merged seed sh symlink");
    std::os::unix::fs::symlink("usr/bin", seed.join("bin"))
        .expect("create merged seed bin symlink");
    let env_target = seed.join("usr/lib/cargo/bin/coreutils/env");
    fs::create_dir_all(env_target.parent().expect("env target has parent"))
        .expect("create env target directory");
    fs::write(&env_target, "seed-env").expect("write env target");
    fs::create_dir_all(seed.join("usr/bin")).expect("create seed bin directory");
    std::os::unix::fs::symlink("../lib/cargo/bin/coreutils/env", seed.join("usr/bin/env"))
        .expect("create seed env symlink");
    fs::create_dir_all(target.join("usr/local/bin")).expect("create OCI bin directory");
    fs::create_dir_all(target.join("bin")).expect("create split OCI bin directory");
    fs::create_dir_all(target.join("usr/bin")).expect("create split OCI usr bin directory");
    fs::write(target.join("usr/bin/bash"), "bash-from-oci").expect("write OCI bash");
    std::os::unix::fs::symlink("/usr/bin/bash", target.join("bin/bash"))
        .expect("create OCI bash symlink");
    std::os::unix::fs::symlink("/usr/bin/sh", target.join("usr/bin/sh"))
        .expect("create broken OCI sh symlink");
    fs::create_dir_all(target.join("etc")).expect("create OCI etc directory");
    fs::write(target.join("usr/local/bin/python"), "python-from-oci").expect("write OCI Python");
    fs::write(target.join("etc/passwd"), "oci-passwd").expect("write OCI collision");
    let static_elf = minimal_static_elf();
    fs::write(seed.join("usr/local/bin/spaced"), &static_elf).expect("write static seed spaced");
    fs::write(seed.join("usr/local/bin/spaced-svc"), &static_elf)
        .expect("write static seed spaced-svc");
    fs::write(guest_bins.join("spaced"), "current-spaced").expect("write current spaced");
    fs::write(guest_bins.join("spaced-svc"), "current-spaced-svc")
        .expect("write current spaced-svc");

    BundleFixture {
        _tempdir: tempdir,
        seed,
        target,
        guest_bins,
        cache,
    }
}

fn minimal_static_elf() -> Vec<u8> {
    let mut bytes = vec![0; 64];
    bytes[..7].copy_from_slice(b"\x7fELF\x02\x01\x01");
    bytes[16..18].copy_from_slice(&object::elf::ET_EXEC.0.to_le_bytes());
    bytes
}

#[rstest]
fn dependency_capture_rejects_missing_libraries_and_preserves_symlink_chains() {
    parse_dynamic_dependencies("libmissing.so => not found")
        .expect_err("missing dependency must fail capture");

    let tempdir = tempfile::tempdir().expect("create dependency fixture");
    let seed = tempdir.path().join("seed");
    let seed_link = tempdir.path().join("seed-link");
    fs::create_dir_all(seed.join("lib")).expect("create library directory");
    fs::create_dir_all(seed.join("usr/lib")).expect("create library target directory");
    fs::write(seed.join("usr/lib/libfixture.so.1.2.3"), "library").expect("write library target");
    std::os::unix::fs::symlink("libfixture.so.1.2.3", seed.join("usr/lib/libfixture.so.1"))
        .expect("create intermediate library symlink");
    std::os::unix::fs::symlink("/usr/lib/libfixture.so.1", seed.join("lib/libfixture.so"))
        .expect("create absolute library symlink");
    std::os::unix::fs::symlink(&seed, &seed_link).expect("create noncanonical seed root");
    let dependencies = vec![PathBuf::from("lib/libfixture.so")];
    let mut entries = BTreeSet::new();

    add_dynamic_dependency_paths(&seed_link, &mut entries, &dependencies)
        .expect("capture dependency symlink chain");

    assert_eq!(
        entries,
        BTreeSet::from([
            PathBuf::from("lib/libfixture.so"),
            PathBuf::from("usr/lib/libfixture.so.1"),
            PathBuf::from("usr/lib/libfixture.so.1.2.3"),
        ])
    );
}

#[rstest]
fn dependency_inspection_does_not_open_root_owned_payloads(bundle_fixture: BundleFixture) {
    let secret = bundle_fixture.seed.join("root/.ssh/fixture");
    fs::set_permissions(&secret, fs::Permissions::from_mode(0o000))
        .expect("make seed payload unreadable");

    let entries = super::dependencies::bundle_entries(&bundle_fixture.seed, false)
        .expect("inspect executable dependencies without opening secret payloads");

    assert!(entries.contains(&PathBuf::from("root/.ssh")));
}

#[rstest]
#[case(false)]
#[case(true)]
fn bundle_injection_preserves_image_ssh_client_configuration(
    bundle_fixture: BundleFixture,
    #[case] image_has_client: bool,
) {
    let client_paths = ["etc/ssh/ssh_config", "etc/ssh/ssh_config.d/client.conf"];
    for relative in client_paths {
        let seed_path = bundle_fixture.seed.join(relative);
        fs::create_dir_all(seed_path.parent().unwrap()).expect("create seed client directory");
        fs::write(seed_path, "seed-client-config").expect("write seed client config");
        if image_has_client {
            let image_path = bundle_fixture.target.join(relative);
            fs::create_dir_all(image_path.parent().unwrap())
                .expect("create image client directory");
            fs::write(image_path, "image-client-config").expect("write image client config");
        }
    }
    for root in [&bundle_fixture.seed, &bundle_fixture.target] {
        fs::create_dir_all(root.join("etc/ssh/sshd_config.d"))
            .expect("create server config directory");
    }
    let image_server_override = bundle_fixture
        .target
        .join("etc/ssh/sshd_config.d/image.conf");
    fs::write(&image_server_override, "DenyUsers root\n").expect("write image server override");
    let bundle =
        GuestBundle::capture_with_privilege(&bundle_fixture.seed, &bundle_fixture.cache, false)
            .expect("capture seed bundle");

    bundle
        .extract_with_privilege(&bundle_fixture.target, false)
        .expect("inject seed bundle");

    for relative in client_paths {
        let path = bundle_fixture.target.join(relative);
        assert_eq!(path.exists(), image_has_client, "{relative}");
        if image_has_client {
            assert_eq!(fs::read_to_string(path).unwrap(), "image-client-config");
        }
    }
    assert_eq!(
        fs::read_to_string(bundle_fixture.target.join("etc/ssh/fixture")).unwrap(),
        "etc/ssh"
    );
    assert!(!image_server_override.exists());
}

#[rstest]
fn bundle_capture_creates_a_non_empty_archive(bundle_fixture: BundleFixture) {
    let _bundle =
        GuestBundle::capture_with_privilege(&bundle_fixture.seed, &bundle_fixture.cache, false)
            .expect("capture seed bundle");

    let archives = fs::read_dir(&bundle_fixture.cache)
        .expect("read bundle cache")
        .collect::<Result<Vec<_>, _>>()
        .expect("read bundle archive entries");

    assert_eq!(archives.len(), 1);
    assert!(
        archives[0]
            .file_type()
            .expect("read bundle archive type")
            .is_file()
    );
    assert!(
        archives[0]
            .metadata()
            .expect("read bundle archive metadata")
            .len()
            > 0
    );
}

#[rstest]
fn bundle_injection_keeps_oci_userspace_and_replaces_the_guest_contract(
    bundle_fixture: BundleFixture,
) {
    fs::write(bundle_fixture.seed.join("usr/bin/tar"), "seed-tar").unwrap();
    let image_override = bundle_fixture
        .target
        .join("etc/systemd/system/spaced.service.d/override.conf");
    fs::create_dir_all(image_override.parent().expect("override has parent"))
        .expect("create image-controlled service override directory");
    fs::write(&image_override, "[Service]\nExecStart=/bin/false\n")
        .expect("write image-controlled service override");
    let bundle =
        GuestBundle::capture_with_privilege(&bundle_fixture.seed, &bundle_fixture.cache, false)
            .expect("capture seed bundle");

    bundle
        .extract_with_privilege(&bundle_fixture.target, false)
        .expect("inject seed bundle");
    install_current_guest_with_privilege(
        &bundle_fixture.target,
        &bundle_fixture.guest_bins,
        "1.2.3",
        false,
    )
    .expect("install current guest binaries");

    assert_eq!(
        fs::read_to_string(bundle_fixture.target.join("usr/bin/tar")).unwrap(),
        "seed-tar"
    );
    assert_eq!(
        fs::read_to_string(bundle_fixture.target.join("usr/local/bin/python"))
            .expect("read OCI Python"),
        "python-from-oci"
    );
    assert_eq!(
        fs::read_to_string(bundle_fixture.target.join("etc/passwd"))
            .expect("read injected account database"),
        "seed:etc/passwd"
    );
    assert_eq!(
        fs::read_to_string(bundle_fixture.target.join("usr/local/bin/spaced"))
            .expect("read current spaced"),
        "current-spaced"
    );
    assert_eq!(
        fs::read_to_string(bundle_fixture.target.join("etc/spaced/bundle-version"))
            .expect("read bundle version"),
        "1.2.3\n"
    );
    assert_eq!(
        fs::read_to_string(bundle_fixture.target.join("usr/bin/env"))
            .expect("read injected env through its symlink"),
        "seed-env"
    );
    assert_eq!(
        fs::read_to_string(bundle_fixture.target.join("usr/bin/bash"))
            .expect("read injected bash from PATH"),
        "seed:bin/bash"
    );
    for relative in ["bin/sh", "usr/bin/sh"] {
        assert_eq!(
            fs::read_to_string(bundle_fixture.target.join(relative))
                .expect("read injected POSIX shell"),
            "seed:bin/sh"
        );
    }
    assert!(!image_override.exists());
}

#[rstest]
#[case("../var/lib/space-ssh")]
#[case("/var/lib/space-ssh")]
#[cfg(target_os = "linux")]
fn bundle_injection_preserves_contained_directory_symlinks(
    bundle_fixture: BundleFixture,
    #[case] link_target: &str,
) {
    let ssh_target = bundle_fixture.target.join("var/lib/space-ssh");
    fs::create_dir_all(&ssh_target).expect("create contained SSH directory");
    fs::write(ssh_target.join("ssh_config"), "image-client-config")
        .expect("write image SSH client config");
    std::os::unix::fs::symlink(link_target, bundle_fixture.target.join("etc/ssh"))
        .expect("create contained directory symlink");
    let bundle =
        GuestBundle::capture_with_privilege(&bundle_fixture.seed, &bundle_fixture.cache, false)
            .expect("capture seed bundle");

    bundle
        .extract_with_privilege(&bundle_fixture.target, false)
        .expect("inject seed bundle through contained symlink");

    assert!(
        fs::symlink_metadata(bundle_fixture.target.join("etc/ssh"))
            .expect("inspect contained directory symlink")
            .file_type()
            .is_symlink()
    );
    assert_eq!(
        fs::read_to_string(ssh_target.join("fixture")).expect("read injected SSH fixture"),
        "etc/ssh"
    );
    assert_eq!(
        fs::read_to_string(ssh_target.join("ssh_config")).expect("read image SSH client config"),
        "image-client-config"
    );
}

#[rstest]
fn bundle_injection_does_not_follow_oci_directory_symlinks(bundle_fixture: BundleFixture) {
    let outside = bundle_fixture
        .target
        .parent()
        .expect("target has parent")
        .join("outside");
    fs::remove_dir_all(bundle_fixture.target.join("etc")).expect("remove OCI etc directory");
    fs::create_dir_all(&outside).expect("create outside directory");
    fs::write(outside.join("passwd"), "outside-passwd").expect("write outside sentinel");
    std::os::unix::fs::symlink(&outside, bundle_fixture.target.join("etc"))
        .expect("create OCI directory symlink");
    let bundle =
        GuestBundle::capture_with_privilege(&bundle_fixture.seed, &bundle_fixture.cache, false)
            .expect("capture seed bundle");

    bundle
        .extract_with_privilege(&bundle_fixture.target, false)
        .expect("inject seed bundle");
    let outside_bin = outside.join("bin");
    fs::create_dir_all(&outside_bin).expect("create outside bin directory");
    fs::write(outside_bin.join("spaced"), "outside-spaced").expect("write outside binary sentinel");
    fs::remove_dir_all(bundle_fixture.target.join("usr/local/bin"))
        .expect("remove OCI bin directory");
    std::os::unix::fs::symlink(&outside_bin, bundle_fixture.target.join("usr/local/bin"))
        .expect("create OCI bin directory symlink");

    install_current_guest_with_privilege(
        &bundle_fixture.target,
        &bundle_fixture.guest_bins,
        "1.2.3",
        false,
    )
    .expect("install current guest binaries");

    assert_eq!(
        fs::read_to_string(outside.join("passwd")).expect("read outside sentinel"),
        "outside-passwd"
    );
    assert_eq!(
        fs::read_to_string(bundle_fixture.target.join("etc/passwd"))
            .expect("read injected account database"),
        "seed:etc/passwd"
    );
    assert_eq!(
        fs::read_to_string(outside_bin.join("spaced")).expect("read outside binary sentinel"),
        "outside-spaced"
    );
    assert_eq!(
        fs::read_to_string(bundle_fixture.target.join("usr/local/bin/spaced"))
            .expect("read current spaced"),
        "current-spaced"
    );
}

#[rstest]
fn bundle_injection_does_not_remove_rootfs_through_ancestor_symlink(bundle_fixture: BundleFixture) {
    let sentinel = bundle_fixture.target.join("oci-root-sentinel");
    fs::write(&sentinel, "keep-rootfs").expect("write rootfs sentinel");
    std::os::unix::fs::symlink("..", bundle_fixture.target.join("etc/ssh"))
        .expect("create symlink to rootfs");
    let bundle =
        GuestBundle::capture_with_privilege(&bundle_fixture.seed, &bundle_fixture.cache, false)
            .expect("capture seed bundle");

    bundle
        .extract_with_privilege(&bundle_fixture.target, false)
        .expect("inject seed bundle");

    assert_eq!(
        fs::read_to_string(sentinel).expect("read rootfs sentinel"),
        "keep-rootfs"
    );
    assert!(bundle_fixture.target.join("etc/ssh").is_dir());
}

// OCI assembly carries the seed's units with a fixed set of the seed's
// executables. Every program a seed unit starts must be in that set: an OCI
// image need not have its own (a workloads unit holding its cgroup with
// `sleep` would fail to start where the image has none, and spaced, which
// requires it, with it).
#[test]
fn every_program_a_seed_unit_runs_is_carried_in_the_bundle() {
    let carried: std::collections::BTreeSet<&str> = super::REQUIRED_PATHS
        .iter()
        .chain(super::OPTIONAL_PATHS)
        .copied()
        .collect();
    let mut programs = Vec::new();
    for line in crate::BUNDLED_ROOTFS_BUILDER_SCRIPT.lines() {
        let Some((key, command)) = line.split_once('=') else {
            continue;
        };
        if !matches!(
            key,
            "ExecStart" | "ExecStartPre" | "ExecStartPost" | "ExecStop" | "ExecReload"
        ) {
            continue;
        }
        let program = command
            .trim_start_matches(['-', '@', '+', '!', ':'])
            .split_whitespace()
            .next()
            .expect("an Exec line names a program");
        programs.push(program.to_owned());
        assert!(
            carried.contains(program.trim_start_matches('/')),
            "{key}={program} is not carried in the OCI guest bundle"
        );
    }
    assert!(
        programs
            .iter()
            .any(|program| program == "/usr/local/bin/spaced-svc")
    );
}
