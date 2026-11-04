use std::fs;
use std::os::unix::fs::MetadataExt as _;
use std::str::FromStr as _;

use oci_spec::distribution::Reference as OciReference;
use rstest::{fixture, rstest};

use super::*;
struct RealAssemblyFixture {
    _tempdir: tempfile::TempDir,
    cache_dir: PathBuf,
    seed_rootfs: PathBuf,
    target_rootfs: PathBuf,
    guest_bin_dir: PathBuf,
    expected_path: PathBuf,
    reference: OciReference,
}

#[cfg(target_os = "linux")]
struct FailedExtractionFixture {
    _tempdir: tempfile::TempDir,
    source: PathBuf,
    target: PathBuf,
}

#[cfg(target_os = "linux")]
fn failed_extraction_fixture() -> FailedExtractionFixture {
    let tempdir = tempfile::tempdir().expect("create copy-tree test directory");
    let source = tempdir.path().join("source");
    fs::create_dir(&source).expect("create source directory");
    fs::write(source.join("payload"), vec![b'x'; 4 * 1024 * 1024]).expect("write source payload");
    let target = tempdir.path().join("not-a-directory");
    fs::write(&target, b"target").expect("write invalid extraction target");

    FailedExtractionFixture {
        _tempdir: tempdir,
        source,
        target,
    }
}

#[fixture]
fn real_assembly_fixture() -> Option<RealAssemblyFixture> {
    std::env::var_os("SPACE_TEST_REAL_OCI_ASSEMBLY")?;

    let seed_rootfs = PathBuf::from(
        std::env::var("SPACE_TEST_SEED_ROOTFS").expect("SPACE_TEST_SEED_ROOTFS is required"),
    );
    let guest_bin_dir = PathBuf::from(
        std::env::var("SPACE_TEST_GUEST_BIN_DIR")
            .unwrap_or_else(|_| "/space/data/spaced".to_string()),
    );
    let reference = std::env::var("SPACE_TEST_OCI_IMAGE")
        .unwrap_or_else(|_| "docker.io/library/python:3.12-slim-bookworm".to_string());
    let reference = OciReference::from_str(&reference).expect("valid SPACE_TEST_OCI_IMAGE");
    let expected_path = std::env::var("SPACE_TEST_OCI_EXPECTED_PATH")
        .unwrap_or_else(|_| "/usr/local/bin/python".to_string());
    let expected_path = Path::new(&expected_path)
        .strip_prefix("/")
        .expect("SPACE_TEST_OCI_EXPECTED_PATH must be absolute")
        .to_path_buf();

    let cache_root = std::env::var_os("SPACE_TEST_OCI_CACHE_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(std::env::temp_dir)
        .join("space-oci-integration-tests");
    fs::create_dir_all(&cache_root).expect("create OCI integration test cache");
    let tempdir = tempfile::Builder::new()
        .prefix("space-oci-integration-")
        .tempdir_in(cache_root)
        .expect("create OCI integration test directory");
    let cache_dir = tempdir.path().join("cache");
    fs::create_dir(&cache_dir).expect("create assembly cache");
    let seed_copy = tempdir.path().join("seed.ext4");
    let target_rootfs = tempdir.path().join("sandbox/instance/rootfs.ext4");

    let mut copy = crate::command_as_root("cp");
    copy.args(["--reflink=auto", "--sparse=always"])
        .arg(seed_rootfs)
        .arg(&seed_copy);
    run_fixture_command(&mut copy, &[]);

    Some(RealAssemblyFixture {
        _tempdir: tempdir,
        cache_dir,
        seed_rootfs: seed_copy,
        target_rootfs,
        guest_bin_dir,
        expected_path,
        reference,
    })
}

#[rstest]
#[tokio::test]
async fn real_oci_image_assembles_with_the_space_guest_bundle(
    real_assembly_fixture: Option<RealAssemblyFixture>,
) {
    let Some(fixture) = real_assembly_fixture else {
        return;
    };
    assemble_from_image(OciImageAssemblyRequest {
        seed_rootfs: fixture.seed_rootfs.clone(),
        target_rootfs: fixture.target_rootfs.clone(),
        target_rootfs_size_mib: 1024,
        reference: fixture.reference.clone(),
        cache_dir: fixture.cache_dir.clone(),
        guest_bin_dir: fixture.guest_bin_dir.clone(),
        bundle_version: "integration-test".to_string(),
    })
    .await
    .expect("assemble OCI rootfs");

    let mountpoint = fixture._tempdir.path().join("inspect");
    fs::create_dir(&mountpoint).expect("create inspection mountpoint");
    with_mounted_ext4(
        &fixture.target_rootfs,
        &mountpoint,
        true,
        RootfsAssemblyOperation::MountRootfs,
        RootfsAssemblyOperation::UnmountRootfs,
        |root| {
            assert!(root.join(&fixture.expected_path).is_file());
            assert!(root.join("usr/local/bin/spaced").is_file());
            assert!(root.join("usr/local/bin/spaced-svc").is_file());
            assert_eq!(
                fs::read_to_string(root.join("etc/spaced/bundle-version"))
                    .expect("read bundle marker"),
                "integration-test\n"
            );
            assert_eq!(
                fs::metadata(root.join("home/user"))
                    .expect("read sandbox user home metadata")
                    .uid(),
                2000
            );
            let mut guest_command = crate::command_as_root("chroot");
            guest_command.arg(root).args([
                "/usr/bin/env",
                "bash",
                "-lc",
                "test -x /usr/local/bin/spaced \
                 && test -x /usr/local/sbin/space-configure-apt-mirror \
                 && modules=(/usr/lib/*/security/pam_limits.so) \
                 && test -e \"${modules[0]}\" \
                 && ! /usr/bin/ldd \"${modules[0]}\" 2>&1 | grep -q 'not found'",
            ]);
            run_fixture_command(&mut guest_command, &[]);
            Ok(())
        },
    )
    .expect("inspect assembled rootfs");
}

// `copy_tree` escalates through sudo when not root. The pipe-status handling
// under test needs no privilege, so the test runs in a child process whose
// PATH holds only a pass-through `sudo` and the host's `tar`. The host's sudo
// is unreachable: a shim that cannot execute fails the copy instead.
#[cfg(target_os = "linux")]
#[test]
fn extraction_failure_is_not_masked_by_archive_sigpipe() {
    use std::os::unix::fs::PermissionsExt as _;

    let bin = tempfile::tempdir().expect("create test PATH directory");
    let tar = std::env::split_paths(&std::env::var_os("PATH").unwrap_or_default())
        .map(|dir| dir.join("tar"))
        .find(|path| path.is_file())
        .expect("tar on PATH");
    std::os::unix::fs::symlink(&tar, bin.path().join("tar")).expect("link tar");
    let sudo = bin.path().join("sudo");
    // `exec` lets the archive die of SIGPIPE as it would under sudo; a
    // shell parent would instead exit 141, an ordinary failure status.
    fs::write(&sudo, "#!/bin/sh\nexec \"$@\"\n").expect("write sudo shim");
    fs::set_permissions(&sudo, fs::Permissions::from_mode(0o755))
        .expect("make sudo shim executable");
    if !pplx_space_test_env::enter_test_process(&[("PATH", Some(bin.path().as_os_str()))]) {
        return;
    }
    let fixture = failed_extraction_fixture();
    let error = copy_tree(&fixture.source, &fixture.target)
        .expect_err("extraction into a regular file must fail");

    // GNU tar's fatal-error status, not the archive's SIGPIPE or a missing
    // program's 127.
    assert!(
        matches!(
            error,
            RootfsAssemblyError::Command {
                operation: RootfsAssemblyOperation::CopyOciRootfs,
                code: Some(2),
            }
        ),
        "unexpected copy error: {error:?}"
    );
}

fn run_fixture_command(command: &mut Command, accepted_codes: &[i32]) {
    let status = command.status().expect("start fixture command");
    assert!(
        status.success()
            || status
                .code()
                .is_some_and(|code| accepted_codes.contains(&code)),
        "fixture command exited with {status}"
    );
}

// The OCI formatter enables ext4 `inline_data`. This fixture covers the
// layouts that inline data changes: inline directories and small files,
// a symlink too long for the inode's block map, an extended attribute too
// large for the inode, setuid permissions and a file capability. Production
// consumers run repair-capable `e2fsck -fy` and accept exit 1, so assembly
// succeeding does not show the layout stayed clean. The test therefore requires
// a read-only `e2fsck -fn` to exit 0 after assembly, after the build's shrink
// and after the template manager's expansion.
#[cfg(target_os = "linux")]
mod inline_data {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt as _;
    use std::os::unix::fs::PermissionsExt as _;

    use super::*;

    const TINY: &[u8] = b"tiny payload\n";
    const PAYLOAD_LEN: usize = 17 * 1024;
    const OWNER: u32 = 2345;
    const XATTR_NAME: &str = "user.space.fixture";
    const XATTR_LEN: usize = 1024;
    const CAPABILITY_NAME: &str = "security.capability";
    // `vfs_cap_data` revision 2 with the effective bit: CAP_NET_BIND_SERVICE
    // permitted, nothing inheritable.
    const CAPABILITY: [u8; 20] = [
        0x01, 0x00, 0x00, 0x02, 0x00, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00,
    ];

    fn payload() -> Vec<u8> {
        (0..PAYLOAD_LEN).map(|index| (index % 251) as u8).collect()
    }

    fn xattr_value() -> Vec<u8> {
        (0..XATTR_LEN)
            .map(|index| b'a' + (index % 26) as u8)
            .collect()
    }

    // 120 bytes: past the 60 bytes a fast symlink keeps in the block map.
    fn long_target() -> String {
        format!("{}/target", "d".repeat(113))
    }

    fn c_path(path: &Path) -> CString {
        CString::new(path.as_os_str().as_bytes()).expect("path has no NUL")
    }

    fn set_xattr(path: &Path, name: &str, value: &[u8]) {
        let path = c_path(path);
        let name = CString::new(name).expect("xattr name has no NUL");
        // SAFETY: both strings are NUL-terminated and the value slice is valid
        // for its length for the duration of the call.
        let result = unsafe {
            libc::setxattr(
                path.as_ptr(),
                name.as_ptr(),
                value.as_ptr().cast(),
                value.len(),
                0,
            )
        };
        assert_eq!(
            result,
            0,
            "setxattr {name:?}: {}",
            std::io::Error::last_os_error()
        );
    }

    fn get_xattr(path: &Path, name: &str) -> Result<Vec<u8>, String> {
        let path = c_path(path);
        let name = CString::new(name).map_err(|error| error.to_string())?;
        let mut value = vec![0_u8; 4096];
        // SAFETY: both strings are NUL-terminated and the buffer is valid and
        // writable for its length for the duration of the call.
        let length = unsafe {
            libc::getxattr(
                path.as_ptr(),
                name.as_ptr(),
                value.as_mut_ptr().cast(),
                value.len(),
            )
        };
        // A negative length is getxattr's error return.
        let length =
            usize::try_from(length).map_err(|_| std::io::Error::last_os_error().to_string())?;
        value.truncate(length);
        Ok(value)
    }

    fn write_source(source: &Path) {
        fs::create_dir_all(source.join("small")).unwrap();
        fs::create_dir_all(source.join("bin")).unwrap();
        fs::write(source.join("small/tiny"), TINY).unwrap();
        fs::write(source.join("small/empty"), b"").unwrap();
        fs::write(source.join("payload"), payload()).unwrap();
        std::os::unix::fs::symlink(long_target(), source.join("long-link")).unwrap();
        fs::write(source.join("small/attributed"), TINY).unwrap();

        let setuid = source.join("bin/setuid");
        fs::write(&setuid, b"#!/bin/sh\n").unwrap();
        fs::set_permissions(&setuid, fs::Permissions::from_mode(0o4755)).unwrap();

        let capable = source.join("bin/capable");
        fs::write(&capable, b"#!/bin/sh\n").unwrap();
        fs::set_permissions(&capable, fs::Permissions::from_mode(0o755)).unwrap();

        let mut chown = crate::command_as_root("chown");
        chown
            .args(["-R", "-h", &format!("{OWNER}:{OWNER}")])
            .arg(source);
        run_fixture_command(&mut chown, &[]);
        // chown clears setuid bits and file capabilities, so set both after it.
        fs::set_permissions(&setuid, fs::Permissions::from_mode(0o4755)).unwrap();
        set_xattr(&capable, CAPABILITY_NAME, &CAPABILITY);
        set_xattr(&source.join("small/attributed"), XATTR_NAME, &xattr_value());
    }

    /// What one mounted view of the image contains. Collected without
    /// panicking so the mount helper always reaches its unmount; the test
    /// asserts on it after the image is unmounted.
    #[derive(Debug, PartialEq, Eq)]
    struct Observation {
        tiny: Result<Vec<u8>, String>,
        empty_len: Result<u64, String>,
        payload_matches: Result<bool, String>,
        long_link: Result<PathBuf, String>,
        xattr: Result<Vec<u8>, String>,
        setuid_mode: Result<u32, String>,
        capability: Result<Vec<u8>, String>,
        owner: Result<(u32, u32), String>,
    }

    impl Observation {
        fn collect(root: &Path) -> Self {
            let text = |error: std::io::Error| error.to_string();
            Self {
                tiny: fs::read(root.join("small/tiny")).map_err(text),
                empty_len: fs::symlink_metadata(root.join("small/empty"))
                    .map(|metadata| metadata.len())
                    .map_err(text),
                payload_matches: fs::read(root.join("payload"))
                    .map(|bytes| bytes == payload())
                    .map_err(text),
                long_link: fs::read_link(root.join("long-link")).map_err(text),
                xattr: get_xattr(&root.join("small/attributed"), XATTR_NAME),
                setuid_mode: fs::symlink_metadata(root.join("bin/setuid"))
                    .map(|metadata| metadata.mode() & 0o7777)
                    .map_err(text),
                capability: get_xattr(&root.join("bin/capable"), CAPABILITY_NAME),
                owner: fs::symlink_metadata(root.join("small/tiny"))
                    .map(|metadata| (metadata.uid(), metadata.gid()))
                    .map_err(text),
            }
        }

        fn expected() -> Self {
            Self {
                tiny: Ok(TINY.to_vec()),
                empty_len: Ok(0),
                payload_matches: Ok(true),
                long_link: Ok(PathBuf::from(long_target())),
                xattr: Ok(xattr_value()),
                setuid_mode: Ok(0o4755),
                capability: Ok(CAPABILITY.to_vec()),
                owner: Ok((OWNER, OWNER)),
            }
        }
    }

    fn observe(image: &Path, mountpoint: &Path, read_only: bool) -> Observation {
        with_mounted_ext4(
            image,
            mountpoint,
            read_only,
            RootfsAssemblyOperation::MountRootfs,
            RootfsAssemblyOperation::UnmountRootfs,
            |root| Ok(Observation::collect(root)),
        )
        .expect("mount and unmount the rootfs")
    }

    fn assert_clean(image: &Path, stage: &str) {
        let output = crate::command_as_root("e2fsck")
            .args(["-fn"])
            .arg(image)
            .output()
            .expect("start e2fsck");
        assert_eq!(
            output.status.code(),
            Some(0),
            "read-only e2fsck after {stage} was not clean\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        );
    }

    const INLINE_DATA_FLAG: u64 = 0x1000_0000;

    /// Fails unless debugfs reports `EXT4_INLINE_DATA_FL` on each path, so the
    /// fixture still exercises inline storage after every stage.
    fn assert_inline(image: &Path, stage: &str) {
        for path in ["/small", "/small/tiny", "/small/attributed"] {
            let output = crate::command_as_root("debugfs")
                .args(["-R", &format!("stat {path}")])
                .arg(image)
                .output()
                .expect("start debugfs");
            let stat = String::from_utf8_lossy(&output.stdout);
            let flags = stat
                .split_whitespace()
                .skip_while(|word| *word != "Flags:")
                .nth(1)
                .and_then(|value| value.strip_prefix("0x"))
                .and_then(|value| u64::from_str_radix(value, 16).ok());
            assert!(
                flags.is_some_and(|flags| flags & INLINE_DATA_FLAG != 0),
                "{path} is not stored inline after {stage}:\n{stat}"
            );
        }
    }

    fn superblock(image: &Path) -> String {
        let output = crate::command_as_root("dumpe2fs")
            .args(["-h"])
            .arg(image)
            .output()
            .expect("start dumpe2fs");
        assert!(output.status.success(), "dumpe2fs -h failed");
        String::from_utf8(output.stdout).expect("dumpe2fs output is UTF-8")
    }

    fn superblock_number(superblock: &str, field: &str) -> u64 {
        superblock
            .lines()
            .find_map(|line| line.strip_prefix(field))
            .and_then(|value| value.trim().parse().ok())
            .unwrap_or_else(|| panic!("dumpe2fs has no numeric {field:?}"))
    }

    fn run_root(program: &str, args: &[&str], image: &Path, accepted_codes: &[i32]) {
        let mut command = crate::command_as_root(program);
        command.args(args).arg(image);
        run_fixture_command(&mut command, accepted_codes);
    }

    #[test]
    fn inline_rootfs_stays_clean_through_shrink_and_expansion() {
        if !pplx_space_test_env::root_or_skip("loop-mount ext4 images and set file capabilities") {
            return;
        }
        let fixture = tempfile::tempdir().unwrap();
        let source = fixture.path().join("source");
        write_source(&source);
        let image = fixture.path().join("rootfs.ext4");
        let mountpoint = fixture.path().join("mount");
        fs::create_dir(&mountpoint).unwrap();
        initialize_rootfs(&image, 1024).unwrap();

        format_ext4(&image).unwrap();
        let features = superblock(&image);
        assert!(
            features
                .lines()
                .any(|line| line.starts_with("Filesystem features:")
                    && line
                        .split_whitespace()
                        .any(|feature| feature == "inline_data")),
            "formatter did not enable inline_data:\n{features}"
        );

        let assembled = with_mounted_ext4(
            &image,
            &mountpoint,
            false,
            RootfsAssemblyOperation::MountRootfs,
            RootfsAssemblyOperation::UnmountRootfs,
            |root| {
                copy_tree(&source, root)?;
                Ok(Observation::collect(root))
            },
        )
        .expect("assemble the rootfs");
        assert_eq!(assembled, Observation::expected(), "after assembly");
        assert_clean(&image, "assembly");
        assert_inline(&image, "assembly");
        check_ext4(&image).expect("assembly check");

        // The build's shrink: repair-capable check, minimum-size resize, and
        // truncation to the filesystem's new size.
        run_root("e2fsck", &["-fy"], &image, &[1]);
        run_root("resize2fs", &["-M"], &image, &[]);
        let shrunk = superblock(&image);
        let shrunk_bytes =
            superblock_number(&shrunk, "Block count:") * superblock_number(&shrunk, "Block size:");
        fs::OpenOptions::new()
            .write(true)
            .open(&image)
            .unwrap()
            .set_len(shrunk_bytes)
            .unwrap();
        assert_clean(&image, "shrink");
        assert_eq!(
            observe(&image, &mountpoint, true),
            Observation::expected(),
            "after shrink"
        );
        assert_clean(&image, "mounting the shrunk image read-only");
        assert_inline(&image, "shrink");

        // The template manager's expansion: grow the file, repair-capable
        // check, then resize to fill it.
        fs::OpenOptions::new()
            .write(true)
            .open(&image)
            .unwrap()
            .set_len(2 * 1024 * 1024 * 1024)
            .unwrap();
        run_root("e2fsck", &["-fy"], &image, &[1]);
        run_root("resize2fs", &[], &image, &[]);
        assert_clean(&image, "expansion");
        assert_eq!(
            observe(&image, &mountpoint, true),
            Observation::expected(),
            "after expansion"
        );
        assert_clean(&image, "mounting the expanded image read-only");
        assert_inline(&image, "expansion");
    }
}
