use super::*;
use std::os::unix::fs::{FileExt, PermissionsExt};

fn digest(bytes: &[u8]) -> Digest {
    let hash = aws_lc_rs::digest::digest(&aws_lc_rs::digest::SHA256, bytes);
    let mut bytes = [0; 32];
    bytes.copy_from_slice(hash.as_ref());
    Digest::sha256(bytes)
}

#[test]
fn only_unsupported_verity_permits_sha_authentication() {
    let pins = SourcePins {
        sha256: digest(b"compiled content"),
        verity: VerityDigest::new(Digest::sha256([1; 32])),
    };
    assert!(matches!(
        catalog_authentication(Err(VerityError::Unsupported), pins).unwrap(),
        Authentication::CatalogSha256(value) if value == pins.sha256
    ));
    assert!(matches!(
        catalog_authentication(Ok(pins.verity), pins).unwrap(),
        Authentication::FsVerity(value) if value == pins.verity
    ));
    for measurement in [
        Ok(VerityDigest::new(Digest::sha256([2; 32]))),
        Err(VerityError::Unsealed),
        Err(VerityError::DigestFormat),
        Err(VerityError::Io {
            operation: "measure",
            source: std::io::Error::from_raw_os_error(libc::EIO),
        }),
    ] {
        catalog_authentication(measurement, pins).unwrap_err();
    }
}

#[test]
fn sha_authentication_pins_the_verified_inode_across_path_replacement() {
    let temporary = tempfile::tempdir().unwrap();
    let root = temporary.path().canonicalize().unwrap();
    let source = root.join("source");
    std::fs::write(&source, b"trusted").unwrap();
    std::fs::set_permissions(&source, std::fs::Permissions::from_mode(0o644)).unwrap();
    let owner = std::fs::metadata(&root).unwrap().uid();
    let expected = Authentication::CatalogSha256(digest(b"trusted"));
    let file = authenticate(open_protected(&source, owner).unwrap(), expected).unwrap();
    // Model a path replacement after authentication. The consumed descriptor
    // and linked boot image must keep the original contents, not the new path.
    std::fs::rename(&source, root.join("original")).unwrap();
    std::fs::write(&source, b"changed").unwrap();
    let mut bytes = [0; 7];
    file.file().read_exact_at(&mut bytes, 0).unwrap();
    assert_eq!(&bytes, b"trusted");
    assert!(matches!(
        authenticate(open_protected(&source, owner).unwrap(), expected),
        Err(AuthenticationError::Mismatch { .. })
    ));
    #[cfg(target_os = "linux")]
    if owner == 0 {
        let linked = root.join("kernel");
        file.link_to(&linked).unwrap();
        assert_eq!(std::fs::read(&linked).unwrap(), b"trusted");
        assert_eq!(
            std::fs::metadata(&linked).unwrap().ino(),
            file.file().metadata().unwrap().ino()
        );
    }
}

#[test]
fn sha_sources_require_safe_files_and_ancestors() {
    let temporary = tempfile::tempdir().unwrap();
    let root = temporary.path().canonicalize().unwrap();
    let source = root.join("source");
    let owner = std::fs::metadata(&root).unwrap().uid();
    std::fs::write(&source, b"trusted").unwrap();
    for mode in [0o664, 0o646, 0o666] {
        std::fs::set_permissions(&source, std::fs::Permissions::from_mode(mode)).unwrap();
        assert!(matches!(
            open_protected(&source, owner),
            Err(AuthenticationError::UnsafeFile)
        ));
    }
    std::fs::set_permissions(&source, std::fs::Permissions::from_mode(0o644)).unwrap();
    let alias = root.join("alias");
    std::os::unix::fs::symlink(&source, &alias).unwrap();
    #[cfg(target_os = "linux")]
    open_protected(&alias, owner).unwrap_err();
    open_protected(&root, owner).unwrap_err();
    std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o777)).unwrap();
    assert!(matches!(
        open_protected(&source, owner),
        Err(AuthenticationError::Directory(_))
    ));
}

#[test]
fn source_failures_preserve_retry_classification() {
    for (errno, transient) in [
        (libc::EIO, true),
        (libc::EINTR, true),
        (libc::EMFILE, true),
        (libc::ENOENT, false),
        (libc::EACCES, false),
    ] {
        let error = AuthenticationError::Io {
            operation: "hash",
            source: std::io::Error::from_raw_os_error(errno),
        };
        assert_eq!(error.is_transient(), transient);
    }
    assert!(
        !AuthenticationError::Mismatch {
            expected: digest(b"expected"),
            actual: digest(b"wrong")
        }
        .is_transient()
    );
}
