use super::*;
use pplx_space_common_core::types::Digest;

#[test]
fn unsealed_files_cannot_be_used_as_authenticated_inputs() {
    let file = tempfile::tempfile().unwrap();
    let expected = VerityDigest::new(Digest::sha256([0; 32]));
    assert!(matches!(
        check(&file, expected),
        Err(VerityError::Unsealed | VerityError::Unsupported)
    ));
}

#[test]
fn sealing_preserves_links_and_checks_the_actual_inode() {
    use std::io::Read;
    use std::os::unix::fs::MetadataExt;
    let Some(directory) = crate::tests::verity_test_directory() else {
        return;
    };
    let filesystem = File::open(directory.path()).unwrap();
    assert!(
        filesystem_has_verity_api(&filesystem)
            .expect("privileged btrfs lane must expose verity ioctls")
    );
    assert!(
        std::fs::read_dir(directory.path())
            .unwrap()
            .next()
            .is_none(),
        "capability detection must not create probe files"
    );
    let source = directory.path().join("source");
    std::fs::write(&source, b"SPACE immutable execution fixture\n").unwrap();
    let file = open_readonly(&source).unwrap();
    let expected: VerityDigest =
        "sha256:01f42ea44ac4d8d5291c536b92a028d6dc7d957b264e7addf3d397bc45d055a3"
            .parse()
            .unwrap();
    let inode = file.metadata().unwrap().ino();
    seal(&file, expected).expect("privileged btrfs lane must support fs-verity");
    assert_eq!(file.metadata().unwrap().ino(), inode);
    assert_eq!(measure(&file).unwrap(), expected);
    assert_eq!(
        File::options()
            .write(true)
            .open(&source)
            .unwrap_err()
            .raw_os_error(),
        Some(libc::EPERM)
    );
    let wrong = VerityDigest::new(Digest::sha256([0; 32]));
    assert!(matches!(
        SealedFile::open(&source, wrong),
        Err(VerityError::Mismatch { .. })
    ));
    let original = SealedFile::open(&source, expected).unwrap();
    let moved = directory.path().join("moved");
    std::fs::rename(&source, &moved).unwrap();
    std::fs::write(&source, b"untrusted replacement").unwrap();
    assert!(matches!(
        SealedFile::open(&source, expected),
        Err(VerityError::Unsealed)
    ));
    #[cfg(target_os = "linux")]
    if file.metadata().unwrap().uid() == 0 {
        let linked = directory.path().join("linked");
        original.link_to(&linked).unwrap();
        assert_eq!(std::fs::metadata(&linked).unwrap().ino(), inode);
        SealedFile::open(&linked, expected).unwrap();
    }
    let mut content = Vec::new();
    original
        .file()
        .try_clone()
        .unwrap()
        .read_to_end(&mut content)
        .unwrap();
    assert_eq!(content, b"SPACE immutable execution fixture\n");
    let copy = directory.path().join("copy");
    std::fs::copy(moved, &copy).unwrap();
    assert!(matches!(
        SealedFile::open(&copy, expected),
        Err(VerityError::Unsealed)
    ));
}
