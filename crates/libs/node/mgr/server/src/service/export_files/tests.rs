use std::os::unix::fs::{PermissionsExt, symlink};

use super::*;

#[test]
fn inventory_preserves_files_without_changing_access_and_bounds_untrusted_entries() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().canonicalize().unwrap();
    let instance = root.join("instance");
    let snapshot = root.join("snapshot");
    std::fs::create_dir(&instance).unwrap();
    std::fs::create_dir(&snapshot).unwrap();
    std::fs::write(instance.join("rootfs.ext4"), b"rootfs").unwrap();
    std::fs::write(instance.join("runtime.json"), b"runtime").unwrap();
    std::fs::write(snapshot.join("memory"), b"memory").unwrap();
    std::fs::write(snapshot.join("vmstate"), b"state").unwrap();
    let original_mode = std::fs::metadata(&instance).unwrap().permissions().mode();
    for include_snapshot in [false, true] {
        let files = inventory(&root, include_snapshot).unwrap();
        let actual: Vec<_> = files
            .iter()
            .map(|file| (file.name.as_str(), file.size_bytes))
            .collect();
        let mut expected = vec![("rootfs.ext4", 6), ("runtime.json", 7)];
        if include_snapshot {
            expected.extend([("snapshot/memory", 6), ("snapshot/vmstate", 5)]);
        }
        assert_eq!(actual, expected);
    }
    assert_eq!(
        std::fs::metadata(&instance).unwrap().permissions().mode(),
        original_mode
    );

    let alias = instance.join("alias");
    symlink(instance.join("rootfs.ext4"), &alias).unwrap();
    assert_eq!(
        inventory(&root, false).unwrap_err().code(),
        tonic::Code::FailedPrecondition
    );
    std::fs::remove_file(&alias).unwrap();
    for index in 0..MAX_EXPORT_ENTRIES {
        std::fs::write(instance.join(format!("extra-{index}")), b"").unwrap();
    }
    assert_eq!(
        inventory(&root, false).unwrap_err().code(),
        tonic::Code::ResourceExhausted
    );
    std::fs::rename(&instance, root.join("original")).unwrap();
    symlink(root.join("original"), &instance).unwrap();
    assert_eq!(
        inventory(&root, false).unwrap_err().code(),
        tonic::Code::FailedPrecondition
    );
}

#[test]
fn export_reader_pins_regular_files_and_streams_sparse_offsets_without_permission_changes() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().canonicalize().unwrap();
    let instance = root.join("instance");
    std::fs::create_dir(&instance).unwrap();
    let path = instance.join("rootfs.ext4");
    let mut bytes = vec![0; 1024 * 1024 + 123];
    bytes[512 * 1024] = 42;
    std::fs::write(&path, &bytes).unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
    let file = open_export_file(&root, "rootfs.ext4").unwrap();
    // A replaced name must not redirect the privileged reader.
    std::fs::rename(&path, instance.join("retained")).unwrap();
    std::fs::write(root.join("outside"), b"must not export").unwrap();
    symlink(root.join("outside"), &path).unwrap();
    let mut chunks = Vec::new();
    stream_file(
        file,
        || false,
        |chunk| {
            chunks.push(chunk);
            Ok(())
        },
    )
    .unwrap();
    assert_eq!(chunks.len(), 2);
    assert_eq!(
        (chunks[0].offset, chunks[0].data.len(), chunks[0].eof),
        (512 * 1024, 512 * 1024, false)
    );
    assert_eq!(
        (chunks[1].offset, chunks[1].data.len(), chunks[1].eof),
        (bytes.len() as u64, 0, true)
    );
    let mut restored = vec![0; bytes.len()];
    for chunk in chunks {
        restored[chunk.offset as usize..chunk.offset as usize + chunk.data.len()]
            .copy_from_slice(&chunk.data);
    }
    assert_eq!(restored, bytes);
    assert_eq!(
        std::fs::metadata(instance.join("retained"))
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o600
    );
    for name in [
        "rootfs.ext4",
        "../outside",
        "snapshot/../outside",
        "/outside",
    ] {
        open_export_file(&root, name).unwrap_err();
    }
    assert_eq!(
        stream_file(
            open_export_file(&root, "retained").unwrap(),
            || true,
            |_| panic!("cancelled reader sent data")
        )
        .unwrap_err()
        .code(),
        tonic::Code::Cancelled
    );
    assert_eq!(
        stream_file(
            open_export_file(&root, "retained").unwrap(),
            || false,
            |_| Err(Status::unavailable("consumer failure"))
        )
        .unwrap_err()
        .code(),
        tonic::Code::Unavailable
    );
}
