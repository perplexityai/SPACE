use super::super::{PrepareParams, prepare, release};
use super::*;
use std::os::unix::fs::MetadataExt;

#[tokio::test]
#[ignore = "requires an explicitly supplied disposable btrfs fixture and root with loop/dm-crypt"]
async fn encrypted_disk_copy_is_independent_sparse_and_replay_safe() {
    let Some(root) = std::env::var_os("SPACE_NODEMGR_TEST_BTRFS_DIR") else {
        return;
    };
    assert!(nix::unistd::geteuid().is_root());
    let fixture = tempfile::tempdir_in(root).unwrap();
    let root = fixture.path().canonicalize().unwrap();
    let origin = root.join("rootfs.ext4");
    File::create(&origin)
        .unwrap()
        .set_len(16 * 1024 * 1024)
        .unwrap();
    let source = SandboxId::new();
    let child = SandboxId::new();
    let source_cow = root.join("source.luks");
    let child_cow = root.join("child.luks");
    let source_key = zeroize::Zeroizing::new(vec![11; 32]);
    let child_key = zeroize::Zeroizing::new(vec![29; 32]);
    let open = |id, cow, key| {
        prepare(PrepareParams {
            sandbox_id: id,
            origin_path: origin.clone(),
            cow_path: cow,
            restore_cow_source: None,
            rootfs_size_bytes: 0,
            dek: key,
        })
    };
    let source_device = open(source, source_cow.clone(), &source_key).await.unwrap();
    let disk = File::options()
        .read(true)
        .write(true)
        .open(source_device.mapper_path)
        .unwrap();
    disk.write_all_at(b"parent secret", 4096).unwrap();
    disk.sync_all().unwrap();
    drop(disk);
    let name = super::super::composite_name(&source);
    let active = run(
        "dmsetup",
        &["info", "--columns", "--noheadings", "-o", "attr", &name],
    )
    .await
    .unwrap();
    let capture = super::super::capture_quiesced(source, async {
        let quiesced = run(
            "dmsetup",
            &["info", "--columns", "--noheadings", "-o", "attr", &name],
        )
        .await
        .unwrap();
        assert_ne!(quiesced, active, "disk I/O was not quiesced during capture");
        Err::<(), _>(tonic::Status::internal("capture failed"))
    })
    .await;
    assert!(capture.is_err());
    assert_eq!(
        run(
            "dmsetup",
            &["info", "--columns", "--noheadings", "-o", "attr", &name]
        )
        .await
        .unwrap(),
        active,
        "capture failure left guest disk I/O suspended"
    );
    release(&source, &origin).await.unwrap();

    // An unlock failure must not publish a partial target or strand mappers.
    assert!(
        stage(child, &source_cow, &child_cow, b"wrong key", &child_key)
            .await
            .is_err()
    );
    assert!(!child_cow.exists());
    for suffix in ["source", "target"] {
        assert!(
            !dm_exists(&format!("sbxroot-{child}-fork-{suffix}"))
                .await
                .unwrap()
        );
    }
    stage(child, &source_cow, &child_cow, &source_key, &child_key)
        .await
        .unwrap();
    let metadata = std::fs::metadata(&child_cow).unwrap();
    assert!(
        metadata.blocks() * 512 < metadata.len() / 2,
        "fork materialized sparse COW holes"
    );
    let mut source_header = [0; 4096];
    let mut child_header = [0; 4096];
    File::open(&source_cow)
        .unwrap()
        .read_exact_at(&mut source_header, 0)
        .unwrap();
    File::open(&child_cow)
        .unwrap()
        .read_exact_at(&mut child_header, 0)
        .unwrap();
    assert_ne!(
        &source_header[..4096],
        &child_header[..4096],
        "child must have a fresh LUKS header"
    );
    let geometry = layout(source_cow.to_str().unwrap()).await.unwrap();
    File::open(&source_cow)
        .unwrap()
        .read_exact_at(&mut source_header, geometry.offset)
        .unwrap();
    File::open(&child_cow)
        .unwrap()
        .read_exact_at(&mut child_header, geometry.offset)
        .unwrap();
    assert_ne!(
        source_header, child_header,
        "equal COW plaintext must have independently encrypted ciphertext"
    );
    assert!(
        open(child, child_cow.clone(), &source_key).await.is_err(),
        "source key unlocked child"
    );
    let child_device = open(child, child_cow.clone(), &child_key).await.unwrap();
    let disk = File::options()
        .read(true)
        .write(true)
        .open(child_device.mapper_path)
        .unwrap();
    let mut bytes = [0; 13];
    disk.read_exact_at(&mut bytes, 4096).unwrap();
    assert_eq!(&bytes, b"parent secret");
    disk.write_all_at(b"child writes!", 4096).unwrap();
    disk.sync_all().unwrap();
    drop(disk);
    release(&child, &origin).await.unwrap();
    std::fs::remove_file(&source_cow).unwrap();
    // Replaying against the canonical child must not need its source anymore.
    stage(child, &source_cow, &child_cow, b"revoked", &child_key)
        .await
        .unwrap();
    let child_device = open(child, child_cow.clone(), &child_key).await.unwrap();
    let disk = File::open(child_device.mapper_path).unwrap();
    disk.read_exact_at(&mut bytes, 4096).unwrap();
    assert_eq!(&bytes, b"child writes!");
    drop(disk);
    release(&child, &origin).await.unwrap();
}
