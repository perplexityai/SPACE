use super::*;
use crate::chunked_rootfs::{ChunkSize, delivery, digest_bytes, publish_chunks};

fn fixture() -> (AuthenticatedManifest, Binding, SourceIdentity) {
    let directory = tempfile::tempdir().unwrap();
    let source = directory.path().join("image");
    let mut raw = vec![0x41; 512 << 10];
    raw.resize(1 << 20, 0);
    raw.extend_from_slice(&vec![0x41; 512 << 10]);
    std::fs::write(&source, &raw).unwrap();
    let digest = digest_bytes(&raw);
    let index = publish_chunks(&source, directory.path(), ChunkSize::Kib512, digest).unwrap();
    let chunk = digest_bytes(&raw[..512 << 10]);
    let frame = std::fs::read(
        directory
            .path()
            .join(crate::chunked_rootfs::object_name(chunk)),
    )
    .unwrap();
    let object = digest_bytes(&frame);
    let (bytes, binding) = delivery::encode(
        &index,
        BTreeMap::from([(
            object,
            delivery::Object {
                compressed_bytes: frame.len() as u64,
                version_id: None,
            },
        )]),
        BTreeMap::from([(
            chunk,
            delivery::Location {
                object,
                compressed_offset: 0,
                compressed_length: frame.len() as u64,
            },
        )]),
    )
    .unwrap();
    let manifest = AuthenticatedManifest::from_bytes(&bytes, binding).unwrap();
    let identity = SourceIdentity {
        subvolume: ReadonlySubvolumeIdentity {
            uuid: Uuid::now_v7(),
            tree_id: 300,
            generation: 9,
        },
        device: 1,
        inode: 44,
        length: raw.len() as u64,
        ctime_seconds: 10,
        ctime_nanos: 20,
    };
    (manifest, binding, identity)
}

fn ready(
    transaction: &Transaction<'_>,
    name: &str,
    binding: Binding,
    manifest: &AuthenticatedManifest,
) -> bool {
    crate::db::upsert_build_in_transaction(
        transaction,
        &crate::db::BuildUpsert {
            name,
            content_hash: &binding.rootfs_digest.to_string(),
            build_date: "2026-01-01T00:00:00Z",
            manifest_json: "{}",
            runtime: "firecracker",
            arch: "x86_64",
            artifact_size_bytes: manifest.index().logical_bytes(),
            state: "ready",
        },
    )
    .unwrap()
}

#[test]
fn source_admission_rolls_back_with_readiness_and_leases_fence_retirement() {
    let (manifest, binding, identity) = fixture();
    let mut conn = crate::db::test_conn();
    {
        let transaction = conn.transaction().unwrap();
        assert!(ready(&transaction, "interrupted", binding, &manifest));
        admit(
            &transaction,
            &"interrupted".parse().unwrap(),
            binding,
            &manifest,
            identity,
        )
        .unwrap();
        // Simulates failure before the single admission/readiness commit.
    }
    assert!(
        crate::db::get_build(&conn, "interrupted", &binding.rootfs_digest.to_string())
            .unwrap()
            .is_none()
    );
    assert!(
        select_and_lease(&mut conn, &manifest)
            .unwrap()
            .candidates
            .is_empty()
    );
    for name in ["source-a", "source-b"] {
        let transaction = conn.transaction().unwrap();
        assert!(ready(&transaction, name, binding, &manifest));
        admit(
            &transaction,
            &name.parse().unwrap(),
            binding,
            &manifest,
            identity,
        )
        .unwrap();
        transaction.commit().unwrap();
    }
    let readers = select_and_lease(&mut conn, &manifest).unwrap();
    assert_eq!(
        readers.candidates.len(),
        2,
        "repeated extents are deduplicated per source, with both sources retained"
    );
    assert!(
        readers
            .candidates
            .iter()
            .all(|candidate| candidate.offset == 0 && candidate.length == 512 << 10)
    );
    let hash = binding.rootfs_digest.to_string();
    assert!(!begin_retirement(&conn, "source-a", &hash).unwrap());
    let subsequent = select_and_lease(&mut conn, &manifest).unwrap();
    assert_eq!(subsequent.candidates.len(), 1);
    assert_eq!(subsequent.candidates[0].template.as_str(), "source-b");
    assert!(
        crate::db::delete_build(&conn, "source-a", &hash).is_err(),
        "a separate delete path cannot remove a leased source"
    );
    release(&conn, readers.lease_id).unwrap();
    assert!(begin_retirement(&conn, "source-a", &hash).unwrap());
    crate::db::delete_build(&conn, "source-a", &hash).unwrap();
    assert!(!begin_retirement(&conn, "source-b", &hash).unwrap());
    // Exclusive startup is the only authority to discard a dead process's leases.
    recover_leases_after_exclusive_start(&conn).unwrap();
    assert!(begin_retirement(&conn, "source-b", &hash).unwrap());
    crate::db::delete_build(&conn, "source-b", &hash).unwrap();
    assert!(
        select_and_lease(&mut conn, &manifest)
            .unwrap()
            .candidates
            .is_empty()
    );
}

#[test]
fn deleting_an_orphan_fences_a_racing_ready_commit() {
    let (manifest, binding, _) = fixture();
    let mut conn = crate::db::test_conn();
    let hash = binding.rootfs_digest.to_string();
    assert!(begin_retirement(&conn, "orphan", &hash).unwrap());
    {
        let transaction = conn.transaction().unwrap();
        assert!(!ready(&transaction, "orphan", binding, &manifest));
        transaction.commit().unwrap();
    }
    crate::db::delete_build(&conn, "orphan", &hash).unwrap();
    let transaction = conn.transaction().unwrap();
    assert!(ready(&transaction, "orphan", binding, &manifest));
}

#[test]
fn template_retirement_blocks_new_admission_and_resumes_after_reader_exit() {
    let (manifest, binding, identity) = fixture();
    let mut conn = crate::db::test_conn();
    let transaction = conn.transaction().unwrap();
    assert!(ready(&transaction, "source-a", binding, &manifest));
    admit(
        &transaction,
        &"source-a".parse().unwrap(),
        binding,
        &manifest,
        identity,
    )
    .unwrap();
    transaction.commit().unwrap();
    let reader = select_and_lease(&mut conn, &manifest).unwrap();
    assert!(!begin_template_retirement(&conn, "source-a").unwrap());
    assert!(
        select_and_lease(&mut conn, &manifest)
            .unwrap()
            .candidates
            .is_empty()
    );
    {
        let transaction = conn.transaction().unwrap();
        assert!(!ready(
            &transaction,
            "source-a",
            Binding {
                rootfs_digest: digest_bytes(b"new release"),
                ..binding
            },
            &manifest
        ));
    }
    assert!(crate::db::delete_template_local_state(&conn, "source-a").is_err());
    release(&conn, reader.lease_id).unwrap();
    assert!(begin_template_retirement(&conn, "source-a").unwrap());
    crate::db::delete_template_local_state(&conn, "source-a").unwrap();
    let transaction = conn.transaction().unwrap();
    assert!(
        ready(&transaction, "source-a", binding, &manifest),
        "completed deletion releases its admission fence"
    );
}
