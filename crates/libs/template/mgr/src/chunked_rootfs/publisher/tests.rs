use super::*;
use crate::chunked_rootfs::{Leaf, decode_chunk, delivery::AuthenticatedManifest};

struct Sink {
    namespace: Namespace,
    objects: BTreeMap<String, Vec<u8>>,
    calls: usize,
    fail_on_call: Option<usize>,
}

#[tokio::test]
async fn dedicated_catalog_preserves_upload_receipts_across_reopen() {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("publisher/catalog.sqlite");
    let source = directory.path().join("image");
    let raw = vec![b'a'; 512 << 10];
    std::fs::write(&source, &raw).unwrap();
    let mut sink = Sink::new("durable");
    let mut catalog = store::Catalog::open(&database).await.unwrap();
    let first = catalog
        .publish(
            File::open(&source).unwrap(),
            &mut sink,
            directory.path(),
            ChunkSize::Kib512,
            digest_bytes(&raw),
        )
        .unwrap();
    assert!(first.statistics.uploaded_packs > 0);
    drop(catalog);
    let mut reopened = store::Catalog::open(&database).await.unwrap();
    let second = reopened
        .publish(
            File::open(source).unwrap(),
            &mut sink,
            directory.path(),
            ChunkSize::Kib512,
            digest_bytes(&raw),
        )
        .unwrap();
    assert_eq!(
        first.binding.manifest_digest,
        second.binding.manifest_digest
    );
    assert_eq!(second.statistics.uploaded_packs, 0);
    assert_eq!(second.statistics.compressed_chunks, 0);
    assert!(!second.statistics.manifest_uploaded);
    reopened
        .acknowledge_registration(sink.namespace.as_str(), &[second.binding.manifest_digest])
        .unwrap();
    reopened
        .acknowledge_registration(sink.namespace.as_str(), &[second.binding.manifest_digest])
        .unwrap();
}

impl Sink {
    fn new(prefix: &str) -> Self {
        Self {
            namespace: Namespace::new(
                &StorageUri::parse(&format!("s3://fixture/{prefix}?region=us-east-1")).unwrap(),
            ),
            objects: BTreeMap::new(),
            calls: 0,
            fail_on_call: None,
        }
    }
}

impl ObjectSink for Sink {
    fn namespace(&self) -> Namespace {
        self.namespace.clone()
    }

    fn put(
        &mut self,
        key: &str,
        source: &Path,
        digest: Digest,
    ) -> Result<UploadReceipt, UploadError> {
        let bytes = std::fs::read(source)?;
        assert_eq!(digest_bytes(&bytes), digest);
        self.calls += 1;
        // The response can be lost after storage commits. Retry must work even
        // though neither an upload receipt nor a ready publication was saved.
        if let Some(previous) = self.objects.insert(key.into(), bytes.clone()) {
            assert_eq!(previous, bytes);
        }
        if self.fail_on_call == Some(self.calls) {
            return Err(
                io::Error::new(io::ErrorKind::ConnectionReset, "injected lost response").into(),
            );
        }
        Ok(UploadReceipt {
            bytes: bytes.len() as u64,
            version_id: None,
        })
    }
}

fn restore(sink: &Sink, publication: &UploadedPublication) -> Vec<u8> {
    let stored = &sink.objects[&manifest_name(publication.binding.manifest_digest)];
    let manifest = AuthenticatedManifest::from_bytes(stored, publication.binding).unwrap();
    let mut result = Vec::new();
    for leaf in manifest.index().leaves() {
        match leaf {
            Leaf::Zero { length } => result.resize(result.len() + *length as usize, 0),
            Leaf::Zstd { length, digest } => {
                let location = &manifest.locations()[digest];
                let object = &sink.objects[&delivery::object_name(location.object)];
                let start = location.compressed_offset as usize;
                result.extend(
                    decode_chunk(
                        &object[start..start + location.compressed_length as usize],
                        *digest,
                        *length,
                    )
                    .unwrap(),
                );
            }
        }
    }
    result
}

#[test]
fn publication_recovers_lost_acknowledgments_and_reuses_only_its_namespace() {
    let directory = tempfile::tempdir().unwrap();
    let source = directory.path().join("rootfs");
    let mut raw = vec![0x41; 512 << 10];
    raw.resize(1 << 20, 0);
    raw.extend_from_slice(&vec![0x41; 512 << 10]);
    raw.extend_from_slice(b"partial final chunk");
    std::fs::write(&source, &raw).unwrap();
    let expected = digest_bytes(&raw);
    for failure in [1, 2] {
        let mut connection = crate::db::test_conn();
        let mut sink = Sink::new("one");
        sink.fail_on_call = Some(failure);
        publish(
            File::open(&source).unwrap(),
            &mut connection,
            &mut sink,
            directory.path(),
            ChunkSize::Kib512,
            expected,
        )
        .unwrap_err();
        assert!(
            store::find_chunk(
                &connection,
                sink.namespace.as_str(),
                digest_bytes(&raw[..512 << 10]),
                512 << 10
            )
            .unwrap()
            .is_some()
                == (failure == 2)
        );
        sink.fail_on_call = None;
        let completed = publish(
            File::open(&source).unwrap(),
            &mut connection,
            &mut sink,
            directory.path(),
            ChunkSize::Kib512,
            expected,
        )
        .unwrap();
        assert_eq!(restore(&sink, &completed), raw);
        assert_eq!(
            store::phase(
                &connection,
                sink.namespace.as_str(),
                completed.binding.manifest_digest
            )
            .unwrap(),
            Some(store::Phase::Uploaded)
        );
        let calls = sink.calls;
        let again = publish(
            File::open(&source).unwrap(),
            &mut connection,
            &mut sink,
            directory.path(),
            ChunkSize::Kib512,
            expected,
        )
        .unwrap();
        assert_eq!(
            again.binding.manifest_digest,
            completed.binding.manifest_digest
        );
        assert_eq!(again.statistics.compressed_chunks, 0);
        assert_eq!(again.statistics.reused_chunks, 2);
        assert_eq!(sink.calls, calls);
        let mut different = Sink::new("two");
        let other = publish(
            File::open(&source).unwrap(),
            &mut connection,
            &mut different,
            directory.path(),
            ChunkSize::Kib512,
            expected,
        )
        .unwrap();
        assert_eq!(other.statistics.reused_chunks, 0);
        assert_eq!(restore(&different, &other), raw);
    }
}

#[test]
fn wrong_source_cannot_publish_a_manifest_or_advance_registration() {
    let directory = tempfile::tempdir().unwrap();
    let source = directory.path().join("rootfs");
    std::fs::write(&source, b"wrong original").unwrap();
    let mut connection = crate::db::test_conn();
    let mut sink = Sink::new("one");
    assert!(matches!(
        publish(
            File::open(&source).unwrap(),
            &mut connection,
            &mut sink,
            directory.path(),
            ChunkSize::Kib512,
            digest_bytes(b"expected original")
        ),
        Err(PublishError::Source)
    ));
    assert!(sink.objects.keys().all(|key| !key.starts_with("m-")));
    assert!(
        !store::advance(
            &connection,
            sink.namespace.as_str(),
            digest_bytes(b"absent"),
            store::Phase::Uploaded,
            store::Phase::Registered
        )
        .unwrap()
    );
}

#[test]
fn fixed_pack_target_splits_frames_and_isolates_an_oversized_frame() {
    use rand::{Rng as _, SeedableRng as _};
    let directory = tempfile::tempdir().unwrap();
    let source = directory.path().join("rootfs");
    let mut raw = vec![0_u8; (65 << 20) + 17];
    rand::rngs::StdRng::seed_from_u64(31).fill_bytes(&mut raw);
    std::fs::write(&source, &raw).unwrap();
    let expected = digest_bytes(&raw);
    for size in [ChunkSize::Kib512, ChunkSize::Mib64] {
        let mut connection = crate::db::test_conn();
        let mut sink = Sink::new("one");
        let completed = publish(
            File::open(&source).unwrap(),
            &mut connection,
            &mut sink,
            directory.path(),
            size,
            expected,
        )
        .unwrap();
        assert_eq!(completed.statistics.uploaded_packs, 2);
        let manifest =
            AuthenticatedManifest::from_bytes(&completed.manifest_bytes, completed.binding)
                .unwrap();
        for (object, metadata) in manifest.objects() {
            let ranges: Vec<_> = manifest
                .locations()
                .values()
                .filter(|location| location.object == *object)
                .collect();
            if metadata.compressed_bytes > PACK_TARGET_BYTES {
                assert_eq!(ranges.len(), 1);
            }
        }
        assert_eq!(restore(&sink, &completed), raw);
        let packs = directory.path().join(format!("packs-{}", size.bytes()));
        std::fs::create_dir(&packs).unwrap();
        for (key, bytes) in &sink.objects {
            if key.starts_with("p-") {
                std::fs::write(packs.join(key), bytes).unwrap();
            }
        }
        for target in [16 << 20, 64 << 20, 256 << 20] {
            let mut repacked_sink = Sink::new("repacked");
            let mut catalog = crate::db::test_conn();
            let original_payload: u64 = manifest
                .locations()
                .values()
                .map(|entry| entry.compressed_length)
                .sum();
            // Lost acknowledgments must not expose a manifest; retry preserves
            // the same immutable objects even if an earlier put succeeded.
            repacked_sink.fail_on_call = Some(1);
            repack::repack(
                &manifest,
                &mut catalog,
                &DirectoryPacks(packs.clone()),
                &mut repacked_sink,
                directory.path(),
                target.to_string().parse().unwrap(),
            )
            .unwrap_err();
            assert!(
                repacked_sink
                    .objects
                    .keys()
                    .all(|key| !key.starts_with("m-"))
            );
            repacked_sink.fail_on_call = None;
            let repacked = repack::repack(
                &manifest,
                &mut catalog,
                &DirectoryPacks(packs.clone()),
                &mut repacked_sink,
                directory.path(),
                target.to_string().parse().unwrap(),
            )
            .unwrap();
            assert_eq!(
                repacked.binding.index_digest,
                completed.binding.index_digest
            );
            assert_eq!(
                repacked.binding.rootfs_digest,
                completed.binding.rootfs_digest
            );
            assert_eq!(repacked.statistics.compressed_chunks, 0);
            assert_eq!(
                repacked.statistics.repacked_compressed_bytes,
                original_payload
            );
            let new_manifest =
                AuthenticatedManifest::from_bytes(&repacked.manifest_bytes, repacked.binding)
                    .unwrap();
            for (digest, old) in manifest.locations() {
                let new = &new_manifest.locations()[digest];
                let old_pack = &sink.objects[&delivery::object_name(old.object)];
                let new_pack = &repacked_sink.objects[&delivery::object_name(new.object)];
                assert_eq!(
                    &old_pack[old.compressed_offset as usize
                        ..(old.compressed_offset + old.compressed_length) as usize],
                    &new_pack[new.compressed_offset as usize
                        ..(new.compressed_offset + new.compressed_length) as usize]
                );
            }
            for (object, descriptor) in new_manifest.objects() {
                if descriptor.compressed_bytes > target {
                    assert_eq!(
                        new_manifest
                            .locations()
                            .values()
                            .filter(|entry| entry.object == *object)
                            .count(),
                        1
                    );
                }
            }
            assert_eq!(restore(&repacked_sink, &repacked), raw);
            if target == 256 << 20 {
                assert_eq!(repacked.statistics.uploaded_packs, 1);
            }
            if target == 16 << 20 && size == ChunkSize::Kib512 {
                assert!(repacked.statistics.uploaded_packs >= 5);
            }
        }
        // A changed source frame fails before any manifest can be acknowledged.
        let first = manifest.objects().keys().next().unwrap();
        let path = packs.join(delivery::object_name(*first));
        use std::os::unix::fs::FileExt;
        File::options()
            .write(true)
            .open(path)
            .unwrap()
            .write_all_at(b"bad!", 0)
            .unwrap();
        let mut bad_sink = Sink::new("corrupt");
        assert!(matches!(
            repack::repack(
                &manifest,
                &mut crate::db::test_conn(),
                &DirectoryPacks(packs),
                &mut bad_sink,
                directory.path(),
                repack::PackTarget::default()
            ),
            Err(repack::RepackError::Frame(_))
        ));
        assert!(bad_sink.objects.keys().all(|key| !key.starts_with("m-")));
    }
}

struct DirectoryPacks(std::path::PathBuf);

impl repack::PackSource for DirectoryPacks {
    fn open(&self, object: Digest) -> io::Result<File> {
        File::open(self.0.join(delivery::object_name(object)))
    }
}
