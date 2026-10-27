use super::*;
use crate::chunked_rootfs::{ChunkSize, delivery, digest_bytes, publisher::*};
use pplx_space_common_core::types::StorageUri;
use std::sync::atomic::{AtomicUsize, Ordering};

struct Store {
    objects: BTreeMap<String, Vec<u8>>,
    requests: AtomicUsize,
    corrupt: bool,
}

impl ObjectSink for Store {
    fn namespace(&self) -> Namespace {
        Namespace::new(&StorageUri::parse("s3://fixture/chunks?region=us-east-1").unwrap())
    }
    fn put(
        &mut self,
        key: &str,
        source: &Path,
        digest: Digest,
    ) -> Result<UploadReceipt, UploadError> {
        let bytes = std::fs::read(source)?;
        assert_eq!(digest_bytes(&bytes), digest);
        let receipt = UploadReceipt {
            bytes: bytes.len() as u64,
            version_id: None,
        };
        self.objects.insert(key.into(), bytes);
        Ok(receipt)
    }
}

#[async_trait::async_trait]
impl RangeFetcher for Store {
    async fn fetch(
        &self,
        object: Digest,
        range: ObjectByteRange,
        destination: &Path,
    ) -> Result<u64, tonic::Status> {
        self.requests.fetch_add(1, Ordering::SeqCst);
        let object = &self.objects[&delivery::object_name(object)];
        assert_eq!(object.len() as u64, range.object_size());
        let mut bytes =
            object[range.offset() as usize..(range.offset() + range.length()) as usize].to_vec();
        if self.corrupt {
            bytes[0] ^= 0xff;
        }
        std::fs::write(destination, bytes).unwrap();
        Ok(range.length())
    }
}

#[tokio::test]
async fn packed_writer_to_bounded_reader_preserves_repeats_zeros_and_eof() {
    let directory = tempfile::tempdir().unwrap();
    let source = directory.path().join("source");
    let mut raw = vec![b'a'; 512 << 10];
    raw.extend_from_slice(&vec![0; 512 << 10]);
    raw.extend_from_slice(&vec![b'a'; 512 << 10]);
    raw.extend_from_slice(b"exact partial EOF");
    std::fs::write(&source, &raw).unwrap();
    let mut store = Store {
        objects: BTreeMap::new(),
        requests: AtomicUsize::new(0),
        corrupt: false,
    };
    let publication = publish(
        File::open(source).unwrap(),
        &mut crate::db::test_conn(),
        &mut store,
        directory.path(),
        ChunkSize::Kib512,
        digest_bytes(&raw),
    )
    .unwrap();
    let manifest = Arc::new(
        AuthenticatedManifest::from_bytes(&publication.manifest_bytes, publication.binding)
            .unwrap(),
    );
    for scheduling in [
        "coupled",
        "pipeline",
        "parallel-frames",
        "overlap-reuse",
        "combined",
    ] {
        store.requests.store(0, Ordering::SeqCst);
        let output_path = directory.path().join(format!("output-{scheduling}"));
        let output = Arc::new(
            File::options()
                .read(true)
                .write(true)
                .create_new(true)
                .open(&output_path)
                .unwrap(),
        );
        let stats = assemble_with_tuning(
            Arc::clone(&manifest),
            BTreeMap::new(),
            output,
            directory.path(),
            &store,
            Tuning::from_values(Some("16"), Some("2097152"), Some(scheduling)).unwrap(),
        )
        .await
        .unwrap();
        assert_eq!(std::fs::read(output_path).unwrap(), raw);
        assert_eq!(stats.zero_logical_bytes, 512 << 10);
        assert_eq!(
            stats.downloaded_logical_bytes,
            raw.len() as u64 - (512 << 10)
        );
        assert_eq!(
            stats.requests, 1,
            "adjacent independent frames share one range; repeated leaves reuse decoded bytes"
        );
        assert_eq!(store.requests.load(Ordering::SeqCst), 1);
        store.corrupt = true;
        let output = Arc::new(tempfile::tempfile().unwrap());
        assert!(matches!(
            assemble_with_tuning(
                Arc::clone(&manifest),
                BTreeMap::new(),
                output,
                directory.path(),
                &store,
                Tuning::from_values(Some("16"), Some("2097152"), Some(scheduling)).unwrap()
            )
            .await,
            Err(AssemblyError::Chunk(_))
        ));
        store.corrupt = false;
    }
}

#[test]
fn frame_buffer_budget_reduces_large_chunk_parallelism() {
    for requested in [1, 4, 16] {
        let limit: RangeConcurrency = requested.to_string().parse().unwrap();
        for bytes in [0, 1 << 20, 129 << 20, 1 << 30] {
            let allowed = limit.for_frame_bytes(bytes).unwrap();
            assert!((1..=requested).contains(&allowed));
            assert!(allowed as u64 * bytes <= 1 << 30);
            if allowed < requested {
                assert!((allowed as u64 + 1) * bytes > 1 << 30);
            }
        }
        limit.for_frame_bytes((1 << 30) + 1).unwrap_err();
    }
}

struct GatedStore {
    store: Store,
    entered: tokio::sync::mpsc::Sender<()>,
    release: tokio::sync::Semaphore,
    active: AtomicUsize,
    peak: AtomicUsize,
}

#[async_trait::async_trait]
impl RangeFetcher for GatedStore {
    async fn fetch(
        &self,
        object: Digest,
        range: ObjectByteRange,
        destination: &Path,
    ) -> Result<u64, tonic::Status> {
        let count = self.active.fetch_add(1, Ordering::SeqCst) + 1;
        self.peak.fetch_max(count, Ordering::SeqCst);
        self.entered.send(()).await.unwrap();
        self.release.acquire().await.unwrap().forget();
        let result = self.store.fetch(object, range, destination).await;
        self.active.fetch_sub(1, Ordering::SeqCst);
        result
    }
}

#[tokio::test]
async fn configured_parallel_ranges_make_progress_without_exceeding_the_limit() {
    let directory = tempfile::tempdir().unwrap();
    let source = directory.path().join("source");
    let raw: Vec<u8> = (1..=20)
        .flat_map(|byte| std::iter::repeat_n(byte, 512 << 10))
        .collect();
    std::fs::write(&source, &raw).unwrap();
    let frame_directory = directory.path().join("frames");
    let index = crate::chunked_rootfs::publish_chunks(
        &source,
        &frame_directory,
        ChunkSize::Kib512,
        digest_bytes(&raw),
    )
    .unwrap();
    let authenticated = crate::chunked_rootfs::AuthenticatedIndex::from_bytes(
        &index.bytes,
        index.digest,
        digest_bytes(&raw),
    )
    .unwrap();
    let mut objects = BTreeMap::new();
    let mut locations = BTreeMap::new();
    let mut stored = BTreeMap::new();
    for leaf in authenticated.leaves() {
        let Leaf::Zstd { digest, .. } = leaf else {
            panic!("fixture has no zero chunks")
        };
        let frame =
            std::fs::read(frame_directory.join(crate::chunked_rootfs::object_name(*digest)))
                .unwrap();
        let object = digest_bytes(&frame);
        let length = frame.len() as u64;
        objects.insert(
            object,
            delivery::Object {
                compressed_bytes: length,
                version_id: None,
            },
        );
        locations.insert(
            *digest,
            delivery::Location {
                object,
                compressed_offset: 0,
                compressed_length: length,
            },
        );
        stored.insert(delivery::object_name(object), frame);
    }
    let (encoded, binding) = delivery::encode(&index, objects, locations).unwrap();
    let manifest = Arc::new(AuthenticatedManifest::from_bytes(&encoded, binding).unwrap());
    for scheduling in [
        "coupled",
        "pipeline",
        "parallel-frames",
        "overlap-reuse",
        "combined",
    ] {
        for concurrency in [1, 4, 16] {
            let (entered, mut arrivals) = tokio::sync::mpsc::channel(20);
            let fetcher = Arc::new(GatedStore {
                store: Store {
                    objects: stored.clone(),
                    requests: AtomicUsize::new(0),
                    corrupt: false,
                },
                entered,
                release: tokio::sync::Semaphore::new(0),
                active: AtomicUsize::new(0),
                peak: AtomicUsize::new(0),
            });
            let path = directory
                .path()
                .join(format!("output-{scheduling}-{concurrency}"));
            let output = Arc::new(
                File::options()
                    .write(true)
                    .create_new(true)
                    .open(&path)
                    .unwrap(),
            );
            let task_manifest = Arc::clone(&manifest);
            let task_fetcher = Arc::clone(&fetcher);
            let scratch = directory.path().to_path_buf();
            let task = tokio::spawn(async move {
                assemble_with_tuning(
                    task_manifest,
                    BTreeMap::new(),
                    output,
                    &scratch,
                    task_fetcher.as_ref(),
                    Tuning::from_values(Some(&concurrency.to_string()), None, Some(scheduling))
                        .unwrap(),
                )
                .await
            });
            // Explicit gates prove simultaneous progress; no simulated per-request
            // latency or speed threshold is part of the correctness test.
            for _ in 0..concurrency {
                tokio::time::timeout(std::time::Duration::from_secs(3), arrivals.recv())
                    .await
                    .unwrap()
                    .unwrap();
            }
            assert_eq!(fetcher.active.load(Ordering::SeqCst), concurrency);
            fetcher.release.add_permits(20);
            task.await.unwrap().unwrap();
            assert_eq!(fetcher.peak.load(Ordering::SeqCst), concurrency);
            assert_eq!(fetcher.active.load(Ordering::SeqCst), 0);
            assert_eq!(std::fs::read(path).unwrap(), raw);
        }
    }
}

#[tokio::test]
async fn request_target_changes_fetch_grouping_not_bytes_or_repeated_chunk_work() {
    let directory = tempfile::tempdir().unwrap();
    // Deterministic synthetic bytes with low compressibility. No private image
    // content is needed to exercise coalescing across request-size boundaries.
    let mut state = 0x0123_4567_89ab_cdef_u64;
    let mut raw: Vec<u8> = (0..(10 << 19))
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state as u8
        })
        .collect();
    raw.extend_from_within(..(512 << 10));
    raw.resize(raw.len() + (512 << 10), 0);
    raw.extend_from_slice(b"partial last leaf");
    let source = directory.path().join("source");
    std::fs::write(&source, &raw).unwrap();
    let mut store = Store {
        objects: BTreeMap::new(),
        requests: AtomicUsize::new(0),
        corrupt: false,
    };
    let publication = publish(
        File::open(source).unwrap(),
        &mut crate::db::test_conn(),
        &mut store,
        directory.path(),
        ChunkSize::Kib512,
        digest_bytes(&raw),
    )
    .unwrap();
    let manifest = Arc::new(
        AuthenticatedManifest::from_bytes(&publication.manifest_bytes, publication.binding)
            .unwrap(),
    );
    let mut counts = Vec::new();
    for target in [1 << 20, 2 << 20, 8 << 20, 32 << 20] {
        let output_path = directory.path().join(format!("output-{target}"));
        let output = Arc::new(
            File::options()
                .create_new(true)
                .write(true)
                .open(&output_path)
                .unwrap(),
        );
        let stats = assemble_with_tuning(
            Arc::clone(&manifest),
            BTreeMap::new(),
            output,
            directory.path(),
            &store,
            Tuning::from_values(Some("16"), Some(&target.to_string()), None).unwrap(),
        )
        .await
        .unwrap();
        assert_eq!(std::fs::read(output_path).unwrap(), raw);
        assert_eq!(stats.overfetch_bytes, 0);
        counts.push((stats.requests, stats.downloaded_bytes));
    }
    assert!(counts[0].0 > counts[1].0 && counts[1].0 > counts[2].0);
    assert_eq!(counts[2].0, 1);
    assert!(counts.iter().all(|(_, bytes)| *bytes == counts[0].1));
}
