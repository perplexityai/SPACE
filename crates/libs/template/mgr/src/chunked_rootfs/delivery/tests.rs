use super::*;
use crate::chunked_rootfs::{ChunkSize, decode_chunk, publish_chunks};
use sha2::Digest as _;

struct Fixture {
    index: PublishedIndex,
    original: Vec<u8>,
    frames: BTreeMap<Digest, Vec<u8>>,
}

fn fixture() -> Fixture {
    let directory = tempfile::tempdir().unwrap();
    let mut original = vec![0x5a; 512 << 10];
    original.resize(1 << 20, 0);
    original.extend_from_slice(&vec![0x33; 512 << 10]);
    original.extend_from_slice(&vec![0x5a; 512 << 10]);
    original.extend_from_slice(b"tail");
    let source = directory.path().join("image.ext4");
    std::fs::write(&source, &original).unwrap();
    let legacy_digest = Digest::sha256(sha2::Sha256::digest(&original).into());
    let objects = directory.path().join("objects");
    let index = publish_chunks(&source, &objects, ChunkSize::Kib512, legacy_digest).unwrap();
    let authenticated =
        AuthenticatedIndex::from_bytes(&index.bytes, index.digest, legacy_digest).unwrap();
    let frames = authenticated
        .leaves()
        .iter()
        .filter_map(|leaf| match leaf {
            Leaf::Zero { .. } => None,
            Leaf::Zstd { digest, .. } => Some((
                *digest,
                std::fs::read(objects.join(crate::chunked_rootfs::object_name(*digest))).unwrap(),
            )),
        })
        .collect();
    Fixture {
        index,
        original,
        frames,
    }
}

fn packed(fixture: &Fixture) -> (Vec<u8>, Binding, Vec<u8>) {
    let mut pack = Vec::new();
    let mut offsets = BTreeMap::new();
    for (digest, frame) in &fixture.frames {
        offsets.insert(*digest, (pack.len() as u64, frame.len() as u64));
        pack.extend_from_slice(frame);
    }
    let object = digest_bytes(&pack);
    let locations = offsets
        .into_iter()
        .map(|(digest, (offset, length))| {
            (
                digest,
                Location {
                    object,
                    compressed_offset: offset,
                    compressed_length: length,
                },
            )
        })
        .collect();
    let objects = BTreeMap::from([(
        object,
        Object {
            compressed_bytes: pack.len() as u64,
            version_id: None,
        },
    )]);
    let (bytes, binding) = encode(&fixture.index, objects, locations).unwrap();
    (bytes, binding, pack)
}

#[test]
fn packed_ranges_reconstruct_legacy_image_and_repacking_preserves_index() {
    let fixture = fixture();
    assert_eq!(fixture.index.image_blake3, digest_bytes(&fixture.original));
    let (bytes, binding, pack) = packed(&fixture);
    let manifest = AuthenticatedManifest::from_bytes(&bytes, binding).unwrap();
    let mut output = Vec::new();
    for leaf in manifest.index().leaves() {
        match leaf {
            Leaf::Zero { length } => output.resize(output.len() + *length as usize, 0),
            Leaf::Zstd { digest, length } => {
                let location = &manifest.locations()[digest];
                let start = location.compressed_offset as usize;
                let end = start + location.compressed_length as usize;
                output
                    .extend_from_slice(&decode_chunk(&pack[start..end], *digest, *length).unwrap());
            }
        }
    }
    assert_eq!(output, fixture.original);
    let mut objects = BTreeMap::new();
    let mut locations = BTreeMap::new();
    for (chunk, frame) in &fixture.frames {
        let object = digest_bytes(frame);
        objects.insert(
            object,
            Object {
                compressed_bytes: frame.len() as u64,
                version_id: None,
            },
        );
        locations.insert(
            *chunk,
            Location {
                object,
                compressed_offset: 0,
                compressed_length: frame.len() as u64,
            },
        );
    }
    let (_, repacked) = encode(&fixture.index, objects, locations).unwrap();
    assert_eq!(binding.index_digest, repacked.index_digest);
    assert_eq!(binding.rootfs_digest, repacked.rootfs_digest);
    assert_ne!(binding.manifest_digest, repacked.manifest_digest);
}

#[test]
fn manifest_rejects_substitution_and_invalid_delivery_ranges() {
    let fixture = fixture();
    let (bytes, binding, _) = packed(&fixture);
    let mut substituted = bytes.clone();
    substituted.push(b' ');
    assert!(matches!(
        AuthenticatedManifest::from_bytes(&substituted, binding),
        Err(ManifestError::Authentication)
    ));
    let original: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    // Distinct full-size leaves remain structurally valid when reordered, but
    // an authenticated outer delivery map cannot replace the logical commitment.
    let mut reordered = original.clone();
    reordered["index"]["leaves"]
        .as_array_mut()
        .unwrap()
        .swap(0, 2);
    let reordered = serde_json::to_vec(&reordered).unwrap();
    assert!(matches!(
        AuthenticatedManifest::from_bytes(
            &reordered,
            Binding {
                manifest_digest: digest_bytes(&reordered),
                ..binding
            }
        ),
        Err(ManifestError::Index(ReadIndexError::Authentication))
    ));
    let keys: Vec<String> = original["locations"]
        .as_object()
        .unwrap()
        .keys()
        .cloned()
        .collect();
    let first = &keys[0];
    let object_key = original["objects"]
        .as_object()
        .unwrap()
        .keys()
        .next()
        .unwrap();
    // Object bounds are independent of per-frame allocations and range limits.
    for (size, accepted) in [
        (128 << 20, true),
        (256 << 20, true),
        ((256 << 20) + 1, false),
    ] {
        let mut sized = original.clone();
        sized["objects"][object_key]["compressed_bytes"] = serde_json::json!(size);
        let encoded = serde_json::to_vec(&sized).unwrap();
        let trusted = Binding {
            manifest_digest: digest_bytes(&encoded),
            ..binding
        };
        assert_eq!(
            AuthenticatedManifest::from_bytes(&encoded, trusted).is_ok(),
            accepted
        );
    }
    let mut cases = Vec::new();
    let mut overrun = original.clone();
    overrun["locations"][first]["compressed_offset"] = serde_json::json!(u64::MAX);
    cases.push(overrun);
    let mut overlap = original.clone();
    overlap["locations"][&keys[1]]["compressed_offset"] =
        overlap["locations"][first]["compressed_offset"].clone();
    cases.push(overlap);
    let mut missing = original.clone();
    missing["locations"].as_object_mut().unwrap().remove(first);
    cases.push(missing);
    let mut wrong_image = original.clone();
    wrong_image["index"]["logical_bytes"] = serde_json::json!(17);
    cases.push(wrong_image);
    let mut empty_frame = original.clone();
    empty_frame["locations"][first]["compressed_length"] = serde_json::json!(0);
    cases.push(empty_frame);
    for malformed in cases {
        let index: crate::chunked_rootfs::Index =
            serde_json::from_value(malformed["index"].clone()).unwrap();
        let bytes = serde_json::to_vec(&malformed).unwrap();
        // Authenticate both commitments so structural rejection cannot be
        // masked by a stale index digest after changing the layout.
        let trusted = Binding {
            index_digest: digest_bytes(&serde_json::to_vec(&index).unwrap()),
            manifest_digest: digest_bytes(&bytes),
            ..binding
        };
        AuthenticatedManifest::from_bytes(&bytes, trusted).unwrap_err();
    }
    let entry = format!(
        "{}:{},",
        serde_json::to_string(first).unwrap(),
        original["locations"][first]
    );
    let duplicate = String::from_utf8(bytes)
        .unwrap()
        .replace("\"locations\":{", &format!("\"locations\":{{{entry}"));
    let trusted = Binding {
        manifest_digest: digest_bytes(duplicate.as_bytes()),
        ..binding
    };
    assert!(matches!(
        AuthenticatedManifest::from_bytes(duplicate.as_bytes(), trusted),
        Err(ManifestError::Json(_))
    ));
}
