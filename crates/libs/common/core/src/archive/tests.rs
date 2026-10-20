use super::*;

const FIXTURE: &[u8] = include_bytes!("fixtures/manifest-v1.bin");
const PAYLOAD: &[u8] = include_bytes!("fixtures/reconstruction.json");
const SCHEMA: ReconstructionSchemaVersion = ReconstructionSchemaVersion::new(NonZeroU16::MAX);

fn reference() -> ManifestReference {
    let sum = include_str!("fixtures/SHA256SUMS")
        .split_whitespace()
        .next()
        .expect("fixture checksum");
    let Digest::Sha256(bytes) = format!("sha256:{sum}").parse().expect("SHA-256 fixture");
    ManifestReference::new(
        SandboxId::from_uuid(Uuid::from_u128(0x019ee004_1111_7000_8000_000000000001)),
        ManifestId::from_uuid(Uuid::from_u128(0x019ee004_2222_7000_8000_000000000002)),
        ManifestSha256::from_bytes(bytes),
    )
}

#[test]
fn matches_independent_python_wire_and_checksum_fixture() {
    let expected = reference();
    let encoded = encode(
        expected.sandbox_id(),
        expected.manifest_id(),
        SCHEMA,
        PAYLOAD,
    )
    .expect("bounded fixture");
    assert_eq!(encoded.bytes(), FIXTURE);
    assert_eq!(encoded.reference(), expected);
    let verified = verify(expected, SCHEMA, FIXTURE).expect("external fixture verifies");
    assert_eq!(verified.payload(), PAYLOAD);
    assert_eq!(verified.schema(), SCHEMA);
    assert_eq!(verified.reference(), expected);
    assert_ne!(expected.sha256().digest(), sha256(PAYLOAD));
    assert!(!format!("{encoded:?} {verified:?}").contains("analysis-workspace"));
}

#[test]
fn rejects_each_envelope_corruption_before_payload_access() {
    let cases = [
        (0, 0xff, VerifyError::InvalidMagic),
        (9, 2, VerifyError::ArchiveVersion(2)),
        (
            10,
            0,
            VerifyError::ReconstructionVersion {
                expected: 65535,
                actual: 255,
            },
        ),
        (12, 0xff, VerifyError::SandboxIdentity),
        (28, 0xff, VerifyError::ManifestIdentity),
        (HEADER_LEN, 0xff, VerifyError::ManifestChecksum),
    ];
    for (offset, value, error) in cases {
        let mut bytes = FIXTURE.to_vec();
        *bytes.get_mut(offset).expect("fixture offset") = value;
        assert_eq!(
            verify(reference(), SCHEMA, &bytes).expect_err("corrupt bytes"),
            error
        );
    }
    for format in [0, 2, u16::MAX] {
        let mut bytes = FIXTURE.to_vec();
        bytes
            .get_mut(8..10)
            .expect("format")
            .copy_from_slice(&format.to_be_bytes());
        assert_eq!(
            verify(reference(), SCHEMA, &bytes).expect_err("unknown format"),
            VerifyError::ArchiveVersion(format)
        );
    }
    for schema in [0_u16, 1, 65534] {
        let mut bytes = FIXTURE.to_vec();
        bytes
            .get_mut(10..12)
            .expect("schema")
            .copy_from_slice(&schema.to_be_bytes());
        assert_eq!(
            verify(reference(), SCHEMA, &bytes).expect_err("unknown schema"),
            VerifyError::ReconstructionVersion {
                expected: 65535,
                actual: schema
            }
        );
    }
    let other = ReconstructionSchemaVersion::new(NonZeroU16::MIN);
    assert_eq!(
        verify(reference(), other, FIXTURE).expect_err("caller codec mismatch"),
        VerifyError::ReconstructionVersion {
            expected: 1,
            actual: 65535
        }
    );
}

#[test]
fn rejects_truncation_trailing_bytes_and_unbounded_lengths() {
    for length in 0..FIXTURE.len() {
        assert!(
            verify(
                reference(),
                SCHEMA,
                FIXTURE.get(..length).expect("fixture prefix")
            )
            .is_err(),
            "accepted truncated length {length}"
        );
    }
    let mut trailing = FIXTURE.to_vec();
    trailing.push(0);
    assert_eq!(
        verify(reference(), SCHEMA, &trailing).expect_err("trailing byte"),
        VerifyError::PayloadLength {
            declared: PAYLOAD.len() as u64,
            actual: PAYLOAD.len() + 1
        }
    );
    for declared in [
        0,
        PAYLOAD.len() as u64 + 1,
        MAX_PAYLOAD_LEN as u64 + 1,
        u64::MAX,
    ] {
        let mut bytes = FIXTURE.to_vec();
        bytes
            .get_mut(44..52)
            .expect("length")
            .copy_from_slice(&declared.to_be_bytes());
        let expected = if declared > MAX_PAYLOAD_LEN as u64 {
            VerifyError::PayloadTooLarge(declared)
        } else {
            VerifyError::PayloadLength {
                declared,
                actual: PAYLOAD.len(),
            }
        };
        assert_eq!(
            verify(reference(), SCHEMA, &bytes).expect_err("bad size"),
            expected
        );
    }
    let oversized = vec![0; MAX_MANIFEST_LEN + 1];
    assert_eq!(
        verify(reference(), SCHEMA, &oversized).expect_err("download bound"),
        VerifyError::ManifestTooLarge
    );
    let oversized_payload = vec![0; MAX_PAYLOAD_LEN + 1];
    assert_eq!(
        encode(
            reference().sandbox_id(),
            reference().manifest_id(),
            SCHEMA,
            &oversized_payload
        )
        .expect_err("encode bound"),
        EncodeError::PayloadTooLarge {
            actual: oversized_payload.len()
        }
    );
    for size in [0, MAX_PAYLOAD_LEN] {
        let payload = vec![0; size];
        let encoded = encode(
            reference().sandbox_id(),
            reference().manifest_id(),
            SCHEMA,
            &payload,
        )
        .expect("boundary size");
        assert_eq!(
            verify(encoded.reference(), SCHEMA, encoded.bytes())
                .expect("boundary verification")
                .payload()
                .len(),
            size
        );
    }
}

#[test]
fn readback_requires_complete_exact_revision_bytes() {
    // This models only the bytes a download returned, not a storage durability receipt.
    let pending =
        encode(reference().sandbox_id(), ManifestId::new(), SCHEMA, PAYLOAD).expect("new revision");
    assert_eq!(
        verify(pending.reference(), SCHEMA, &[]).expect_err("manifest not written yet"),
        VerifyError::TruncatedHeader
    );
    assert_eq!(
        verify(pending.reference(), SCHEMA, FIXTURE).expect_err("old revision read back"),
        VerifyError::ManifestIdentity
    );
    let different_sandbox = ManifestReference::new(
        SandboxId::new(),
        reference().manifest_id(),
        reference().sha256(),
    );
    assert_eq!(
        verify(different_sandbox, SCHEMA, FIXTURE).expect_err("wrong sandbox"),
        VerifyError::SandboxIdentity
    );
    let wrong_catalog = ManifestReference::new(
        reference().sandbox_id(),
        reference().manifest_id(),
        ManifestSha256::from_bytes([0; 32]),
    );
    assert_eq!(
        verify(wrong_catalog, SCHEMA, FIXTURE).expect_err("wrong catalog checksum"),
        VerifyError::ManifestChecksum
    );
    let downloaded = pending.bytes().to_vec();
    assert_eq!(
        verify(pending.reference(), SCHEMA, &downloaded)
            .expect("complete readback")
            .payload(),
        PAYLOAD
    );
}

#[cfg(feature = "serde")]
#[test]
fn reconstruction_codec_consumes_verified_bytes_and_preserves_configuration() {
    use crate::types::{EnvMap, MemorySize, VcpuCount};
    use std::collections::BTreeMap;

    // Representative subset only; it deliberately assigns no production schema.
    #[derive(Debug, serde::Deserialize, serde::Serialize, PartialEq, Eq)]
    #[serde(deny_unknown_fields)]
    struct FixtureReconstruction {
        name: String,
        template_release_id: Uuid,
        vcpu_count: VcpuCount,
        memory_bytes: MemorySize,
        env: EnvMap,
        metadata: BTreeMap<String, String>,
    }

    fn decode(verified: VerifiedManifest<'_>) -> FixtureReconstruction {
        assert_eq!(verified.schema(), SCHEMA);
        serde_json::from_slice(verified.payload()).expect("fixture schema")
    }

    let reconstructed = decode(verify(reference(), SCHEMA, FIXTURE).expect("verify before serde"));
    assert_eq!(reconstructed.name, "analysis-workspace");
    assert_eq!(
        serde_json::to_value(&reconstructed).expect("configuration"),
        serde_json::from_slice::<serde_json::Value>(PAYLOAD)
            .expect("independent expected configuration")
    );
    let reencoded = encode(
        reference().sandbox_id(),
        ManifestId::new(),
        SCHEMA,
        &serde_json::to_vec(&reconstructed).expect("fixture encode"),
    )
    .expect("envelope");
    assert_eq!(
        decode(
            verify(reencoded.reference(), SCHEMA, reencoded.bytes())
                .expect("reencoded verification")
        ),
        reconstructed
    );
}
