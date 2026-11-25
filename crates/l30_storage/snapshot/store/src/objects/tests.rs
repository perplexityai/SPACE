use super::{MAX_SMALL_OBJECT_BYTES, decode};

#[test]
fn an_uncompressed_object_reads_as_is() {
    let body = br#"{"version":2}"#.to_vec();
    assert_eq!(decode(body.clone()).expect("decodes"), body);
}

#[test]
fn an_uncompressed_object_past_the_cap_is_refused() {
    let len = usize::try_from(MAX_SMALL_OBJECT_BYTES).expect("fits") + 1;
    decode(vec![b' '; len]).expect_err("over the cap");
}

#[test]
fn a_zstd_object_reads_decoded() {
    let body = br#"{"version":2}"#.to_vec();
    let compressed = zstd::encode_all(body.as_slice(), 3).expect("compresses");
    assert_eq!(decode(compressed).expect("decodes"), body);
}

#[test]
fn a_zstd_object_past_the_cap_is_refused() {
    let len = usize::try_from(MAX_SMALL_OBJECT_BYTES).expect("fits") + 1;
    let compressed = zstd::encode_all(vec![b' '; len].as_slice(), 3).expect("compresses");
    decode(compressed).expect_err("over the cap");
}

#[test]
fn a_corrupt_zstd_object_is_refused() {
    decode(vec![0x28, 0xb5, 0x2f, 0xfd, 0, 1, 2]).expect_err("corrupt");
}
