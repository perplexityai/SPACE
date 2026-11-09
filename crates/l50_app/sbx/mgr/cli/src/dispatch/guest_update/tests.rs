use super::*;

#[test]
fn oversized_policy_stops_reading_at_the_limit() {
    let limit = pplx_space_spaced_update::MAX_METADATA_BYTES;
    let mut source = std::io::Cursor::new(vec![b' '; limit * 2]);
    assert_eq!(
        read_policy_bytes(&mut source).unwrap_err().kind(),
        std::io::ErrorKind::InvalidData
    );
    assert_eq!(source.position(), limit as u64 + 1);
    assert_eq!(read_policy_bytes(b"{}".as_slice()).unwrap(), b"{}");
}
