use super::*;

/// `btrfs send` of a read-only snapshot holding one synthetic text file,
/// captured from a real kernel: header, `SUBVOL`, `MKFILE`, two `WRITE`s and
/// metadata commands, then `END`.
const STREAM: &[u8] = include_bytes!("../../../fixtures/btrfs_send_v1.stream");

fn check(pieces: &[&[u8]]) -> Result<(), FramingError> {
    let mut framing = SendStreamFraming::new();
    for piece in pieces {
        framing.feed(piece)?;
    }
    framing.finish()
}

#[test]
fn crc_matches_the_kernel_send_parameters() {
    assert_eq!(CRC32C.checksum(b"123456789"), CRC32C_RAW.check);
}

#[test]
fn a_kernel_stream_passes_however_it_is_chunked() {
    assert_eq!(check(&[STREAM]), Ok(()));
    for split in 0..=STREAM.len() {
        let (head, tail) = STREAM.split_at(split);
        assert_eq!(check(&[head, tail]), Ok(()), "split at {split}");
    }
    let bytes: Vec<&[u8]> = STREAM.chunks(1).collect();
    assert_eq!(check(&bytes), Ok(()));
}

/// A restarted send: the partial first stream stops inside a command, and the
/// restart's complete stream follows it.
#[test]
fn a_restarted_send_fails_at_the_interrupted_command() {
    let cut = STREAM.len() / 2;
    let restarted = [&STREAM[..cut], STREAM].concat();
    assert!(matches!(
        check(&[&restarted]),
        Err(FramingError::Crc { offset, .. }) if (offset as usize) < cut
    ));
}

#[test]
fn a_second_stream_after_end_fails() {
    assert_eq!(
        check(&[STREAM, STREAM]),
        Err(FramingError::DataAfterEnd {
            offset: STREAM.len() as u64
        })
    );
}

#[test]
fn a_stream_without_its_end_fails() {
    assert!(matches!(
        check(&[&STREAM[..STREAM.len() - 1]]),
        Err(FramingError::Truncated { .. })
    ));
    assert!(matches!(
        check(&[]),
        Err(FramingError::Truncated { offset: 0 })
    ));
}

#[test]
fn a_corrupted_byte_fails_its_command() {
    let mut corrupted = STREAM.to_vec();
    let middle = corrupted.len() / 2;
    corrupted[middle] ^= 0x01;
    assert!(matches!(
        check(&[&corrupted]),
        Err(FramingError::Crc { .. })
    ));
}

#[test]
fn a_stream_without_the_header_fails() {
    assert_eq!(check(&[&STREAM[1..]]), Err(FramingError::BadMagic));
}
