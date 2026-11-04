use super::*;

/// `btrfs send` of a read-only snapshot holding one synthetic text file,
/// captured from a real kernel: header, `SUBVOL`, `MKFILE`, two `WRITE`s and
/// metadata commands, then `END`.
const STREAM: &[u8] = include_bytes!("../../fixtures/btrfs_send_v1.stream");

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

#[test]
fn only_v1_commands_are_accepted_even_under_a_v1_header() {
    // The receiver dispatches commands independently of the header version.
    // In particular, a v1 label must not admit ENCODED_WRITE (25).
    for version in [1_u32, 2, 3] {
        for cmd in [0_u16, 1, 22, 23, 24, 25, u16::MAX] {
            let mut stream = MAGIC.to_vec();
            stream.extend(version.to_le_bytes());
            for command in [cmd, END_COMMAND] {
                let mut header = [0_u8; 10];
                header[4..6].copy_from_slice(&command.to_le_bytes());
                let crc = CRC32C.checksum(&header);
                header[6..10].copy_from_slice(&crc.to_le_bytes());
                stream.extend(header);
            }
            let accepted = version == 1 && matches!(cmd, 1 | 22);
            assert_eq!(
                check(&[&stream]).is_ok(),
                accepted,
                "version={version}, command={cmd}"
            );
            assert_eq!(
                salvage(&stream).is_some(),
                accepted,
                "salvage version={version}, command={cmd}"
            );
        }
    }
}

fn salvage(bytes: &[u8]) -> Option<u64> {
    salvage_until(bytes, &|| false)
}

fn salvage_until(bytes: &[u8], stopped: &dyn Fn() -> bool) -> Option<u64> {
    use std::io::Write;
    let mut file = tempfile::tempfile().unwrap();
    file.write_all(bytes).unwrap();
    last_complete_stream(&file, stopped).unwrap()
}

#[test]
fn a_valid_stream_needs_no_salvage() {
    assert_eq!(salvage(STREAM), Some(0));
}

/// Wherever the kernel interrupted the first send, the restart's complete
/// stream is found where it begins: every cut through the stream header, the
/// first commands and the tail, and a prime stride through the rest.
#[test]
fn a_restarted_send_salvages_its_complete_stream_wherever_it_was_cut() {
    let len = STREAM.len();
    let cuts = (1..len.min(128))
        .chain((128..len).step_by(61))
        .chain(len.saturating_sub(64).max(1)..len);
    for cut in cuts {
        let restarted = [&STREAM[..cut], STREAM].concat();
        assert_eq!(salvage(&restarted), Some(cut as u64), "cut at {cut}");
    }
}

#[test]
fn repeated_restarts_salvage_the_last_complete_stream() {
    let (a, b) = (STREAM.len() / 3, STREAM.len() / 2);
    let restarted = [&STREAM[..a], &STREAM[..b], STREAM].concat();
    assert_eq!(salvage(&restarted), Some((a + b) as u64));
}

#[test]
fn nothing_is_salvaged_without_a_complete_valid_stream() {
    let cut = STREAM.len() / 2;
    assert_eq!(salvage(&[&STREAM[..cut], &STREAM[..cut]].concat()), None);
    let mut corrupted = [&STREAM[..cut], STREAM].concat();
    let last = corrupted.len() - STREAM.len() / 2;
    corrupted[last] ^= 0x01;
    assert_eq!(salvage(&corrupted), None);
    assert_eq!(salvage(&[]), None);
    assert_eq!(salvage(&STREAM[1..]), None);
}

/// A header-shaped prefix that does not start a valid stream is passed over.
#[test]
fn a_header_shaped_prefix_is_not_salvaged() {
    let fake = [&STREAM[..17], b"not a command".as_slice(), STREAM].concat();
    assert_eq!(salvage(&fake), Some(30));
}

/// A stream header split across the scan's reads is still found.
#[test]
fn a_header_across_a_read_boundary_is_found() {
    for before in [
        SALVAGE_CHUNK - 9,
        SALVAGE_CHUNK - 1,
        SALVAGE_CHUNK,
        SALVAGE_CHUNK + 3,
    ] {
        let prefixed = [vec![0u8; before].as_slice(), STREAM].concat();
        assert_eq!(
            salvage(&prefixed),
            Some(before as u64),
            "{before} bytes before"
        );
    }
}

/// Header-shaped runs whose first command fails do not use up the partial
/// stream budget; only the much larger lookalike cap ends the search.
#[test]
fn header_lookalikes_do_not_use_up_the_partial_stream_budget() {
    let lookalike = [&STREAM[..17], b"x".as_slice()].concat();
    for count in [MAX_PARTIAL_STREAMS, MAX_HEADER_LOOKALIKES - 1] {
        let prefixed = [lookalike.repeat(count).as_slice(), STREAM].concat();
        assert_eq!(
            salvage(&prefixed),
            Some((count * lookalike.len()) as u64),
            "{count} lookalikes"
        );
    }
    let prefixed = [lookalike.repeat(MAX_HEADER_LOOKALIKES).as_slice(), STREAM].concat();
    assert_eq!(salvage(&prefixed), None);
}

#[test]
fn the_search_stops_after_its_partial_stream_budget() {
    let cut = STREAM.len() / 2;
    for (count, expected) in [
        (
            MAX_PARTIAL_STREAMS - 1,
            Some(((MAX_PARTIAL_STREAMS - 1) * cut) as u64),
        ),
        (MAX_PARTIAL_STREAMS, None),
    ] {
        let restarted = [STREAM[..cut].repeat(count).as_slice(), STREAM].concat();
        assert_eq!(salvage(&restarted), expected, "{count} partial streams");
    }
}

/// A stop requested mid-search ends it without a salvage, even when a
/// complete stream follows.
#[test]
fn a_stopped_search_salvages_nothing() {
    let restarted = [&STREAM[..STREAM.len() / 2], STREAM].concat();
    assert_eq!(salvage_until(&restarted, &|| true), None);
    let checks = std::cell::Cell::new(0);
    let stop_after_first_read = || {
        checks.set(checks.get() + 1);
        checks.get() > 1
    };
    assert_eq!(salvage_until(&restarted, &stop_after_first_read), None);
}
