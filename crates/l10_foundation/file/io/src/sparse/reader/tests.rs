use super::*;
use std::io::{SeekFrom, Write};

fn fixture(length: usize, writes: &[(u64, &[u8])]) -> File {
    let mut file = tempfile::tempfile().unwrap();
    file.set_len(length as u64).unwrap();
    for (offset, bytes) in writes {
        file.seek(SeekFrom::Start(*offset)).unwrap();
        file.write_all(bytes).unwrap();
    }
    file.seek(SeekFrom::Start(0)).unwrap();
    file
}

#[test]
fn sparse_reader_matches_ordinary_reads() {
    let dense = vec![0x5a; 32 * 1024 + 17];
    for (length, writes) in [
        (0, vec![]),
        (123, vec![]),
        (dense.len(), vec![(0, dense.as_slice())]),
        (
            96 * 1024 + 13,
            vec![(8193, b"first".as_slice()), (65531, b"last")],
        ),
        (65536, vec![(0, b"head".as_slice()), (65532, b"tail")]),
    ] {
        for start in [0, 3, length as u64 + 1] {
            let mut file = fixture(length, &writes);
            file.seek(SeekFrom::Start(start)).unwrap();
            let mut expected = Vec::new();
            file.read_to_end(&mut expected).unwrap();
            file.seek(SeekFrom::Start(start)).unwrap();
            let mut reader = SparseFileReader::new(file).unwrap();
            assert_eq!(reader.read(&mut []).unwrap(), 0);
            let mut actual = Vec::new();
            let mut buffer = [0xff; 317];
            loop {
                let read = reader.read(&mut buffer).unwrap();
                if read == 0 {
                    break;
                }
                actual.extend_from_slice(&buffer[..read]);
                // Borrowed handles may move the cursor between reads.
                reader.file().seek(SeekFrom::End(0)).unwrap();
            }
            assert_eq!(actual, expected, "length {length}, start {start}");
            assert_eq!(reader.read(&mut buffer).unwrap(), 0);
        }
    }
}

#[test]
fn unsupported_boundary_seeking_resumes_without_losing_or_repeating_bytes() {
    let bytes: Vec<u8> = (0u8..251).cycle().take(257).collect();
    for errno in [
        libc::EINVAL,
        libc::ENOTSUP,
        libc::EOPNOTSUPP,
        libc::ENOSYS,
        libc::ENOTTY,
    ] {
        // Exercise fallback before probing data, after consuming data, and
        // after SEEK_DATA succeeds but SEEK_HOLE moves the cursor and fails.
        for fail_at in [1, 2, 3, 4] {
            let file = fixture(bytes.len(), &[(0, &bytes)]);
            let mut reader = SparseFileReader::new(file).unwrap();
            let mut calls = 0;
            let mut seek = |file: &File, offset: u64, boundary: Boundary| {
                calls += 1;
                if calls == fail_at {
                    let mut descriptor = file;
                    descriptor.seek(SeekFrom::End(0))?;
                    return Err(io::Error::from_raw_os_error(errno));
                }
                Ok(match boundary {
                    Boundary::Data => offset,
                    Boundary::Hole => (offset + 64).min(bytes.len() as u64),
                })
            };
            let mut actual = Vec::new();
            let mut buffer = [0; 19];
            loop {
                let n = reader.read_with(&mut buffer, &mut seek).unwrap();
                if n == 0 {
                    break;
                }
                actual.extend_from_slice(&buffer[..n]);
                reader.file().seek(SeekFrom::End(0)).unwrap();
            }
            assert_eq!(actual, bytes, "errno {errno}, boundary {fail_at}");
        }
    }
}

#[test]
fn sparse_reader_propagates_io_errors_and_rejects_invalid_extents() {
    for errno in [
        libc::EIO,
        libc::EBADF,
        libc::EACCES,
        libc::EINTR,
        libc::ENXIO,
    ] {
        for fail_on_hole in [false, true] {
            if errno == libc::ENXIO && !fail_on_hole {
                // No more data is a valid trailing hole, covered by byte equality.
                continue;
            }
            let mut reader = SparseFileReader::new(fixture(128, &[])).unwrap();
            let error = reader
                .read_with(&mut [0; 8], |_, _, boundary| {
                    if fail_on_hole && matches!(boundary, Boundary::Data) {
                        Ok(0)
                    } else {
                        Err(io::Error::from_raw_os_error(errno))
                    }
                })
                .unwrap_err();
            assert_eq!(error.raw_os_error(), Some(errno));
        }
    }
    for (data, hole) in [(129, 128), (0, 0), (0, 129)] {
        let mut reader = SparseFileReader::new(fixture(128, &[])).unwrap();
        let error = reader
            .read_with(&mut [0; 8], |_, _, boundary| {
                Ok(match boundary {
                    Boundary::Data => data,
                    Boundary::Hole => hole,
                })
            })
            .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    }
}

#[test]
fn sparse_reader_detects_length_changes_even_in_a_cached_hole() {
    for final_length in [4, 256] {
        let file = fixture(128, &[]);
        let modifier = file.try_clone().unwrap();
        let mut reader = SparseFileReader::new(file).unwrap();
        let no_data = |_: &File, _, _| Err(io::Error::from_raw_os_error(libc::ENXIO));
        assert_eq!(reader.read_with(&mut [0; 8], no_data).unwrap(), 8);
        modifier.set_len(final_length).unwrap();
        let mut rest = Vec::new();
        assert_eq!(
            reader.read_to_end(&mut rest).unwrap_err().kind(),
            io::ErrorKind::UnexpectedEof
        );
    }
    let file = fixture(128, &[(0, &[7; 128])]);
    let modifier = file.try_clone().unwrap();
    let mut reader = SparseFileReader::new(file).unwrap();
    reader
        .read_with(&mut [0; 8], |_, _, b| {
            Ok(match b {
                Boundary::Data => 0,
                Boundary::Hole => 128,
            })
        })
        .unwrap();
    modifier.set_len(8).unwrap();
    assert_eq!(
        reader.read(&mut [0; 8]).unwrap_err().kind(),
        io::ErrorKind::UnexpectedEof
    );
}
