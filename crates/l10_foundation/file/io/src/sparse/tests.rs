use super::*;

fn patterned_bytes(len: usize) -> Vec<u8> {
    (0..len)
        .map(|index| {
            if (index / 37) % 3 == 0 {
                0
            } else {
                (index.wrapping_mul(31) % 251 + 1) as u8
            }
        })
        .collect()
}

fn round_trip(
    expected: &[u8],
    block_bytes: usize,
    chunk_bytes: usize,
    flush_each_chunk: bool,
) -> (u64, u64) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("output");
    let mut writer = SparseFileWriter::new(File::create(&path).unwrap(), block_bytes).unwrap();
    for chunk in expected.chunks(chunk_bytes) {
        writer.write_all(chunk).unwrap();
        if flush_each_chunk {
            writer.flush().unwrap();
        }
    }
    let mut output = writer.finish().unwrap();

    assert_eq!(output.stream_position().unwrap(), expected.len() as u64);
    assert_eq!(std::fs::read(&path).unwrap(), expected);
    let metadata = std::fs::metadata(&path).unwrap();
    #[cfg(unix)]
    let allocated = allocated_bytes(&path);
    #[cfg(not(unix))]
    let allocated = metadata.len();
    (metadata.len(), allocated)
}

#[cfg(unix)]
fn allocated_bytes(path: &std::path::Path) -> u64 {
    use std::os::unix::fs::MetadataExt;

    std::fs::metadata(path).unwrap().blocks() * 512
}

#[test]
fn rejects_zero_block_size() {
    let file = tempfile::tempfile().unwrap();
    SparseFileWriter::new(file, 0).unwrap_err();
}

#[test]
fn zero_reduction_handles_unaligned_slices_and_tail_bytes() {
    let mut bytes = vec![0_u8; 8 + 3 * size_of::<u64>() + 7];

    for start in 0..8 {
        let slice = &mut bytes[start..start + 3 * size_of::<u64>() + 7];
        assert!(is_all_zero(slice));
        for nonzero_at in 0..slice.len() {
            slice[nonzero_at] = 1;
            assert!(
                !is_all_zero(slice),
                "nonzero byte at offset {nonzero_at} was not classified"
            );
            slice[nonzero_at] = 0;
        }
    }
}

#[test]
fn empty_input_produces_an_empty_file() {
    let (logical, allocated) = round_trip(&[], 4096, 1, false);
    assert_eq!(logical, 0);
    assert_eq!(allocated, 0);
}

#[test]
fn empty_writes_do_not_advance_the_output() {
    let mut writer = SparseFileWriter::new(tempfile::tempfile().unwrap(), 4096).unwrap();
    assert_eq!(writer.write(&[]).unwrap(), 0);
    assert_eq!(writer.write(&[]).unwrap(), 0);
    let mut output = writer.finish().unwrap();
    assert_eq!(output.stream_position().unwrap(), 0);
    assert_eq!(output.metadata().unwrap().len(), 0);
}

#[cfg(unix)]
#[test]
fn observer_reports_candidates_from_sparse_output() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("sparse");
    let expected = vec![0; 8 * 1024 * 1024 + 173];
    let observer = Arc::new(AtomicU64::new(0));
    let mut writer =
        SparseFileWriter::with_observer(File::create(&path).unwrap(), 4096, Arc::clone(&observer))
            .unwrap();

    for chunk in expected.chunks(997) {
        writer.write_all(chunk).unwrap();
    }
    let (_, candidate_hole_bytes) = writer.finish_with_stats().unwrap();

    assert_eq!(std::fs::read(&path).unwrap(), expected);
    assert_eq!(candidate_hole_bytes, expected.len() as u64);
    assert_eq!(observer.load(Ordering::Relaxed), candidate_hole_bytes);
    assert!(allocated_bytes(&path) < expected.len() as u64 / 2);
}

#[test]
fn materializes_only_complete_blocks_at_a_buffer_threshold() {
    const BLOCK_BYTES: usize = 4096;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("sparse");
    let mut writer = SparseFileWriter::new(File::create(&path).unwrap(), BLOCK_BYTES).unwrap();

    writer.write_all(&[0; 2 * BLOCK_BYTES]).unwrap();
    writer
        .materialize_complete_blocks_if_buffered_at_least((2 * BLOCK_BYTES) as u64)
        .unwrap();
    assert_eq!(writer.materialized_through(), (2 * BLOCK_BYTES) as u64);
    assert_eq!(
        std::fs::metadata(&path).unwrap().len(),
        (2 * BLOCK_BYTES) as u64
    );

    writer.write_all(&[0; 17]).unwrap();
    writer
        .materialize_complete_blocks_if_buffered_at_least(1)
        .unwrap();
    assert_eq!(
        writer.materialized_through(),
        (2 * BLOCK_BYTES) as u64,
        "an incomplete classification block must remain buffered"
    );

    writer.finish().unwrap();
    assert_eq!(
        std::fs::metadata(path).unwrap().len(),
        (2 * BLOCK_BYTES + 17) as u64
    );
}

#[test]
fn reports_holes_without_reporting_data_ranges() {
    // arrange
    const BLOCK_BYTES: usize = 4096;
    let mut writer = SparseFileWriter::new(tempfile::tempfile().unwrap(), BLOCK_BYTES).unwrap();

    // act
    writer.write_all(&[0; 2 * BLOCK_BYTES]).unwrap();
    let materialization = writer.take_materialization();

    // assert
    let (data_ranges, hole_bytes) = materialization.into_parts();
    assert!(data_ranges.is_empty());
    assert_eq!(hole_bytes, (2 * BLOCK_BYTES) as u64);
}

#[test]
fn reports_only_materialized_data_ranges_from_mixed_sparse_output() {
    // arrange
    const BLOCK_BYTES: usize = 4096;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("mixed");
    let mut writer = SparseFileWriter::new(File::create(&path).unwrap(), BLOCK_BYTES).unwrap();
    let mut expected = vec![0; 3 * BLOCK_BYTES];
    expected[BLOCK_BYTES..2 * BLOCK_BYTES].fill(0xa5);

    // act
    writer.write_all(&expected).unwrap();
    let (file, _, materialization) = writer.finish_with_stats_and_materialization().unwrap();

    // assert
    let (data_ranges, hole_bytes) = materialization.into_parts();
    assert_eq!(
        data_ranges,
        vec![SparseDataRange::new(BLOCK_BYTES as u64, BLOCK_BYTES as u64)]
    );
    assert_eq!(hole_bytes, (2 * BLOCK_BYTES) as u64);
    assert_eq!(file.metadata().unwrap().len(), expected.len() as u64);
    assert_eq!(std::fs::read(path).unwrap(), expected);
}

#[test]
fn supports_odd_block_sizes_and_byte_at_a_time_writes() {
    let expected = patterned_bytes(16 * 1024 + 19);
    round_trip(&expected, 3, 1, false);
}

#[test]
fn preserves_bytes_across_block_and_write_size_matrix() {
    for block_bytes in [1, 3, 4096, 64 * 1024] {
        let expected = patterned_bytes(block_bytes * 3 + 173);
        for chunk_bytes in [1, 2, 7, 997, 4095, 64 * 1024 + 1] {
            round_trip(&expected, block_bytes, chunk_bytes, false);
        }
    }
}

#[test]
fn explicit_flushes_preserve_fragmented_bytes() {
    let expected = patterned_bytes(128 * 1024 + 31);
    for chunk_bytes in [1, 113, 997, 4097, 64 * 1024 + 1] {
        round_trip(&expected, 4096, chunk_bytes, true);
    }
}

#[test]
fn flush_materializes_without_classifying_an_incomplete_block() {
    const BLOCK_BYTES: usize = 4096;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("output");
    let observer = Arc::new(AtomicU64::new(0));
    let mut expected = vec![0; BLOCK_BYTES];
    expected[113] = 0x5a;
    let mut writer = SparseFileWriter::with_observer(
        File::create(&path).unwrap(),
        BLOCK_BYTES,
        Arc::clone(&observer),
    )
    .unwrap();

    writer.write_all(&expected[..113]).unwrap();
    writer.flush().unwrap();
    assert_eq!(
        std::fs::read(&path).unwrap(),
        expected[..113],
        "Write::flush must make every accepted byte observable"
    );
    assert_eq!(
        observer.load(Ordering::Relaxed),
        0,
        "flush must not classify a partial zero prefix before the block is complete"
    );

    writer.write_all(&expected[113..]).unwrap();
    let (_, candidate_hole_bytes) = writer.finish_with_stats().unwrap();
    let actual = std::fs::read(&path).unwrap();
    assert_eq!(actual, expected);
    assert_eq!(candidate_hole_bytes, 0);
}

#[test]
fn preserves_data_on_both_sides_of_block_boundaries() {
    let mut expected = vec![0; 5 * 4096 + 17];
    for offset in [0, 4095, 4096, 4097, 3 * 4096 - 1, expected.len() - 1] {
        expected[offset] = 0x5a;
    }
    round_trip(&expected, 4096, 997, false);
}

#[test]
fn flushes_data_larger_than_the_internal_buffer() {
    let expected = patterned_bytes(DATA_BUFFER_BYTES * 2 + 173);
    round_trip(&expected, 4096, 127 * 1024 + 13, false);
}

#[cfg(unix)]
#[test]
fn all_zero_full_and_partial_blocks_remain_sparse() {
    for (block_bytes, logical_bytes) in [
        (3, 10_001),
        (4096, 4 * 1024 * 1024 + 173),
        (64 * 1024, 8 * 1024 * 1024 + 31),
    ] {
        let expected = vec![0; logical_bytes];
        let (logical, allocated) = round_trip(&expected, block_bytes, 997, false);
        assert_eq!(logical, logical_bytes as u64);
        assert!(
            allocated < logical / 16,
            "zero output should be sparse: block={block_bytes} logical={logical} allocated={allocated}"
        );
    }
}

#[cfg(unix)]
#[test]
fn coalesces_leading_interior_and_trailing_zero_runs() {
    let mut expected = vec![0; 12 * 1024 * 1024 + 173];
    expected[2 * 1024 * 1024..2 * 1024 * 1024 + 4096].fill(0xa5);
    expected[7 * 1024 * 1024 + 13..7 * 1024 * 1024 + 8192].fill(0x5a);

    let (logical, allocated) = round_trip(&expected, 4096, 64 * 1024 + 7, false);
    assert!(
        allocated < logical / 8,
        "isolated data should not fill surrounding holes: logical={logical} allocated={allocated}"
    );
}

#[cfg(unix)]
#[test]
fn preserves_fragmented_input_and_zero_holes_across_flushes() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("output");
    let mut expected = vec![0; 4 * 1024 * 1024 + 173];
    expected[2 * 1024 * 1024 + 17] = 1;
    expected[2 * 1024 * 1024 + 4095] = 2;
    let mut writer = SparseFileWriter::new(File::create(&path).unwrap(), 4096).unwrap();

    writer.write_all(&expected[..113]).unwrap();
    writer.flush().unwrap();
    for chunk in expected[113..].chunks(997) {
        writer.write_all(chunk).unwrap();
    }
    writer.finish().unwrap();

    assert_eq!(std::fs::read(&path).unwrap(), expected);
    let metadata = std::fs::metadata(&path).unwrap();
    let allocated_bytes = allocated_bytes(&path);
    assert!(
        allocated_bytes < metadata.len() / 4,
        "sparse output should retain holes: logical={} allocated={allocated_bytes}",
        metadata.len()
    );
}
