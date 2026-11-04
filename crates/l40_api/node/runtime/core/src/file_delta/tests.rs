use super::*;

#[test]
fn bounded_delta_rejects_expansion_and_invalid_pages() {
    for (target_size, base_size, offsets) in [
        (8193_u64, 0, vec![]),
        (0, 8193, vec![]),
        (1, 0, vec![0, 0]),
        (8192, 0, vec![8192]),
        (8192, 0, vec![u64::MAX]),
        (8192, 0, vec![1]),
        (8192, 0, vec![0, 0]),
        (8192, 0, vec![4096, 0]),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let base = dir.path().join("base");
        std::fs::write(&base, vec![0; base_size]).unwrap();
        let mut encoded = Vec::new();
        encoded.extend_from_slice(MAGIC);
        encoded.push(VERSION);
        encoded.extend_from_slice(&(PAGE_SIZE as u32).to_le_bytes());
        encoded.extend_from_slice(&(offsets.len() as u64).to_le_bytes());
        encoded.extend_from_slice(&target_size.to_le_bytes());
        for offset in offsets {
            encoded.extend_from_slice(&offset.to_le_bytes());
            encoded.extend_from_slice(&[1; PAGE_SIZE]);
        }
        let diff = dir.path().join("diff");
        std::fs::write(&diff, zstd::encode_all(encoded.as_slice(), 1).unwrap()).unwrap();
        let output = dir.path().join("output");
        assert!(matches!(
            apply_diff_with_limit(&base, &diff, &output, 8192),
            Err(DiffError::SizeLimitExceeded { .. } | DiffError::InvalidPageLayout)
        ));
        if output.exists() {
            assert!(std::fs::metadata(output).unwrap().len() <= 8192);
        }
    }

    let dir = tempfile::tempdir().unwrap();
    let base = dir.path().join("base");
    let current = dir.path().join("current");
    let diff = dir.path().join("diff");
    let output = dir.path().join("output");
    std::fs::write(&base, b"old").unwrap();
    std::fs::write(&current, b"new content").unwrap();
    compute_diff(&base, &current, &diff).unwrap();
    assert_eq!(
        apply_diff_with_limit(&base, &diff, &output, 11).unwrap(),
        11
    );
    assert_eq!(std::fs::read(output).unwrap(), b"new content");
}

#[test]
fn test_diff_apply_roundtrip() {
    let dir = tempfile::tempdir().unwrap();

    let base_path = dir.path().join("base");
    let current_path = dir.path().join("current");
    let diff_path = dir.path().join("diff");
    let restored_path = dir.path().join("restored");

    // Create base: 4 pages of zeros
    let base_data = vec![0u8; PAGE_SIZE * 4];
    std::fs::write(&base_path, &base_data).unwrap();

    // Create current: dirty pages 0 and 2
    let mut current_data = base_data.clone();
    current_data[..PAGE_SIZE].fill(0xAA);
    current_data[PAGE_SIZE * 2..PAGE_SIZE * 3].fill(0xBB);
    std::fs::write(&current_path, &current_data).unwrap();

    let stats = compute_diff(&base_path, &current_path, &diff_path).unwrap();
    assert_eq!(stats.dirty_pages, 2);
    assert_eq!(stats.total_pages, 4);

    let size = apply_diff(&base_path, &diff_path, &restored_path).unwrap();
    assert_eq!(size, current_data.len() as u64);

    let restored = std::fs::read(&restored_path).unwrap();
    assert_eq!(restored, current_data);
}

#[test]
fn test_identical_files_produce_empty_diff() {
    let dir = tempfile::tempdir().unwrap();

    let base_path = dir.path().join("base");
    let current_path = dir.path().join("current");
    let diff_path = dir.path().join("diff");
    let restored_path = dir.path().join("restored");

    let data = vec![0x42u8; PAGE_SIZE * 3];
    std::fs::write(&base_path, &data).unwrap();
    std::fs::write(&current_path, &data).unwrap();

    let stats = compute_diff(&base_path, &current_path, &diff_path).unwrap();
    assert_eq!(stats.dirty_pages, 0);
    assert_eq!(stats.total_pages, 3);

    let size = apply_diff(&base_path, &diff_path, &restored_path).unwrap();
    assert_eq!(size, data.len() as u64);

    let restored = std::fs::read(&restored_path).unwrap();
    assert_eq!(restored, data);
}

#[test]
fn test_current_smaller_than_base() {
    let dir = tempfile::tempdir().unwrap();

    let base_path = dir.path().join("base");
    let current_path = dir.path().join("current");
    let diff_path = dir.path().join("diff");
    let restored_path = dir.path().join("restored");

    // Base: 3 pages of zeros
    let base_data = vec![0u8; PAGE_SIZE * 3];
    std::fs::write(&base_path, &base_data).unwrap();

    // Current: 1 page with some dirty data
    let mut current_data = vec![0u8; PAGE_SIZE];
    current_data[..64].fill(0xFF);
    std::fs::write(&current_path, &current_data).unwrap();

    let stats = compute_diff(&base_path, &current_path, &diff_path).unwrap();
    assert_eq!(stats.total_pages, 1);
    assert_eq!(stats.dirty_pages, 1);

    let size = apply_diff(&base_path, &diff_path, &restored_path).unwrap();
    // After apply_diff, restored should be exactly 1 page (not 3)
    assert_eq!(size, PAGE_SIZE as u64);

    let restored = std::fs::read(&restored_path).unwrap();
    assert_eq!(restored, current_data);
}

#[test]
fn test_current_larger_than_base() {
    let dir = tempfile::tempdir().unwrap();

    let base_path = dir.path().join("base");
    let current_path = dir.path().join("current");
    let diff_path = dir.path().join("diff");
    let restored_path = dir.path().join("restored");

    // Base: 1 page
    let base_data = vec![0u8; PAGE_SIZE];
    std::fs::write(&base_path, &base_data).unwrap();

    // Current: 3 pages with dirty data
    let mut current_data = vec![0u8; PAGE_SIZE * 3];
    current_data[..PAGE_SIZE].fill(0xCC);
    current_data[PAGE_SIZE..PAGE_SIZE * 2].fill(0xDD);
    current_data[PAGE_SIZE * 2..PAGE_SIZE * 3].fill(0xEE);
    std::fs::write(&current_path, &current_data).unwrap();

    let stats = compute_diff(&base_path, &current_path, &diff_path).unwrap();
    assert_eq!(stats.dirty_pages, 3); // all pages differ
    assert_eq!(stats.total_pages, 3);

    let size = apply_diff(&base_path, &diff_path, &restored_path).unwrap();
    assert_eq!(size, current_data.len() as u64);

    let restored = std::fs::read(&restored_path).unwrap();
    assert_eq!(restored, current_data);
}

/// Pins the suspend-restore path's invariant: when `dest` already exists
/// as a reflink-clone of `base` (the case on cross-node restore after
/// `prepare_sandbox` reflinks the template into `instance/rootfs.ext4`),
/// `apply_diff` must overwrite the dirty pages in place and end up byte
/// equal to the suspended `current`. The pre-existing dest content must
/// be replaced — `apply_diff` short-circuits the base copy only when
/// `dest` is missing.
#[test]
fn test_apply_diff_overwrites_existing_dest() {
    let dir = tempfile::tempdir().unwrap();

    let base_path = dir.path().join("base");
    let current_path = dir.path().join("current");
    let diff_path = dir.path().join("diff");
    let dest_path = dir.path().join("dest");

    // Base: 4 pages of zeros.
    let base_data = vec![0u8; PAGE_SIZE * 4];
    std::fs::write(&base_path, &base_data).unwrap();

    // Current: page 1 dirtied to 0xAA, all others equal base.
    let mut current_data = base_data.clone();
    current_data[PAGE_SIZE..PAGE_SIZE * 2].fill(0xAA);
    std::fs::write(&current_path, &current_data).unwrap();

    // Simulate `prepare_sandbox` having already produced dest as a
    // copy of base (here a plain byte copy stands in for reflink — same
    // observable contents).
    std::fs::copy(&base_path, &dest_path).unwrap();

    let stats = compute_diff(&base_path, &current_path, &diff_path).unwrap();
    assert_eq!(stats.dirty_pages, 1);
    assert_eq!(stats.total_pages, 4);

    let size = apply_diff(&base_path, &diff_path, &dest_path).unwrap();
    assert_eq!(size, current_data.len() as u64);

    let restored = std::fs::read(&dest_path).unwrap();
    assert_eq!(restored, current_data);
}
