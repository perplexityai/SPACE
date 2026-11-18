use super::*;

fn ext4(block_size_shift: u32, blocks: u64) -> [u8; 1024] {
    let mut header = [0; 1024];
    header[0x18..0x1c].copy_from_slice(&block_size_shift.to_le_bytes());
    header[0x04..0x08].copy_from_slice(&(blocks as u32).to_le_bytes());
    header[0x150..0x154].copy_from_slice(&((blocks >> 32) as u32).to_le_bytes());
    header[0x60..0x64].copy_from_slice(&0x80_u32.to_le_bytes());
    header
}

#[test]
fn ext4_growth_uses_filesystem_blocks_and_retries_without_shrinking() {
    let original = ext4(2, 128 * 1024 * 1024 * 1024 / 4096);
    let geometry = growth_geometry(&original, 256 * 1024 * 1024 * 1024).unwrap();
    assert_eq!(geometry.current_bytes, 128 * 1024 * 1024 * 1024);
    assert_eq!(geometry.target_blocks, 67_108_864);
    let completed = ext4(2, geometry.target_blocks);
    assert_eq!(
        growth_geometry(&completed, 256 * 1024 * 1024 * 1024)
            .unwrap()
            .current_bytes,
        256 * 1024 * 1024 * 1024,
    );
    assert_eq!(
        growth_geometry(&completed, 128 * 1024 * 1024 * 1024)
            .unwrap_err()
            .code(),
        tonic::Code::FailedPrecondition,
    );
    // The upper block-count word is meaningful only for ext4's 64-bit format.
    let large = ext4(2, (1_u64 << 32) + 1);
    assert_eq!(
        growth_geometry(&large, 256 * 1024 * 1024 * 1024)
            .unwrap_err()
            .code(),
        tonic::Code::FailedPrecondition,
    );
}

#[test]
fn malformed_ext4_geometry_is_refused_before_any_resize_ioctl() {
    for header in [ext4(32, 1), ext4(2, 0)] {
        assert_eq!(
            growth_geometry(&header, 256 * 1024 * 1024 * 1024)
                .unwrap_err()
                .code(),
            tonic::Code::FailedPrecondition,
        );
    }
}
