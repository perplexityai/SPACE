use std::fs::File;
use std::io;
use std::os::unix::fs::FileExt;

const BLOCK: u64 = 4096;
const BUFFER: usize = 1024 * 1024;

/// Compare allocated ranges in both files, including holes punched in the warm
/// file. Only changed blocks enter the encrypted snapshot. Physical extent
/// addresses are not evidence of equal bytes: filesystem relocation can race us.
pub(super) fn copy_changed_blocks(source: &File, origin: &File, target: &File) -> io::Result<u64> {
    source.sync_all()?;
    let size = source.metadata()?.len();
    if size != origin.metadata()?.len() || size % BLOCK != 0 {
        return Err(io::Error::other(
            "pool rootfs sizes differ or are unaligned",
        ));
    }
    let mut ranges = allocated_ranges(source, size)?;
    ranges.extend(allocated_ranges(origin, size)?);
    ranges.sort_unstable();
    let mut merged: Vec<(u64, u64)> = Vec::new();
    for (start, end) in ranges {
        let start = start / BLOCK * BLOCK;
        let end = end.div_ceil(BLOCK).saturating_mul(BLOCK).min(size);
        if let Some(last) = merged.last_mut().filter(|last| last.1 >= start) {
            last.1 = last.1.max(end);
        } else {
            merged.push((start, end));
        }
    }
    // The output is O_DIRECT: align both the allocation and each block write.
    let mut allocation = vec![0_u8; BUFFER + BLOCK as usize];
    let alignment = allocation.as_ptr().align_offset(BLOCK as usize);
    let buffer = allocation
        .get_mut(alignment..alignment + BUFFER)
        .ok_or_else(|| io::Error::other("unaligned copy buffer"))?;
    let mut original = vec![0_u8; BUFFER];
    let mut copied = 0;
    for (mut offset, end) in merged {
        while offset < end {
            let count = (end - offset).min(BUFFER as u64) as usize;
            let buffer = buffer.get_mut(..count).ok_or(io::ErrorKind::InvalidInput)?;
            let original = original
                .get_mut(..count)
                .ok_or(io::ErrorKind::InvalidInput)?;
            source.read_exact_at(buffer, offset)?;
            origin.read_exact_at(original, offset)?;
            let changes = buffer
                .as_chunks::<{ BLOCK as usize }>()
                .0
                .iter()
                .zip(original.as_chunks::<{ BLOCK as usize }>().0.iter())
                .map(|(warm, base)| warm != base)
                .chain(std::iter::once(false));
            let mut start = None;
            for (block, changed) in changes.enumerate() {
                let index = block * BLOCK as usize;
                if changed {
                    start.get_or_insert(index);
                } else if let Some(start) = start.take() {
                    let bytes = buffer
                        .get(start..index)
                        .ok_or(io::ErrorKind::InvalidInput)?;
                    target.write_all_at(bytes, offset + start as u64)?;
                    copied += bytes.len() as u64;
                }
            }
            offset += count as u64;
        }
    }
    target.sync_all()?;
    Ok(copied)
}

#[cfg(target_os = "linux")]
fn allocated_ranges(file: &File, size: u64) -> io::Result<Vec<(u64, u64)>> {
    use nix::errno::Errno;
    use nix::unistd::{Whence, lseek};
    let mut result = Vec::new();
    let mut offset = 0;
    while offset < size {
        let start = match lseek(file, offset as i64, Whence::SeekData) {
            Ok(start) => start.cast_unsigned(),
            Err(Errno::ENXIO) => break,
            Err(Errno::EINVAL) => return Ok(vec![(0, size)]),
            Err(error) => return Err(error.into()),
        };
        let end = lseek(file, start as i64, Whence::SeekHole)?
            .cast_unsigned()
            .min(size);
        if end <= start {
            return Err(io::Error::other("sparse rootfs extent did not advance"));
        }
        result.push((start, end));
        offset = end;
    }
    Ok(result)
}

#[cfg(not(target_os = "linux"))]
fn allocated_ranges(_file: &File, size: u64) -> io::Result<Vec<(u64, u64)>> {
    Ok(vec![(0, size)])
}
