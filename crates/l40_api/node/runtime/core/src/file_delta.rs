use std::io::{Read, Seek, SeekFrom, Write};
use std::ops::Range;
use std::path::{Path, PathBuf};

use thiserror::Error;

const PAGE_SIZE: usize = 4096;
const DIFF_SCAN_CHUNK_SIZE: usize = PAGE_SIZE * 256;
const MAGIC: &[u8; 4] = b"SPFD";
const VERSION: u8 = 0x02;
// Header: magic(4) + version(1) + page_size(4) + page_count(8) + target_size(8) = 25 bytes
const HEADER_SIZE: usize = 4 + 1 + 4 + 8 + 8;

#[derive(Error, Debug)]
pub enum DiffError {
    #[error("stat base file {0}: {1}")]
    StatBaseFile(PathBuf, #[source] std::io::Error),

    #[error("stat current file {0}: {1}")]
    StatCurrentFile(PathBuf, #[source] std::io::Error),

    #[error("open base file {0}: {1}")]
    OpenBaseFile(PathBuf, #[source] std::io::Error),

    #[error("open current file {0}: {1}")]
    OpenCurrentFile(PathBuf, #[source] std::io::Error),

    #[error("read current chunk at offset {offset}: {source}")]
    ReadCurrentChunk {
        offset: u64,
        #[source]
        source: std::io::Error,
    },

    #[error("read base chunk at offset {offset}: {source}")]
    ReadBaseChunk {
        offset: u64,
        #[source]
        source: std::io::Error,
    },

    #[error("create diff file {0}: {1}")]
    CreateDiffFile(PathBuf, #[source] std::io::Error),

    #[error("zstd encoder initialization: {0}")]
    ZstdEncoderInit(#[source] std::io::Error),

    #[error("zstd decoder initialization: {0}")]
    ZstdDecoderInit(#[source] std::io::Error),

    #[error("write magic: {0}")]
    WriteMagic(#[source] std::io::Error),

    #[error("write version: {0}")]
    WriteVersion(#[source] std::io::Error),

    #[error("write page_size: {0}")]
    WritePageSize(#[source] std::io::Error),

    #[error("write page_count: {0}")]
    WritePageCount(#[source] std::io::Error),

    #[error("write target_size: {0}")]
    WriteTargetSize(#[source] std::io::Error),

    #[error("seek current to offset {offset}: {source}")]
    SeekCurrent {
        offset: u64,
        #[source]
        source: std::io::Error,
    },

    #[error("re-read current at offset {offset}: {source}")]
    ReReadCurrent {
        offset: u64,
        #[source]
        source: std::io::Error,
    },

    #[error("write offset: {0}")]
    WriteOffset(#[source] std::io::Error),

    #[error("write page data: {0}")]
    WritePageData(#[source] std::io::Error),

    #[error("zstd encoder finish: {0}")]
    ZstdEncoderFinish(#[source] std::io::Error),

    #[error("stat diff file {0}: {1}")]
    StatDiffFile(PathBuf, #[source] std::io::Error),

    #[error("invalid diff magic")]
    InvalidMagic,

    #[error("unsupported diff version: {0}")]
    UnsupportedVersion(u8),

    #[error("unexpected page size: {0}")]
    UnexpectedPageSize(usize),

    #[error("delta output or base exceeds the {limit} byte limit")]
    SizeLimitExceeded { limit: u64 },

    #[error("delta pages must be aligned, ordered, and contained in the target file")]
    InvalidPageLayout,

    #[error("invalid diff header field {0}")]
    InvalidHeaderField(&'static str),

    #[error("invalid diff buffer range {range:?} with length {len}")]
    InvalidBufferRange { range: Range<usize>, len: usize },

    #[error("copy base file {base} to dest file {dest}: {source}")]
    CopyBaseToDest {
        base: PathBuf,
        dest: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("open diff file {0}: {1}")]
    OpenDiffFile(PathBuf, #[source] std::io::Error),

    #[error("open dest file {0}: {1}")]
    OpenDestFile(PathBuf, #[source] std::io::Error),

    #[error("read diff header: {0}")]
    ReadDiffHeader(#[source] std::io::Error),

    #[error("read page offset: {0}")]
    ReadPageOffset(#[source] std::io::Error),

    #[error("read page data: {0}")]
    ReadPageData(#[source] std::io::Error),

    #[error("extend dest to {offset}: {source}")]
    ExtendDest {
        offset: u64,
        #[source]
        source: std::io::Error,
    },

    #[error("seek dest to offset {offset}: {source}")]
    SeekDest {
        offset: u64,
        #[source]
        source: std::io::Error,
    },

    #[error("write page at offset {offset}: {source}")]
    WritePage {
        offset: u64,
        #[source]
        source: std::io::Error,
    },

    #[error("set dest to target size {size}: {source}")]
    SetDestSize {
        size: u64,
        #[source]
        source: std::io::Error,
    },

    #[error("seek dest end: {0}")]
    SeekDestEnd(#[source] std::io::Error),
}

pub struct DiffStats {
    pub base_size: u64,
    pub current_size: u64,
    pub diff_size: u64,
    pub dirty_pages: u64,
    pub total_pages: u64,
}

/// Compute a streaming page-level diff between base and current files.
/// Writes a zstd-compressed diff to dest_path. Never holds more than a few pages in memory.
pub fn compute_diff(
    base_path: &Path,
    current_path: &Path,
    dest_path: &Path,
) -> Result<DiffStats, DiffError> {
    let base_meta = std::fs::metadata(base_path)
        .map_err(|e| DiffError::StatBaseFile(base_path.to_path_buf(), e))?;
    let current_meta = std::fs::metadata(current_path)
        .map_err(|e| DiffError::StatCurrentFile(current_path.to_path_buf(), e))?;

    let base_size = base_meta.len();
    let current_size = current_meta.len();
    let total_pages = (current_size as usize).div_ceil(PAGE_SIZE);

    let mut base_file = std::fs::File::open(base_path)
        .map_err(|e| DiffError::OpenBaseFile(base_path.to_path_buf(), e))?;
    let mut current_file = std::fs::File::open(current_path)
        .map_err(|e| DiffError::OpenCurrentFile(current_path.to_path_buf(), e))?;

    // Pass 1: stream through both files, find dirty page offsets.
    // Scan in larger chunks instead of one syscall per 4 KiB page.
    let mut dirty_offsets = Vec::with_capacity(total_pages);
    let pages_per_chunk = DIFF_SCAN_CHUNK_SIZE / PAGE_SIZE;
    let mut base_buf = vec![0u8; DIFF_SCAN_CHUNK_SIZE];
    let mut cur_buf = vec![0u8; DIFF_SCAN_CHUNK_SIZE];

    for chunk_start_page in (0..total_pages).step_by(pages_per_chunk) {
        let chunk_start = (chunk_start_page * PAGE_SIZE) as u64;
        let pages_in_chunk = pages_per_chunk.min(total_pages - chunk_start_page);
        let chunk_capacity = pages_in_chunk * PAGE_SIZE;

        let cur_len = bytes_remaining(current_size, chunk_start, chunk_capacity);
        let cur_buf_len = cur_buf.len();
        let cur_chunk = checked_slice_mut(&mut cur_buf, 0..cur_len).map_err(|()| {
            DiffError::InvalidBufferRange {
                range: 0..cur_len,
                len: cur_buf_len,
            }
        })?;
        read_exact_len(&mut current_file, cur_chunk).map_err(|e| DiffError::ReadCurrentChunk {
            offset: chunk_start,
            source: e,
        })?;

        let base_len = bytes_remaining(base_size, chunk_start, chunk_capacity);
        if base_len > 0 {
            let base_buf_len = base_buf.len();
            let base_chunk = checked_slice_mut(&mut base_buf, 0..base_len).map_err(|()| {
                DiffError::InvalidBufferRange {
                    range: 0..base_len,
                    len: base_buf_len,
                }
            })?;
            read_exact_len(&mut base_file, base_chunk).map_err(|e| DiffError::ReadBaseChunk {
                offset: chunk_start,
                source: e,
            })?;
        }

        for page_offset in 0..pages_in_chunk {
            let page_idx = chunk_start_page + page_offset;
            let offset = (page_idx * PAGE_SIZE) as u64;
            let rel_start = page_offset * PAGE_SIZE;
            let rel_end = rel_start + PAGE_SIZE;
            let cur_page_len = page_len_at(current_size, offset);
            let base_page_len = page_len_at(base_size, offset);

            if cur_page_len != base_page_len {
                dirty_offsets.push(offset);
                continue;
            }

            let cur_range = rel_start..rel_start + cur_page_len;
            let cur_page = checked_slice(&cur_buf, cur_range.clone()).map_err(|()| {
                DiffError::InvalidBufferRange {
                    range: cur_range,
                    len: cur_buf.len(),
                }
            })?;
            let base_range = rel_start..rel_start + base_page_len;
            let base_page = checked_slice(&base_buf, base_range.clone()).map_err(|()| {
                DiffError::InvalidBufferRange {
                    range: base_range,
                    len: base_buf.len(),
                }
            })?;
            if cur_page != base_page {
                dirty_offsets.push(offset);
            }

            // Avoid retaining stale data when the next chunk is partial and the
            // same backing buffers are reused.
            if rel_end > cur_len {
                let range = rel_start + cur_page_len..rel_end;
                let len = cur_buf.len();
                checked_slice_mut(&mut cur_buf, range.clone())
                    .map_err(|()| DiffError::InvalidBufferRange { range, len })?
                    .fill(0);
            }
            if rel_end > base_len {
                let range = rel_start + base_page_len..rel_end;
                let len = base_buf.len();
                checked_slice_mut(&mut base_buf, range.clone())
                    .map_err(|()| DiffError::InvalidBufferRange { range, len })?
                    .fill(0);
            }
        }
    }

    // Pass 2: write diff file — re-read current at dirty offsets
    let out_file = std::fs::File::create(dest_path)
        .map_err(|e| DiffError::CreateDiffFile(dest_path.to_path_buf(), e))?;
    let mut encoder = zstd::Encoder::new(out_file, 3).map_err(DiffError::ZstdEncoderInit)?;

    // Write header
    encoder.write_all(MAGIC).map_err(DiffError::WriteMagic)?;
    encoder
        .write_all(&[VERSION])
        .map_err(DiffError::WriteVersion)?;
    encoder
        .write_all(&(PAGE_SIZE as u32).to_le_bytes())
        .map_err(DiffError::WritePageSize)?;
    encoder
        .write_all(&(dirty_offsets.len() as u64).to_le_bytes())
        .map_err(DiffError::WritePageCount)?;
    encoder
        .write_all(&(current_size).to_le_bytes())
        .map_err(DiffError::WriteTargetSize)?;

    // Write dirty pages
    let mut cur_buf = [0u8; PAGE_SIZE];
    for &offset in &dirty_offsets {
        current_file
            .seek(SeekFrom::Start(offset))
            .map_err(|e| DiffError::SeekCurrent { offset, source: e })?;
        let n = read_page(&mut current_file, &mut cur_buf)
            .map_err(|e| DiffError::ReReadCurrent { offset, source: e })?;

        encoder
            .write_all(&offset.to_le_bytes())
            .map_err(DiffError::WriteOffset)?;
        // Always write a full page (zero-pad if last page is short)
        if let Some(tail) = cur_buf.get_mut(n..) {
            tail.fill(0);
        }
        encoder
            .write_all(&cur_buf)
            .map_err(DiffError::WritePageData)?;
    }

    encoder.finish().map_err(DiffError::ZstdEncoderFinish)?;

    let diff_size = std::fs::metadata(dest_path)
        .map_err(|e| DiffError::StatDiffFile(dest_path.to_path_buf(), e))?
        .len();

    Ok(DiffStats {
        base_size,
        current_size,
        diff_size,
        dirty_pages: dirty_offsets.len() as u64,
        total_pages: total_pages as u64,
    })
}

fn checked_slice(buf: &[u8], range: Range<usize>) -> Result<&[u8], ()> {
    buf.get(range).ok_or(())
}

fn checked_slice_mut(buf: &mut [u8], range: Range<usize>) -> Result<&mut [u8], ()> {
    buf.get_mut(range).ok_or(())
}

fn bytes_remaining(total_size: u64, offset: u64, max_len: usize) -> usize {
    total_size.saturating_sub(offset).min(max_len as u64) as usize
}

fn page_len_at(total_size: u64, offset: u64) -> usize {
    bytes_remaining(total_size, offset, PAGE_SIZE)
}

fn read_exact_len(file: &mut std::fs::File, buf: &mut [u8]) -> std::io::Result<()> {
    let mut total = 0;
    while total < buf.len() {
        let Some(remaining) = buf.get_mut(total..) else {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "buffer read offset exceeded buffer length",
            ));
        };
        match file.read(remaining) {
            Ok(0) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "file ended before expected length",
                ));
            }
            Ok(n) => total += n,
            Err(ref e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(e),
        }
    }
    Ok(())
}

/// Apply a page-level diff to a base file and write to dest.
/// Uses btrfs reflink (instant CoW clone) when available, falls back to regular copy.
/// Only dirty pages cause new block allocations on btrfs.
/// Returns the size of the resulting file.
pub fn apply_diff(base_path: &Path, diff_path: &Path, dest_path: &Path) -> Result<u64, DiffError> {
    apply_diff_with_limit(base_path, diff_path, dest_path, i64::MAX as u64)
}

/// Apply a peer-supplied delta with a receiver-owned output bound. Validate
/// declared expansion before cloning the base; validate every page before I/O.
pub fn apply_diff_with_limit(
    base_path: &Path,
    diff_path: &Path,
    dest_path: &Path,
    max_output_bytes: u64,
) -> Result<u64, DiffError> {
    let diff_file = std::fs::File::open(diff_path)
        .map_err(|e| DiffError::OpenDiffFile(diff_path.to_path_buf(), e))?;
    let mut decoder = zstd::Decoder::new(diff_file).map_err(DiffError::ZstdDecoderInit)?;

    // Read header
    let mut header = [0u8; HEADER_SIZE];
    decoder
        .read_exact(&mut header)
        .map_err(DiffError::ReadDiffHeader)?;

    if header_field(&header, 0..4, "magic")? != MAGIC {
        return Err(DiffError::InvalidMagic);
    }
    let version = *header
        .get(4)
        .ok_or(DiffError::InvalidHeaderField("version"))?;
    if version != VERSION {
        return Err(DiffError::UnsupportedVersion(version));
    }
    let page_size = read_header_u32(&header, 5..9, "page_size")? as usize;
    let page_count = read_header_u64(&header, 9..17, "page_count")?;
    let target_size = read_header_u64(&header, 17..25, "target_size")?;

    if page_size != PAGE_SIZE {
        return Err(DiffError::UnexpectedPageSize(page_size));
    }
    if target_size > max_output_bytes || target_size > i64::MAX as u64 {
        return Err(DiffError::SizeLimitExceeded {
            limit: max_output_bytes,
        });
    }
    if page_count > target_size.div_ceil(PAGE_SIZE as u64) {
        return Err(DiffError::InvalidPageLayout);
    }

    // A shrink delta can still have a large base. Bound the initial clone too,
    // including the non-reflink fallback, before allocating destination data.
    if !dest_path.exists() {
        let base_size = std::fs::metadata(base_path)
            .map_err(|e| DiffError::StatBaseFile(base_path.to_path_buf(), e))?
            .len();
        if base_size > max_output_bytes {
            return Err(DiffError::SizeLimitExceeded {
                limit: max_output_bytes,
            });
        }
        crate::reflink_copy(base_path, dest_path).map_err(|e| DiffError::CopyBaseToDest {
            base: base_path.to_path_buf(),
            dest: dest_path.to_path_buf(),
            source: e,
        })?;
    }

    // Open dest for in-place writes
    let mut dest_file = std::fs::OpenOptions::new()
        .write(true)
        .open(dest_path)
        .map_err(|e| DiffError::OpenDestFile(dest_path.to_path_buf(), e))?;

    let mut offset_buf = [0u8; 8];
    let mut page_buf = [0u8; PAGE_SIZE];
    let mut max_end: u64 = dest_file
        .seek(SeekFrom::End(0))
        .map_err(DiffError::SeekDestEnd)?;
    if max_end > max_output_bytes {
        return Err(DiffError::SizeLimitExceeded {
            limit: max_output_bytes,
        });
    }
    let mut previous_offset = None;

    for _ in 0..page_count {
        decoder
            .read_exact(&mut offset_buf)
            .map_err(DiffError::ReadPageOffset)?;
        let offset = u64::from_le_bytes(offset_buf);
        if offset >= target_size
            || !offset.is_multiple_of(PAGE_SIZE as u64)
            || previous_offset.is_some_and(|previous| offset <= previous)
        {
            return Err(DiffError::InvalidPageLayout);
        }
        previous_offset = Some(offset);

        decoder
            .read_exact(&mut page_buf)
            .map_err(DiffError::ReadPageData)?;

        // Extend file if needed (current was larger than base)
        let write_len = usize::try_from((target_size - offset).min(PAGE_SIZE as u64))
            .map_err(|_| DiffError::InvalidPageLayout)?;
        let page_end = offset + write_len as u64;
        if page_end > max_end {
            dest_file
                .set_len(page_end)
                .map_err(|e| DiffError::ExtendDest {
                    offset: page_end,
                    source: e,
                })?;
            max_end = page_end;
        }

        dest_file
            .seek(SeekFrom::Start(offset))
            .map_err(|e| DiffError::SeekDest { offset, source: e })?;
        dest_file
            .write_all(
                page_buf
                    .get(..write_len)
                    .ok_or(DiffError::InvalidBufferRange {
                        range: 0..write_len,
                        len: page_buf.len(),
                    })?,
            )
            .map_err(|e| DiffError::WritePage { offset, source: e })?;
    }

    // Truncate or extend to match the original current file size
    dest_file
        .set_len(target_size)
        .map_err(|e| DiffError::SetDestSize {
            size: target_size,
            source: e,
        })?;

    Ok(target_size)
}

fn header_field<'a>(
    header: &'a [u8],
    range: Range<usize>,
    field: &'static str,
) -> Result<&'a [u8], DiffError> {
    header
        .get(range)
        .ok_or(DiffError::InvalidHeaderField(field))
}

fn read_header_u32(
    header: &[u8],
    range: Range<usize>,
    field: &'static str,
) -> Result<u32, DiffError> {
    let bytes = header_field(header, range, field)?;
    if bytes.len() != 4 {
        return Err(DiffError::InvalidHeaderField(field));
    }
    let mut array = [0u8; 4];
    array.copy_from_slice(bytes);
    Ok(u32::from_le_bytes(array))
}

fn read_header_u64(
    header: &[u8],
    range: Range<usize>,
    field: &'static str,
) -> Result<u64, DiffError> {
    let bytes = header_field(header, range, field)?;
    if bytes.len() != 8 {
        return Err(DiffError::InvalidHeaderField(field));
    }
    let mut array = [0u8; 8];
    array.copy_from_slice(bytes);
    Ok(u64::from_le_bytes(array))
}

/// Read up to PAGE_SIZE bytes from a file. Returns actual bytes read.
fn read_page(file: &mut std::fs::File, buf: &mut [u8; PAGE_SIZE]) -> std::io::Result<usize> {
    let mut total = 0;
    while total < PAGE_SIZE {
        let Some(remaining) = buf.get_mut(total..) else {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "page read offset exceeded page length",
            ));
        };
        match file.read(remaining) {
            Ok(0) => break,
            Ok(n) => total += n,
            Err(ref e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(e),
        }
    }
    Ok(total)
}

#[cfg(test)]
mod tests;
