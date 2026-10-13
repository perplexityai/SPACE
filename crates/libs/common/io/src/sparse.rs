//! Sparse file output for zero-heavy decoded artifacts.
//!
//! The writer preserves the logical byte stream while replacing complete
//! all-zero blocks with filesystem holes. Callers choose the classification
//! block size: small blocks retain more holes, while larger blocks reduce the
//! CPU cost of scanning and buffering large filesystem images.

use std::fs::File;
use std::io::{Seek, SeekFrom, Write};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

const DATA_BUFFER_BYTES: usize = 4 * 1024 * 1024;

fn is_all_zero(bytes: &[u8]) -> bool {
    let (words, suffix) = bytes.as_chunks::<8>();
    // Written ciphertext and other dense data usually fail immediately. Keep
    // the vectorizable reduction for zero runs and blocks with a zero prefix.
    if words.first().is_some_and(|word| *word != [0; 8]) {
        return false;
    }
    let word_bits = words
        .iter()
        .fold(0_u64, |bits, word| bits | u64::from_ne_bytes(*word));
    let edge_bits = suffix.iter().copied().fold(0_u8, |bits, byte| bits | byte);
    word_bits == 0 && edge_bits == 0
}

/// A contiguous file range materialized with data writes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SparseDataRange {
    offset: u64,
    length: u64,
}

impl SparseDataRange {
    pub const fn new(offset: u64, length: u64) -> Self {
        Self { offset, length }
    }

    pub const fn offset(self) -> u64 {
        self.offset
    }

    pub const fn length(self) -> u64 {
        self.length
    }
}

/// Sparse classifications completed since the previous report.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct SparseMaterialization {
    data_ranges: Vec<SparseDataRange>,
    hole_bytes: u64,
}

impl SparseMaterialization {
    pub fn into_parts(self) -> (Vec<SparseDataRange>, u64) {
        (self.data_ranges, self.hole_bytes)
    }
}

impl From<SparseDataRange> for SparseMaterialization {
    fn from(range: SparseDataRange) -> Self {
        Self {
            data_ranges: vec![range],
            hole_bytes: 0,
        }
    }
}

/// A [`Write`] implementation that leaves complete all-zero blocks sparse.
///
/// [`Write::flush`] makes every accepted byte visible by materializing an
/// incomplete classification block as data. The remainder of that logical
/// block also stays dense so later writes preserve the configured alignment.
/// Use [`Self::finish`] to finalize an unflushed trailing block sparsely.
#[derive(Debug)]
pub struct SparseFileWriter {
    inner: File,
    block_bytes: usize,
    block: Vec<u8>,
    dense_bytes_until_block_boundary: usize,
    data: Vec<u8>,
    pending_hole_bytes: u64,
    candidate_hole_bytes: u64,
    position: u64,
    materialization: SparseMaterialization,
    candidate_observer: Option<Arc<AtomicU64>>,
    #[cfg(target_os = "macos")]
    holes: Vec<(u64, u64)>,
}

impl SparseFileWriter {
    /// Creates a sparse writer that classifies zeroes in `block_bytes` chunks.
    pub fn new(inner: File, block_bytes: usize) -> std::io::Result<Self> {
        Self::with_options(inner, block_bytes, None)
    }

    /// Creates a sparse writer that reports complete classified blocks as the
    /// materialized output flushes.
    pub fn with_observer(
        inner: File,
        block_bytes: usize,
        candidate_observer: Arc<AtomicU64>,
    ) -> std::io::Result<Self> {
        Self::with_options(inner, block_bytes, Some(candidate_observer))
    }

    /// Returns the logical offset already materialized in the underlying file.
    ///
    /// Bytes still buffered for sparse classification are not included.
    pub fn materialized_through(&self) -> u64 {
        self.position
    }

    /// Borrows the underlying file for range writeback and cache advice.
    pub fn file(&self) -> &File {
        &self.inner
    }

    /// Takes classifications completed since the previous call.
    ///
    /// Data ranges are reported only after bytes reach the underlying file.
    /// Complete zero blocks are reported as holes as soon as they are
    /// classified, before a later seek materializes their logical span.
    pub fn take_materialization(&mut self) -> SparseMaterialization {
        std::mem::take(&mut self.materialization)
    }

    /// Materializes complete classified blocks once their buffered logical
    /// length reaches `threshold_bytes`.
    ///
    /// An incomplete classification block remains buffered so calling this
    /// method does not make a sparse candidate dense.
    pub fn materialize_complete_blocks_if_buffered_at_least(
        &mut self,
        threshold_bytes: u64,
    ) -> std::io::Result<()> {
        let buffered_bytes = (self.data.len() as u64).saturating_add(self.pending_hole_bytes);
        if buffered_bytes < threshold_bytes {
            return Ok(());
        }
        self.flush_data()?;
        self.flush_hole()
    }

    fn with_options(
        inner: File,
        block_bytes: usize,
        candidate_observer: Option<Arc<AtomicU64>>,
    ) -> std::io::Result<Self> {
        if block_bytes == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "sparse block size must be non-zero",
            ));
        }
        Ok(Self {
            inner,
            block_bytes,
            block: Vec::with_capacity(block_bytes),
            dense_bytes_until_block_boundary: 0,
            data: Vec::with_capacity(DATA_BUFFER_BYTES),
            pending_hole_bytes: 0,
            candidate_hole_bytes: 0,
            position: 0,
            materialization: SparseMaterialization::default(),
            candidate_observer,
            #[cfg(target_os = "macos")]
            holes: Vec::new(),
        })
    }

    /// Finalizes pending data and holes and returns the underlying file.
    pub fn finish(self) -> std::io::Result<File> {
        self.finish_with_stats().map(|(file, _)| file)
    }

    /// Finalizes the output and returns the file and classified hole bytes.
    pub fn finish_with_stats(self) -> std::io::Result<(File, u64)> {
        self.finish_with_stats_and_materialization()
            .map(|(file, candidate_hole_bytes, _)| (file, candidate_hole_bytes))
    }

    /// Finalizes output and also returns classifications not previously taken.
    pub fn finish_with_stats_and_materialization(
        mut self,
    ) -> std::io::Result<(File, u64, SparseMaterialization)> {
        self.flush_block()?;
        self.flush_output()?;
        let materialization = self.take_materialization();
        Ok((self.inner, self.candidate_hole_bytes, materialization))
    }

    #[cfg(target_os = "macos")]
    fn punch_recorded_holes(&mut self) {
        for (offset, length) in self.holes.drain(..) {
            if let Err(error) = punch_hole(&self.inner, offset, length) {
                tracing::debug!(%error, "filesystem does not support sparse hole punching");
                break;
            }
        }
    }

    fn block_bytes(&self) -> usize {
        self.block_bytes
    }

    fn flush_block(&mut self) -> std::io::Result<()> {
        if self.block.is_empty() {
            return Ok(());
        }
        let mut block = std::mem::take(&mut self.block);
        let result = self.process_block(&block);
        block.clear();
        self.block = block;
        result
    }

    fn process_block(&mut self, block: &[u8]) -> std::io::Result<()> {
        if is_all_zero(block) {
            self.candidate_hole_bytes =
                self.candidate_hole_bytes
                    .checked_add(block.len() as u64)
                    .ok_or_else(|| std::io::Error::other("sparse candidate exceeds u64::MAX"))?;
            self.pending_hole_bytes = self
                .pending_hole_bytes
                .checked_add(block.len() as u64)
                .ok_or_else(|| std::io::Error::other("sparse hole exceeds u64::MAX"))?;
            self.materialization.hole_bytes = self
                .materialization
                .hole_bytes
                .checked_add(block.len() as u64)
                .ok_or_else(|| {
                    std::io::Error::other("sparse materialized holes exceed u64::MAX")
                })?;
            return Ok(());
        }
        self.append_data(block)
    }

    fn flush_partial_block_as_data(&mut self) -> std::io::Result<()> {
        if self.block.is_empty() {
            return Ok(());
        }
        self.dense_bytes_until_block_boundary = self.block_bytes() - self.block.len();
        let mut block = std::mem::take(&mut self.block);
        let result = self.append_data(&block);
        block.clear();
        self.block = block;
        result
    }

    fn append_data(&mut self, bytes: &[u8]) -> std::io::Result<()> {
        self.flush_hole()?;
        self.data.extend_from_slice(bytes);
        if self.data.len() >= DATA_BUFFER_BYTES {
            self.flush_data()?;
        }
        Ok(())
    }

    fn flush_data(&mut self) -> std::io::Result<()> {
        if self.data.is_empty() {
            return Ok(());
        }
        let offset = self.position;
        let length = self.data.len() as u64;
        self.inner.write_all(&self.data)?;
        self.position = self
            .position
            .checked_add(length)
            .ok_or_else(|| std::io::Error::other("sparse output exceeds u64::MAX"))?;
        if let Some(previous) = self.materialization.data_ranges.last_mut()
            && previous.offset.checked_add(previous.length) == Some(offset)
        {
            previous.length = previous.length.checked_add(length).ok_or_else(|| {
                std::io::Error::other("sparse materialized data range exceeds u64::MAX")
            })?;
        } else {
            self.materialization
                .data_ranges
                .push(SparseDataRange::new(offset, length));
        }
        self.data.clear();
        Ok(())
    }

    fn flush_hole(&mut self) -> std::io::Result<()> {
        if self.pending_hole_bytes == 0 {
            return Ok(());
        }
        self.flush_data()?;
        let start = self.position;
        let end = start
            .checked_add(self.pending_hole_bytes)
            .ok_or_else(|| std::io::Error::other("sparse output exceeds u64::MAX"))?;
        self.inner.set_len(end)?;
        self.inner.seek(SeekFrom::Start(end))?;
        #[cfg(target_os = "macos")]
        self.holes.push((start, self.pending_hole_bytes));
        self.position = end;
        self.pending_hole_bytes = 0;
        Ok(())
    }

    fn flush_output(&mut self) -> std::io::Result<()> {
        self.flush_data()?;
        self.flush_hole()?;
        self.inner.flush()?;
        #[cfg(target_os = "macos")]
        self.punch_recorded_holes();
        if let Some(observer) = self.candidate_observer.as_ref() {
            observer.store(self.candidate_hole_bytes, Ordering::Relaxed);
        }
        Ok(())
    }
}

impl Write for SparseFileWriter {
    fn write(&mut self, mut bytes: &[u8]) -> std::io::Result<usize> {
        let input_len = bytes.len();
        if self.dense_bytes_until_block_boundary > 0 {
            let take = bytes.len().min(self.dense_bytes_until_block_boundary);
            let (prefix, rest) = bytes.split_at(take);
            self.append_data(prefix)?;
            self.dense_bytes_until_block_boundary -= take;
            bytes = rest;
        }
        if !self.block.is_empty() {
            let take = bytes.len().min(self.block_bytes() - self.block.len());
            let (prefix, rest) = bytes.split_at(take);
            self.block.extend_from_slice(prefix);
            bytes = rest;
            if self.block.len() == self.block_bytes() {
                self.flush_block()?;
            }
        }

        while bytes.len() >= self.block_bytes() {
            let (block, rest) = bytes.split_at(self.block_bytes());
            self.process_block(block)?;
            bytes = rest;
        }
        self.block.extend_from_slice(bytes);
        Ok(input_len)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.flush_partial_block_as_data()?;
        self.flush_output()
    }
}

#[cfg(target_os = "macos")]
fn punch_hole(file: &File, offset: u64, length: u64) -> std::io::Result<()> {
    use std::os::fd::AsRawFd;

    const PUNCH_ALIGNMENT_BYTES: u64 = 4096;
    let end = offset
        .checked_add(length)
        .ok_or_else(|| std::io::Error::other("sparse hole exceeds u64::MAX"))?;
    let offset = offset
        .checked_add(PUNCH_ALIGNMENT_BYTES - 1)
        .ok_or_else(|| std::io::Error::other("sparse hole offset exceeds u64::MAX"))?
        / PUNCH_ALIGNMENT_BYTES
        * PUNCH_ALIGNMENT_BYTES;
    let aligned_end = end / PUNCH_ALIGNMENT_BYTES * PUNCH_ALIGNMENT_BYTES;
    let Some(length) = aligned_end.checked_sub(offset) else {
        return Ok(());
    };
    if length == 0 {
        return Ok(());
    }
    let mut range = libc::fpunchhole_t {
        fp_flags: 0,
        reserved: 0,
        fp_offset: offset
            .try_into()
            .map_err(|_| std::io::Error::other("sparse hole offset exceeds off_t::MAX"))?,
        fp_length: length
            .try_into()
            .map_err(|_| std::io::Error::other("sparse hole length exceeds off_t::MAX"))?,
    };
    // SAFETY: `file` owns a valid descriptor and `range` remains initialized
    // for the duration of this synchronous fcntl call.
    let result = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_PUNCHHOLE, &mut range) };
    if result == -1 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(())
}

#[cfg(test)]
mod tests;
