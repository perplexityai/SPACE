//! Single-writer machinery for ranged downloads.
//!
//! Ranged downloads fetch chunks concurrently over several S3 connections,
//! but exactly one task writes the destination file, sequentially. Concurrent
//! writers to one file convoy on btrfs: `btrfs_buffered_write` holds the
//! inode's write lock across an inline wait for in-flight page writeback, so
//! N ranged writers serialize behind whichever one is waiting, while the
//! disk sits idle. A single sequential writer trails the flusher instead of
//! overlapping it and never joins that convoy.
//!
//! Writes use buffered I/O. Direct I/O would add alignment requirements and
//! does not remove the need to serialize writes to this inode.
//!
//! The writer targets a unique per-attempt scratch file and atomically
//! renames it over the destination only in [`SequentialWriter::commit`]. A
//! timed-out attempt's un-cancellable in-flight write therefore can never
//! land bytes inside a newer attempt's output: each attempt owns its own
//! inode, and whichever rename lands last replaces the whole file. Scratch
//! files orphaned by killed processes are swept before each new attempt.

use std::collections::BTreeMap;
use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use tracing::warn;

/// Buffers out-of-order items until they can be consumed sequentially.
///
/// The scheduler bounds how far chunk fetches may run ahead of
/// [`Self::next_index`], which in turn bounds how many chunks this window
/// ever holds.
pub(crate) struct ReorderWindow<T> {
    next: usize,
    pending: BTreeMap<usize, T>,
}

impl<T> ReorderWindow<T> {
    pub(crate) fn new() -> Self {
        Self {
            next: 0,
            pending: BTreeMap::new(),
        }
    }

    /// Index of the next item the consumer needs.
    pub(crate) fn next_index(&self) -> usize {
        self.next
    }

    /// Number of items parked out of order.
    pub(crate) fn pending_len(&self) -> usize {
        self.pending.len()
    }

    /// Parks a completed item. Indices must be unique.
    pub(crate) fn insert(&mut self, index: usize, item: T) {
        let replaced = self.pending.insert(index, item);
        debug_assert!(replaced.is_none(), "duplicate chunk index {index}");
    }

    /// Removes and returns the next in-order item, if it has arrived.
    pub(crate) fn pop_ready(&mut self) -> Option<T> {
        let item = self.pending.remove(&self.next)?;
        self.next += 1;
        Some(item)
    }
}

/// Disambiguates concurrent attempts targeting the same destination within
/// one process; combined with the pid and wall-clock nanos this makes scratch
/// names unique across restarts too.
static SCRATCH_COUNTER: AtomicU64 = AtomicU64::new(0);

fn unique_scratch_path(dst: &Path) -> PathBuf {
    let nanos = jiff::Timestamp::now().as_nanosecond();
    let seq = SCRATCH_COUNTER.fetch_add(1, Ordering::Relaxed);
    let mut name = dst.file_name().unwrap_or_default().to_os_string();
    name.push(format!(".dl-{}-{nanos}-{seq}.tmp", std::process::id()));
    dst.with_file_name(name)
}

/// Removes scratch siblings of `dst` left by attempts that never ran their
/// `Drop` — SIGKILL (the pod's 120 s termination grace regularly cuts off
/// the much longer transfer drain), OOM kill, or node reboot — so orphaned
/// multi-gigabyte scratch files cannot accumulate in persistent cache
/// storage. Safe under the per-destination job serialization: a matching
/// file belongs either to a dead attempt of this same destination or to a
/// detached zombie of the current process, whose in-flight writes target an
/// already-open inode (unlinking is invisible to it) and whose commit is
/// unreachable after cancellation. Best-effort: failures are logged, never
/// fatal.
fn remove_stale_scratch_siblings(dst: &Path) {
    let Some(dir) = dst.parent() else { return };
    let Some(file_name) = dst.file_name().and_then(|n| n.to_str()) else {
        return;
    };
    let prefix = format!("{file_name}.dl-");
    let entries = match std::fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(err) => {
            warn!(
                dir = %dir.display(),
                error = %err,
                "failed to scan for stale download scratch files"
            );
            return;
        }
    };
    for entry in entries.flatten() {
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        if !(name.starts_with(&prefix) && name.ends_with(".tmp")) {
            continue;
        }
        match std::fs::remove_file(entry.path()) {
            Ok(()) => warn!(
                orphan = %entry.path().display(),
                "removed stale download scratch file from a prior attempt"
            ),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => warn!(
                orphan = %entry.path().display(),
                error = %err,
                "failed to remove stale download scratch file"
            ),
        }
    }
}

/// A byte sink the sequential writer appends into, with an explicit
/// end-of-stream hook. The plain download writes straight to the scratch
/// [`File`]; a streaming-decode download wraps that file in a transform
/// (e.g. a zstd decoder) that may buffer and must flush trailing output.
///
/// `finish` consumes the sink — finalization cannot be repeated and no
/// write can follow it, so transforms don't need their own post-finish
/// state machines. It returns whether the stream was actually transformed
/// (decoded) rather than passed through, so callers can skip a redundant
/// second decode pass.
pub trait FinishingWrite: Write + Send {
    fn finish(self: Box<Self>) -> std::io::Result<bool>;
}

impl FinishingWrite for File {
    fn finish(mut self: Box<Self>) -> std::io::Result<bool> {
        self.flush()?;
        Ok(false)
    }
}

/// Sweeps stale scratch siblings, then creates a unique scratch sibling of
/// `dst` for sequential buffered writing. Runs blocking filesystem work;
/// call from the blocking pool.
///
/// Returns the scratch's RAII [`ScratchGuard`] alongside the open file, so
/// cleanup ownership exists from the moment the file does: if the caller's
/// future is cancelled while the blocking call runs, Tokio drops the
/// closure's unclaimed result — dropping the guard — and the uniquely
/// named scratch file is reclaimed instead of orphaned.
pub(crate) fn open_scratch(dst: &Path) -> std::io::Result<(File, ScratchGuard)> {
    remove_stale_scratch_siblings(dst);
    let scratch = unique_scratch_path(dst);
    let file = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&scratch)?;
    Ok((
        file,
        ScratchGuard {
            scratch,
            installed: false,
        },
    ))
}

/// Owns the scratch file's lifetime: unless disarmed by a successful
/// commit, dropping the guard removes the scratch file — a failed or
/// abandoned attempt must reclaim its own scratch, since the unique name
/// means no later attempt will truncate-and-reuse it.
#[derive(Debug)]
pub(crate) struct ScratchGuard {
    scratch: PathBuf,
    installed: bool,
}

impl Drop for ScratchGuard {
    fn drop(&mut self) {
        if self.installed {
            return;
        }
        if let Err(err) = std::fs::remove_file(&self.scratch)
            && err.kind() != std::io::ErrorKind::NotFound
        {
            warn!(
                scratch = %self.scratch.display(),
                error = %err,
                "failed to remove abandoned download scratch file"
            );
        }
    }
}

/// The single sequential writer for one ranged download.
///
/// All write methods are synchronous and intended to run inside
/// `tokio::task::spawn_blocking`; the async orchestrator moves the writer in
/// and out of each blocking call. Dropping the writer without sealing
/// removes the scratch file. [`Self::prepare_commit`] consumes the writer
/// and its sink into a [`SealedDownload`], so writing after finalization —
/// or finalizing twice — is unrepresentable.
pub(crate) struct SequentialWriter {
    sink: Box<dyn FinishingWrite>,
    written: u64,
    guard: ScratchGuard,
    dst: PathBuf,
    /// Opaque admission guard for the sink's CPU work. The writer moves in
    /// and out of every blocking write call, so the guard lives until the
    /// sealed download is consumed or dropped — including a detached
    /// blocking write whose async caller was cancelled — which is exactly
    /// the span the admitted work can run for.
    admission: Option<Box<dyn std::any::Any + Send>>,
}

impl std::fmt::Debug for SequentialWriter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SequentialWriter")
            .field("written", &self.written)
            .field("guard", &self.guard)
            .field("dst", &self.dst)
            .finish_non_exhaustive()
    }
}

impl SequentialWriter {
    /// Opens a plain (untransformed) writer over a fresh scratch sibling of
    /// `dst`.
    #[cfg(test)]
    pub(crate) fn open(dst: &Path) -> std::io::Result<Self> {
        let (file, scratch) = open_scratch(dst)?;
        Ok(Self::from_parts(
            Box::new(file),
            scratch,
            dst.to_path_buf(),
            None,
        ))
    }

    /// Assembles a writer from an already-opened scratch sink. `sink` must
    /// wrap the scratch [`File`] returned by [`open_scratch`] (possibly
    /// behind a transform).
    pub(crate) fn from_parts(
        sink: Box<dyn FinishingWrite>,
        scratch: ScratchGuard,
        dst: PathBuf,
        admission: Option<Box<dyn std::any::Any + Send>>,
    ) -> Self {
        Self {
            sink,
            written: 0,
            guard: scratch,
            dst,
            admission,
        }
    }

    /// Logical *input* bytes written so far — the wire byte count, before
    /// any transform expands them.
    pub(crate) fn written(&self) -> u64 {
        self.written
    }

    /// Appends one chunk.
    pub(crate) fn write_chunk(&mut self, chunk: &[u8]) -> std::io::Result<()> {
        self.sink.write_all(chunk)?;
        self.written += chunk.len() as u64;
        Ok(())
    }

    /// Verifies the input byte count, then consumes the writer and its
    /// sink (flushing any trailing transform output) into a
    /// [`SealedDownload`]. Runs on the blocking pool like the writes; the
    /// scratch file is complete but not yet installed when this returns.
    pub(crate) fn prepare_commit(self, expected: u64) -> std::io::Result<SealedDownload> {
        if self.written != expected {
            return Err(std::io::Error::other(format!(
                "single-writer download wrote {} bytes, expected {expected}",
                self.written
            )));
        }
        let transformed = self.sink.finish()?;
        Ok(SealedDownload {
            guard: self.guard,
            dst: self.dst,
            transformed,
            _admission: self.admission,
        })
    }
}

/// A finished, verified download whose sink has been consumed: the only
/// remaining operations are inspecting [`Self::transformed`] and the
/// atomic [`Self::commit`]. Dropping it uncommitted reclaims the scratch
/// file.
pub(crate) struct SealedDownload {
    guard: ScratchGuard,
    dst: PathBuf,
    transformed: bool,
    _admission: Option<Box<dyn std::any::Any + Send>>,
}

impl std::fmt::Debug for SealedDownload {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SealedDownload")
            .field("guard", &self.guard)
            .field("dst", &self.dst)
            .field("transformed", &self.transformed)
            .finish_non_exhaustive()
    }
}

impl SealedDownload {
    /// Whether the sink reported the stream as transformed at finish.
    pub(crate) fn transformed(&self) -> bool {
        self.transformed
    }

    /// Atomically renames the scratch file over the destination.
    ///
    /// Deliberately NOT run on the blocking pool: a dropped `spawn_blocking`
    /// join handle does not cancel its closure, so a timed-out attempt's
    /// detached closure could otherwise install a stale file after a retry
    /// installed a newer one. Called inline on the async task, the rename is
    /// only reachable while the download future is still alive — a cancelled
    /// attempt drops the sealed download instead, which reclaims the scratch
    /// file. A rename is one metadata syscall; the sub-millisecond stall is
    /// acceptable once per download.
    pub(crate) fn commit(mut self) -> std::io::Result<()> {
        std::fs::rename(&self.guard.scratch, &self.dst)?;
        self.guard.installed = true;
        Ok(())
    }
}

#[cfg(test)]
mod tests;
