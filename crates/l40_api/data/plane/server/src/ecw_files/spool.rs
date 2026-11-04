//! The disk spool that `POST /files` stages uploads in before writing them
//! to the guest.
//!
//! A request stages all of its parts, one after another, in a single file.
//! A named file per part would leave a directory entry cached for every
//! upload, each with a name never used again, and that cache is kernel memory
//! charged to the pod. `tempfile::tempfile_in` opens the file with
//! `O_TMPFILE` where the filesystem supports it, so it never has a name, is
//! freed when its last descriptor closes, and leaves nothing behind even if
//! the process is killed. Elsewhere, including Linux filesystems without
//! `O_TMPFILE`, it creates a randomly named file and unlinks it at once,
//! leaving one cached entry per request rather than one per part.

use std::{os::unix::fs::FileExt as _, sync::Arc};

use futures_util::{Stream, stream};
use tokio::io::AsyncWriteExt as _;

use super::{E2B_FILE_WRITE_CHUNK_BYTES, EcwProxyError, checked_file_content_len};

/// The spool one request stages its uploads in, freed once this guard, every
/// `StagedUpload` taken from it, their `chunks` streams, and any read or
/// write still running are gone: on success, error, and cancellation alike.
/// Only one part is staged at a time, since each `SpooledUpload` borrows this
/// guard until it finishes.
#[derive(Debug, Default)]
pub(super) struct SpoolFiles {
    file: Option<SpoolFile>,
    dir: Option<std::path::PathBuf>,
}

/// One request's spool file. Fields drop in order, so an idle writer's
/// descriptor closes first, and the last descriptor to close, the reader's or
/// one held by a write still running, closes off the runtime.
#[derive(Debug)]
struct SpoolFile {
    /// Appends each part after the last. Readers use positional reads, so
    /// this descriptor's offset moves only with what is written.
    writer: tokio::fs::File,
    reader: Arc<ClosingFile>,
    /// The end of the finished parts, where the next part starts.
    len: u64,
    /// A part was started and hasn't finished. Its bytes may or may not have
    /// reached the file, so the writer's offset is no longer known to equal
    /// `len`, and no later part may start.
    part_open: bool,
}

impl SpoolFiles {
    #[cfg(test)]
    pub(super) fn in_dir(dir: std::path::PathBuf) -> Self {
        Self {
            file: None,
            dir: Some(dir),
        }
    }

    /// The request's spool file, created on first use, so a request rejected
    /// before its first part creates none.
    async fn file(&mut self) -> Result<&mut SpoolFile, EcwProxyError> {
        if let Some(ref mut file) = self.file {
            return Ok(file);
        }
        let dir = self.dir.clone().unwrap_or_else(std::env::temp_dir);
        // Creating the file is a blocking syscall; keep it off the runtime.
        let (writer, reader) = tokio::task::spawn_blocking(move || {
            let file = tempfile::tempfile_in(dir)?;
            let writer = file.try_clone()?;
            Ok::<_, std::io::Error>((writer, file))
        })
        .await
        .map_err(|error| {
            tracing::warn!(%error, "ECW upload spool file creation task failed");
            EcwProxyError::internal("Failed to stage upload")
        })?
        .map_err(|error| {
            tracing::warn!(%error, "failed to create ECW upload spool file");
            EcwProxyError::internal("Failed to stage upload")
        })?;
        Ok(self.file.insert(SpoolFile {
            writer: tokio::fs::File::from_std(writer),
            reader: Arc::new(ClosingFile(Some(reader))),
            len: 0,
            part_open: false,
        }))
    }

    /// Starts staging a part at the end of the spool file. Fails if an
    /// earlier part was abandoned or failed before it finished, since where
    /// its bytes ended isn't known.
    pub(super) async fn create(&mut self) -> Result<SpooledUpload<'_>, EcwProxyError> {
        let spool = self.file().await?;
        if spool.part_open {
            tracing::warn!("ECW upload spool part started after an unfinished one");
            return Err(EcwProxyError::internal("Failed to stage upload"));
        }
        spool.part_open = true;
        Ok(SpooledUpload {
            spool,
            bytes_written: 0,
            write_failed: false,
        })
    }

    /// Swaps the spool's writer for `writer`, to make writes fail or stall.
    #[cfg(test)]
    pub(super) async fn replace_writer(&mut self, writer: std::fs::File) {
        let spool = self
            .file()
            .await
            .unwrap_or_else(|error| panic!("failed to create the spool file: {}", error.message));
        spool.writer = tokio::fs::File::from_std(writer);
    }

    #[cfg(test)]
    pub(super) fn has_file(&self) -> bool {
        self.file.is_some()
    }

    #[cfg(test)]
    pub(super) async fn tracked_file(&mut self) -> std::sync::Weak<ClosingFile> {
        let spool = self
            .file()
            .await
            .unwrap_or_else(|error| panic!("failed to create the spool file: {}", error.message));
        Arc::downgrade(&spool.reader)
    }
}

/// A spool file descriptor that closes on a blocking task. Closing the last
/// descriptor of an unnamed file frees its blocks, which can take as long as
/// an unlink, so it stays off the runtime threads. It closes inline only
/// while unwinding or without a runtime.
#[derive(Debug)]
pub(super) struct ClosingFile(Option<std::fs::File>);

impl Drop for ClosingFile {
    fn drop(&mut self) {
        let Some(file) = self.0.take() else {
            return;
        };
        // While unwinding, a failure to spawn the blocking task would panic
        // again and abort, so close inline instead.
        if std::thread::panicking() {
            drop(file);
            return;
        }
        match tokio::runtime::Handle::try_current() {
            // Detached: closing needs nothing from whoever dropped it.
            Ok(runtime) => drop(runtime.spawn_blocking(move || drop(file))),
            Err(_) => drop(file),
        }
    }
}

/// A finished part: where it lies in its request's spool file. It keeps the
/// file open, so it can be read for as long as it lives.
#[derive(Debug)]
pub(super) struct StagedUpload {
    file: Arc<ClosingFile>,
    offset: u64,
    len: usize,
}

impl StagedUpload {
    /// The part's length, at most `E2B_FILE_CONTENT_LIMIT_BYTES`.
    pub(super) fn len(&self) -> usize {
        self.len
    }

    /// Claims `extra` more bytes than were staged.
    #[cfg(test)]
    pub(super) fn overstate_len(&mut self, extra: usize) {
        self.len += extra;
    }

    /// The whole part, in chunks of at most `E2B_FILE_WRITE_CHUNK_BYTES`.
    /// Each chunk is a positional read on a blocking task, so every call
    /// streams the part from its start, and neither it nor an abandoned
    /// earlier attempt moves the spool file's shared offset. A file that ends
    /// inside the part fails the stream rather than ending it early.
    pub(super) fn chunks(&self) -> impl Stream<Item = std::io::Result<Vec<u8>>> + Send + 'static {
        let file = Arc::clone(&self.file);
        let end = self.offset.saturating_add(self.len as u64);
        stream::try_unfold(self.offset, move |position| {
            let file = Arc::clone(&file);
            async move {
                let len = usize::try_from(end.saturating_sub(position))
                    .unwrap_or(usize::MAX)
                    .min(E2B_FILE_WRITE_CHUNK_BYTES);
                if len == 0 {
                    return Ok(None);
                }
                let chunk = tokio::task::spawn_blocking(move || {
                    let Some(file) = file.0.as_ref() else {
                        return Err(std::io::Error::other("spool file already closed"));
                    };
                    let mut chunk = vec![0; len];
                    let read = file.read_at(&mut chunk, position)?;
                    chunk.truncate(read);
                    Ok(chunk)
                })
                .await
                .map_err(std::io::Error::other)??;
                if chunk.is_empty() {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        "spool file ended inside a staged upload",
                    ));
                }
                let next = position + chunk.len() as u64;
                Ok(Some((chunk, next)))
            }
        })
    }
}

/// A part being staged. It borrows its request's `SpoolFiles`, so no other
/// part can start while it lives; dropping it before `finish` succeeds leaves
/// the spool refusing new parts.
#[derive(Debug)]
pub(super) struct SpooledUpload<'a> {
    spool: &'a mut SpoolFile,
    bytes_written: usize,
    /// A write failed or was cancelled before it finished; once set, it stays
    /// set. tokio reports a failed background write once and then forgets it,
    /// and a cancelled write may have handed tokio only some of its bytes, so
    /// a later write or flush could succeed without the part's bytes matching
    /// its count.
    write_failed: bool,
}

impl SpooledUpload<'_> {
    pub(super) async fn write_chunk(&mut self, data: &[u8]) -> Result<(), EcwProxyError> {
        if self.write_failed {
            return Err(EcwProxyError::internal("Failed to stage upload"));
        }
        let bytes_written = checked_file_content_len(self.bytes_written, data.len())?;
        // Cleared only if the write succeeds, so a failed or cancelled write
        // leaves it set, and the check above keeps it set from then on.
        self.write_failed = true;
        if let Err(error) = self.spool.writer.write_all(data).await {
            tracing::warn!(%error, "failed to write ECW upload spool file");
            return Err(EcwProxyError::internal("Failed to stage upload"));
        }
        self.write_failed = false;
        // Counted once tokio accepts it. A failed background write surfaces
        // only on a later write or on `finish`'s flush, so the count is
        // trusted only after `finish` succeeds; `part_open` stops any part
        // from following one that didn't.
        self.bytes_written = bytes_written;
        Ok(())
    }

    /// Flushes the part to the spool file and returns where it lies. Only a
    /// flushed part counts: every byte has then landed, in order, right after
    /// the previous part.
    pub(super) async fn finish(self) -> Result<StagedUpload, EcwProxyError> {
        if self.write_failed {
            return Err(EcwProxyError::internal("Failed to stage upload"));
        }
        self.spool.writer.flush().await.map_err(|error| {
            tracing::warn!(%error, "failed to flush ECW upload spool file");
            EcwProxyError::internal("Failed to stage upload")
        })?;
        let offset = self.spool.len;
        // At most `E2B_FILE_CONTENT_LIMIT_BYTES`, checked on every write.
        self.spool.len += self.bytes_written as u64;
        self.spool.part_open = false;
        Ok(StagedUpload {
            file: Arc::clone(&self.spool.reader),
            offset,
            len: self.bytes_written,
        })
    }
}
