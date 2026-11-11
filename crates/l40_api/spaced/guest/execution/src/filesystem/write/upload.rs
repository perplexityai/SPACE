//! Writing an upload's bytes in large disk requests.
//!
//! The guest disk takes one physically contiguous segment per request, so a
//! write reaches it in as many requests as its data spans separate physical
//! runs. In a guest that has been running a while, page-cache pages and newly
//! faulted buffers are scattered, and only huge pages guarantee contiguity.
//! A non-append upload past [`SMALL_UPLOAD_BYTES`] is therefore collected in
//! a huge-page buffer and written from it with O_DIRECT where the file and
//! its filesystem allow it; a smaller one borrows a buffer only for its final
//! write. Without a huge page, or without a free buffer, the writes still
//! work, in smaller requests.

use std::io;
use std::sync::Arc;

pub(super) const UPLOAD_WRITE_BYTES: usize = 2 * 1024 * 1024;
/// O_DIRECT offsets and lengths must be whole filesystem blocks.
pub(super) const DIRECT_IO_ALIGNMENT: usize = 4096;
/// An upload at most this large is collected in ordinary memory and written
/// in one write when it finishes, copied into a free huge-page buffer and
/// written direct where it can be: too common to hold a 2 MiB buffer while
/// its frames arrive, but common enough that its disk requests add up.
pub(super) const SMALL_UPLOAD_BYTES: usize = 256 * 1024;
/// Upload buffers one filesystem service holds at most, in use or idle. An
/// upload that finds none free writes its frames as they arrive, through the
/// page cache, where they reach the disk as single-page requests against its
/// operations limit; waiting for a buffer instead would stop reading frames
/// on the connection all uploads share. Eight (16 MiB at most) keep
/// concurrent megabyte uploads at the default drive bandwidth limit.
pub(super) const UPLOAD_BUFFERS_PER_SERVICE: usize = 8;

/// The upload buffers of one filesystem service. Idle buffers are kept, so
/// a later upload need not find a free huge page in a guest whose memory has
/// fragmented since, but their pages are released lazily: the kernel
/// reclaims them first under memory pressure.
pub(in crate::filesystem) struct UploadBufferPool {
    idle: std::sync::Mutex<Vec<UploadBuffer>>,
    /// One permit per buffer that may exist; each buffer holds its own.
    slots: Arc<tokio::sync::Semaphore>,
}

impl UploadBufferPool {
    pub(in crate::filesystem) fn new() -> Self {
        Self {
            idle: std::sync::Mutex::new(Vec::new()),
            slots: Arc::new(tokio::sync::Semaphore::new(UPLOAD_BUFFERS_PER_SERVICE)),
        }
    }

    /// An idle buffer, else a new one if the service has fewer than
    /// [`UPLOAD_BUFFERS_PER_SERVICE`], else `None`. Call from a blocking
    /// thread: a new buffer is faulted in here.
    pub(super) fn take(&self) -> Option<UploadBuffer> {
        if let Some(buffer) = self.lock().pop() {
            return Some(buffer);
        }
        let slot = Arc::clone(&self.slots).try_acquire_owned().ok()?;
        Some(UploadBuffer::allocate(slot))
    }

    /// Keep a finished upload's buffer for the next one.
    pub(super) fn give(&self, mut buffer: UploadBuffer) {
        buffer.filled = 0;
        release_lazily(buffer.window_mut());
        self.lock().push(buffer);
    }

    #[cfg(test)]
    pub(super) fn idle(&self) -> usize {
        self.lock().len()
    }

    #[cfg(test)]
    pub(super) fn held(&self) -> usize {
        UPLOAD_BUFFERS_PER_SERVICE - self.slots.available_permits()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Vec<UploadBuffer>> {
        self.idle
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

/// An upload buffer aligned to its size, so one huge page can back it.
///
/// Invariants: `start + UPLOAD_WRITE_BYTES <= storage.len()`, and
/// `filled <= UPLOAD_WRITE_BYTES`.
pub(super) struct UploadBuffer {
    /// Twice the buffer's size, so that a window aligned to it fits inside.
    storage: Box<[u8]>,
    start: usize,
    pub(super) filled: usize,
    /// The pool's permit for this buffer, returned when it is dropped.
    _slot: tokio::sync::OwnedSemaphorePermit,
}

impl UploadBuffer {
    /// Allocate on the caller's (blocking) thread: the buffer is faulted in
    /// here, and a huge-page fault may compact memory first.
    fn allocate(slot: tokio::sync::OwnedSemaphorePermit) -> Self {
        let storage = vec![0_u8; 2 * UPLOAD_WRITE_BYTES].into_boxed_slice();
        let start = storage.as_ptr().align_offset(UPLOAD_WRITE_BYTES);
        assert!(
            start < UPLOAD_WRITE_BYTES,
            "a byte buffer reaches any alignment within one alignment's length"
        );
        let mut buffer = Self {
            storage,
            start,
            filled: 0,
            _slot: slot,
        };
        let window = buffer.window_mut();
        advise_huge_pages(window);
        window.fill(0);
        buffer
    }

    #[cfg(test)]
    pub(super) fn address(&self) -> usize {
        self.window().as_ptr() as usize
    }

    fn window(&self) -> &[u8] {
        self.storage
            .get(self.start..self.start + UPLOAD_WRITE_BYTES)
            .expect("the window lies inside its storage")
    }

    fn window_mut(&mut self) -> &mut [u8] {
        self.storage
            .get_mut(self.start..self.start + UPLOAD_WRITE_BYTES)
            .expect("the window lies inside its storage")
    }

    pub(super) fn filled(&self) -> &[u8] {
        self.window()
            .get(..self.filled)
            .expect("the filled length never exceeds the window")
    }

    /// Copy as much of `data` as fits and return how much that was.
    pub(super) fn fill(&mut self, data: &[u8]) -> usize {
        let filled = self.filled;
        let spare = self
            .window_mut()
            .get_mut(filled..)
            .expect("the filled length never exceeds the window");
        let count = data.len().min(spare.len());
        let (target, source) = (spare.split_at_mut(count).0, data.split_at(count).0);
        target.copy_from_slice(source);
        self.filled += count;
        count
    }

    fn is_full(&self) -> bool {
        self.filled == UPLOAD_WRITE_BYTES
    }
}

#[cfg(target_os = "linux")]
fn advise_huge_pages(window: &mut [u8]) {
    // SAFETY: advice on memory this buffer owns, at a huge-page-aligned
    // address. MADV_HUGEPAGE changes how the kernel backs the range, never its
    // contents, and on failure the range keeps ordinary pages, which still work.
    let _advised = unsafe {
        libc::madvise(
            window.as_mut_ptr().cast(),
            window.len(),
            libc::MADV_HUGEPAGE,
        )
    };
}

#[cfg(not(target_os = "linux"))]
fn advise_huge_pages(_window: &mut [u8]) {}

/// Let the kernel reclaim an idle buffer's pages under memory pressure. Until
/// it does they stay in place, and writing to them cancels the release; a
/// reclaimed page is faulted back in, zeroed, when the buffer is next filled.
#[cfg(target_os = "linux")]
fn release_lazily(window: &mut [u8]) {
    // SAFETY: advice on memory this buffer owns, at a page-aligned address.
    // MADV_FREE only lets the kernel discard the contents, which a buffer
    // being returned no longer needs; on failure the pages simply stay.
    let _released =
        unsafe { libc::madvise(window.as_mut_ptr().cast(), window.len(), libc::MADV_FREE) };
}

#[cfg(not(target_os = "linux"))]
fn release_lazily(_window: &mut [u8]) {}

#[cfg(target_os = "linux")]
pub(super) fn set_direct_io(file: &std::fs::File, enabled: bool) -> io::Result<()> {
    use nix::fcntl::{FcntlArg, OFlag, fcntl};
    let flags = OFlag::from_bits_retain(fcntl(file, FcntlArg::F_GETFL)?);
    let flags = if enabled {
        flags | OFlag::O_DIRECT
    } else {
        flags - OFlag::O_DIRECT
    };
    fcntl(file, FcntlArg::F_SETFL(flags))?;
    Ok(())
}

#[cfg(not(target_os = "linux"))]
pub(super) fn set_direct_io(_file: &std::fs::File, enabled: bool) -> io::Result<()> {
    if enabled {
        Err(io::Error::from(io::ErrorKind::Unsupported))
    } else {
        Ok(())
    }
}

pub(super) struct UploadWriter {
    file: std::fs::File,
    mode: UploadMode,
    buffers: Arc<UploadBufferPool>,
}

enum UploadMode {
    /// Each frame is written as it arrives: an append, whose data is visible
    /// as it streams and whose offset need not be block-aligned, or an
    /// upload that outgrew [`SMALL_UPLOAD_BYTES`] and found no buffer free.
    Streaming,
    /// An upload not yet past [`SMALL_UPLOAD_BYTES`], collected in memory.
    Small(Vec<u8>),
    /// Frames are collected in a huge-page buffer, lent to the blocking pool
    /// while a write is in flight.
    Collecting {
        buffer: Option<UploadBuffer>,
        direct: bool,
    },
}

impl UploadWriter {
    pub(super) async fn new(
        file: tokio::fs::File,
        append: bool,
        buffers: Arc<UploadBufferPool>,
    ) -> io::Result<Self> {
        Ok(Self {
            file: file.into_std().await,
            mode: if append {
                UploadMode::Streaming
            } else {
                UploadMode::Small(Vec::new())
            },
            buffers,
        })
    }

    pub(super) async fn write(&mut self, data: &[u8]) -> io::Result<()> {
        let UploadMode::Small(collected) = &mut self.mode else {
            return self.write_through(data).await;
        };
        if collected.len() + data.len() <= SMALL_UPLOAD_BYTES {
            collected.extend_from_slice(data);
            return Ok(());
        }
        let collected = std::mem::take(collected);
        self.outgrow_small().await;
        self.write_through(&collected).await?;
        self.write_through(data).await
    }

    /// Move an upload that outgrew [`SMALL_UPLOAD_BYTES`] to a huge-page
    /// buffer, or to streaming if every buffer is in use. A non-append
    /// upload starts at offset 0 of a new temporary file or a truncated one,
    /// and full buffers keep it block-aligned, so only its final partial
    /// block is written without O_DIRECT. Only regular files are written
    /// direct: on a FIFO, O_DIRECT selects packet mode instead.
    async fn outgrow_small(&mut self) {
        let buffers = Arc::clone(&self.buffers);
        let file = self.file.try_clone();
        let collecting = blocking(move || {
            let Some(buffer) = buffers.take() else {
                return Ok(None);
            };
            let direct = file.is_ok_and(|file| {
                file.metadata()
                    .is_ok_and(|metadata| metadata.file_type().is_file())
                    && set_direct_io(&file, true).is_ok()
            });
            Ok(Some((buffer, direct)))
        })
        .await;
        self.mode = match collecting {
            Ok(Some((buffer, direct))) => UploadMode::Collecting {
                buffer: Some(buffer),
                direct,
            },
            Ok(None) | Err(_) => UploadMode::Streaming,
        };
    }

    async fn write_through(&mut self, mut data: &[u8]) -> io::Result<()> {
        if data.is_empty() {
            return Ok(());
        }
        let UploadMode::Collecting { .. } = &self.mode else {
            let file = self.file.try_clone()?;
            let data = data.to_vec();
            return blocking(move || std::io::Write::write_all(&mut &file, &data)).await;
        };
        while !data.is_empty() {
            let UploadMode::Collecting {
                buffer: Some(buffer),
                ..
            } = &mut self.mode
            else {
                return Err(io::Error::other("upload buffer lost by a failed write"));
            };
            let copied = buffer.fill(data);
            data = data.get(copied..).unwrap_or_default();
            if buffer.is_full() {
                self.write_buffer().await?;
            }
        }
        Ok(())
    }

    /// Write what is collected and return the file.
    pub(super) async fn finish(mut self) -> io::Result<std::fs::File> {
        if let UploadMode::Small(collected) = &mut self.mode
            && !collected.is_empty()
        {
            let collected = std::mem::take(collected);
            let file = self.file.try_clone()?;
            let buffers = Arc::clone(&self.buffers);
            blocking(move || write_small_upload(&file, &collected, &buffers)).await?;
        }
        self.write_buffer().await?;
        Ok(self.discard())
    }

    /// Return the file without writing what is collected.
    pub(super) fn discard(self) -> std::fs::File {
        if let UploadMode::Collecting {
            buffer: Some(buffer),
            ..
        } = self.mode
        {
            self.buffers.give(buffer);
        }
        self.file
    }

    async fn write_buffer(&mut self) -> io::Result<()> {
        let UploadMode::Collecting { buffer, direct } = &mut self.mode else {
            return Ok(());
        };
        let Some(mut taken) = buffer.take_if(|buffer| buffer.filled > 0) else {
            return Ok(());
        };
        let file = self.file.try_clone()?;
        let was_direct = *direct;
        let (returned, still_direct) = blocking(move || {
            let still_direct = write_upload_buffer(&file, taken.filled(), was_direct)?;
            taken.filled = 0;
            Ok((taken, still_direct))
        })
        .await?;
        *buffer = Some(returned);
        *direct = still_direct;
        Ok(())
    }
}

pub(super) async fn blocking<T: Send + 'static>(
    work: impl FnOnce() -> io::Result<T> + Send + 'static,
) -> io::Result<T> {
    tokio::task::spawn_blocking(work)
        .await
        .map_err(|error| io::Error::other(format!("upload write task failed: {error}")))?
}

/// Write a small upload in one call. From ordinary memory, its pages reach
/// the disk as up to one request each; copied into a free huge-page buffer
/// and written direct, its whole blocks take one request.
fn write_small_upload(
    file: &std::fs::File,
    data: &[u8],
    buffers: &UploadBufferPool,
) -> io::Result<()> {
    let direct = data.len() >= DIRECT_IO_ALIGNMENT
        && file
            .metadata()
            .is_ok_and(|metadata| metadata.file_type().is_file())
        && set_direct_io(file, true).is_ok();
    let Some(mut buffer) = direct.then(|| buffers.take()).flatten() else {
        if direct {
            set_direct_io(file, false)?;
        }
        return std::io::Write::write_all(&mut &*file, data);
    };
    buffer.fill(data);
    let written = write_upload_buffer(file, buffer.filled(), true).map(drop);
    buffers.give(buffer);
    written
}

/// Write `data` at the file's offset; returns whether O_DIRECT stays on. The
/// block-aligned part goes direct; a final partial block, and whatever the
/// filesystem refuses to take direct, is written buffered from where the
/// direct writes stopped.
fn write_upload_buffer(mut file: &std::fs::File, data: &[u8], direct: bool) -> io::Result<bool> {
    use std::io::Write;
    let mut written = 0;
    let mut direct = direct;
    if direct {
        let aligned = data.len() - data.len() % DIRECT_IO_ALIGNMENT;
        while written < aligned {
            match file.write(data.get(written..aligned).unwrap_or_default()) {
                Ok(0) => return Err(io::Error::from(io::ErrorKind::WriteZero)),
                Ok(count) => written += count,
                Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
                Err(error) if error.raw_os_error() == Some(libc::EINVAL) => break,
                Err(error) => return Err(error),
            }
        }
        if written < data.len() {
            set_direct_io(file, false)?;
            direct = false;
        }
    }
    file.write_all(data.get(written..).unwrap_or_default())?;
    Ok(direct)
}

#[cfg(test)]
mod tests;
