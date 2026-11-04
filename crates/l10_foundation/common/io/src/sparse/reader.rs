use std::fs::File;
use std::io::{self, Read, Seek};

use super::extent::{Boundary, Extent, next_extent_with, seek_boundary};

/// Reads an immutable regular file's logical bytes, synthesizing zeros for holes.
///
/// Unsupported hole seeking falls back to ordinary reads at the current logical
/// offset. Extent queries require the `sparse-read` feature (also enabled by
/// `reflink`); without it, reads use the ordinary path. Other I/O errors propagate.
/// The reader starts at the file cursor but subsequent reads are independent of
/// it. It uses only the caller's buffer, without collecting an extent map.
/// Callers must keep the file unchanged for the duration of the read, just as
/// when hashing it with ordinary reads. Length changes are detected, but this
/// does not provide a snapshot against concurrent writes of the same length.
#[derive(Debug)]
pub struct SparseFileReader {
    file: File,
    length: u64,
    position: u64,
    extent: Option<Extent>,
}

impl SparseFileReader {
    pub fn new(mut file: File) -> io::Result<Self> {
        let metadata = file.metadata()?;
        if !metadata.is_file() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "sparse reader requires a regular file",
            ));
        }
        let position = file.stream_position()?;
        Ok(Self {
            file,
            length: metadata.len(),
            position,
            extent: None,
        })
    }

    /// Borrows the file for metadata and cache advice. Cursor changes through
    /// this handle do not affect the reader's logical position.
    pub fn file(&self) -> &File {
        &self.file
    }

    fn check_length(&self) -> io::Result<()> {
        if self.file.metadata()?.len() != self.length {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "file length changed during sparse read",
            ));
        }
        Ok(())
    }

    fn read_with(
        &mut self,
        buffer: &mut [u8],
        mut seek: impl FnMut(&File, u64, Boundary) -> io::Result<u64>,
    ) -> io::Result<usize> {
        if buffer.is_empty() {
            return Ok(0);
        }
        if self.position >= self.length {
            self.check_length()?;
            return Ok(0);
        }
        if self.extent.is_none() {
            self.check_length()?;
            self.extent = Some(next_extent_with(
                &self.file,
                self.position,
                self.length,
                &mut seek,
            )?);
        }
        let end = match self.extent {
            Some(Extent::Data(end) | Extent::Hole(end)) => end,
            Some(Extent::Unsupported) | None => self.length,
        };
        let count = usize::try_from((end - self.position).min(buffer.len() as u64))
            .map_err(|_| io::Error::other("sparse read exceeds buffer size"))?;
        let buffer = buffer
            .get_mut(..count)
            .ok_or_else(|| io::Error::other("sparse read exceeds buffer size"))?;
        let read = if matches!(self.extent, Some(Extent::Hole(_))) {
            buffer.fill(0);
            count
        } else {
            let read = read_at(&self.file, buffer, self.position)?;
            if read == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "file truncated during sparse read",
                ));
            }
            read
        };
        self.position += read as u64;
        if self.position == end && !matches!(self.extent, Some(Extent::Unsupported)) {
            self.extent = None;
        }
        Ok(read)
    }
}

impl Read for SparseFileReader {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        self.read_with(buffer, seek_boundary)
    }
}

fn read_at(file: &File, buffer: &mut [u8], offset: u64) -> io::Result<usize> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::FileExt;
        file.read_at(buffer, offset)
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::FileExt;
        file.seek_read(buffer, offset)
    }
    #[cfg(not(any(unix, windows)))]
    {
        let mut file = file;
        file.seek(io::SeekFrom::Start(offset))?;
        file.read(buffer)
    }
}

#[cfg(test)]
mod tests;
