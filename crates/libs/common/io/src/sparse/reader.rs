use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom};

#[derive(Debug)]
enum Extent {
    Unknown,
    Data(u64),
    Hole(u64),
    Plain,
}

#[derive(Clone, Copy)]
enum Boundary {
    Data,
    Hole,
}

/// Reads an immutable regular file's logical bytes, synthesizing zeros for holes.
///
/// Unsupported hole seeking falls back to ordinary reads at the current logical
/// offset. Other I/O errors propagate. The reader owns the file cursor and uses
/// only the caller's buffer; it does not collect an extent map or buffer the file.
/// Callers must keep the file unchanged for the duration of the read, just as
/// when hashing it with ordinary reads. Length changes are detected, but this
/// does not provide a snapshot against concurrent writes of the same length.
#[derive(Debug)]
pub struct SparseFileReader {
    file: File,
    length: u64,
    position: u64,
    extent: Extent,
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
            extent: Extent::Unknown,
        })
    }

    /// Borrows the file for metadata and cache advice, without moving its cursor.
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

    fn next_extent(
        &self,
        seek: &mut impl FnMut(&File, u64, Boundary) -> io::Result<u64>,
    ) -> io::Result<Extent> {
        let data = match seek(&self.file, self.position, Boundary::Data) {
            Ok(offset) => offset,
            // ENXIO means there is no more data, not an unsupported operation.
            Err(error) if error.raw_os_error() == Some(libc::ENXIO) => self.length,
            Err(error) => return Err(error),
        };
        if data < self.position || data > self.length {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "data extent outside the remaining file",
            ));
        }
        if data > self.position {
            return Ok(Extent::Hole(data));
        }
        let hole = seek(&self.file, self.position, Boundary::Hole)?;
        if hole <= self.position || hole > self.length {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "hole extent does not advance within the file",
            ));
        }
        Ok(Extent::Data(hole))
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
        if matches!(self.extent, Extent::Unknown) {
            self.check_length()?;
            let extent = match self.next_extent(&mut seek) {
                Ok(extent) => extent,
                Err(error) if unsupported(&error) => Extent::Plain,
                Err(error) => return Err(error),
            };
            // Probing either boundary can move the descriptor, including before
            // an unsupported SEEK_HOLE. Resume at our logical offset, never the
            // last probed boundary.
            self.file.seek(SeekFrom::Start(self.position))?;
            self.extent = extent;
        }
        let end = match self.extent {
            Extent::Data(end) | Extent::Hole(end) => end,
            Extent::Plain | Extent::Unknown => self.length,
        };
        let count = usize::try_from((end - self.position).min(buffer.len() as u64))
            .map_err(|_| io::Error::other("sparse read exceeds buffer size"))?;
        let buffer = buffer
            .get_mut(..count)
            .ok_or_else(|| io::Error::other("sparse read exceeds buffer size"))?;
        let read = if matches!(self.extent, Extent::Hole(_)) {
            buffer.fill(0);
            count
        } else {
            let read = self.file.read(buffer)?;
            if read == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "file truncated during sparse read",
                ));
            }
            read
        };
        self.position += read as u64;
        if self.position == end && !matches!(self.extent, Extent::Plain) {
            self.extent = Extent::Unknown;
        }
        Ok(read)
    }
}

impl Read for SparseFileReader {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        self.read_with(buffer, seek_boundary)
    }
}

fn unsupported(error: &io::Error) -> bool {
    error.kind() == io::ErrorKind::Unsupported
        || matches!(
            error.raw_os_error(),
            Some(libc::EINVAL | libc::EOPNOTSUPP | libc::ENOSYS | libc::ENOTTY)
        )
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn seek_boundary(file: &File, offset: u64, boundary: Boundary) -> io::Result<u64> {
    use std::os::fd::AsRawFd;

    let offset = libc::off_t::try_from(offset)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "file offset exceeds off_t"))?;
    let whence = match boundary {
        Boundary::Data => libc::SEEK_DATA,
        Boundary::Hole => libc::SEEK_HOLE,
    };
    // SAFETY: the reader owns a valid descriptor and passes a checked offset.
    let result = unsafe { libc::lseek(file.as_raw_fd(), offset, whence) };
    if result < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(result.cast_unsigned())
    }
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn seek_boundary(_file: &File, _offset: u64, _boundary: Boundary) -> io::Result<u64> {
    Err(io::ErrorKind::Unsupported.into())
}

#[cfg(test)]
mod tests;
