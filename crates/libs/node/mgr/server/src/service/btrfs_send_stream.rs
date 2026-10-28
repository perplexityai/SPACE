//! Protocol v1 framing shared by snapshot upload and restore.
//!
//! `BTRFS_IOC_SEND` writes into a pipe that volume-manager drains. When the
//! pipe is full, a signal, stop or cgroup freeze on the sending thread makes
//! `pipe_write` return `-ERESTARTSYS`; the ioctl passes it up and the kernel
//! restarts the whole send. The restarted send appends a new stream after the
//! partial output and still reports success, so the upload would store a
//! stream that `btrfs receive` rejects with "crc32 mismatch in command".
//!
//! Each command carries the kernel's CRC-32C (seed 0, no final xor) over its
//! header, with the CRC field zeroed, and its payload. Verifying it here, and
//! requiring exactly one stream that ends with `END`, turns a restarted send
//! into a failed transfer that volume-manager retries from the start.
//!
//! SPACE sends with flags=0 and version=0 (v1). Restore must check command
//! numbers as well as the version: btrfs receive dispatches ENCODED_WRITE even
//! in a v1-labelled stream. This gate excludes userspace decompression; it
//! does not authenticate artifacts or validate command paths and clone sources.

const MAGIC: &[u8; 13] = b"btrfs-stream\0";
/// `BTRFS_SEND_C_END`.
const END_COMMAND: u16 = 21;
/// Far above the kernel's per-command buffer for either protocol version.
const MAX_COMMAND_LEN: u32 = 1 << 20;

const CRC32C_RAW: crc::Algorithm<u32> = crc::Algorithm {
    width: 32,
    poly: 0x1edc_6f41,
    init: 0,
    refin: true,
    refout: true,
    xorout: 0,
    check: 0x58e3_fa20,
    residue: 0,
};
static CRC32C: crc::Crc<u32, crc::Table<16>> = crc::Crc::<u32, crc::Table<16>>::new(&CRC32C_RAW);

type Digest = crc::Digest<'static, u32, crc::Table<16>>;

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub(super) enum FramingError {
    #[error("send stream does not start with the btrfs-stream header")]
    BadMagic,
    #[error("send stream version {0} is not supported")]
    Version(u32),
    #[error("send command {cmd} at offset {offset} is not a protocol v1 command")]
    Command { offset: u64, cmd: u16 },
    #[error("send command at offset {offset} declares {len} payload bytes")]
    CommandTooLarge { offset: u64, len: u32 },
    #[error("send command {cmd} at offset {offset} fails its CRC")]
    Crc { offset: u64, cmd: u16 },
    #[error("send stream continues past its END command at offset {offset}")]
    DataAfterEnd { offset: u64 },
    #[error("send stream ended before its END command, inside the item at offset {offset}")]
    Truncated { offset: u64 },
}

enum State {
    StreamHeader([u8; 17], usize),
    CommandHeader([u8; 10], usize),
    Payload {
        cmd: u16,
        stored: u32,
        remaining: u32,
        digest: Digest,
    },
    Ended,
}

/// Incremental checker over the bytes of one send stream, in order.
pub(super) struct SendStreamFraming {
    state: State,
    /// Stream offset where the item being read began.
    item_offset: u64,
    offset: u64,
}

/// Move bytes from `data` into `header` until it is full. Returns whether it is.
fn fill(header: &mut [u8], filled: &mut usize, data: &mut &[u8], offset: &mut u64) -> bool {
    while let Some(slot) = header.get_mut(*filled) {
        let Some((&byte, rest)) = data.split_first() else {
            return false;
        };
        *slot = byte;
        *filled += 1;
        *data = rest;
        *offset += 1;
    }
    true
}

impl SendStreamFraming {
    pub(super) fn new() -> Self {
        Self {
            state: State::StreamHeader([0; 17], 0),
            item_offset: 0,
            offset: 0,
        }
    }

    pub(super) fn feed(&mut self, mut data: &[u8]) -> Result<(), FramingError> {
        while !data.is_empty() {
            match &mut self.state {
                State::StreamHeader(header, filled) => {
                    if !fill(header, filled, &mut data, &mut self.offset) {
                        continue;
                    }
                    if !header.starts_with(MAGIC) {
                        return Err(FramingError::BadMagic);
                    }
                    let [.., v0, v1, v2, v3] = *header;
                    let version = u32::from_le_bytes([v0, v1, v2, v3]);
                    if version != 1 {
                        return Err(FramingError::Version(version));
                    }
                    self.start_command();
                }
                State::CommandHeader(header, filled) => {
                    if !fill(header, filled, &mut data, &mut self.offset) {
                        continue;
                    }
                    let [l0, l1, l2, l3, c0, c1, s0, s1, s2, s3] = *header;
                    let cmd = u16::from_le_bytes([c0, c1]);
                    if !(1..=22).contains(&cmd) {
                        return Err(FramingError::Command {
                            offset: self.item_offset,
                            cmd,
                        });
                    }
                    let len = u32::from_le_bytes([l0, l1, l2, l3]);
                    if len > MAX_COMMAND_LEN {
                        return Err(FramingError::CommandTooLarge {
                            offset: self.item_offset,
                            len,
                        });
                    }
                    let mut digest = CRC32C.digest();
                    digest.update(&[l0, l1, l2, l3, c0, c1, 0, 0, 0, 0]);
                    self.state = State::Payload {
                        cmd,
                        stored: u32::from_le_bytes([s0, s1, s2, s3]),
                        remaining: len,
                        digest,
                    };
                    if len == 0 {
                        self.finish_command()?;
                    }
                }
                State::Payload {
                    remaining, digest, ..
                } => {
                    let take = data
                        .len()
                        .min(usize::try_from(*remaining).unwrap_or(usize::MAX));
                    let (now, rest) = data.split_at_checked(take).unwrap_or((data, &[]));
                    digest.update(now);
                    *remaining =
                        remaining.saturating_sub(u32::try_from(now.len()).unwrap_or(u32::MAX));
                    self.offset += u64::try_from(now.len()).unwrap_or(u64::MAX);
                    data = rest;
                    if *remaining == 0 {
                        self.finish_command()?;
                    }
                }
                State::Ended => {
                    return Err(FramingError::DataAfterEnd {
                        offset: self.offset,
                    });
                }
            }
        }
        Ok(())
    }

    /// The stream ended: accept it only after its END command.
    pub(super) fn finish(&self) -> Result<(), FramingError> {
        match self.state {
            State::Ended => Ok(()),
            _ => Err(FramingError::Truncated {
                offset: self.item_offset,
            }),
        }
    }

    /// Whether the stream's first command passed its CRC.
    fn passed_first_command(&self) -> bool {
        self.item_offset > STREAM_HEADER as u64
    }

    fn start_command(&mut self) {
        self.item_offset = self.offset;
        self.state = State::CommandHeader([0; 10], 0);
    }

    fn finish_command(&mut self) -> Result<(), FramingError> {
        let State::Payload {
            cmd,
            stored,
            digest,
            ..
        } = std::mem::replace(&mut self.state, State::Ended)
        else {
            return Ok(());
        };
        if digest.finalize() != stored {
            return Err(FramingError::Crc {
                offset: self.item_offset,
                cmd,
            });
        }
        if cmd == END_COMMAND {
            self.item_offset = self.offset;
        } else {
            self.start_command();
        }
        Ok(())
    }
}

/// Most partial streams [`last_complete_stream`] passes over: a restarted send
/// leaves one per restart, and restarts are rare. A partial stream is a
/// header whose first command passes its CRC, so a send stream stored in the
/// guest's own files can count as one too.
const MAX_PARTIAL_STREAMS: usize = 64;
/// Most header-shaped runs of bytes whose first command fails that
/// [`last_complete_stream`] passes over. Each costs one read of at most
/// [`SALVAGE_CHUNK`]; the cap bounds the search when guest file data holds
/// many.
const MAX_HEADER_LOOKALIKES: usize = 4096;
const SALVAGE_CHUNK: usize = 1 << 20;
const STREAM_HEADER: usize = 17;

/// Where the complete stream of a restarted send begins.
///
/// Before the framing check above existed, a send the kernel restarted mid-pipe
/// was stored as its partial output followed by a complete stream of the same
/// read-only snapshot. Returns the offset of the earliest stream header from
/// which every command passes its CRC and the stream ends with END exactly at
/// the end of `file`. `Some(0)` means `file` is already one valid stream;
/// `None` means no header starts a valid stream, so nothing can be salvaged.
/// The earliest valid header is the complete stream's own: a header-shaped run
/// of bytes inside its payload cannot validate past the next real command.
///
/// Each header is validated as the scan reaches it, so the search reads the
/// file once plus each candidate's stream. It gives up after
/// [`MAX_PARTIAL_STREAMS`] partial streams or [`MAX_HEADER_LOOKALIKES`]
/// header-shaped runs, and returns `None` once `stopped` returns true; it is
/// checked before every read.
pub(in crate::service) fn last_complete_stream(
    file: &std::fs::File,
    stopped: &dyn Fn() -> bool,
) -> std::io::Result<Option<u64>> {
    use std::os::unix::fs::FileExt;
    let chunk = chunk_for(file)?.max(1);
    let (mut buf, mut validate_buf) = (vec![0; chunk], vec![0; chunk]);
    let (mut partial_streams, mut lookalikes) = (0, 0);
    // Unscanned tail of the previous read, then the new read; `base` is the
    // file offset of `window[0]`.
    let mut window = Vec::with_capacity(chunk + STREAM_HEADER);
    let (mut base, mut next) = (0u64, 0u64);
    loop {
        if stopped() {
            return Ok(None);
        }
        let read = file.read_at(&mut buf, next)?;
        next += read as u64;
        window.extend_from_slice(buf.get(..read).unwrap_or_default());
        let mut at = 0;
        while let Some(skip) = window
            .get(at..)
            .and_then(|rest| rest.iter().position(|&byte| byte == MAGIC[0]))
        {
            at += skip;
            let Some(header) = window.get(at..at + STREAM_HEADER) else {
                break;
            };
            if is_stream_header(header) {
                let offset = base + at as u64;
                match stream_from(file, offset, &mut validate_buf, stopped)? {
                    Candidate::Complete => return Ok(Some(offset)),
                    Candidate::Partial => {
                        partial_streams += 1;
                        if partial_streams == MAX_PARTIAL_STREAMS {
                            return Ok(None);
                        }
                    }
                    Candidate::NotAStream => {
                        lookalikes += 1;
                        if lookalikes == MAX_HEADER_LOOKALIKES {
                            return Ok(None);
                        }
                    }
                }
            }
            at += 1;
        }
        if read == 0 {
            return Ok(None);
        }
        // Keep the bytes that may begin a header the next read completes.
        let keep_from = window.len().saturating_sub(STREAM_HEADER - 1);
        window.drain(..keep_from);
        base += keep_from as u64;
    }
}

/// What a stream header found by [`last_complete_stream`] starts.
enum Candidate {
    /// A valid stream that ends with END exactly at the end of the file.
    Complete,
    /// A stream whose first command passes its CRC but that does not run to
    /// the end of the file, such as a restart's partial output.
    Partial,
    /// Header-shaped bytes whose first command fails, or a stop.
    NotAStream,
}

/// Read size for a scan of `file`: whole small files, else [`SALVAGE_CHUNK`].
fn chunk_for(file: &std::fs::File) -> std::io::Result<usize> {
    let len = file.metadata()?.len();
    Ok(usize::try_from(len).map_or(SALVAGE_CHUNK, |len| len.min(SALVAGE_CHUNK)))
}

fn is_stream_header(header: &[u8]) -> bool {
    let version = header
        .get(MAGIC.len()..)
        .and_then(|bytes| <[u8; 4]>::try_from(bytes).ok())
        .map(u32::from_le_bytes);
    header.starts_with(MAGIC) && version == Some(1)
}

fn stream_from(
    file: &std::fs::File,
    offset: u64,
    buf: &mut [u8],
    stopped: &dyn Fn() -> bool,
) -> std::io::Result<Candidate> {
    use std::os::unix::fs::FileExt;
    let mut framing = SendStreamFraming::new();
    let mut at = offset;
    let failed = |framing: &SendStreamFraming| {
        if framing.passed_first_command() {
            Candidate::Partial
        } else {
            Candidate::NotAStream
        }
    };
    loop {
        if stopped() {
            return Ok(Candidate::NotAStream);
        }
        let read = file.read_at(buf, at)?;
        if read == 0 {
            return Ok(match framing.finish() {
                Ok(()) => Candidate::Complete,
                Err(_) => failed(&framing),
            });
        }
        if framing.feed(buf.get(..read).unwrap_or_default()).is_err() {
            return Ok(failed(&framing));
        }
        at += read as u64;
    }
}

#[cfg(test)]
mod tests;
