//! Framing check for a send stream before node-manager forwards it.
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
    #[error("send command at offset {offset} declares {len} payload bytes")]
    CommandTooLarge { offset: u64, len: u32 },
    #[error(
        "send command {cmd} at offset {offset} fails its CRC; the send was interrupted and restarted"
    )]
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
                    if !(1..=2).contains(&version) {
                        return Err(FramingError::Version(version));
                    }
                    self.start_command();
                }
                State::CommandHeader(header, filled) => {
                    if !fill(header, filled, &mut data, &mut self.offset) {
                        continue;
                    }
                    let [l0, l1, l2, l3, c0, c1, s0, s1, s2, s3] = *header;
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
                        cmd: u16::from_le_bytes([c0, c1]),
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

#[cfg(test)]
mod tests;
