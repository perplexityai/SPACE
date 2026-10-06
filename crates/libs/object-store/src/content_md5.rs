//! `Content-MD5` digests for upload bodies.
//!
//! File uploads stream their bodies, so the SDK signs them as
//! `UNSIGNED-PAYLOAD` and the store has nothing to check the received bytes
//! against. When a stalled attempt is abandoned mid-body, the SDK retries on a
//! new connection, and a store that commits whatever arrived before the
//! disconnect stores a short object as if it were complete — possibly after
//! the good retry (versitygw through v1.8.0, versity/versitygw#2414). With a
//! `Content-MD5` header, S3 and compatible stores must reject any body that
//! does not match the digest (`BadDigest`), so a truncated attempt fails
//! instead of committing.
//!
//! Every attempt re-reads the same bytes, so one digest covers all retries. A
//! header rather than an aws-chunked checksum trailer: see `s3_base_config`
//! for why trailers stay off.

use std::io::{BufReader, Read, Seek, SeekFrom, Write};
use std::path::Path;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use md5::{Digest as _, Md5};

/// Read size for hashing a file range. Bounds memory regardless of the range
/// length; a part can be gigabytes.
const READ_CHUNK_BYTES: usize = 1024 * 1024;

/// `Content-MD5` value (base64 of the MD5 digest) for an in-memory body.
pub(crate) fn of_bytes(body: &[u8]) -> String {
    BASE64_STANDARD.encode(Md5::digest(body))
}

/// `Content-MD5` value for `length` bytes of `path` starting at `offset`,
/// streamed in fixed-size chunks. Blocking: call from the blocking pool.
///
/// Fails if the file ends before the range does, rather than digesting fewer
/// bytes than the upload will declare.
pub(crate) fn of_file_range(path: &Path, offset: u64, length: u64) -> std::io::Result<String> {
    let mut file = std::fs::File::open(path)?;
    file.seek(SeekFrom::Start(offset))?;
    let mut range = BufReader::with_capacity(READ_CHUNK_BYTES, file.take(length));
    let mut hasher = Md5::new();
    let hashed = std::io::copy(&mut range, &mut DigestWriter(&mut hasher))?;
    if hashed != length {
        return Err(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            format!(
                "{} ended {} bytes into the {length}-byte range at offset {offset}",
                path.display(),
                hashed,
            ),
        ));
    }
    Ok(BASE64_STANDARD.encode(hasher.finalize()))
}

/// Feeds [`std::io::copy`] into a hasher.
struct DigestWriter<'a>(&'a mut Md5);

impl Write for DigestWriter<'_> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.update(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests;
