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

use std::io::{BufRead, BufReader, Read, Seek, SeekFrom};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

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
/// bytes than the upload will declare. Fails once `cancelled` is set, checked
/// before every chunk: a blocking read cannot be aborted, so this is how a
/// dropped upload stops reading a range nobody will send.
pub(crate) fn of_file_range(
    path: &Path,
    offset: u64,
    length: u64,
    cancelled: &AtomicBool,
) -> std::io::Result<String> {
    let mut file = std::fs::File::open(path)?;
    file.seek(SeekFrom::Start(offset))?;
    let (digest, hashed) = digest_chunks(file.take(length), cancelled)?;
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
    Ok(digest)
}

/// Digests `reader` to its end in [`READ_CHUNK_BYTES`] reads, returning the
/// `Content-MD5` value and the byte count, or an error once `cancelled` is set.
fn digest_chunks(reader: impl Read, cancelled: &AtomicBool) -> std::io::Result<(String, u64)> {
    let mut reader = BufReader::with_capacity(READ_CHUNK_BYTES, reader);
    let mut hasher = Md5::new();
    let mut hashed = 0;
    loop {
        if cancelled.load(Ordering::Relaxed) {
            return Err(std::io::Error::other(format!(
                "digest cancelled after {hashed} bytes"
            )));
        }
        let chunk = match reader.fill_buf() {
            Ok([]) => break,
            Ok(chunk) => chunk,
            Err(err) if err.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(err) => return Err(err),
        };
        hasher.update(chunk);
        let read = chunk.len();
        hashed += read as u64;
        reader.consume(read);
    }
    Ok((BASE64_STANDARD.encode(hasher.finalize()), hashed))
}

/// Sets the shared cancellation flag when dropped. Held by the future awaiting
/// a blocking [`of_file_range`], so dropping or aborting that future stops the
/// read at its next chunk.
#[derive(Default)]
pub(crate) struct CancelOnDrop(Arc<AtomicBool>);

impl CancelOnDrop {
    pub(crate) fn flag(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.0)
    }
}

impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests;
