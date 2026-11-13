//! Streaming AES-256-GCM file/byte encryption.
//!
//! A portable, domain-neutral building block: callers supply a 32-byte
//! key and an AAD and get back authenticated ciphertext framed in the
//! SPACEAEA wire format below. It is the artifact-protection mechanic
//! Volume Manager composes into snapshot transfers, but it knows
//! nothing about keys, KEKs, sandboxes, or S3 layout — those policies
//! live in the orchestrator.
//!
//! Both the file-shape ([`encrypt_file_blocking`] /
//! [`decrypt_file_blocking`]) and stream-shape
//! ([`encrypt_stream_blocking`] / [`decrypt_stream_blocking`]) entry
//! points produce/consume the identical wire format, so a caller can
//! chain an upstream transformer (e.g. a `zstd` encoder wrapping the
//! source) into the AEAD without an intermediate temp file and still
//! decrypt with either shape.
//!
//! # On-disk / on-wire format
//!
//! ```text
//! ┌────────────┬─────────┬─────────┬──────────────┬──────────────────────────┐
//! │ magic (8)  │ ver (1) │ rsvd(1) │ nonce_pfx 7  │ chunk_ct ‖ tag × N       │
//! │ "SPACEAEA" │ 0x01    │ 0x00    │ random       │ all but last full; last  │
//! │            │         │         │              │ may be shorter (≥0 bytes)│
//! └────────────┴─────────┴─────────┴──────────────┴──────────────────────────┘
//! ```
//!
//! Header is 17 bytes. Each non-last plaintext chunk is exactly
//! [`CHUNK_SIZE`]; the last chunk may be shorter (an empty plaintext
//! file produces a single zero-byte last chunk plus tag). Per-chunk
//! AES-GCM tag is [`TAG_LEN`] bytes. The BE32 STREAM construction binds
//! chunk index and a last-block flag into the AES-GCM nonce, so
//! reordering, replaying, or truncating chunks all fail authentication.
//!
//! AAD is supplied by the caller and bound into every chunk; the same
//! bytes must be supplied at decrypt time or authentication fails.

use std::fs::File;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use aead_stream::{DecryptorBE32, EncryptorBE32};
use aes_gcm::Aes256Gcm;
use aes_gcm::aead::{KeyInit, Payload};

/// Magic bytes at byte 0 of every encrypted file. Eight bytes so the
/// header is fixed-size and the magic alone is enough to refuse
/// running plaintext-format inputs through the decryptor.
const FILE_MAGIC: &[u8; 8] = b"SPACEAEA";

/// On-disk format version. Bumped only when the header or chunk
/// framing changes incompatibly.
const FORMAT_VERSION: u8 = 1;

/// Reserved byte for forward compatibility; must be zero on read.
const RESERVED: u8 = 0;

/// Length of the BE32 STREAM nonce prefix. AES-256-GCM uses a 12-byte
/// nonce; STREAM consumes 5 of those (4 byte BE counter + 1 byte
/// last-block flag), leaving 7 for the prefix.
const NONCE_PREFIX_LEN: usize = 7;

/// Per-chunk AES-GCM authentication tag length.
const TAG_LEN: usize = 16;

/// Plaintext chunk size. 1 MiB keeps memory bounded for multi-GiB
/// snapshots and amortises the 16-byte-per-chunk tag overhead to
/// ~0.0015 %.
pub const CHUNK_SIZE: usize = 1024 * 1024;

/// Ciphertext chunk size: plaintext chunk + AEAD tag.
const CT_CHUNK_SIZE: usize = CHUNK_SIZE + TAG_LEN;

/// Total bytes of the on-disk header (magic + version + reserved +
/// nonce prefix). Public so callers can reason about the per-file
/// overhead.
pub const HEADER_LEN: usize = FILE_MAGIC.len() + 1 + 1 + NONCE_PREFIX_LEN;

/// Length of an AES-256 key in bytes.
pub const KEY_LEN: usize = 32;

/// Errors surfaced by the encrypt/decrypt functions.
///
/// Variants distinguish the reasons a crypto stage can fail so callers
/// can map each to the right retry/permanent outcome:
///
/// - I/O errors are typically retriable (transient disk pressure, etc).
/// - Header / framing / AEAD failures are permanent — retrying the
///   same on-disk inputs will keep failing.
/// - Bad-key length is a programming error and surfaces as permanent.
#[derive(Debug, thiserror::Error)]
pub enum CryptoError {
    /// Underlying read/write/seek of `src` or `dst`.
    #[error("crypto io: {0}")]
    Io(#[from] std::io::Error),

    /// `key` is not exactly [`KEY_LEN`] bytes.
    #[error("invalid DEK length: expected {KEY_LEN} bytes, got {0}")]
    InvalidKeyLength(usize),

    /// Encrypted file is shorter than [`HEADER_LEN`] or does not start
    /// with [`FILE_MAGIC`].
    #[error("invalid encrypted-file header")]
    BadHeader,

    /// Header version is not one this build knows how to read.
    #[error("unsupported encrypted-file version: {0}")]
    UnsupportedVersion(u8),

    /// Encrypted file ended mid-chunk (no authentication tag present).
    /// Returned only when *no* ciphertext bytes followed the header —
    /// in-chunk truncation is caught by the AEAD and surfaces as
    /// [`Self::AuthFailed`] via the BE32 last-block flag.
    #[error("encrypted file is truncated")]
    Truncated,

    /// AEAD authentication failed: DEK mismatch, AAD mismatch, chunk
    /// reordering, or tampering with the ciphertext.
    #[error("encrypted file failed authentication")]
    AuthFailed,
}

/// Encrypt `src` into `dst` using AES-256-GCM in the BE32 STREAM
/// construction. `dek` must be exactly [`KEY_LEN`] bytes. `aad` is
/// bound into every chunk; the caller must supply the same bytes at
/// decrypt time.
///
/// Synchronous: the caller is expected to invoke this inside
/// `tokio::task::spawn_blocking` because AES-GCM on each 1 MiB chunk
/// is CPU-bound and would stall the async runtime if run inline.
pub fn encrypt_file_blocking(
    src: &Path,
    dst: &Path,
    dek: &[u8],
    aad: &[u8],
) -> Result<(), CryptoError> {
    let mut src_file = File::open(src)?;
    let mut dst_file = File::create(dst)?;
    encrypt_stream_blocking(&mut src_file, &mut dst_file, dek, aad)
}

/// Stream-shape sibling of [`encrypt_file_blocking`]. Reads plaintext
/// from any [`Read`] and writes the SPACEAEA-framed ciphertext to any
/// [`Write`], so a caller can chain an upstream transformer (e.g.
/// `zstd::stream::read::Encoder` wrapping the source file) directly
/// into the AEAD without an intermediate temp file.
///
/// Wire format and 1 MiB chunk size are identical to
/// [`encrypt_file_blocking`] — anything encrypted via either function
/// decrypts via either [`decrypt_file_blocking`] or
/// [`decrypt_stream_blocking`]. Same caller contract: invoke from a
/// blocking task, supply a [`KEY_LEN`]-byte DEK held in zeroizing
/// memory, and pass the matching `aad` at decrypt time.
pub fn encrypt_stream_blocking<R: Read, W: Write>(
    src: &mut R,
    dst: &mut W,
    dek: &[u8],
    aad: &[u8],
) -> Result<(), CryptoError> {
    if dek.len() != KEY_LEN {
        return Err(CryptoError::InvalidKeyLength(dek.len()));
    }

    let mut nonce_prefix = [0u8; NONCE_PREFIX_LEN];
    rand::fill(&mut nonce_prefix[..]);

    dst.write_all(FILE_MAGIC)?;
    dst.write_all(&[FORMAT_VERSION, RESERVED])?;
    dst.write_all(&nonce_prefix)?;

    let cipher =
        Aes256Gcm::new_from_slice(dek).map_err(|_err| CryptoError::InvalidKeyLength(dek.len()))?;
    let mut encryptor = EncryptorBE32::from_aead(cipher, (&nonce_prefix).into());

    let mut cur = vec![0u8; CHUNK_SIZE];
    let mut nxt = vec![0u8; CHUNK_SIZE];

    let mut cur_n = read_some(src, &mut cur)?;

    loop {
        let nxt_n = read_some(src, &mut nxt)?;
        if nxt_n == 0 {
            let ct = encryptor
                .encrypt_last(Payload {
                    msg: filled_prefix(&cur, cur_n)?,
                    aad,
                })
                .map_err(|_err| CryptoError::AuthFailed)?;
            dst.write_all(&ct)?;
            dst.flush()?;
            return Ok(());
        }
        let ct = encryptor
            .encrypt_next(Payload {
                msg: filled_prefix(&cur, cur_n)?,
                aad,
            })
            .map_err(|_err| CryptoError::AuthFailed)?;
        dst.write_all(&ct)?;
        std::mem::swap(&mut cur, &mut nxt);
        cur_n = nxt_n;
    }
}

/// Decrypt `src` (produced by [`encrypt_file_blocking`]) into `dst`.
/// `dek` must match the key used at encryption and `aad` must be the
/// exact same bytes. Header magic, version, and chunk framing are all
/// authenticated; any mismatch fails with [`CryptoError::AuthFailed`]
/// or [`CryptoError::BadHeader`] depending on where the divergence
/// is.
///
/// Synchronous: see [`encrypt_file_blocking`] for why.
pub fn decrypt_file_blocking(
    src: &Path,
    dst: &Path,
    dek: &[u8],
    aad: &[u8],
) -> Result<(), CryptoError> {
    let mut src_file = File::open(src)?;
    let mut dst_file = File::create(dst)?;
    decrypt_stream_blocking(&mut src_file, &mut dst_file, dek, aad)
}

/// Stream-shape sibling of [`decrypt_file_blocking`]. Reads
/// SPACEAEA-framed ciphertext from any [`Read`] and writes the
/// recovered plaintext to any [`Write`], so a caller can pipe the
/// output directly into a downstream transformer (e.g.
/// `zstd::stream::write::Decoder` wrapping the destination file)
/// without an intermediate temp.
///
/// Same wire format, AAD contract, and authentication semantics as
/// [`decrypt_file_blocking`] — any byte change, chunk reorder, or
/// AAD divergence surfaces as [`CryptoError::AuthFailed`].
pub fn decrypt_stream_blocking<R: Read, W: Write>(
    src: &mut R,
    dst: &mut W,
    dek: &[u8],
    aad: &[u8],
) -> Result<(), CryptoError> {
    if dek.len() != KEY_LEN {
        return Err(CryptoError::InvalidKeyLength(dek.len()));
    }

    let mut header = [0u8; HEADER_LEN];
    src.read_exact(&mut header)?;
    let Some(header_rest) = header.strip_prefix(FILE_MAGIC) else {
        return Err(CryptoError::BadHeader);
    };
    let Some((&version, header_rest)) = header_rest.split_first() else {
        return Err(CryptoError::BadHeader);
    };
    if version != FORMAT_VERSION {
        return Err(CryptoError::UnsupportedVersion(version));
    }
    let Some((&reserved, nonce_prefix)) = header_rest.split_first() else {
        return Err(CryptoError::BadHeader);
    };
    if reserved != RESERVED {
        return Err(CryptoError::BadHeader);
    }
    let nonce_prefix: [u8; NONCE_PREFIX_LEN] = nonce_prefix
        .try_into()
        .map_err(|_err| CryptoError::BadHeader)?;

    let cipher =
        Aes256Gcm::new_from_slice(dek).map_err(|_err| CryptoError::InvalidKeyLength(dek.len()))?;
    let mut decryptor = DecryptorBE32::from_aead(cipher, (&nonce_prefix).into());

    let mut cur = vec![0u8; CT_CHUNK_SIZE];
    let mut nxt = vec![0u8; CT_CHUNK_SIZE];

    let cur_n = read_some(src, &mut cur)?;
    if cur_n == 0 {
        return Err(CryptoError::Truncated);
    }
    let mut cur_n = cur_n;

    loop {
        let nxt_n = read_some(src, &mut nxt)?;
        if nxt_n == 0 {
            let pt = decryptor
                .decrypt_last(Payload {
                    msg: filled_prefix(&cur, cur_n)?,
                    aad,
                })
                .map_err(|_err| CryptoError::AuthFailed)?;
            dst.write_all(&pt)?;
            dst.flush()?;
            return Ok(());
        }
        let pt = decryptor
            .decrypt_next(Payload {
                msg: filled_prefix(&cur, cur_n)?,
                aad,
            })
            .map_err(|_err| CryptoError::AuthFailed)?;
        dst.write_all(&pt)?;
        std::mem::swap(&mut cur, &mut nxt);
        cur_n = nxt_n;
    }
}

/// Sibling path with a `.enc.tmp` suffix, for callers that stage the
/// encrypted artifact next to its plaintext source.
pub fn encrypted_sibling(src: &Path) -> PathBuf {
    let mut p = src.to_path_buf();
    let mut name = p.file_name().unwrap_or_default().to_os_string();
    name.push(".enc.tmp");
    p.set_file_name(name);
    p
}

/// Sibling path with a `.plaintext.tmp` suffix for the decrypted
/// output. Distinct suffix from a zstd `.dec.tmp` convention so the
/// crypto pass and a decompress pass never share a path even on the
/// same base name.
pub fn decrypted_sibling(src: &Path) -> PathBuf {
    let mut p = src.to_path_buf();
    let mut name = p.file_name().unwrap_or_default().to_os_string();
    name.push(".plaintext.tmp");
    p.set_file_name(name);
    p
}

/// Read up to `buf.len()` bytes from `r`, looping over short reads
/// until the buffer is full or EOF. Returns the number of bytes
/// actually read (0 on immediate EOF).
fn read_some<R: Read>(r: &mut R, buf: &mut [u8]) -> std::io::Result<usize> {
    let mut filled = 0;
    while filled < buf.len() {
        let Some(remaining) = buf.get_mut(filled..) else {
            return Err(std::io::Error::other(
                "read cursor moved beyond buffer length",
            ));
        };
        match r.read(remaining) {
            Ok(0) => break,
            Ok(n) if n <= remaining.len() => filled += n,
            Ok(_) => {
                return Err(std::io::Error::other(
                    "reader reported more bytes than the output buffer can hold",
                ));
            }
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(error),
        }
    }
    Ok(filled)
}

fn filled_prefix(buf: &[u8], filled: usize) -> Result<&[u8], CryptoError> {
    buf.get(..filled).ok_or_else(|| {
        CryptoError::Io(std::io::Error::other(
            "read cursor moved beyond buffer length",
        ))
    })
}

#[cfg(test)]
mod tests;
