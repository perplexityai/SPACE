use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::mpsc::{Receiver, SyncSender};
use std::thread;

use pplx_space_common_core::types::{
    Digest, DigestParseError, HashAlg, TEMPLATE_ROOTFS_FILE, TEMPLATE_ROOTFS_ZSTD_FILE,
    TEMPLATE_ROOTFS_ZSTD_LEVEL,
};
use pplx_space_common_io::sparse::SparseFileWriter;
use pplx_space_common_svc::page_cache;
use sha2::{Digest as _, Sha256};

use crate::content_hash::ContentHasher;

type Sha256Context = Sha256;

const ROOTFS_SPARSE_BLOCK_BYTES: usize = 64 * 1024;
const COMPRESSED_INPUT_BUFFER_SIZE: usize = 8 * 1024 * 1024;
const COMPRESSED_INPUT_PIPELINE_DEPTH: usize = 3;
const DECODE_BUFFER_SIZE: usize = 16 * 1024 * 1024;
const DECODE_PIPELINE_DEPTH: usize = 4;
// Compression waves must not pin every core of a metal host. The pod runs
// without a CPU limit, so `available_parallelism` sees the full host; this cap
// preserves the ceiling the old 16-CPU cgroup limit imposed.
const MAX_ENCODER_WORKERS: u32 = 16;

#[derive(Debug, thiserror::Error)]
pub(crate) enum RootfsZstdError {
    #[error("invalid expected rootfs digest: {0}")]
    InvalidDigest(#[from] DigestParseError),
    #[error("{operation} {path}: {source}")]
    Io {
        operation: &'static str,
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("zstd {operation}: {source}")]
    Zstd {
        operation: &'static str,
        #[source]
        source: std::io::Error,
    },
    #[error("rootfs zstd materialization pipeline disconnected while {operation}")]
    Pipeline { operation: &'static str },
    #[error("rootfs zstd materialization writer thread panicked")]
    WriterThreadPanic,
    #[error("rootfs zstd materialization hash thread panicked")]
    HashThreadPanic,
    #[error("decoded rootfs digest {actual} disagrees with expected {expected}")]
    ExpandedDigestMismatch { expected: String, actual: String },
}

impl RootfsZstdError {
    pub(crate) fn reason(&self) -> &'static str {
        match self {
            Self::InvalidDigest(_) => "invalid_digest",
            Self::Io { .. } => "io",
            Self::Zstd { .. } => "zstd",
            Self::Pipeline { .. } => "pipeline",
            Self::WriterThreadPanic => "writer_thread_panic",
            Self::HashThreadPanic => "hash_thread_panic",
            Self::ExpandedDigestMismatch { .. } => "expanded_digest_mismatch",
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct RootfsZstdStats {
    pub(crate) rootfs_size: u64,
    pub(crate) compressed_size: u64,
    pub(crate) compressed_hash: String,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct RootfsZstdDecompressStats {
    pub(crate) rootfs_size: u64,
    pub(crate) compressed_size: u64,
    /// Expanded rootfs digest, recomputed from the decoded bytes. When the
    /// caller supplied an expected digest it equals that digest, because
    /// the decoder refuses to publish bytes that disagree with it.
    pub(crate) rootfs_hash: String,
    pub(crate) compressed_hash: String,
}

pub(crate) fn rootfs_path(build_dir: &Path) -> PathBuf {
    build_dir.join(TEMPLATE_ROOTFS_FILE)
}

pub(crate) fn compressed_rootfs_path(build_dir: &Path) -> PathBuf {
    build_dir.join(TEMPLATE_ROOTFS_ZSTD_FILE)
}

pub(crate) fn compress_rootfs(
    rootfs_path: &Path,
    compressed_path: &Path,
) -> Result<RootfsZstdStats, RootfsZstdError> {
    let rootfs_size = std::fs::metadata(rootfs_path)
        .map_err(|source| RootfsZstdError::Io {
            operation: "stat rootfs",
            path: rootfs_path.display().to_string(),
            source,
        })?
        .len();
    let input = std::fs::File::open(rootfs_path).map_err(|source| RootfsZstdError::Io {
        operation: "open rootfs",
        path: rootfs_path.display().to_string(),
        source,
    })?;
    let tmp = tmp_path(compressed_path);
    let output = std::fs::File::create(&tmp).map_err(|source| RootfsZstdError::Io {
        operation: "create compressed rootfs temp",
        path: tmp.display().to_string(),
        source,
    })?;
    let digesting_output = DigestingWriter::new(BufWriter::new(output));
    let mut encoder =
        zstd::stream::write::Encoder::new(digesting_output, TEMPLATE_ROOTFS_ZSTD_LEVEL).map_err(
            |source| RootfsZstdError::Zstd {
                operation: "open compressor",
                source,
            },
        )?;
    let workers = encoder_workers(
        std::thread::available_parallelism()
            .map(std::num::NonZeroUsize::get)
            .unwrap_or(1),
    );
    encoder
        .multithread(workers)
        .map_err(|source| RootfsZstdError::Zstd {
            operation: "configure compressor workers",
            source,
        })?;
    let mut reader = BufReader::new(input);
    std::io::copy(&mut reader, &mut encoder).map_err(|source| RootfsZstdError::Zstd {
        operation: "compress rootfs",
        source,
    })?;
    page_cache::advise_dontneed(reader.get_ref(), rootfs_path, "compress rootfs input");
    let mut digesting_output = encoder.finish().map_err(|source| RootfsZstdError::Zstd {
        operation: "finish compressor",
        source,
    })?;
    digesting_output
        .flush()
        .map_err(|source| RootfsZstdError::Io {
            operation: "flush compressed rootfs temp",
            path: tmp.display().to_string(),
            source,
        })?;
    let compressed_hash = digesting_output.finish_hash();
    // This is a cache artifact. Startup recovery revalidates hashes, so avoid
    // fsyncing the temp file or parent directory before the atomic rename.
    std::fs::rename(&tmp, compressed_path).map_err(|source| RootfsZstdError::Io {
        operation: "rename compressed rootfs",
        path: compressed_path.display().to_string(),
        source,
    })?;
    let compressed_size = std::fs::metadata(compressed_path)
        .map_err(|source| RootfsZstdError::Io {
            operation: "stat compressed rootfs",
            path: compressed_path.display().to_string(),
            source,
        })?
        .len();
    Ok(RootfsZstdStats {
        rootfs_size,
        compressed_size,
        compressed_hash,
    })
}

/// Decode `compressed_path` into `rootfs_path`, hashing the decoded bytes.
///
/// With `expected_rootfs_hash` set, the decoded digest is checked before
/// the output is renamed into place: bytes that disagree never appear at
/// `rootfs_path`. That matters because disk recovery adopts whatever file
/// sits there without rehashing it, so a check that ran after publishing
/// would still leave the bad bytes recoverable until cleanup caught up.
pub(crate) fn decompress_rootfs(
    compressed_path: &Path,
    rootfs_path: &Path,
    expected_rootfs_hash: Option<&str>,
    backing_policy: crate::rootfs_backing::RootfsBackingPolicy,
) -> Result<RootfsZstdDecompressStats, RootfsZstdError> {
    let algorithm = expected_rootfs_hash
        .map(str::parse::<Digest>)
        .transpose()?
        .map_or(HashAlg::Sha256, Digest::algorithm);
    let compressed_size = std::fs::metadata(compressed_path)
        .map_err(|source| RootfsZstdError::Io {
            operation: "stat compressed rootfs",
            path: compressed_path.display().to_string(),
            source,
        })?
        .len();
    let input = std::fs::File::open(compressed_path).map_err(|source| RootfsZstdError::Io {
        operation: "open compressed rootfs",
        path: compressed_path.display().to_string(),
        source,
    })?;
    let tmp = tmp_path(rootfs_path);
    let output = std::fs::File::create(&tmp).map_err(|source| RootfsZstdError::Io {
        operation: "create rootfs temp",
        path: tmp.display().to_string(),
        source,
    })?;
    backing_policy
        .prepare_empty(&output)
        .map_err(|source| RootfsZstdError::Io {
            operation: "set rootfs backing policy",
            path: tmp.display().to_string(),
            source,
        })?;
    let buffered_input = HashedCompressedInput::new(
        input,
        COMPRESSED_INPUT_BUFFER_SIZE,
        COMPRESSED_INPUT_PIPELINE_DEPTH,
    )
    .map_err(|source| RootfsZstdError::Io {
        operation: "start compressed rootfs hash stage",
        path: compressed_path.display().to_string(),
        source,
    })?;
    let mut decoder =
        zstd::stream::read::Decoder::with_buffer(buffered_input).map_err(|source| {
            RootfsZstdError::Zstd {
                operation: "open decompressor",
                source,
            }
        })?;
    decoder
        .window_log_max(31)
        .map_err(|source| RootfsZstdError::Zstd {
            operation: "configure decompressor window",
            source,
        })?;
    let (writer_tx, writer_rx) = std::sync::mpsc::sync_channel(DECODE_PIPELINE_DEPTH);
    let (free_tx, free_rx) = std::sync::mpsc::sync_channel(DECODE_PIPELINE_DEPTH);
    for _ in 0..DECODE_PIPELINE_DEPTH {
        free_tx
            .send(vec![0u8; DECODE_BUFFER_SIZE])
            .map_err(|_| RootfsZstdError::Pipeline {
                operation: "seed decode buffer pool",
            })?;
    }
    let writer_tmp = tmp.clone();
    let writer_handle = thread::Builder::new()
        .name("rootfs-zstd-writer".to_string())
        .spawn(move || write_decoded_rootfs_chunks(output, writer_tmp, writer_rx, free_tx))
        .map_err(|source| RootfsZstdError::Io {
            operation: "spawn rootfs writer thread",
            path: tmp.display().to_string(),
            source,
        })?;
    let (hash_tx, hash_rx) = std::sync::mpsc::sync_channel(DECODE_PIPELINE_DEPTH);
    let hash_handle = thread::Builder::new()
        .name("rootfs-zstd-hash".to_string())
        .spawn(move || hash_decoded_rootfs_chunks(hash_rx, writer_tx, algorithm))
        .map_err(|source| RootfsZstdError::Io {
            operation: "spawn rootfs hash thread",
            path: tmp.display().to_string(),
            source,
        })?;
    let pipeline = MaterializationPipeline::new(hash_tx, hash_handle, writer_handle);
    loop {
        let buffer = match free_rx.recv() {
            Ok(buffer) => buffer,
            Err(_) => {
                return Err(pipeline.abort(RootfsZstdError::Pipeline {
                    operation: "wait for reusable decode buffer",
                }));
            }
        };
        if buffer.len() != DECODE_BUFFER_SIZE {
            return Err(pipeline.abort(RootfsZstdError::Pipeline {
                operation: "reuse full decode buffer",
            }));
        }
        let chunk = match read_decoded_rootfs_chunk(&mut decoder, buffer) {
            Ok(Some(chunk)) => chunk,
            Ok(None) => break,
            Err(source) => {
                return Err(pipeline.abort(RootfsZstdError::Zstd {
                    operation: "decompress rootfs",
                    source,
                }));
            }
        };
        if pipeline.send_decoded(chunk).is_err() {
            return Err(pipeline.abort(RootfsZstdError::Pipeline {
                operation: "send decoded rootfs chunk to hash stage",
            }));
        }
    }
    let compressed_hash = decoder
        .finish()
        .finish_hash_and_advise_dontneed(compressed_path, "decompress compressed rootfs input");
    let (hash_output, sparse_output) = pipeline.finish()?;
    let compressed_hash = compressed_hash?;
    if hash_output.decoded_size != sparse_output.decoded_size {
        return Err(RootfsZstdError::Pipeline {
            operation: "verify hash/write decoded size agreement",
        });
    }
    if let Some(expected) = expected_rootfs_hash
        && hash_output.rootfs_hash != expected
    {
        // Leave nothing at the output path: a later recovery pass would
        // take a published file at face value.
        let _ = std::fs::remove_file(&tmp);
        return Err(RootfsZstdError::ExpandedDigestMismatch {
            expected: expected.to_string(),
            actual: hash_output.rootfs_hash,
        });
    }
    // This is a cache artifact whose digest was verified above, so avoid
    // fsyncing the temp file or parent directory before the atomic rename.
    std::fs::rename(&tmp, rootfs_path).map_err(|source| RootfsZstdError::Io {
        operation: "rename rootfs",
        path: rootfs_path.display().to_string(),
        source,
    })?;
    let rootfs_size = std::fs::metadata(rootfs_path)
        .map_err(|source| RootfsZstdError::Io {
            operation: "stat rootfs",
            path: rootfs_path.display().to_string(),
            source,
        })?
        .len();
    Ok(RootfsZstdDecompressStats {
        rootfs_size,
        compressed_size,
        rootfs_hash: hash_output.rootfs_hash,
        compressed_hash,
    })
}

/// Stream an uncompressed rootfs on disk to produce `(<algorithm>:<hex>, size)`.
///
/// Used by `volume::do_pull` when the build's `manifest.json` declares
/// the bootable rootfs without a `.zst` sibling — a first-class
/// supported shape under the manifest-driven pull design (see
/// [`crate::volume::materialize_rootfs`]), not transitional code.
/// `manifest.json` is the source of truth for which artifacts a build
/// ships, so a manifest that lists `files: { "rootfs.ext4": {…} }`
/// alone is exactly as valid as one listing both an uncompressed and a
/// `.zst` sibling.
///
/// The compressed-rootfs path doesn't need this helper because
/// [`decompress_rootfs`] computes both hashes as a side effect of
/// streaming through the decoder; here we read the file directly
/// because there's no decompression step.
pub(crate) fn hash_rootfs_file(
    rootfs_path: &Path,
    algorithm: HashAlg,
) -> Result<(String, u64), RootfsZstdError> {
    let input = std::fs::File::open(rootfs_path).map_err(|source| RootfsZstdError::Io {
        operation: "open rootfs",
        path: rootfs_path.display().to_string(),
        source,
    })?;
    let mut reader = BufReader::new(input);
    let mut rootfs_size = 0u64;
    let mut rootfs_hasher = ContentHasher::new(algorithm);
    let mut buffer = vec![0u8; 1024 * 1024];
    loop {
        let read = reader
            .read(&mut buffer)
            .map_err(|source| RootfsZstdError::Io {
                operation: "hash rootfs",
                path: rootfs_path.display().to_string(),
                source,
            })?;
        if read == 0 {
            break;
        }
        let bytes = buffer.get(..read).ok_or_else(|| RootfsZstdError::Io {
            operation: "hash rootfs",
            path: rootfs_path.display().to_string(),
            source: invalid_data("read byte count exceeded rootfs hash buffer"),
        })?;
        rootfs_hasher.update(bytes);
        rootfs_size = rootfs_size.saturating_add(usize_to_u64_saturating(read));
    }
    page_cache::advise_dontneed(reader.get_ref(), rootfs_path, "hash rootfs input");
    Ok((rootfs_hasher.finish().to_string(), rootfs_size))
}

#[derive(Debug)]
struct SparseRootfsOutput {
    decoded_size: u64,
}

#[derive(Debug)]
struct RootfsHashOutput {
    decoded_size: u64,
    rootfs_hash: String,
}

struct DecodedRootfsChunk {
    buffer: Vec<u8>,
    len: usize,
}

impl DecodedRootfsChunk {
    fn bytes(&self) -> Result<&[u8], RootfsZstdError> {
        self.buffer
            .get(..self.len)
            .ok_or(RootfsZstdError::Pipeline {
                operation: "read decoded chunk bytes",
            })
    }

    fn into_buffer(self) -> Vec<u8> {
        self.buffer
    }
}

fn read_decoded_rootfs_chunk<R: Read>(
    reader: &mut R,
    mut buffer: Vec<u8>,
) -> std::io::Result<Option<DecodedRootfsChunk>> {
    let mut len = 0usize;
    while len < buffer.len() {
        let tail = buffer
            .get_mut(len..)
            .ok_or_else(|| invalid_data("decoded read offset exceeded buffer length"))?;
        let read = reader.read(tail)?;
        if read == 0 {
            break;
        }
        len = len.saturating_add(read);
    }
    if len == 0 {
        Ok(None)
    } else {
        Ok(Some(DecodedRootfsChunk { buffer, len }))
    }
}

struct DigestingWriter<W> {
    inner: W,
    hasher: Sha256Context,
}

impl<W> DigestingWriter<W> {
    fn new(inner: W) -> Self {
        Self {
            inner,
            hasher: sha256_hasher(),
        }
    }

    fn finish_hash(self) -> String {
        sha256_digest(self.hasher)
    }
}

impl<W: Write> Write for DigestingWriter<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let written = self.inner.write(buf)?;
        let bytes = buf
            .get(..written)
            .ok_or_else(|| invalid_data("written byte count exceeded digest buffer"))?;
        self.hasher.update(bytes);
        Ok(written)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

struct CompressedInputChunk {
    buffer: Vec<u8>,
    len: usize,
}

impl CompressedInputChunk {
    fn bytes(&self) -> std::io::Result<&[u8]> {
        self.buffer
            .get(..self.len)
            .ok_or_else(|| invalid_data("compressed chunk length exceeded its buffer"))
    }
}

/// Buffered compressed input whose SHA-256 is computed on its own thread.
///
/// Hashing inline cost the decoder thread, the pipeline's slowest stage,
/// about 0.75 CPU-seconds per compressed GB. Each chunk read here is shared
/// with the hash thread, which hands the buffer back once hashed; the
/// fixed pool bounds memory and makes reads wait if hashing falls behind.
///
/// A buffer the hash thread never receives must not be dropped, or the pool
/// shrinks until a read waits forever: an empty fill is kept as `spare`, end
/// of input is remembered, and an error after a partial fill is reported on
/// the next read so the bytes already read are still delivered and hashed.
struct HashedCompressedInput<R> {
    inner: R,
    current: Option<std::sync::Arc<CompressedInputChunk>>,
    spare: Option<std::sync::Arc<CompressedInputChunk>>,
    position: usize,
    eof: bool,
    pending_error: Option<std::io::Error>,
    hash_tx: Option<SyncSender<std::sync::Arc<CompressedInputChunk>>>,
    free_rx: Receiver<std::sync::Arc<CompressedInputChunk>>,
    hash_handle: Option<thread::JoinHandle<Result<String, RootfsZstdError>>>,
}

impl<R: Read> HashedCompressedInput<R> {
    fn new(inner: R, chunk_size: usize, depth: usize) -> std::io::Result<Self> {
        let (hash_tx, hash_rx) = std::sync::mpsc::sync_channel(depth);
        let (free_tx, free_rx) = std::sync::mpsc::sync_channel(depth);
        for _ in 0..depth {
            free_tx
                .send(std::sync::Arc::new(CompressedInputChunk {
                    buffer: vec![0u8; chunk_size],
                    len: 0,
                }))
                .map_err(|_| invalid_data("seed compressed input buffer pool"))?;
        }
        let hash_handle = thread::Builder::new()
            .name("rootfs-zstd-input-hash".to_string())
            .spawn(move || hash_compressed_rootfs_chunks(hash_rx, free_tx))?;
        Ok(Self {
            inner,
            current: None,
            spare: None,
            position: 0,
            eof: false,
            pending_error: None,
            hash_tx: Some(hash_tx),
            free_rx,
            hash_handle: Some(hash_handle),
        })
    }

    fn finish_hash(&mut self) -> Result<String, RootfsZstdError> {
        self.hash_tx.take();
        self.current.take();
        self.hash_handle
            .take()
            .expect("compressed hash thread joined at most once")
            .join()
            .map_err(|_| RootfsZstdError::HashThreadPanic)?
    }

    fn next_chunk(&mut self) -> std::io::Result<()> {
        if let Some(error) = self.pending_error.take() {
            return Err(error);
        }
        // Release this reader's share first: the pool only hands back
        // buffers that no stage still reads.
        self.current.take();
        self.position = 0;
        let mut chunk = match self.spare.take() {
            Some(chunk) => chunk,
            None => self
                .free_rx
                .recv()
                .map_err(|_| invalid_data("compressed hash stage stopped"))?,
        };
        let Some(filled) = std::sync::Arc::get_mut(&mut chunk) else {
            return Err(invalid_data("compressed input buffer still shared"));
        };
        filled.len = 0;
        while filled.len < filled.buffer.len() {
            let Some(tail) = filled.buffer.get_mut(filled.len..) else {
                break;
            };
            match self.inner.read(tail) {
                Ok(0) => {
                    self.eof = true;
                    break;
                }
                Ok(read) => filled.len = filled.len.saturating_add(read),
                Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
                Err(error) => {
                    self.pending_error = Some(error);
                    break;
                }
            }
        }
        if filled.len == 0 {
            self.spare = Some(chunk);
            return self.pending_error.take().map_or(Ok(()), Err);
        }
        self.hash_tx
            .as_ref()
            .ok_or_else(|| invalid_data("compressed input read after hashing finished"))?
            .send(std::sync::Arc::clone(&chunk))
            .map_err(|_| invalid_data("compressed hash stage stopped"))?;
        self.current = Some(chunk);
        Ok(())
    }
}

impl HashedCompressedInput<std::fs::File> {
    fn finish_hash_and_advise_dontneed(
        mut self,
        path: &Path,
        operation: &'static str,
    ) -> Result<String, RootfsZstdError> {
        let hash = self.finish_hash();
        page_cache::advise_dontneed(&self.inner, path, operation);
        hash
    }
}

impl<R: Read> std::io::BufRead for HashedCompressedInput<R> {
    fn fill_buf(&mut self) -> std::io::Result<&[u8]> {
        let exhausted = self
            .current
            .as_ref()
            .is_none_or(|chunk| self.position >= chunk.len);
        if exhausted && (!self.eof || self.pending_error.is_some()) {
            self.next_chunk()?;
        }
        match self.current.as_ref() {
            Some(chunk) => chunk
                .bytes()?
                .get(self.position..)
                .ok_or_else(|| invalid_data("compressed read position exceeded chunk length")),
            None => Ok(&[]),
        }
    }

    fn consume(&mut self, amount: usize) {
        self.position = self.position.saturating_add(amount);
    }
}

impl<R: Read> Read for HashedCompressedInput<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let available = std::io::BufRead::fill_buf(self)?;
        let read = available.len().min(buf.len());
        buf.get_mut(..read)
            .zip(available.get(..read))
            .ok_or_else(|| invalid_data("compressed read exceeded available bytes"))
            .map(|(target, source)| target.copy_from_slice(source))?;
        std::io::BufRead::consume(self, read);
        Ok(read)
    }
}

impl<R> Drop for HashedCompressedInput<R> {
    fn drop(&mut self) {
        self.hash_tx.take();
        if let Some(handle) = self.hash_handle.take() {
            let _ = handle.join();
        }
    }
}

fn hash_compressed_rootfs_chunks(
    chunk_rx: Receiver<std::sync::Arc<CompressedInputChunk>>,
    free_tx: SyncSender<std::sync::Arc<CompressedInputChunk>>,
) -> Result<String, RootfsZstdError> {
    let mut hasher = sha256_hasher();
    for chunk in chunk_rx {
        hasher.update(chunk.bytes().map_err(|_| RootfsZstdError::Pipeline {
            operation: "read compressed chunk bytes",
        })?);
        // The reader may have stopped; a closed pool only ends recycling.
        let _ = free_tx.send(chunk);
    }
    Ok(sha256_digest(hasher))
}

fn write_decoded_rootfs_chunks(
    output: std::fs::File,
    tmp: PathBuf,
    chunk_rx: Receiver<DecodedRootfsChunk>,
    free_tx: SyncSender<Vec<u8>>,
) -> Result<SparseRootfsOutput, RootfsZstdError> {
    let mut sparse_writer =
        SparseFileWriter::new(output, ROOTFS_SPARSE_BLOCK_BYTES).map_err(|source| {
            RootfsZstdError::Io {
                operation: "create sparse rootfs writer",
                path: tmp.display().to_string(),
                source,
            }
        })?;
    let mut decoded_size = 0u64;
    for chunk in chunk_rx {
        decoded_size = decoded_size.saturating_add(usize_to_u64_saturating(chunk.len));
        sparse_writer
            .write_all(chunk.bytes()?)
            .map_err(|source| RootfsZstdError::Io {
                operation: "write sparse rootfs temp",
                path: tmp.display().to_string(),
                source,
            })?;
        let _ = free_tx.send(chunk.into_buffer());
    }
    let output = sparse_writer
        .finish()
        .map_err(|source| RootfsZstdError::Io {
            operation: "finalize sparse rootfs temp",
            path: tmp.display().to_string(),
            source,
        })?;
    page_cache::advise_dontneed(&output, &tmp, "write decompressed rootfs output");
    Ok(SparseRootfsOutput { decoded_size })
}

fn hash_decoded_rootfs_chunks(
    chunk_rx: Receiver<DecodedRootfsChunk>,
    writer_tx: SyncSender<DecodedRootfsChunk>,
    algorithm: HashAlg,
) -> Result<RootfsHashOutput, RootfsZstdError> {
    let mut hasher = ContentHasher::new(algorithm);
    let mut decoded_size = 0u64;
    for chunk in chunk_rx {
        decoded_size = decoded_size.saturating_add(usize_to_u64_saturating(chunk.len));
        hasher.update(chunk.bytes()?);
        writer_tx
            .send(chunk)
            .map_err(|_| RootfsZstdError::Pipeline {
                operation: "send hashed rootfs chunk to sparse writer",
            })?;
    }
    let rootfs_hash = hasher.finish().to_string();
    Ok(RootfsHashOutput {
        decoded_size,
        rootfs_hash,
    })
}

struct MaterializationPipeline {
    decoded_tx: Option<SyncSender<DecodedRootfsChunk>>,
    hash_handle: Option<thread::JoinHandle<Result<RootfsHashOutput, RootfsZstdError>>>,
    writer_handle: Option<thread::JoinHandle<Result<SparseRootfsOutput, RootfsZstdError>>>,
}

impl MaterializationPipeline {
    fn new(
        hash_tx: SyncSender<DecodedRootfsChunk>,
        hash_handle: thread::JoinHandle<Result<RootfsHashOutput, RootfsZstdError>>,
        writer_handle: thread::JoinHandle<Result<SparseRootfsOutput, RootfsZstdError>>,
    ) -> Self {
        Self {
            decoded_tx: Some(hash_tx),
            hash_handle: Some(hash_handle),
            writer_handle: Some(writer_handle),
        }
    }

    fn send_decoded(&self, chunk: DecodedRootfsChunk) -> Result<(), DecodedRootfsChunk> {
        self.decoded_tx
            .as_ref()
            .expect("pipeline input is open until finish or abort")
            .send(chunk)
            .map_err(|err| err.0)
    }

    fn abort(mut self, primary: RootfsZstdError) -> RootfsZstdError {
        self.close_decoded_input();
        match self.join() {
            Ok(_) => primary,
            Err(error) => error,
        }
    }

    fn finish(mut self) -> Result<(RootfsHashOutput, SparseRootfsOutput), RootfsZstdError> {
        self.close_decoded_input();
        self.join()
    }

    fn close_decoded_input(&mut self) {
        self.decoded_tx.take();
    }

    fn join(&mut self) -> Result<(RootfsHashOutput, SparseRootfsOutput), RootfsZstdError> {
        let writer_handle = self
            .writer_handle
            .take()
            .expect("writer thread joined at most once");
        let hash_handle = self
            .hash_handle
            .take()
            .expect("hash thread joined at most once");
        let hash_result = join_hash_thread(hash_handle);
        let writer_result = join_writer_thread(writer_handle);
        materialization_result(hash_result, writer_result)
    }
}

impl Drop for MaterializationPipeline {
    fn drop(&mut self) {
        self.close_decoded_input();
        if let Some(hash_handle) = self.hash_handle.take() {
            let _ = join_hash_thread(hash_handle);
        }
        if let Some(writer_handle) = self.writer_handle.take() {
            let _ = join_writer_thread(writer_handle);
        }
    }
}

fn materialization_result(
    hash_result: Result<RootfsHashOutput, RootfsZstdError>,
    writer_result: Result<SparseRootfsOutput, RootfsZstdError>,
) -> Result<(RootfsHashOutput, SparseRootfsOutput), RootfsZstdError> {
    match (hash_result, writer_result) {
        (Ok(hash_output), Ok(sparse_output)) => Ok((hash_output, sparse_output)),
        (Ok(_), Err(error)) => Err(error),
        (Err(error), Ok(_)) => Err(error),
        // The hash stage commonly reports a disconnected writer after the
        // writer has hit the concrete I/O error. Prefer that root cause.
        (Err(_), Err(error)) => Err(error),
    }
}

fn join_hash_thread(
    handle: thread::JoinHandle<Result<RootfsHashOutput, RootfsZstdError>>,
) -> Result<RootfsHashOutput, RootfsZstdError> {
    handle
        .join()
        .map_err(|_| RootfsZstdError::HashThreadPanic)?
}

fn join_writer_thread(
    handle: thread::JoinHandle<Result<SparseRootfsOutput, RootfsZstdError>>,
) -> Result<SparseRootfsOutput, RootfsZstdError> {
    handle
        .join()
        .map_err(|_| RootfsZstdError::WriterThreadPanic)?
}

fn sha256_hasher() -> Sha256Context {
    Sha256::new()
}

fn sha256_digest(hasher: Sha256Context) -> String {
    format!("sha256:{}", hex::encode(hasher.finalize()))
}

fn invalid_data(message: &'static str) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, message)
}

fn usize_to_u32_saturating(value: usize) -> u32 {
    u32::try_from(value).unwrap_or(u32::MAX)
}

fn encoder_workers(available_parallelism: usize) -> u32 {
    usize_to_u32_saturating(available_parallelism).clamp(1, MAX_ENCODER_WORKERS)
}

fn usize_to_u64_saturating(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

#[cfg(test)]
fn sha256_bytes(bytes: &[u8]) -> String {
    format!("sha256:{}", hex::encode(Sha256::digest(bytes)))
}

fn tmp_path(path: &Path) -> PathBuf {
    // Each decompress writes to a process-unique tmp file so that two
    // concurrent decompresses to the same target — possible when a cancelled
    // future's spawn_blocking is still running and a fresh caller arrives —
    // don't truncate each other's writes mid-stream. Either rename wins the
    // race; both writers produce byte-identical output, so the surviving
    // `rootfs.ext4` is valid.
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let pid = std::process::id();
    path.with_file_name(format!(
        "{}.{pid}-{n}.tmp",
        path.file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("rootfs")
    ))
}

#[cfg(test)]
mod tests;
