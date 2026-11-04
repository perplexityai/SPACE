//! The writers a pulled stream decodes into: optional zstd, decryption, and the streamed-pull
//! admission.

use super::*;

/// Buffered writer size between the (optional) zstd decoder and the
/// destination file when decrypt + decompress run back-to-back on the
/// pull side. Match the decoder output window to batch file writes and their
/// accounting. Both buffers remain bounded per admitted decoder.
pub(super) const DECRYPT_PULL_WRITE_BUF_BYTES: usize = 1024 * 1024;

/// A stream destination whose buffered state must be finalized after its
/// upstream decoder reaches end-of-stream.
pub(super) trait CompletingWrite: Write {
    fn complete(self) -> std::io::Result<()>;
}

impl<W: Write + ?Sized> CompletingWrite for &mut W {
    fn complete(self) -> std::io::Result<()> {
        self.flush()
    }
}

/// Pull-side writer adapter that peeks the first four bytes written
/// into it, compares against the zstd magic, and either routes the
/// stream through a `zstd::stream::write::Decoder` (if the magic
/// matches AND `decompress_if_zstd` is true) or passes the bytes
/// through to the inner sink unchanged.
///
/// Lets `decrypt_stream_blocking` feed straight into this writer so a
/// pull of an encrypted-and-compressed object decrypts + decompresses
/// in one streaming pass: no `.plaintext.tmp` intermediate file, no
/// re-read of the decrypted bytes for the magic sniff. The decision
/// is final after the first four bytes — empty or sub-4-byte writes
/// keep the bytes pending until enough has arrived to decide, and
/// `finish` flushes any still-pending bytes through the passthrough
/// path so a sub-4-byte plaintext (never zstd-magic) still round-trips
/// correctly.
pub(super) struct MaybeZstdDecodingWriter<Input, Output, Select>
where
    Input: Write,
    Output: Write,
    Select: FnOnce(Input, bool) -> std::io::Result<Output>,
{
    pub(super) state: MaybeZstdState<Input, Output>,
    pub(super) decompress_if_zstd: bool,
    pub(super) pending: Vec<u8>,
    pub(super) select_sink: Option<Select>,
}

pub(super) enum MaybeZstdState<Input: Write, Output: Write> {
    /// First four bytes not yet seen; sink is parked here until we can
    /// decide whether to wrap it with a zstd decoder.
    Init(Input),
    /// First four bytes were the zstd magic and the caller wanted
    /// transparent decompress; subsequent writes flow through the
    /// decoder. Built on the raw zio writer (not
    /// `stream::write::Decoder`) because only the zio writer exposes the
    /// fallible end-of-stream `finish` that rejects a truncated frame —
    /// the higher-level decoder can only flush, which silently accepts
    /// incomplete input.
    Decoding(
        zstd::stream::zio::Writer<std::io::BufWriter<Output>, zstd::stream::raw::Decoder<'static>>,
    ),
    /// First four bytes either weren't the zstd magic, or the caller
    /// asked for raw (`raw = true`); subsequent writes go straight to
    /// the buffered sink.
    Passthrough(std::io::BufWriter<Output>),
    /// Transition slot: `state` holds this value briefly while we
    /// move the inner writer between variants. Should never be the
    /// observed state at the start or end of a `Write` call.
    Transient,
}

/// A `BufWriter` flush that failed while finishing a
/// [`MaybeZstdDecodingWriter`]. The sink's error stays the source, so a
/// typed sink error such as a decode output-cap overflow survives the wrap.
#[derive(Debug, thiserror::Error)]
#[error("flush BufWriter at {stage}: {source}")]
pub(super) struct BufferedFlushError {
    pub(super) stage: &'static str,
    #[source]
    pub(super) source: std::io::Error,
}

impl BufferedFlushError {
    pub(super) fn io<W>(stage: &'static str, error: std::io::IntoInnerError<W>) -> std::io::Error {
        std::io::Error::other(Self {
            stage,
            source: error.into_error(),
        })
    }
}

#[cfg(test)]
pub(super) fn identity_decoding_sink<W: Write>(
    sink: W,
    _decompressing: bool,
) -> std::io::Result<W> {
    Ok(sink)
}

#[cfg(test)]
impl<W: Write> MaybeZstdDecodingWriter<W, W, fn(W, bool) -> std::io::Result<W>> {
    pub(super) fn new(sink: W, decompress_if_zstd: bool) -> Self {
        Self::with_sink_selector(sink, decompress_if_zstd, identity_decoding_sink::<W>)
    }
}

impl<Input, Output, Select> MaybeZstdDecodingWriter<Input, Output, Select>
where
    Input: Write,
    Output: Write,
    Select: FnOnce(Input, bool) -> std::io::Result<Output>,
{
    pub(super) fn with_sink_selector(
        sink: Input,
        decompress_if_zstd: bool,
        select_sink: Select,
    ) -> Self {
        Self {
            state: MaybeZstdState::Init(sink),
            decompress_if_zstd,
            pending: Vec::with_capacity(ZSTD_MAGIC.len()),
            select_sink: Some(select_sink),
        }
    }

    pub(super) fn select_sink(
        &mut self,
        sink: Input,
        decompressing: bool,
    ) -> std::io::Result<Output> {
        self.select_sink
            .take()
            .ok_or_else(|| std::io::Error::other("zstd sink selector already consumed"))?(
            sink,
            decompressing,
        )
    }

    /// Consume the writer. Returns `true` iff the stream was routed
    /// through a zstd decoder (i.e. transparent decompression actually
    /// happened). Flushes the trailing buffer and finalises the
    /// decoder and destination when applicable.
    pub(super) fn finish(mut self) -> std::io::Result<bool>
    where
        Output: CompletingWrite,
    {
        // If we never saw 4 bytes of plaintext (sub-magic-length input,
        // or fully empty), flush the pending bytes through the
        // passthrough path so the file at least contains whatever we
        // did receive.
        let pending = std::mem::take(&mut self.pending);
        match std::mem::replace(&mut self.state, MaybeZstdState::Transient) {
            MaybeZstdState::Init(sink) => {
                let mut sink = self.select_sink(sink, false)?;
                sink.write_all(&pending)?;
                sink.complete()?;
                Ok(false)
            }
            MaybeZstdState::Decoding(mut decoder) => {
                // Fallible end-of-stream finalization FIRST: the raw
                // decoder rejects a truncated or unterminated zstd frame
                // with UnexpectedEof ("incomplete frame"). Flushing alone
                // would silently accept partial input and commit corrupt
                // decoded output. Then recover the BufWriter and
                // explicitly flush it through to the destination file —
                // without the explicit BufWriter::into_inner the trailing
                // bytes would only land on disk via Drop, and a Drop
                // error would be silently swallowed instead of surfaced
                // as a JobError::Retriable.
                decoder.finish()?;
                let (buf_writer, _decoder) = decoder.into_inner();
                let sink = buf_writer
                    .into_inner()
                    .map_err(|e| BufferedFlushError::io("decoder finish", e))?;
                sink.complete()?;
                Ok(true)
            }
            MaybeZstdState::Passthrough(buf_writer) => {
                let sink = buf_writer
                    .into_inner()
                    .map_err(|e| BufferedFlushError::io("stream finish", e))?;
                sink.complete()?;
                Ok(false)
            }
            MaybeZstdState::Transient => Err(std::io::Error::other(
                "MaybeZstdDecodingWriter::finish observed transient state",
            )),
        }
    }
}

impl<Input, Output, Select> Write for MaybeZstdDecodingWriter<Input, Output, Select>
where
    Input: Write,
    Output: Write,
    Select: FnOnce(Input, bool) -> std::io::Result<Output>,
{
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        // Decide once: accumulate up to 4 bytes in `pending`, then
        // transition state and dispatch the remaining bytes of this
        // write call into the chosen sink.
        if matches!(self.state, MaybeZstdState::Init(_)) {
            let needed = ZSTD_MAGIC.len() - self.pending.len();
            let take = needed.min(buf.len());
            let (prefix_chunk, suffix) = buf.split_at(take);
            self.pending.extend_from_slice(prefix_chunk);
            if self.pending.len() < ZSTD_MAGIC.len() {
                return Ok(take);
            }
            let is_zstd = self.pending == ZSTD_MAGIC;
            let want_decompress = self.decompress_if_zstd && is_zstd;
            let prefix = std::mem::take(&mut self.pending);
            let sink = match std::mem::replace(&mut self.state, MaybeZstdState::Transient) {
                MaybeZstdState::Init(sink) => sink,
                _ => unreachable!("matches!(.., Init) above"),
            };
            let sink = self.select_sink(sink, want_decompress)?;
            if want_decompress {
                let buf_writer =
                    std::io::BufWriter::with_capacity(DECRYPT_PULL_WRITE_BUF_BYTES, sink);
                let raw_decoder = zstd::stream::raw::Decoder::new()
                    .map_err(|e| std::io::Error::other(format!("zstd decoder init: {e}")))?;
                let mut decoder = zstd::stream::zio::Writer::new_with_capacity(
                    buf_writer,
                    raw_decoder,
                    DECRYPT_PULL_WRITE_BUF_BYTES,
                );
                decoder.write_all(&prefix)?;
                if !suffix.is_empty() {
                    decoder.write_all(suffix)?;
                }
                self.state = MaybeZstdState::Decoding(decoder);
            } else {
                let mut buf_writer =
                    std::io::BufWriter::with_capacity(DECRYPT_PULL_WRITE_BUF_BYTES, sink);
                buf_writer.write_all(&prefix)?;
                if !suffix.is_empty() {
                    buf_writer.write_all(suffix)?;
                }
                self.state = MaybeZstdState::Passthrough(buf_writer);
            }
            return Ok(buf.len());
        }

        match &mut self.state {
            MaybeZstdState::Decoding(decoder) => decoder.write(buf),
            MaybeZstdState::Passthrough(buf_writer) => buf_writer.write(buf),
            MaybeZstdState::Init(_) | MaybeZstdState::Transient => unreachable!(
                "Init handled above; Transient never observed outside the state-swap window"
            ),
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        match &mut self.state {
            MaybeZstdState::Init(sink) => sink.flush(),
            MaybeZstdState::Decoding(decoder) => decoder.flush(),
            MaybeZstdState::Passthrough(buf_writer) => buf_writer.flush(),
            MaybeZstdState::Transient => Ok(()),
        }
    }
}

pub(super) enum DecryptedFileSinkInner {
    Dense(std::fs::File),
    Classified(Box<SparseFileWriter>),
}

pub(super) struct DecryptedFileSink {
    pub(super) inner: DecryptedFileSinkInner,
    pub(super) writeback: StreamWriteback,
    pub(super) materialized_through: u64,
}

impl DecryptedFileSink {
    pub(super) fn new(
        file: std::fs::File,
        mode: MaterializationMode,
        block_bytes: usize,
        candidate_observer: Arc<std::sync::atomic::AtomicU64>,
        writeback_coordinator: Option<Arc<StreamWritebackCoordinator>>,
        timing: Option<Arc<StreamPullTiming>>,
        priority: JobPriority,
    ) -> std::io::Result<Self> {
        let inner = match mode {
            MaterializationMode::Disabled | MaterializationMode::Shadow => {
                DecryptedFileSinkInner::Dense(file)
            }
            MaterializationMode::Enabled => DecryptedFileSinkInner::Classified(Box::new(
                SparseFileWriter::with_observer(file, block_bytes, candidate_observer)?,
            )),
        };
        Ok(Self {
            inner,
            writeback: StreamWriteback::new(writeback_coordinator, timing, priority),
            materialized_through: 0,
        })
    }

    pub(super) fn expose_classified_debt_before_blocking(&mut self) -> std::io::Result<()> {
        let DecryptedFileSinkInner::Classified(writer) = &mut self.inner else {
            return Ok(());
        };
        writer.materialize_complete_blocks_if_buffered_at_least(1)?;
        self.materialized_through = writer.materialized_through();
        let materialization = writer.take_materialization();
        self.writeback.classify(writer.file(), materialization)
    }
}

impl CompletingWrite for DecryptedFileSink {
    fn complete(self) -> std::io::Result<()> {
        let Self {
            inner,
            mut writeback,
            materialized_through: _,
        } = self;
        let flush_started_at = Instant::now();
        let flush_cpu_started_at = current_thread_cpu_us();
        let result: std::io::Result<(std::fs::File, Option<SparseMaterialization>)> = match inner {
            DecryptedFileSinkInner::Dense(mut file) => {
                file.flush()?;
                Ok((file, None))
            }
            DecryptedFileSinkInner::Classified(writer) => {
                let (file, _, materialization) = writer.finish_with_stats_and_materialization()?;
                Ok((file, Some(materialization)))
            }
        };
        if let Some(timing) = &writeback.timing {
            StreamPullTiming::add_wall_and_thread_cpu(
                &timing.output_writer_us,
                &timing.output_writer_cpu_us,
                flush_started_at,
                flush_cpu_started_at,
            );
        }
        let (file, materialization) = result?;
        if let Some(materialization) = materialization {
            writeback.classify(&file, materialization)?;
        }
        writeback.flush_pending(&file)?;
        Ok(())
    }
}

impl Write for DecryptedFileSink {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        if !self.writeback.try_reserve(buf.len())? {
            if matches!(&self.inner, DecryptedFileSinkInner::Classified(_)) {
                self.expose_classified_debt_before_blocking()?;
            }
            let file = match &self.inner {
                DecryptedFileSinkInner::Dense(file) => file,
                DecryptedFileSinkInner::Classified(writer) => writer.file(),
            };
            self.writeback.flush_pending(file)?;
            self.writeback.reserve(buf.len())?;
        }
        let write_started_at = Instant::now();
        let write_cpu_started_at = current_thread_cpu_us();
        let write_offset = self.materialized_through;
        let result = match &mut self.inner {
            DecryptedFileSinkInner::Dense(file) => file.write(buf).and_then(|written| {
                self.materialized_through =
                    self.materialized_through
                        .checked_add(written as u64)
                        .ok_or_else(|| std::io::Error::other("streamed output exceeds u64::MAX"))?;
                Ok((written, None))
            }),
            DecryptedFileSinkInner::Classified(writer) => writer.write(buf).and_then(|written| {
                writer.materialize_complete_blocks_if_buffered_at_least(
                    STREAM_WRITEBACK_WINDOW_BYTES,
                )?;
                self.materialized_through = writer.materialized_through();
                Ok((written, Some(writer.take_materialization())))
            }),
        };
        if let Some(timing) = &self.writeback.timing {
            StreamPullTiming::add_wall_and_thread_cpu(
                &timing.output_writer_us,
                &timing.output_writer_cpu_us,
                write_started_at,
                write_cpu_started_at,
            );
        }
        let (written, materialization) = match result {
            Ok(result) => result,
            Err(error) => {
                self.writeback.release_unwritten(buf.len());
                return Err(error);
            }
        };
        if let Some(timing) = &self.writeback.timing {
            StreamPullTiming::add(&timing.decoded_output_bytes, written as u64);
        }
        self.writeback.record_written(written);
        self.writeback
            .release_unwritten(buf.len().saturating_sub(written));
        let file = match &self.inner {
            DecryptedFileSinkInner::Dense(file) => file,
            DecryptedFileSinkInner::Classified(writer) => writer.file(),
        };
        if let Some(materialization) = materialization {
            self.writeback.classify(file, materialization)?;
        } else if written != 0 {
            self.writeback
                .classify_dense(file, write_offset, written as u64)?;
        }
        Ok(written)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        let flush_started_at = Instant::now();
        let flush_cpu_started_at = current_thread_cpu_us();
        let result = match &mut self.inner {
            DecryptedFileSinkInner::Dense(file) => file.flush().map(|()| None),
            DecryptedFileSinkInner::Classified(writer) => {
                writer.flush()?;
                self.materialized_through = writer.materialized_through();
                Ok(Some(writer.take_materialization()))
            }
        };
        if let Some(timing) = &self.writeback.timing {
            StreamPullTiming::add_wall_and_thread_cpu(
                &timing.output_writer_us,
                &timing.output_writer_cpu_us,
                flush_started_at,
                flush_cpu_started_at,
            );
        }
        let materialization = result?;
        let file = match &self.inner {
            DecryptedFileSinkInner::Dense(file) => file,
            DecryptedFileSinkInner::Classified(writer) => writer.file(),
        };
        if let Some(materialization) = materialization {
            self.writeback.classify(file, materialization)?;
        }
        self.writeback.flush_pending(file)?;
        Ok(())
    }
}

pub(super) type StreamSinkSelector =
    Box<dyn FnOnce(std::fs::File, bool) -> std::io::Result<DecryptedFileSink> + Send>;

/// Adapts [`MaybeZstdDecodingWriter`] to the object store's
/// [`pplx_space_object_store::FinishingWrite`] so the ranged download's
/// sequential writer decodes inline (one pass: fetch, decode, write).
/// Sparse selection happens only after the zstd-magic decision so a
/// passthrough stream remains dense.
pub(super) struct StreamDecodeSink {
    pub(super) inner: MaybeZstdDecodingWriter<std::fs::File, DecryptedFileSink, StreamSinkSelector>,
    pub(super) timing: Arc<StreamPullTiming>,
}

impl StreamDecodeSink {
    pub(super) fn new(
        file: std::fs::File,
        mode_if_decompressed: MaterializationMode,
        block_bytes: usize,
        candidate_observer: Arc<std::sync::atomic::AtomicU64>,
        writeback_coordinator: Arc<StreamWritebackCoordinator>,
        timing: Arc<StreamPullTiming>,
        priority: JobPriority,
    ) -> Self {
        let timing_for_sink = Arc::clone(&timing);
        let select_sink: StreamSinkSelector = Box::new(move |file, decompressing| {
            let mode = if decompressing {
                mode_if_decompressed
            } else {
                MaterializationMode::Disabled
            };
            DecryptedFileSink::new(
                file,
                mode,
                block_bytes,
                candidate_observer,
                Some(writeback_coordinator),
                Some(timing_for_sink),
                priority,
            )
        });
        Self {
            inner: MaybeZstdDecodingWriter::with_sink_selector(file, true, select_sink),
            timing,
        }
    }
}

impl Write for StreamDecodeSink {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let write_started_at = Instant::now();
        let write_cpu_started_at = current_thread_cpu_us();
        let result = self.inner.write(buf);
        StreamPullTiming::add_wall_and_thread_cpu(
            &self.timing.decode_writer_us,
            &self.timing.decode_writer_cpu_us,
            write_started_at,
            write_cpu_started_at,
        );
        if let Ok(written) = &result {
            StreamPullTiming::add(&self.timing.compressed_input_bytes, *written as u64);
        }
        result
    }

    fn flush(&mut self) -> std::io::Result<()> {
        let flush_started_at = Instant::now();
        let flush_cpu_started_at = current_thread_cpu_us();
        let result = self.inner.flush();
        StreamPullTiming::add_wall_and_thread_cpu(
            &self.timing.decode_writer_us,
            &self.timing.decode_writer_cpu_us,
            flush_started_at,
            flush_cpu_started_at,
        );
        result
    }
}

impl pplx_space_object_store::FinishingWrite for StreamDecodeSink {
    fn finish(self: Box<Self>) -> std::io::Result<bool> {
        let StreamDecodeSink { inner, timing } = *self;
        let finish_started_at = Instant::now();
        let finish_cpu_started_at = current_thread_cpu_us();
        let result = inner.finish();
        StreamPullTiming::add_wall_and_thread_cpu(
            &timing.decode_writer_us,
            &timing.decode_writer_cpu_us,
            finish_started_at,
            finish_cpu_started_at,
        );
        result
    }
}

/// Codec admission bridge for streaming-decode pulls: one permit spans the
/// whole overlapped download+decode. The streamed path only runs for
/// ranged-size objects, whose decode cannot be proven small before the
/// bytes arrive (the frame header is not on disk yet), so admission is
/// unconditional — the conservative direction, and exactly what keeps the
/// highly-compressible multi-GiB snapshot class bounded. The artifact-class
/// bypass in `decode_admission` does not apply here: a stream needs more
/// than one download chunk of wire bytes (8 MiB minimum, 32 MiB by default),
/// far above any legitimate `metadata` or `vmstate` object, so such an object
/// is already suspect and keeps its permit.
pub(super) struct StreamedPullAdmission<'a> {
    pub(super) executor: &'a TransferExecutor,
    pub(super) kind: &'a str,
    pub(super) artifact: &'static str,
    pub(super) mode: MaterializationMode,
    pub(super) priority: JobPriority,
}

#[async_trait::async_trait]
impl pplx_space_object_store::DecodeAdmission for StreamedPullAdmission<'_> {
    async fn admit(
        &self,
        _object_size: u64,
    ) -> Result<
        Option<pplx_space_object_store::DecodeGuard>,
        pplx_space_object_store::DecodeAdmissionError,
    > {
        let wait_started = Instant::now();
        // Typed so the failure crosses the object-store boundary as a source
        // error rather than a string.
        let permit = self
            .executor
            .codec
            .acquire(self.executor.codec_priority(self.priority))
            .await
            .map_err(pplx_space_object_store::DecodeAdmissionError::admission)?;
        emit_transfer_phase_duration(
            self.kind,
            self.artifact,
            TransferPhase::DecompressWait,
            self.mode,
            wait_started,
        );
        Ok(Some(Box::new(permit)))
    }
}
