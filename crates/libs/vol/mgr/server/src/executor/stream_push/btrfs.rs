//! Encode the privileged send stream without an uncompressed staging file.
//! The terminal source frame, encoder finalization, and HEAD all gate success.
use super::*;
use pplx_space_node_mgr_client::proto::{BtrfsSendChunk, BtrfsSendLease};
use std::sync::atomic::{AtomicU64, Ordering};
use tokio_stream::StreamExt;

#[cfg(test)]
mod tests;

struct RemoteReader {
    stream: std::pin::Pin<
        Box<dyn tokio_stream::Stream<Item = Result<BtrfsSendChunk, tonic::Status>> + Send>,
    >,
    buffer: std::io::Cursor<Vec<u8>>,
    runtime: tokio::runtime::Handle,
    cancellation: CancellationToken,
    consumed: u64,
    finished: bool,
}

impl Read for RemoteReader {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        if out.is_empty() {
            return Ok(0);
        }
        loop {
            if self.cancellation.is_cancelled() {
                return Err(io::Error::other("send compression cancelled"));
            }
            let count = self.buffer.read(out)?;
            if count != 0 {
                self.consumed += count as u64;
                return Ok(count);
            }
            if self.finished {
                return Ok(0);
            }
            let chunk = self.runtime.block_on(async {
                tokio::select! {
                    biased;
                    _ = self.cancellation.cancelled() => Err(io::Error::other("send compression cancelled")),
                    next = self.stream.next() => next.transpose().map_err(io::Error::other),
                }
            })?.ok_or_else(|| io::Error::other("send stream ended without completion"))?;
            if let Some(total) = chunk.completed_bytes {
                if total != self.consumed || !chunk.data.is_empty() {
                    return Err(io::Error::other("invalid send completion length"));
                }
                // Also require successful gRPC trailers and reject extra frames.
                let trailing = self.runtime.block_on(async {
                    tokio::select! {
                        biased;
                        _ = self.cancellation.cancelled() => Err(io::Error::other("send compression cancelled")),
                        next = self.stream.next() => next.transpose().map_err(io::Error::other),
                    }
                })?;
                if trailing.is_some() {
                    return Err(io::Error::other("data after send completion"));
                }
                self.finished = true;
            } else {
                if chunk.data.is_empty() || chunk.data.len() > 256 * 1024 {
                    return Err(io::Error::other("invalid send data frame"));
                }
                self.buffer = std::io::Cursor::new(chunk.data);
            }
        }
    }
}

impl TransferExecutor {
    pub(in crate::executor) async fn execute_btrfs_push(
        &self,
        job: &Job,
        creds: &object_store::ObjectCredentials,
        target: ObjectTarget<'_>,
        cancellation: CancellationToken,
    ) -> Result<i64, JobError> {
        let payload: crate::btrfs_push::Payload = job.payload_as()?;
        let cancellation = cancellation.child_token();
        let _cancel_on_drop = cancellation.clone().drop_guard();
        let streaming = self
            .stream_push
            .as_ref()
            .filter(|_| self.config.compress)
            .zip(self.store.streaming_upload());
        // Share the existing live-producer and codec limits. A budget-disabled
        // retry uses a temp file and only the codec permit; it cannot strand a
        // durable source accepted while streaming was enabled.
        let producer = if let Some((config, _)) = streaming {
            Some(tokio::select! {
                biased;
                _ = cancellation.cancelled() => return Err(JobError::Retriable("send admission cancelled".into())),
                permit = config.producers.clone().acquire_owned() => permit.map_err(|_| JobError::Retriable("send admission closed".into()))?,
            })
        } else {
            None
        };
        let mut client = self
            .node_manager
            .clone()
            .unwrap_or_else(pplx_space_node_mgr_client::lazy);
        // Reader admission is capacity, not a failed upload attempt. Wait
        // within the worker's transfer deadline without holding a codec slot.
        let mut delay = Duration::from_millis(50);
        let stream = loop {
            let response = tokio::select! {
                biased;
                _ = cancellation.cancelled() => return Err(JobError::Retriable("send admission cancelled".into())),
                response = client.btrfs_read_send_source(BtrfsSendLease { lease_id: payload.send_lease.to_string() }) => response,
            };
            match response {
                Ok(response) => break response.into_inner(),
                Err(error) if error.code() == tonic::Code::ResourceExhausted => {
                    tokio::select! {
                        biased;
                        _ = cancellation.cancelled() => return Err(JobError::Retriable("send admission cancelled".into())),
                        _ = tokio::time::sleep(delay) => {},
                    }
                    delay = (delay * 2).min(Duration::from_secs(1));
                }
                Err(error) => return Err(crate::btrfs_push::classify(error)),
            }
        };
        let codec = tokio::select! {
            biased;
            _ = cancellation.cancelled() => return Err(JobError::Retriable("send admission cancelled".into())),
            slot = self.acquire_codec_slot(job.priority) => slot?,
        };
        let input = RemoteReader {
            stream: Box::pin(stream),
            buffer: std::io::Cursor::new(Vec::new()),
            runtime: tokio::runtime::Handle::current(),
            cancellation: cancellation.clone(),
            consumed: 0,
            finished: false,
        };
        let uses_streaming = streaming.is_some();
        let (logical, wire) = if let Some((config, uploader)) = streaming {
            let admission = ProducerAdmission {
                codec,
                _producer: producer.expect("streaming acquired producer admission"),
            };
            let (reader, writer) = tokio::io::duplex(PIPE_BYTES);
            let count = Arc::new(AtomicU64::new(0));
            let actual = count.clone();
            let token = cancellation.clone();
            let task = tokio::task::spawn_blocking(move || {
                let started = Instant::now();
                let mut input = input;
                let mut output = PipeWriter {
                    writer,
                    runtime: tokio::runtime::Handle::current(),
                    cancellation: token,
                    admission,
                    waited: Duration::ZERO,
                };
                let result = {
                    let mut buffered = std::io::BufWriter::with_capacity(PIPE_BYTES, &mut output);
                    zstd::stream::copy_encode(&mut input, &mut buffered, ZSTD_LEVEL)
                        .and_then(|()| buffered.flush())
                };
                if result.is_ok() {
                    actual.store(input.consumed, Ordering::Release);
                }
                stream_push_producer("rootfs_btrfs_send", started, output.waited, result.is_ok());
                result
            });
            let source = UploadSource::new(
                reader,
                async move { task.await.map_err(io::Error::other)? },
                cancellation.clone(),
            );
            // Unknown send length cannot use an estimated size as an integrity
            // check. Bound the encoded object by the actual multipart capacity.
            let options = StreamingUploadOptions::new(
                config.part_bytes,
                16,
                config.part_bytes as u64 * 10_000,
            )
            .map_err(|error| JobError::Permanent(error.to_string()))?;
            let wire = uploader
                .put_stream(creds, target, source, config.budget.clone(), options)
                .await
                .map_err(|error| match error {
                    StreamingUploadError::Store(error) => self.object_store_error(job, &error),
                    StreamingUploadError::InvalidConfiguration(_)
                    | StreamingUploadError::SizeLimit { .. } => {
                        JobError::Permanent(error.to_string())
                    }
                    error => JobError::Retriable(error.to_string()),
                })?;
            (count.load(Ordering::Acquire), wire)
        } else {
            let directory = self.shadow_staging_dir.clone();
            let compress = self.config.compress;
            let (temp, logical) = tokio::task::spawn_blocking(move || -> io::Result<_> {
                let _codec = codec;
                let mut input = input;
                let mut temp = tempfile::Builder::new()
                    .prefix("send-")
                    .suffix(".zst.tmp")
                    .tempfile_in(directory)?;
                if compress {
                    zstd::stream::copy_encode(&mut input, &mut temp, ZSTD_LEVEL)?;
                } else {
                    std::io::copy(&mut input, &mut temp)?;
                }
                temp.flush()?;
                Ok((temp, input.consumed))
            })
            .await
            .map_err(|error| JobError::Retriable(error.to_string()))?
            .map_err(|error| JobError::Retriable(error.to_string()))?;
            let wire = temp
                .as_file()
                .metadata()
                .map_err(|error| JobError::Retriable(error.to_string()))?
                .len();
            use pplx_space_metrics::volume_manager::{
                PushPayloadEncoding, PushPayloadPhase, push_payload,
            };
            push_payload(
                "rootfs_btrfs_send",
                if compress {
                    PushPayloadEncoding::Zstd
                } else {
                    PushPayloadEncoding::Raw
                },
                PushPayloadPhase::Prepared,
                logical,
                wire,
            );
            self.store
                .put_file(creds, target, temp.path())
                .await
                .map_err(|error| self.object_store_error(job, &error))?;
            (logical, wire)
        };
        if uses_streaming {
            stream_push_payload(
                "rootfs_btrfs_send",
                StreamPushPhase::Uploaded,
                logical,
                wire,
            );
        }
        match self.store.head(creds, target).await {
            Ok(Some(meta)) if meta.size == wire => {
                if uses_streaming {
                    stream_push_payload(
                        "rootfs_btrfs_send",
                        StreamPushPhase::Verified,
                        logical,
                        wire,
                    );
                } else {
                    use pplx_space_metrics::volume_manager::{
                        PushPayloadEncoding, PushPayloadPhase, push_payload,
                    };
                    push_payload(
                        "rootfs_btrfs_send",
                        if self.config.compress {
                            PushPayloadEncoding::Zstd
                        } else {
                            PushPayloadEncoding::Raw
                        },
                        PushPayloadPhase::Verified,
                        logical,
                        wire,
                    );
                }
                bytes_len_to_job_i64(logical, "btrfs send size")
            }
            Ok(_) => Err(JobError::Retriable(
                "btrfs upload HEAD length mismatch or missing object".into(),
            )),
            Err(error) => Err(self.object_store_error(job, &error)),
        }
    }
}
