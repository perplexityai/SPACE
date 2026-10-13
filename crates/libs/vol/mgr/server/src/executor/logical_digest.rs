//! Logical content proof from the same reads that feed compression/encryption.
//! A partial reader never produces a proof. Publication belongs to the job's
//! fenced completion transition, not to an individual producer finishing.

use pplx_space_common_core::types::{Digest, SnapshotArtifact};
use pplx_space_common_jobs::JobError;
use sha2::{Digest as _, Sha256};
use std::io::{self, Read};
use std::sync::{Arc, Mutex};

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub(crate) struct LogicalArtifactDigest {
    pub artifact: SnapshotArtifact,
    pub digest: Digest,
    pub size_bytes: u64,
    #[serde(default)]
    pub metadata_binding_digest: Option<Digest>,
}

type CompletedDigest = (Digest, u64, Option<Digest>);

#[derive(Clone, Default)]
pub(super) struct DigestSink {
    result: Arc<Mutex<Option<CompletedDigest>>>,
    metadata: bool,
}

impl DigestSink {
    pub fn for_artifact(artifact: SnapshotArtifact) -> Self {
        Self {
            metadata: artifact == SnapshotArtifact::Metadata,
            ..Self::default()
        }
    }
    pub fn completed(&self) -> Result<(Digest, u64, Option<Digest>), JobError> {
        self.result
            .lock()
            .map_err(|_| JobError::permanent("logical digest lock poisoned"))?
            .as_ref()
            .copied()
            .ok_or_else(|| JobError::retriable("logical upload source was not read to completion"))
    }
}

pub(super) struct DigestReader<R> {
    inner: R,
    sink: Option<DigestSink>,
    hash: Sha256,
    bytes: u64,
    metadata_bytes: Vec<u8>,
}

impl<R> DigestReader<R> {
    pub fn new(inner: R, sink: Option<DigestSink>) -> Self {
        Self {
            inner,
            sink,
            hash: Sha256::new(),
            bytes: 0,
            metadata_bytes: Vec::new(),
        }
    }
    pub fn inner(&self) -> &R {
        &self.inner
    }
}

impl<R: Read> Read for DigestReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let n = self.inner.read(buf)?;
        if let Some(sink) = &self.sink {
            if n != 0 {
                let bytes = buf
                    .get(..n)
                    .ok_or_else(|| io::Error::other("reader returned an invalid byte count"))?;
                self.hash.update(bytes);
                // Bounded optional metadata proof: malformed/legacy/oversized
                // metadata does not fail ordinary uploads, but grants require it.
                if sink.metadata && self.metadata_bytes.len() <= 1024 * 1024 {
                    let remaining = (1024 * 1024 + 1) - self.metadata_bytes.len();
                    self.metadata_bytes
                        .extend(bytes.iter().take(remaining).copied());
                }
                self.bytes = self
                    .bytes
                    .checked_add(n as u64)
                    .ok_or_else(|| io::Error::other("logical upload size overflow"))?;
            } else if !buf.is_empty() {
                let digest = Digest::sha256(self.hash.clone().finalize().into());
                let binding = (sink.metadata && self.metadata_bytes.len() <= 1024 * 1024)
                    .then(|| {
                        pplx_space_common_core::types::snapshot_metadata_binding(
                            &self.metadata_bytes,
                        )
                        .ok()
                    })
                    .flatten()
                    .map(|bytes| Digest::sha256(Sha256::digest(bytes).into()));
                *sink
                    .result
                    .lock()
                    .map_err(|_| io::Error::other("logical digest lock poisoned"))? =
                    Some((digest, self.bytes, binding));
            }
        }
        Ok(n)
    }
}

#[cfg(test)]
#[path = "logical_digest_tests.rs"]
mod tests;
