//! Plaintext identities and completion receipts in the existing transfer payload.
use super::{Job, JobError};
use aws_lc_rs::digest::{Context, SHA256};
use pplx_space_volmgr_client::Integrity;
use std::io::Read;
use std::{
    path::Path,
    sync::{Arc, Mutex},
};
use tokio_util::sync::CancellationToken;

#[derive(Clone)]
pub(crate) enum CompletionStore {
    Database(Arc<Mutex<rusqlite::Connection>>),
    #[cfg(test)]
    UnpersistedTest,
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct Receipt {
    attempt: i32,
    integrity: Integrity,
}

pub(crate) fn receipt(job: &Job) -> Result<Option<Integrity>, JobError> {
    if !matches!(job.kind.as_str(), "push" | "push_btrfs_send" | "pull") {
        return Ok(None);
    }
    let payload = super::TransferPayload::from_json(&job.payload)?;
    let Some(receipt) = payload.completion_integrity else {
        return Ok(None);
    };
    if receipt.attempt < 1 || receipt.attempt > job.attempts {
        return Err(JobError::Permanent("invalid completion attempt".into()));
    }
    if job.kind == "pull" && job.status != "completed" {
        return Ok(None);
    }
    Ok(Some(receipt.integrity))
}

pub(super) fn payload_with_receipt(job: &Job, integrity: Integrity) -> Result<String, JobError> {
    // Only replace the typed receipt. Other payload fields belong to their
    // producing version; preserve even unknown fields and their JSON values.
    let mut fields: std::collections::BTreeMap<String, Box<serde_json::value::RawValue>> =
        serde_json::from_str(&job.payload)
            .map_err(|e| JobError::Permanent(format!("invalid transfer payload: {e}")))?;
    let receipt = serde_json::value::to_raw_value(&Receipt {
        attempt: job.attempts,
        integrity,
    })
    .map_err(|e| JobError::Permanent(format!("encode completion receipt: {e}")))?;
    fields.insert("completion_integrity".into(), receipt);
    serde_json::to_string(&fields)
        .map_err(|e| JobError::Permanent(format!("encode transfer payload: {e}")))
}

impl CompletionStore {
    pub(crate) fn record(&self, job: &Job, integrity: Integrity) -> Result<(), JobError> {
        if !matches!(job.kind.as_str(), "push" | "push_btrfs_send") {
            return Err(JobError::Permanent(
                "pull evidence must commit with completion".into(),
            ));
        }
        match self {
            #[cfg(test)]
            Self::UnpersistedTest => Ok(()),
            Self::Database(db) => {
                let db = db
                    .lock()
                    .map_err(|_| JobError::Retriable("transfer database lock poisoned".into()))?;
                let payload = payload_with_receipt(job, integrity)?;
                let updated =
                    pplx_space_common_jobs::db::update_job_attempt_payload(&db, job, &payload)
                        .map_err(|e| {
                            JobError::Retriable(format!("persist transfer receipt: {e}"))
                        })?;
                if !updated {
                    return Err(JobError::Retriable(
                        "transfer completed before receipt".into(),
                    ));
                }
                Ok(())
            }
        }
    }
}

pub(super) struct Hasher {
    context: Context,
    size: u64,
}
impl Hasher {
    pub(super) fn new() -> Self {
        Self {
            context: Context::new(&SHA256),
            size: 0,
        }
    }
    pub(super) fn update(&mut self, bytes: &[u8]) {
        self.context.update(bytes);
        self.size += bytes.len() as u64;
    }
    pub(super) fn finish(self) -> Integrity {
        Integrity {
            digest: pplx_space_common_core::types::Digest::sha256(
                self.context
                    .finish()
                    .as_ref()
                    .try_into()
                    .expect("SHA256 is 32 bytes"),
            ),
            size_bytes: self.size,
        }
    }
}

pub(super) async fn hash_file(
    path: &Path,
    cancellation: &CancellationToken,
) -> Result<Integrity, JobError> {
    let path = path.to_owned();
    let cancellation = cancellation.child_token();
    let _cancel_on_drop = cancellation.clone().drop_guard();
    tokio::task::spawn_blocking(move || {
        let mut file = std::fs::File::open(path)
            .map_err(|e| JobError::Retriable(format!("open transfer input for integrity: {e}")))?;
        let mut hash = Hasher::new();
        let mut buffer = vec![0; 256 * 1024];
        loop {
            if cancellation.is_cancelled() {
                return Err(JobError::Retriable("integrity check cancelled".into()));
            }
            let count = file
                .read(&mut buffer)
                .map_err(|e| JobError::Retriable(format!("read transfer integrity: {e}")))?;
            if count == 0 {
                return Ok(hash.finish());
            }
            hash.update(
                buffer
                    .get(..count)
                    .ok_or_else(|| JobError::Permanent("invalid integrity read length".into()))?,
            );
        }
    })
    .await
    .map_err(|e| JobError::Retriable(format!("join transfer integrity: {e}")))?
}

pub(super) async fn file_matches(
    path: &Path,
    expected: Integrity,
    cancellation: &CancellationToken,
) -> Result<bool, JobError> {
    if cancellation.is_cancelled() {
        return Err(JobError::Retriable("integrity check cancelled".into()));
    }
    let size = tokio::fs::metadata(path)
        .await
        .map_err(|e| JobError::Retriable(format!("stat transfer integrity: {e}")))?
        .len();
    Ok(size == expected.size_bytes && hash_file(path, cancellation).await? == expected)
}
