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

#[derive(serde::Serialize, serde::Deserialize)]
struct Receipt {
    attempt: i32,
    integrity: Integrity,
}

pub(crate) fn receipt(job: &Job) -> Result<Option<Integrity>, JobError> {
    let value: serde_json::Value = serde_json::from_str(&job.payload)
        .map_err(|e| JobError::Permanent(format!("invalid transfer payload: {e}")))?;
    let Some(value) = value.get("completion_integrity") else {
        return Ok(None);
    };
    let receipt: Receipt = serde_json::from_value(value.clone())
        .map_err(|e| JobError::Permanent(format!("invalid completion receipt: {e}")))?;
    if receipt.attempt < 1 || receipt.attempt > job.attempts {
        return Err(JobError::Permanent("invalid completion attempt".into()));
    }
    Ok(Some(receipt.integrity))
}

impl CompletionStore {
    pub(crate) fn record(&self, job: &Job, integrity: Integrity) -> Result<(), JobError> {
        match self {
            #[cfg(test)]
            Self::UnpersistedTest => Ok(()),
            Self::Database(db) => {
                let db = db
                    .lock()
                    .map_err(|_| JobError::Retriable("transfer database lock poisoned".into()))?;
                let current = pplx_space_common_jobs::db::get_job(&db, &job.job_id)
                    .map_err(|e| JobError::Retriable(format!("read transfer receipt: {e}")))?
                    .ok_or_else(|| JobError::Retriable("transfer no longer exists".into()))?;
                // The worker and submitter use this same connection mutex. Fence
                // cancellation, attempt ownership, and any contract replacement.
                if current.status != "running"
                    || current.attempts != job.attempts
                    || current.lease_owner != job.lease_owner
                    || current.payload != job.payload
                {
                    return Err(JobError::Retriable(
                        "transfer ownership changed before receipt".into(),
                    ));
                }
                let mut payload: serde_json::Value = serde_json::from_str(&current.payload)
                    .map_err(|e| JobError::Permanent(format!("invalid transfer payload: {e}")))?;
                let encoded = serde_json::to_value(Receipt {
                    attempt: job.attempts,
                    integrity,
                })
                .map_err(|e| JobError::Permanent(format!("encode completion receipt: {e}")))?;
                payload
                    .as_object_mut()
                    .ok_or_else(|| JobError::Permanent("transfer payload is not an object".into()))?
                    .insert("completion_integrity".into(), encoded);
                let updated = pplx_space_common_jobs::db::update_active_job_payload(
                    &db,
                    &job.job_id,
                    &payload.to_string(),
                    current.priority,
                )
                .map_err(|e| JobError::Retriable(format!("persist transfer receipt: {e}")))?;
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
            sha256: self
                .context
                .finish()
                .as_ref()
                .try_into()
                .expect("SHA256 is 32 bytes"),
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

pub(super) async fn verify_file(
    path: &Path,
    expected: Integrity,
    cancellation: &CancellationToken,
) -> Result<(), JobError> {
    let size = tokio::fs::metadata(path)
        .await
        .map_err(|e| JobError::Retriable(format!("stat transfer integrity: {e}")))?
        .len();
    if size != expected.size_bytes || hash_file(path, cancellation).await? != expected {
        return Err(JobError::Permanent(
            "downloaded artifact does not match its expected content identity".into(),
        ));
    }
    Ok(())
}
