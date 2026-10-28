use super::*;
use crate::proto::{ContentIntegrity, TransferJob, TransferJobStatus};
use pplx_space_common_core::types::JobId;

#[test]
fn only_matching_completed_uploads_supply_receipts() {
    let id = JobId::new();
    let good = TransferJob {
        job_id: id.to_string(),
        kind: "push".into(),
        status: TransferJobStatus::Completed as i32,
        bytes_transferred: 3,
        integrity: Some(ContentIntegrity {
            digest: format!("sha256:{}", "12".repeat(32)),
            size_bytes: 3,
        }),
        ..Default::default()
    };
    for problem in [
        "none",
        "stream",
        "missing",
        "size",
        "digest",
        "job",
        "kind",
        "running",
        "failed",
        "cancelled",
    ] {
        let mut job = good.clone();
        match problem {
            "stream" => job.kind = "push_btrfs_send".into(),
            "missing" => job.integrity = None,
            "size" => job.bytes_transferred += 1,
            "digest" => job.integrity.as_mut().unwrap().digest = "unknown".into(),
            "job" => job.job_id = JobId::new().to_string(),
            "kind" => job.kind = "pull".into(),
            "running" => job.status = TransferJobStatus::Running as i32,
            "failed" => job.status = TransferJobStatus::Failed as i32,
            "cancelled" => job.status = TransferJobStatus::Cancelled as i32,
            _ => {}
        }
        for allow_stream in [false, true] {
            let result = completed_upload_integrity(&job, id, allow_stream);
            assert_eq!(
                result.is_ok(),
                problem == "none" || (problem == "stream" && allow_stream),
                "{problem}, {allow_stream}"
            );
            assert_eq!(
                matches!(result, Err(UploadReceiptError::Missing)),
                problem == "missing"
            );
        }
    }
}
