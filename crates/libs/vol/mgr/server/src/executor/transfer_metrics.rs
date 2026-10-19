//! Transfer phases and the duration, throughput and decompression-allocation metrics they emit.

use super::*;

/// Closed vocabulary for the `phase` tag on transfer metrics. The string
/// form exists only at the emission boundary; everything in between
/// passes the enum so additions stay exhaustive and typos can't compile.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum TransferPhase {
    Download,
    DownloadDecode,
    Decrypt,
    DecryptDecompress,
    DecryptWait,
    Decompress,
    DecompressWait,
    CompressWait,
    Promote,
    ShadowReflink,
}

impl TransferPhase {
    pub(super) fn as_str(self) -> &'static str {
        match self {
            Self::Download => "download",
            Self::DownloadDecode => "download_decode",
            Self::Decrypt => "decrypt",
            Self::DecryptDecompress => "decrypt_decompress",
            Self::DecryptWait => "decrypt_wait",
            Self::Decompress => "decompress",
            Self::DecompressWait => "decompress_wait",
            Self::CompressWait => "compress_wait",
            Self::Promote => "promote",
            Self::ShadowReflink => "shadow_reflink",
        }
    }
}

impl std::fmt::Display for TransferPhase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

pub(super) fn emit_transfer_phase_duration(
    kind: &str,
    artifact: &str,
    phase: TransferPhase,
    mode: MaterializationMode,
    started_at: Instant,
) {
    pplx_space_metrics::volume_manager::transfer_phase_duration(
        kind,
        artifact,
        phase.as_str(),
        mode,
        started_at,
    );
}

pub(super) fn emit_transfer_throughput_mib_s(
    kind: &str,
    artifact: &str,
    phase: TransferPhase,
    mode: MaterializationMode,
    bytes: u64,
    started_at: Instant,
) {
    let elapsed = started_at.elapsed().as_secs_f64();
    if elapsed > 0.0 {
        let mib_s = (bytes as f64 / 1024.0 / 1024.0) / elapsed;
        pplx_space_metrics::volume_manager::transfer_throughput_mib_s(
            kind,
            artifact,
            phase.as_str(),
            mode,
            mib_s,
        );
    }
}

#[cfg(unix)]
pub(super) async fn observe_decompression_allocation(
    kind: &str,
    artifact: &str,
    job_id: &JobId,
    subject_id: &str,
    local_path: &Path,
    report: DecompressionReport,
) -> Option<MaterializationObservation> {
    use std::os::unix::fs::MetadataExt;

    let metadata = match tokio::fs::metadata(local_path).await {
        Ok(metadata) => metadata,
        Err(error) => {
            tracing::debug!(
                %error,
                job_id = %job_id,
                subject_id,
                local_path = %local_path.display(),
                "could not observe decompressed artifact allocation"
            );
            return None;
        }
    };
    let logical_bytes = metadata.len();
    let allocated_bytes = metadata.blocks().saturating_mul(512);
    let saved_bytes = logical_bytes.saturating_sub(allocated_bytes);
    let allocation_ratio = if logical_bytes == 0 {
        0.0
    } else {
        allocated_bytes as f64 / logical_bytes as f64
    };
    let layout = if report.mode == MaterializationMode::Enabled {
        MaterializationLayout::Sparse
    } else {
        MaterializationLayout::Dense
    };
    pplx_space_metrics::volume_manager::transfer_materialization_allocation(
        kind,
        artifact,
        report.mode,
        layout,
        logical_bytes,
        allocated_bytes,
        report.candidate_hole_bytes,
    );
    let projected_allocated_bytes = logical_bytes.saturating_sub(report.candidate_hole_bytes);
    tracing::info!(
        job_id = %job_id,
        subject_id,
        local_path = %local_path.display(),
        artifact,
        layout = %layout,
        mode = %report.mode,
        logical_bytes,
        allocated_bytes,
        candidate_hole_bytes = report.candidate_hole_bytes,
        projected_allocated_bytes,
        saved_bytes,
        allocation_ratio,
        "decompressed artifact allocation observed"
    );
    Some(MaterializationObservation {
        mode: report.mode,
        layout,
        logical_bytes,
        allocated_bytes,
        candidate_hole_bytes: report.candidate_hole_bytes,
    })
}
