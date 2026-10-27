//! Decode the bounded result of the host-owned CUDA scrub candidate.
//!
//! Coverage is not qualification. The owning host procedure must additionally
//! authenticate its inputs, reconcile the exact allocation and process, prove
//! holder absence, and establish its identity/firmware and reuse guarantees.

use pplx_space_common_core::types::SandboxActivationAttemptId;
use serde::Deserialize;

pub const MAX_SANITIZER_CONSOLE_BYTES: usize = 256 * 1024;
const MAX_REPORT_BYTES: usize = 4096;
const PREFIX: &[u8] = b"SPACE_GPU_SANITIZER_REPORT_V1 ";
const BOOT_FAILURE: &[u8] = b"SPACE_GPU_SANITIZER_BOOT_FAILED";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CudaScrubLimits {
    minimum_bytes: u64,
    maximum_bytes: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CudaAllocationCoverage(WireCoverage);

#[derive(Clone, Copy, Debug, Eq, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
struct WireCoverage {
    reported_total_bytes: u64,
    free_bytes_before: u64,
    overwritten_and_verified_bytes: u64,
    uncovered_reported_bytes: u64,
    allocation_count: u32,
    verified_passes: u32,
}

impl CudaAllocationCoverage {
    pub fn overwritten_and_verified_bytes(self) -> u64 {
        self.0.overwritten_and_verified_bytes
    }

    pub fn uncovered_reported_bytes(self) -> u64 {
        self.0.uncovered_reported_bytes
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ScrubReportError {
    #[error("invalid compiled CUDA scrub coverage limits")]
    Limits,
    #[error("sanitizer console exceeds {MAX_SANITIZER_CONSOLE_BYTES} bytes")]
    ConsoleLimit,
    #[error("sanitizer report exceeds {MAX_REPORT_BYTES} bytes")]
    ReportLimit,
    #[error("sanitizer emitted no completion report")]
    Missing,
    #[error("sanitizer emitted more than one completion report")]
    Multiple,
    #[error("sanitizer guest bootstrap failed")]
    Bootstrap,
    #[error("invalid sanitizer report: {0}")]
    Decode(#[from] serde_json::Error),
    #[error("sanitizer report belongs to a different execution attempt")]
    Attempt,
    #[error("sanitizer reported failure: {0}")]
    Failed(String),
    #[error("sanitizer coverage does not satisfy the pinned procedure")]
    Coverage,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
enum CoverageKind {
    CudaAllocations,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case", deny_unknown_fields)]
enum Report {
    ScrubCompleted {
        attempt: SandboxActivationAttemptId,
        coverage_kind: CoverageKind,
        coverage: WireCoverage,
    },
    Failed {
        attempt: SandboxActivationAttemptId,
        coverage_kind: CoverageKind,
        error: String,
    },
}

impl CudaScrubLimits {
    pub fn new(minimum_bytes: u64, maximum_bytes: u64) -> Result<Self, ScrubReportError> {
        if minimum_bytes == 0
            || minimum_bytes > maximum_bytes
            || maximum_bytes > 1024 * 1024 * 1024 * 1024
        {
            return Err(ScrubReportError::Limits);
        }
        Ok(Self {
            minimum_bytes,
            maximum_bytes,
        })
    }

    /// Call only after the exact host-owned execution is terminal. The caller
    /// must bound its file/stream read too; this bound cannot undo an oversized
    /// allocation made before entry. Process exit status is deliberately absent:
    /// a guest bootstrap failure can power off with a successful VMM exit.
    pub fn verify_report(
        self,
        attempt: SandboxActivationAttemptId,
        console: &[u8],
    ) -> Result<CudaAllocationCoverage, ScrubReportError> {
        if console.len() > MAX_SANITIZER_CONSOLE_BYTES {
            return Err(ScrubReportError::ConsoleLimit);
        }
        let mut report = None;
        for line in console.split(|byte| *byte == b'\n') {
            if line.starts_with(BOOT_FAILURE) {
                return Err(ScrubReportError::Bootstrap);
            }
            if let Some(raw) = line.strip_prefix(PREFIX) {
                if raw.len() > MAX_REPORT_BYTES {
                    return Err(ScrubReportError::ReportLimit);
                }
                if report.replace(raw).is_some() {
                    return Err(ScrubReportError::Multiple);
                }
            }
        }
        let coverage =
            match serde_json::from_slice::<Report>(report.ok_or(ScrubReportError::Missing)?)? {
                Report::ScrubCompleted {
                    attempt: observed,
                    coverage_kind: CoverageKind::CudaAllocations,
                    coverage,
                } => {
                    if observed != attempt {
                        return Err(ScrubReportError::Attempt);
                    }
                    coverage
                }
                Report::Failed {
                    attempt: observed,
                    coverage_kind: CoverageKind::CudaAllocations,
                    error,
                } => {
                    if observed != attempt {
                        return Err(ScrubReportError::Attempt);
                    }
                    return Err(ScrubReportError::Failed(error.chars().take(512).collect()));
                }
            };
        if !(self.minimum_bytes <= coverage.overwritten_and_verified_bytes
            && coverage.overwritten_and_verified_bytes <= coverage.free_bytes_before
            && coverage.free_bytes_before <= coverage.reported_total_bytes
            && coverage.reported_total_bytes <= self.maximum_bytes
            && coverage
                .reported_total_bytes
                .checked_sub(coverage.overwritten_and_verified_bytes)
                == Some(coverage.uncovered_reported_bytes)
            && (1..=1024).contains(&coverage.allocation_count)
            && coverage.verified_passes == 3)
        {
            return Err(ScrubReportError::Coverage);
        }
        Ok(CudaAllocationCoverage(coverage))
    }
}

#[cfg(test)]
mod tests;
