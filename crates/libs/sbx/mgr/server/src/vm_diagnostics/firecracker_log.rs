//! Firecracker's log file, written with `--show-level`:
//! `<time> [<instance>:<thread>:<LEVEL>] <message>`.
//!
//! Only WARN and ERROR lines are re-logged. The file is regular, so reading it
//! never affects the VMM; each poll reads at most [`MAX_READ_BYTES`].

use std::io::SeekFrom;
use std::path::PathBuf;
use std::time::SystemTime;

use pplx_space_metrics::sandbox::vm::{VmDiagnosticLevel, VmDiagnosticOutcome, VmDiagnosticSource};
use tokio::io::{AsyncReadExt, AsyncSeekExt};

use super::{LogBudget, sanitized};

const MAX_READ_BYTES: u64 = 64 * 1024;
/// A partial line longer than this is not Firecracker's and is dropped.
const MAX_LINE_BYTES: usize = 8 * 1024;
const MAX_MESSAGE_BYTES: usize = 512;

pub(super) struct LogTail {
    path: PathBuf,
    readers_started_at: SystemTime,
    /// Where the next read starts; `None` until the file is first seen.
    offset: Option<u64>,
    partial: Vec<u8>,
}

impl LogTail {
    pub(super) fn new(path: PathBuf, readers_started_at: SystemTime) -> Self {
        Self {
            path,
            readers_started_at,
            offset: None,
            partial: Vec::new(),
        }
    }

    pub(super) async fn poll(&mut self, sandbox_id: &str, budget: &mut LogBudget) {
        let Ok(mut file) = tokio::fs::File::open(&self.path).await else {
            return;
        };
        let Ok(metadata) = file.metadata().await else {
            return;
        };
        let len = metadata.len();
        let start = match self.offset {
            Some(offset) if offset <= len => offset,
            // Shorter than what was read: a new file in a rebuilt jail.
            Some(_) => {
                self.partial.clear();
                0
            }
            None if created_since(&metadata, self.readers_started_at) => 0,
            // A log from before this process: its lines were already handled.
            None => len,
        };
        self.offset = Some(start);
        if start == len || file.seek(SeekFrom::Start(start)).await.is_err() {
            return;
        }
        let mut chunk = Vec::new();
        let Ok(read) = (&mut file)
            .take(MAX_READ_BYTES)
            .read_to_end(&mut chunk)
            .await
        else {
            return;
        };
        self.offset = Some(start + read as u64);
        self.accept(sandbox_id, &chunk, budget);
    }

    fn accept(&mut self, sandbox_id: &str, mut bytes: &[u8], budget: &mut LogBudget) {
        while let Some(newline) = bytes.iter().position(|byte| *byte == b'\n') {
            let (head, rest) = bytes.split_at(newline);
            bytes = rest.get(1..).unwrap_or_default();
            self.partial.extend_from_slice(head);
            let line = std::mem::take(&mut self.partial);
            forward_line(sandbox_id, &String::from_utf8_lossy(&line), budget);
        }
        self.partial.extend_from_slice(bytes);
        if self.partial.len() > MAX_LINE_BYTES {
            self.partial.clear();
        }
    }
}

fn created_since(metadata: &std::fs::Metadata, instant: SystemTime) -> bool {
    metadata
        .created()
        .or_else(|_| metadata.modified())
        .is_ok_and(|created| created >= instant)
}

fn forward_line(sandbox_id: &str, line: &str, budget: &mut LogBudget) {
    let Some((level, message)) = classify(line) else {
        return;
    };
    let Some(suppressed) = budget.admit() else {
        pplx_space_metrics::sandbox::vm::vm_diagnostics(
            VmDiagnosticSource::Firecracker,
            level,
            VmDiagnosticOutcome::RateLimited,
            1,
        );
        return;
    };
    pplx_space_metrics::sandbox::vm::vm_diagnostics(
        VmDiagnosticSource::Firecracker,
        level,
        VmDiagnosticOutcome::Logged,
        1,
    );
    let message = sanitized(message, MAX_MESSAGE_BYTES);
    match level {
        VmDiagnosticLevel::Warn => tracing::warn!(
            sandbox_id,
            firecracker_message = message.as_str(),
            suppressed_since_last = suppressed,
            "firecracker warning"
        ),
        VmDiagnosticLevel::Error => tracing::error!(
            sandbox_id,
            firecracker_message = message.as_str(),
            suppressed_since_last = suppressed,
            "firecracker error"
        ),
    }
}

/// The level and message of a WARN or ERROR line; `None` for every other line.
fn classify(line: &str) -> Option<(VmDiagnosticLevel, &str)> {
    let (_, header_and_message) = line.split_once(" [")?;
    let (header, message) = header_and_message.split_once("] ")?;
    let level = match header.rsplit(':').next()? {
        "WARN" => VmDiagnosticLevel::Warn,
        "ERROR" => VmDiagnosticLevel::Error,
        _ => return None,
    };
    Some((level, message.trim_end()))
}
