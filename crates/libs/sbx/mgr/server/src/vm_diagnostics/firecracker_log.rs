//! Firecracker's log file, written with `--show-level`:
//! `<time> [<instance>:<thread>:<LEVEL>] <message>`.
//!
//! Only ERROR lines are re-logged. Firecracker warns on every snapshot restore,
//! so WARN lines stay in the file, where the watchdog's failure-time tail of it
//! still reads them. Reading a regular file never affects the VMM; each poll
//! reads at most [`MAX_READ_BYTES`]. Polls block: they run on the shared log
//! poll's blocking job, never on the runtime.
//!
//! That one job reads every VM's log, and the jail is the sandbox's to write,
//! so the shared poll reads only regular files. The path is opened without
//! blocking and without following a final symlink (Firecracker creates the
//! file itself), and anything that is not a regular file is skipped for that
//! poll: a FIFO or device there cannot hold up the other VMs' logs.

use std::io::{Read, Seek, SeekFrom};
use std::os::unix::fs::OpenOptionsExt as _;
use std::path::PathBuf;
use std::time::SystemTime;

use pplx_space_metrics::sandbox::vm::{VmDiagnosticLevel, VmDiagnosticOutcome, VmDiagnosticSource};

use super::{LogBudget, sanitized};

const MAX_READ_BYTES: u64 = 64 * 1024;
/// A partial line longer than this is not Firecracker's and is dropped.
const MAX_LINE_BYTES: usize = 8 * 1024;
const MAX_MESSAGE_BYTES: usize = 512;

/// Bytes one poll read from a log, not yet accepted.
pub(super) struct LogRead {
    start: u64,
    /// The file was shorter than what was read of it, so the read restarted at 0.
    restarted: bool,
    chunk: Vec<u8>,
}

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

    /// Read what is new in the log, changing nothing: the read is applied by
    /// [`Self::accept_read`]. `None` when there is no regular file to read.
    pub(super) fn read(&self) -> Option<LogRead> {
        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NONBLOCK | libc::O_NOFOLLOW)
            .open(&self.path)
            .ok()?;
        let metadata = file.metadata().ok()?;
        if !metadata.is_file() {
            return None;
        }
        let len = metadata.len();
        let (start, restarted) = match self.offset {
            Some(offset) if offset <= len => (offset, false),
            // Shorter than what was read: a new file in a rebuilt jail.
            Some(_) => (0, true),
            None if created_since(&metadata, self.readers_started_at) => (0, false),
            // A log from before this process: its lines were already handled.
            None => (len, false),
        };
        let mut chunk = Vec::new();
        if start < len && file.seek(SeekFrom::Start(start)).is_ok() {
            // A failed read leaves nothing to accept; the next poll retries.
            if (&mut file)
                .take(MAX_READ_BYTES)
                .read_to_end(&mut chunk)
                .is_err()
            {
                chunk.clear();
            }
        }
        Some(LogRead {
            start,
            restarted,
            chunk,
        })
    }

    /// Advance past `read` and re-log its complete lines.
    pub(super) fn accept_read(&mut self, read: LogRead, sandbox_id: &str, budget: &mut LogBudget) {
        if read.restarted {
            self.partial.clear();
        }
        self.offset = Some(read.start + read.chunk.len() as u64);
        self.accept(sandbox_id, &read.chunk, budget);
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
    let Some(message) = error_message(line) else {
        return;
    };
    let Some(suppressed) = budget.admit() else {
        pplx_space_metrics::sandbox::vm::vm_diagnostics(
            VmDiagnosticSource::Firecracker,
            VmDiagnosticLevel::Error,
            VmDiagnosticOutcome::RateLimited,
            1,
        );
        return;
    };
    pplx_space_metrics::sandbox::vm::vm_diagnostics(
        VmDiagnosticSource::Firecracker,
        VmDiagnosticLevel::Error,
        VmDiagnosticOutcome::Logged,
        1,
    );
    let message = sanitized(message, MAX_MESSAGE_BYTES);
    tracing::error!(
        sandbox_id,
        firecracker_message = message.as_str(),
        suppressed_since_last = suppressed,
        "firecracker error"
    );
}

/// The message of an ERROR line; `None` for every other line.
fn error_message(line: &str) -> Option<&str> {
    let (_, header_and_message) = line.split_once(" [")?;
    let (header, message) = header_and_message.split_once("] ")?;
    (header.rsplit(':').next()? == "ERROR").then(|| message.trim_end())
}
