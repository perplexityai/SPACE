//! Firecracker's and the guest's own diagnostics, made visible on the node.
//!
//! Firecracker runs daemonized in its jail and spaced runs inside the guest;
//! neither writes anywhere a log or metrics collector reads. sandbox-manager
//! reads both and re-emits them, tagged with the sandbox:
//!
//! - Firecracker's metrics flushes, from a FIFO node-manager creates in the
//!   jail, become the node-wide `sandbox.firecracker.*` metrics.
//! - Firecracker's WARN and ERROR log lines, tailed from its log file, are
//!   re-logged here with `sandbox_id`. INFO and DEBUG lines are not.
//! - spaced's WARN and ERROR callsites arrive on the health probe's Ping and
//!   are re-logged by [`forward_guest_diagnostics`].
//!
//! Nothing here can slow a VM. Firecracker opens its FIFO non-blocking and
//! drops, and counts, a flush the reader has not drained; the log is a regular
//! file read in bounded chunks. Re-logging is rate limited per sandbox, and the
//! metrics carry no sandbox id.

mod firecracker_log;
mod firecracker_metrics;
mod guest;

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Mutex;
use std::time::{Duration, SystemTime};

use pplx_space_common_core::host::linux::paths::{
    FIRECRACKER_LOG_FILE, FIRECRACKER_METRICS_FIFO, jail_root,
};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tokio_util::task::AbortOnDropHandle;

pub(crate) use guest::{RestoredGuests, forward_guest_diagnostics};

/// How often the log is tailed and a closed metrics FIFO reopened.
const POLL_INTERVAL: Duration = Duration::from_secs(2);

/// The per-sandbox output readers, one task per running Firecracker process.
pub(crate) struct VmOutputReaders {
    /// Logs created before this process started were already read by the
    /// previous sandbox-manager, so tailing them starts at their end.
    started_at: SystemTime,
    shutdown: CancellationToken,
    readers: Mutex<HashMap<String, Reader>>,
}

struct Reader {
    runtime_pid: u32,
    _task: AbortOnDropHandle<()>,
}

impl VmOutputReaders {
    pub(crate) fn new(shutdown: CancellationToken) -> Self {
        Self {
            started_at: SystemTime::now(),
            shutdown,
            readers: Mutex::new(HashMap::new()),
        }
    }

    /// Start reading a sandbox's Firecracker outputs. A sandbox already read
    /// for the same Firecracker process keeps its reader; a new process (a
    /// restart or a restore) gets a fresh one. A sandbox with no jail on this
    /// host has nothing to read.
    pub(crate) async fn attach(&self, sandbox_id: &str, runtime_pid: Option<u32>) {
        let Some(runtime_pid) = runtime_pid else {
            return;
        };
        let jail_root = jail_root(sandbox_id);
        if !tokio::fs::metadata(&jail_root)
            .await
            .is_ok_and(|metadata| metadata.is_dir())
        {
            return;
        }
        self.attach_at(sandbox_id, runtime_pid, jail_root);
    }

    /// [`Self::attach`] for a jail that exists at `jail_root`.
    pub(crate) fn attach_at(&self, sandbox_id: &str, runtime_pid: u32, jail_root: PathBuf) {
        let mut readers = self
            .readers
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if readers
            .get(sandbox_id)
            .is_some_and(|reader| reader.runtime_pid == runtime_pid)
        {
            return;
        }
        let task = tokio::spawn(read_vm_outputs(
            sandbox_id.to_string(),
            jail_root,
            self.started_at,
            self.shutdown.child_token(),
        ));
        readers.insert(
            sandbox_id.to_string(),
            Reader {
                runtime_pid,
                _task: AbortOnDropHandle::new(task),
            },
        );
    }

    #[cfg(test)]
    pub(crate) fn is_attached(&self, sandbox_id: &str) -> bool {
        self.readers
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .contains_key(sandbox_id)
    }

    pub(crate) fn detach(&self, sandbox_id: &str) {
        self.readers
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(sandbox_id);
    }
}

async fn read_vm_outputs(
    sandbox_id: String,
    jail_root: PathBuf,
    readers_started_at: SystemTime,
    shutdown: CancellationToken,
) {
    let mut metrics =
        firecracker_metrics::MetricsReader::new(jail_root.join(FIRECRACKER_METRICS_FIFO));
    let mut log =
        firecracker_log::LogTail::new(jail_root.join(FIRECRACKER_LOG_FILE), readers_started_at);
    let mut budget = LogBudget::new();
    let mut tick = tokio::time::interval(POLL_INTERVAL);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            biased;
            () = shutdown.cancelled() => return,
            () = metrics.read_available() => {}
            _ = tick.tick() => {
                metrics.reopen_if_closed();
                log.poll(&sandbox_id, &mut budget).await;
            }
        }
    }
}

/// A sandbox's allowance of re-logged Firecracker lines: a burst of
/// [`LogBudget::BURST`], then one line per [`LogBudget::REFILL`]. Lines past
/// it are counted, and the next logged line reports how many were skipped.
struct LogBudget {
    tokens: u32,
    refilled_at: Instant,
    suppressed: u64,
}

impl LogBudget {
    const BURST: u32 = 20;
    const REFILL: Duration = Duration::from_secs(15);

    fn new() -> Self {
        Self {
            tokens: Self::BURST,
            refilled_at: Instant::now(),
            suppressed: 0,
        }
    }

    /// `Some(suppressed since the last logged line)` when this line may be
    /// logged, `None` when it is over budget.
    fn admit(&mut self) -> Option<u64> {
        let now = Instant::now();
        let earned = now.duration_since(self.refilled_at).as_secs() / Self::REFILL.as_secs();
        if earned > 0 {
            self.tokens = self
                .tokens
                .saturating_add(u32::try_from(earned).unwrap_or(u32::MAX))
                .min(Self::BURST);
            self.refilled_at = now;
        }
        if self.tokens == 0 {
            self.suppressed = self.suppressed.saturating_add(1);
            return None;
        }
        self.tokens -= 1;
        Some(std::mem::take(&mut self.suppressed))
    }
}

/// Untrusted text made fit to log: at most `max` bytes, cut on a character
/// boundary, with control characters replaced. Firecracker's lines can carry
/// guest-written values, and the guest's strings are the guest's.
fn sanitized(text: &str, max: usize) -> String {
    let mut end = text.len().min(max);
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    text.get(..end)
        .unwrap_or_default()
        .chars()
        .map(|c| if c.is_control() { '\u{fffd}' } else { c })
        .collect()
}
