//! Firecracker's and the guest's own diagnostics, made visible on the node.
//!
//! Firecracker runs daemonized in its jail and spaced runs inside the guest;
//! neither writes anywhere a log or metrics collector reads. sandbox-manager
//! reads both and re-emits them, tagged with the sandbox:
//!
//! - Firecracker's metrics flushes, from a FIFO node-manager creates in the
//!   jail, become the node-wide `sandbox.firecracker.*` metrics.
//! - Firecracker's ERROR log lines, tailed from its log file, are re-logged
//!   here with `sandbox_id`. WARN, INFO and DEBUG lines are not; the
//!   watchdog's failure-time tail of the file still captures them.
//! - spaced's WARN and ERROR callsites arrive on the health probe's Ping and
//!   are re-logged by [`forward_guest_diagnostics`].
//!
//! Nothing here can slow a VM. Firecracker opens its FIFO non-blocking and
//! drops, and counts, a flush the reader has not drained; the log is read in
//! bounded chunks, and the shared poll reads only regular files. Re-logging is
//! rate limited per sandbox, and the metrics carry no sandbox id.
//!
//! Each running VM has one task, which reads its metrics FIFO without the
//! blocking pool. Every log is tailed by one shared poll: each tick, and
//! promptly after an attach, a single blocking job reads every attached
//! sandbox's log in turn, so the number of VMs never adds blocking threads.

mod firecracker_log;
mod firecracker_metrics;
mod guest;

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use pplx_space_common_core::host::linux::paths::{
    FIRECRACKER_LOG_FILE, FIRECRACKER_METRICS_FIFO, jail_root,
};
use tokio::sync::Notify;
use tokio::time::Instant;
use tokio_util::sync::{CancellationToken, DropGuard};
use tokio_util::task::AbortOnDropHandle;

pub(crate) use guest::{RestoredGuests, forward_guest_diagnostics};

/// How often the logs are tailed and a closed metrics FIFO reopened. An attach
/// also wakes the log poll, so a new sandbox's log is read without waiting.
const POLL_INTERVAL: Duration = Duration::from_secs(2);

/// The per-sandbox output readers: a metrics task per running Firecracker
/// process, and one poll of all their logs.
pub(crate) struct VmOutputReaders {
    /// Logs created before this process started were already read by the
    /// previous sandbox-manager, so tailing them starts at their end.
    started_at: SystemTime,
    /// Owned by these readers and cancelled when they are dropped, so a log
    /// poll's blocking job, which aborting its task cannot stop, stops too.
    shutdown: CancellationToken,
    readers: Arc<Mutex<HashMap<String, Reader>>>,
    /// Wakes the log poll for a new attachment. Wakes coalesce: a burst of
    /// attaches still runs one job at a time.
    attached: Arc<Notify>,
    _log_poll: AbortOnDropHandle<()>,
    _cancel_on_drop: DropGuard,
}

struct Reader {
    runtime_pid: u32,
    log: Arc<LogRegistration>,
    _metrics: AbortOnDropHandle<()>,
}

impl Drop for Reader {
    /// Detached or replaced: a log poll that already copied this registration
    /// may still finish a read it started, but once this returns it never
    /// re-logs a line or advances the tail for a reader that is gone. Waits at
    /// most for an in-memory accept, never for I/O.
    fn drop(&mut self) {
        *lock(&self.log.retired) = true;
    }
}

/// One attachment's log, shared with the log poll's copy of the attachments.
struct LogRegistration {
    /// The admission lock: set under it on retirement, and held by the poll
    /// while it accepts a read, never while it reads.
    retired: Mutex<bool>,
    /// Locked only by the log poll, so attach and detach never wait on a read.
    state: Mutex<VmLog>,
}

impl LogRegistration {
    fn poll(&self) {
        let mut state = lock(&self.state);
        if *lock(&self.retired) {
            return;
        }
        let Some(read) = state.tail.read() else {
            return;
        };
        self.admit(&mut state, read);
    }

    /// Accept `read` unless the registration was retired since: the read may
    /// have raced a detach or a replace, and a replacement reads the log itself.
    fn admit(&self, state: &mut VmLog, read: firecracker_log::LogRead) {
        let retired = lock(&self.retired);
        if *retired {
            return;
        }
        let VmLog {
            sandbox_id,
            tail,
            budget,
        } = state;
        tail.accept_read(read, sandbox_id, budget);
    }
}

struct VmLog {
    sandbox_id: String,
    tail: firecracker_log::LogTail,
    budget: LogBudget,
}

impl VmOutputReaders {
    pub(crate) fn new(shutdown: CancellationToken) -> Self {
        let shutdown = shutdown.child_token();
        let readers = Arc::new(Mutex::new(HashMap::new()));
        let attached = Arc::new(Notify::new());
        let log_poll = tokio::spawn(poll_logs(
            Arc::clone(&readers),
            Arc::clone(&attached),
            shutdown.clone(),
        ));
        Self {
            started_at: SystemTime::now(),
            _cancel_on_drop: shutdown.clone().drop_guard(),
            shutdown,
            readers,
            attached,
            _log_poll: AbortOnDropHandle::new(log_poll),
        }
    }

    /// Start reading a sandbox's Firecracker outputs. A sandbox already read
    /// for the same Firecracker process keeps its reader; a new process (a
    /// restart or a restore) gets a fresh one, and its log is read without
    /// waiting for the next tick. A sandbox with no jail on this host has
    /// nothing to read.
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
        let mut readers = lock(&self.readers);
        if readers
            .get(sandbox_id)
            .is_some_and(|reader| reader.runtime_pid == runtime_pid)
        {
            return;
        }
        let metrics = tokio::spawn(read_metrics(
            jail_root.join(FIRECRACKER_METRICS_FIFO),
            self.shutdown.child_token(),
        ));
        let log = VmLog {
            sandbox_id: sandbox_id.to_string(),
            tail: firecracker_log::LogTail::new(
                jail_root.join(FIRECRACKER_LOG_FILE),
                self.started_at,
            ),
            budget: LogBudget::new(),
        };
        readers.insert(
            sandbox_id.to_string(),
            Reader {
                runtime_pid,
                log: Arc::new(LogRegistration {
                    retired: Mutex::new(false),
                    state: Mutex::new(log),
                }),
                _metrics: AbortOnDropHandle::new(metrics),
            },
        );
        drop(readers);
        self.attached.notify_one();
    }

    /// The registration a log poll would copy for `sandbox_id`.
    #[cfg(test)]
    fn registration(&self, sandbox_id: &str) -> Arc<LogRegistration> {
        Arc::clone(&lock(&self.readers)[sandbox_id].log)
    }

    #[cfg(test)]
    pub(crate) fn is_attached(&self, sandbox_id: &str) -> bool {
        lock(&self.readers).contains_key(sandbox_id)
    }

    pub(crate) fn detach(&self, sandbox_id: &str) {
        lock(&self.readers).remove(sandbox_id);
    }

    /// One tick's log poll, run on the caller's thread.
    #[cfg(test)]
    fn poll_logs_now(&self) {
        poll_each_log(&attached_logs(&self.readers), &self.shutdown);
    }
}

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

async fn read_metrics(fifo: PathBuf, shutdown: CancellationToken) {
    let mut metrics = firecracker_metrics::MetricsReader::new(fifo);
    let mut tick = tokio::time::interval(POLL_INTERVAL);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            biased;
            () = shutdown.cancelled() => return,
            () = metrics.read_available() => {}
            _ = tick.tick() => metrics.reopen_if_closed(),
        }
    }
}

/// Tail every attached log once per tick, and after an attach, in one
/// blocking job. The next job waits for the last, so this poll never holds
/// more than one blocking thread; attaches during a job wake it once more.
async fn poll_logs(
    readers: Arc<Mutex<HashMap<String, Reader>>>,
    attached: Arc<Notify>,
    shutdown: CancellationToken,
) {
    let mut tick = tokio::time::interval(POLL_INTERVAL);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            biased;
            () = shutdown.cancelled() => return,
            _ = tick.tick() => {}
            () = attached.notified() => {}
        }
        let logs = attached_logs(&readers);
        if logs.is_empty() {
            continue;
        }
        let job_shutdown = shutdown.clone();
        let job = tokio::task::spawn_blocking(move || poll_each_log(&logs, &job_shutdown));
        tokio::select! {
            biased;
            () = shutdown.cancelled() => return,
            _ = job => {}
        }
    }
}

/// The attached logs, taken without holding the readers' lock past the copy.
fn attached_logs(readers: &Mutex<HashMap<String, Reader>>) -> Vec<Arc<LogRegistration>> {
    lock(readers)
        .values()
        .map(|reader| Arc::clone(&reader.log))
        .collect()
}

/// A log that is missing, unreadable or not a regular file is skipped until
/// the next tick; the others are still read. A registration detached or
/// replaced since the copy re-logs nothing: its reader is gone, and a
/// replacement reads the path itself.
fn poll_each_log(logs: &[Arc<LogRegistration>], shutdown: &CancellationToken) {
    for log in logs {
        if shutdown.is_cancelled() {
            return;
        }
        log.poll();
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

#[cfg(test)]
mod tests;
