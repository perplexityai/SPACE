//! Minimal fake process model for the simulated worker.
//!
//! The simulator has no guest, so processes are records that emit a fixed
//! event script: `Started`, one `stdout` line echoing the command, and an
//! `Exit`. A command whose argv mentions `sleep` stays alive until it is
//! killed or sent a terminating signal, which lets clients exercise
//! list/signal/kill against a running process; anything else exits `0`
//! immediately after its output.
//!
//! Reconnects follow the guest's journal contract (`ConnectProcessRequest`):
//! output records carry a sequence starting at `0`; a connect with an
//! `output_cursor` replays every record at or after that sequence, a cursor
//! past the output ends the stream with the guest's tagged `OUT_OF_RANGE`,
//! and a connect without one follows the live tail and replays nothing. The
//! simulator retains all output, so it never reports a cursor as evicted, and
//! `ProcessConnected` reports that the stream starts at the cursor. The
//! process table is per sandbox and is dropped with the sandbox. An exited
//! process stays listable with its exit, as the guest retains one, until then.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use pplx_space_sbx_mgr_client::proto as sbx;
use tokio::sync::{Mutex, watch};
use tokio_stream::{Stream, StreamExt, wrappers::WatchStream};
use tonic::Status;

/// Signals that end a fake process; everything else is acknowledged and
/// ignored, like delivering `SIGCONT` to a real one.
const TERMINATING_SIGNALS: [i32; 3] = [2, 9, 15];
const FIRST_PID: u32 = 1000;

type ExitSender = watch::Sender<Option<sbx::SpacedProcessExit>>;
type ExitReceiver = watch::Receiver<Option<sbx::SpacedProcessExit>>;

#[derive(Debug, thiserror::Error)]
pub enum SimProcessError {
    #[error("process {process_id} not found in sandbox {sandbox_id}")]
    NotFound {
        sandbox_id: String,
        process_id: String,
    },
}

impl From<SimProcessError> for Status {
    fn from(error: SimProcessError) -> Self {
        Status::not_found(error.to_string())
    }
}

/// Inputs the simulator keeps from a start request.
#[derive(Clone, Debug, Default)]
pub struct SimProcessSpec {
    pub command: Vec<String>,
    pub cwd: Option<String>,
    pub env: HashMap<String, String>,
    pub user: Option<String>,
    pub tag: Option<String>,
    pub pty: bool,
    /// A background start returns after `Started`, as in production; the
    /// process stays in the table for list/connect/signal/kill.
    pub background: bool,
}

#[derive(Clone)]
struct SimProcess {
    info: sbx::SpacedProcessInfo,
    /// Output journal: record `i` has sequence `i`.
    output: Vec<Vec<u8>>,
    exit_tx: Arc<ExitSender>,
    exit_rx: ExitReceiver,
    ended_at: Arc<std::sync::OnceLock<prost_types::Timestamp>>,
}

impl SimProcess {
    fn exited(&self) -> bool {
        self.exit_rx.borrow().is_some()
    }

    fn end(&self, exit: sbx::SpacedProcessExit) {
        let _ = self
            .ended_at
            .set(prost_types::Timestamp::from(std::time::SystemTime::now()));
        let _ = self.exit_tx.send(Some(exit));
    }

    /// What the list reports: with its exit once it has ended.
    fn listed_info(&self) -> sbx::SpacedProcessInfo {
        sbx::SpacedProcessInfo {
            exit: *self.exit_rx.borrow(),
            ended_at: self.ended_at.get().copied(),
            ..self.info.clone()
        }
    }
}

/// Per-sandbox table of fake processes shared by every RPC handler.
#[derive(Clone, Default)]
pub struct SimProcessTable {
    inner: Arc<Mutex<HashMap<String, HashMap<String, SimProcess>>>>,
    next_pid: Arc<AtomicU32>,
}

/// Event stream returned by start and connect.
pub type SimProcessEventStream =
    std::pin::Pin<Box<dyn Stream<Item = Result<sbx::SpacedProcessEvent, Status>> + Send>>;

impl SimProcessTable {
    /// Register a process and return its `Started`/`stdout`/`Exit` stream.
    pub async fn start(&self, sandbox_id: &str, spec: SimProcessSpec) -> SimProcessEventStream {
        let pid = FIRST_PID + self.next_pid.fetch_add(1, Ordering::Relaxed);
        let process_id = format!("sim-proc-{pid}");
        let output = vec![format!("sim: {}\n", spec.command.join(" ")).into_bytes()];
        let (exit_tx, exit_rx) = watch::channel(None);
        let process = SimProcess {
            info: sbx::SpacedProcessInfo {
                process_id: process_id.clone(),
                pid,
                command: spec.command.clone(),
                cwd: spec.cwd,
                env: spec.env,
                user: spec.user,
                tag: spec.tag,
                pty: Some(spec.pty),
                exit: None,
                ended_at: None,
            },
            output,
            exit_tx: Arc::new(exit_tx),
            exit_rx,
            ended_at: Arc::default(),
        };
        if !spec.command.iter().any(|arg| arg.contains("sleep")) {
            process.end(exited(0));
        }
        let stream = event_stream(
            &process,
            ConnectMode::Start {
                background: spec.background,
            },
        );
        self.inner
            .lock()
            .await
            .entry(sandbox_id.to_string())
            .or_default()
            .insert(process_id, process);
        stream
    }

    /// Reattach to a process: `Connected`, then the journal from
    /// `output_cursor` onward, then its exit.
    ///
    /// `output_cursor` is the number of output records the caller already
    /// holds, as the guest defines it. `None` follows the live tail: nothing
    /// already emitted is replayed, so a reconnect does not deliver stdout a
    /// second time. The data plane's backend-retry path passes the count it
    /// has delivered so far; the public SSE resume passes nothing.
    pub async fn connect(
        &self,
        sandbox_id: &str,
        process_id: &str,
        output_cursor: Option<u64>,
    ) -> Result<SimProcessEventStream, SimProcessError> {
        let process = self.get(sandbox_id, process_id).await?;
        Ok(event_stream(
            &process,
            ConnectMode::Connect { output_cursor },
        ))
    }

    /// Tear down a sandbox's execution: end every process that is still
    /// running, so any open start/connect stream sees its `Exit`, then forget
    /// the table. Called from every path that stops execution (stop, kill,
    /// delete, cleanup) so a restarted or recreated sandbox id starts with an
    /// empty table and records do not accumulate for the session.
    pub async fn terminate_sandbox(&self, sandbox_id: &str) {
        let Some(processes) = self.inner.lock().await.remove(sandbox_id) else {
            return;
        };
        for process in processes.values() {
            if !process.exited() {
                process.end(signaled(9));
            }
        }
    }

    /// Processes that have not exited, and with `include_exited` also those
    /// that have, with their exit; with `process_id`, only that one. Oldest
    /// pid first.
    pub async fn list(
        &self,
        sandbox_id: &str,
        include_exited: bool,
        process_id: Option<&str>,
    ) -> Vec<sbx::SpacedProcessInfo> {
        let map = self.inner.lock().await;
        let mut listed: Vec<_> = map
            .get(sandbox_id)
            .into_iter()
            .flat_map(|processes| processes.iter())
            .filter(|(id, process)| {
                process_id.is_none_or(|process_id| id.as_str() == process_id)
                    && (include_exited || !process.exited())
            })
            .map(|(_, process)| process.listed_info())
            .collect();
        listed.sort_by_key(|info| info.pid);
        listed
    }

    /// Deliver a signal; terminating signals end the process as `Signaled`.
    ///
    /// A process that has already exited is `NotFound`, as in the guest, whose
    /// signal handler only resolves running processes. Acknowledging it would
    /// tell a client that a completed process was signaled.
    pub async fn signal(
        &self,
        sandbox_id: &str,
        process_id: &str,
        signal: i32,
    ) -> Result<(), SimProcessError> {
        let process = self.get(sandbox_id, process_id).await?;
        if process.exited() {
            return Err(SimProcessError::NotFound {
                sandbox_id: sandbox_id.to_string(),
                process_id: process_id.to_string(),
            });
        }
        if TERMINATING_SIGNALS.contains(&signal) {
            process.end(signaled(signal));
        }
        Ok(())
    }

    /// `SIGKILL` semantics.
    pub async fn kill(&self, sandbox_id: &str, process_id: &str) -> Result<(), SimProcessError> {
        self.signal(sandbox_id, process_id, 9).await
    }

    async fn get(&self, sandbox_id: &str, process_id: &str) -> Result<SimProcess, SimProcessError> {
        self.inner
            .lock()
            .await
            .get(sandbox_id)
            .and_then(|processes| processes.get(process_id))
            .cloned()
            .ok_or_else(|| SimProcessError::NotFound {
                sandbox_id: sandbox_id.to_string(),
                process_id: process_id.to_string(),
            })
    }
}

#[derive(Clone, Copy)]
enum ConnectMode {
    /// Fresh start: `Started`, then the whole journal and the exit. A
    /// background start ends after `Started`, like production's stream.
    Start { background: bool },
    /// Reattach: `Connected`, then the journal from `output_cursor`; `None`
    /// is the live tail, which replays nothing, and a cursor past the output
    /// fails.
    Connect { output_cursor: Option<u64> },
}

fn event_stream(process: &SimProcess, mode: ConnectMode) -> SimProcessEventStream {
    let process_id = process.info.process_id.clone();
    let pid = process.info.pid;
    let tail = process.output.len() as u64;
    let (first, replay_from) = match mode {
        ConnectMode::Start { background } => {
            let started = sbx::spaced_process_event::Event::Started(sbx::SpacedProcessStarted {
                start_time: None,
            });
            if background {
                return Box::pin(tokio_stream::iter(vec![Ok(event(
                    &process_id,
                    Some(pid),
                    started,
                ))]));
            }
            (started, 0)
        }
        ConnectMode::Connect { output_cursor } => {
            // Nothing is evicted, so the stream starts at the cursor.
            let connected =
                sbx::spaced_process_event::Event::Connected(sbx::SpacedProcessConnected {
                    output_cursor,
                });
            if output_cursor.is_some_and(|cursor| cursor > tail) {
                return Box::pin(tokio_stream::iter(vec![
                    Ok(event(&process_id, Some(pid), connected)),
                    Err(cursor_ahead_status()),
                ]));
            }
            (connected, output_cursor.unwrap_or(tail))
        }
    };
    let mut initial = vec![Ok(event(&process_id, Some(pid), first))];
    initial.extend(
        process
            .output
            .iter()
            .skip(replay_from as usize)
            .map(|data| {
                Ok(event(
                    &process_id,
                    Some(pid),
                    sbx::spaced_process_event::Event::Stdout(sbx::SpacedProcessOutput {
                        data: data.clone(),
                    }),
                ))
            }),
    );
    let exit_process_id = process_id.clone();
    let exit = WatchStream::new(process.exit_rx.clone())
        .filter_map(|exit| exit)
        .take(1)
        .map(move |exit| {
            Ok(event(
                &exit_process_id,
                Some(pid),
                sbx::spaced_process_event::Event::Exit(exit),
            ))
        });
    Box::pin(tokio_stream::iter(initial).chain(exit))
}

/// The guest's status for a connect whose cursor is past the output.
fn cursor_ahead_status() -> Status {
    let mut status =
        Status::out_of_range("process output cursor is past the output the process has written");
    status.metadata_mut().insert(
        pplx_space_common_core::spaced::ERROR_REASON_METADATA_KEY,
        tonic::metadata::MetadataValue::from_static(
            pplx_space_common_core::spaced::PROCESS_OUTPUT_CURSOR_AHEAD,
        ),
    );
    status
}

fn event(
    process_id: &str,
    pid: Option<u32>,
    event: sbx::spaced_process_event::Event,
) -> sbx::SpacedProcessEvent {
    sbx::SpacedProcessEvent {
        process_id: process_id.to_string(),
        pid,
        event: Some(event),
    }
}

fn exited(exit_code: i32) -> sbx::SpacedProcessExit {
    sbx::SpacedProcessExit {
        outcome: Some(sbx::spaced_process_exit::Outcome::Exited(
            sbx::SpacedProcessExited { exit_code },
        )),
    }
}

fn signaled(signal: i32) -> sbx::SpacedProcessExit {
    sbx::SpacedProcessExit {
        outcome: Some(sbx::spaced_process_exit::Outcome::Signaled(
            sbx::SpacedProcessSignaled {
                signal: Some(signal),
            },
        )),
    }
}

#[cfg(test)]
mod tests;
