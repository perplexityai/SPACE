use super::*;
use crate::filesystem::map_io_error;
use crate::guest_path;
use crate::idempotency::{
    IdempotencyLookup, IdempotencyOperation, IdempotencyRecord, require_idempotency_key,
};
use crate::identity::GuestAccount;
use dashmap::DashMap;
use pplx_space_common_core::types::{
    GuestUserName, Pii, ProcessCommand, ProcessId, ProcessRecoveryToken, ProcessSignal,
};
use std::os::fd::{AsFd, AsRawFd, OwnedFd, RawFd};
use std::sync::Weak;
use std::sync::atomic::{AtomicU64, Ordering};
use std::{collections::VecDeque, io};
use tokio::io::AsyncRead;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, TryAcquireError};

mod pty;
mod recovery;
use pty::PtyIo;
use recovery::{ProcessAddress, TargetState};

mod child_io;
mod journal;
mod launch;
mod registry;

use child_io::*;
pub(crate) use journal::*;
use launch::*;
pub(crate) use registry::*;

mod output;

use output::*;

const LOG_TARGET: &str = module_path!();

const DEFAULT_LOGIN_PATH: &str = "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin";

// A start phase slower than this is logged with every phase's duration once
// the spawn returns, so a slow start names its cause. The account lookup
// includes its wait for a blocking-pool thread.
const SLOW_START_PHASE: Duration = Duration::from_secs(1);

struct RecoveryLaunch {
    fence: pplx_space_spaced_guest_protocol::recovery::RecoveryFence,
    // Retained through launch and key publication, even after the caller leaves.
    _activation: tokio::sync::OwnedRwLockReadGuard<()>,
}

struct RecoveryStartAdmission {
    reservation: crate::idempotency::IdempotencyReservation,
    launch: RecoveryLaunch,
}

#[cfg(test)]
thread_local! {
    /// Refuses the spawn threads of starts made on this thread, as a guest
    /// out of threads would.
    static REFUSE_SPAWN_THREADS: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

impl ExecutionService {
    /// Wraps a process adapter with predecode payload admission.
    ///
    /// The adapter must preserve request extensions when forwarding to this
    /// service so handlers can reuse admission instead of charging twice.
    pub fn process_grpc_service<S: Process>(
        &self,
        adapter: S,
    ) -> pplx_space_common_grpc::RequestAdmission<ProcessServer<S>> {
        use pplx_space_spaced_guest_protocol::process_limits;
        pplx_space_common_grpc::RequestAdmission::new(
            ProcessServer::new(adapter)
                .max_decoding_message_size(process_limits::MAX_MESSAGE_BYTES),
            pplx_space_common_grpc::QueuedSlots::new(self.inner.process_payload_slots.clone()),
            process_limits::PAYLOAD_METHODS,
        )
        .with_observation(pplx_space_metrics::payload::Budget::GuestProcess)
    }
}

async fn resolve_process_account(
    tasks: tokio_util::task::TaskTracker,
    slots: Arc<Semaphore>,
    ingress: Option<pplx_space_common_grpc::RequestPermit>,
    resolve: impl FnOnce() -> Result<GuestAccount, Status> + Send + 'static,
) -> Result<(GuestAccount, pplx_space_common_grpc::RequestPermit), Status> {
    let admission = crate::payload::acquire(
        &slots,
        pplx_space_spaced_guest_protocol::process_limits::START_PAYLOAD_SLOTS,
        pplx_space_metrics::payload::Budget::GuestProcess,
        ingress,
    )?;
    // NSS may block on external account providers. Keep the existing Start
    // admission with the physical lookup even if its requesting future drops.
    admission
        .request_lifetime()
        .observe(tasks.spawn_blocking(move || resolve().map(|account| (account, admission))))
        .await
        .map_err(|error| Status::internal(format!("guest account lookup task failed: {error}")))?
}

fn output_data_bytes(event: &ProcessEvent) -> u64 {
    use pplx_space_spaced_guest_protocol::proto::bridge_guest::process_event::Event;
    match event.event.as_ref() {
        Some(Event::Stdout(output) | Event::Stderr(output)) => output.data.len() as u64,
        _ => unreachable!("process journal only retains output events"),
    }
}

/// What a resumed reader's `ProcessOutputGap` event is addressed with.
struct OutputGapResume {
    process_id: String,
    pid: Option<u32>,
}

impl ProcessJournalCursor {
    fn advance_to(&mut self, next_sequence: u64) {
        self.next_sequence = next_sequence;
        if let Some(primary) = &self.primary {
            primary.store(next_sequence, Ordering::Release);
            if let Some(journal) = self.journal.upgrade() {
                journal.consumed.send_replace(());
            }
        }
    }

    /// Moves an opted-in reader past output the journal no longer holds.
    fn skip_output_gap(
        &mut self,
        resume_sequence: u64,
        output_bytes_before: u64,
    ) -> Option<ProcessEvent> {
        let resume = self.gap_resume.as_ref()?;
        let gap = process_output_gap_event(
            resume,
            resume_sequence - self.next_sequence,
            resume_sequence,
            output_bytes_before,
        );
        self.advance_to(resume_sequence);
        Some(gap)
    }

    /// After the record is gone, everything unread before the exit is a gap.
    fn skip_evicted_output(&mut self) -> Option<ProcessEvent> {
        let (next_sequence, output_bytes) = match &*self.changed.borrow() {
            ProcessJournalChange::Exit {
                next_sequence,
                output_bytes,
                ..
            } if self.next_sequence < *next_sequence => (*next_sequence, *output_bytes),
            _ => return None,
        };
        self.skip_output_gap(next_sequence, output_bytes)
    }
}

fn process_output_gap_event(
    resume: &OutputGapResume,
    skipped_events: u64,
    output_cursor: u64,
    output_bytes_before: u64,
) -> ProcessEvent {
    use pplx_space_spaced_guest_protocol::proto::bridge_guest::{
        ProcessOutputGap, process_event::Event,
    };
    ProcessEvent {
        process_id: resume.process_id.clone(),
        pid: resume.pid,
        event: Some(Event::OutputGap(ProcessOutputGap {
            skipped_events,
            output_cursor,
            output_bytes_before,
        })),
    }
}

enum ProcessExitOutcome {
    Exited(i32),
    Signaled(i32),
    TimedOut(i32),
    Unknown,
}

fn process_id_in_use() -> Status {
    Status::already_exists("process id is already in use by a running or retained process")
}

fn validate_process_tag(tag: Option<String>) -> Result<Option<String>, Status> {
    if tag
        .as_ref()
        .is_some_and(|tag| tag.len() > MAX_PROCESS_TAG_BYTES || tag.contains('\0'))
    {
        return Err(Status::invalid_argument(
            "process tag must be NUL-free and at most 4096 bytes",
        ));
    }
    Ok(tag)
}

fn process_status_outcome(status: Option<std::process::ExitStatus>) -> ProcessExitOutcome {
    let Some(status) = status else {
        return ProcessExitOutcome::Unknown;
    };
    if let Some(code) = status.code() {
        return ProcessExitOutcome::Exited(code);
    }
    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt;
        if let Some(signal) = status.signal() {
            return ProcessExitOutcome::Signaled(signal);
        }
    }
    ProcessExitOutcome::Unknown
}

fn terminate_and_reap_failed_start(
    mut child: tokio::process::Child,
    process_reservation: ProcessRecordReservation,
    tasks: &tokio_util::task::TaskTracker,
) {
    let pid = child.id();
    if let Some(pid) = pid {
        // The child has not been waited, so its process-group identity cannot
        // have been reused, including when opening the pidfd failed.
        if let Ok(process_group) = i32::try_from(pid) {
            if let Err(error) = nix::sys::signal::killpg(
                nix::unistd::Pid::from_raw(process_group),
                nix::sys::signal::Signal::SIGKILL,
            ) && error != nix::errno::Errno::ESRCH
            {
                tracing::warn!(%error, pid, "failed to terminate process group after start failure");
            }
        } else {
            tracing::error!(pid, "child pid is outside the Unix process-group range");
        }
    }
    // Also address the Child directly if group signalling failed or no numeric
    // group identity was available. A completed Child makes this a no-op.
    if let Err(error) = child.start_kill() {
        tracing::warn!(%error, ?pid, "failed to kill child after start failure");
    }

    // Cleanup outlives the request and retains admission until reaping finishes.
    // A slow or failed kill must not allow unbounded unreaped failed starts.
    tasks.spawn(async move {
        let _process_reservation = process_reservation;
        if let Err(error) = child.wait().await {
            tracing::warn!(%error, ?pid, "failed to reap child after start failure");
        }
    });
}

impl ExecutionService {
    fn replay_started(
        &self,
        result: Result<IdempotencyRecord, Status>,
    ) -> Result<Response<<Self as Process>::StartStream>, Status> {
        let (process_id, expected) = match result? {
            IdempotencyRecord::ProcessStarted { process_id } => (process_id, None),
            IdempotencyRecord::RecoveryProcessStarted {
                process_id,
                incarnation,
            } => (process_id, Some(incarnation)),
        };
        let replay = self
            .inner
            .processes
            .replay(&process_id, Some(0), false, false)
            .ok_or_else(|| Status::failed_precondition("process replay state expired"))?;
        if let Some(expected) = expected {
            let (ProcessReplay::Running { started, .. } | ProcessReplay::Completed { started, .. }) =
                &replay;
            let actual = match &started.event {
                Some(pplx_space_spaced_guest_protocol::proto::operations::process_event::Event::Started(event)) => event.process_incarnation.as_deref(),
                _ => None,
            };
            if actual != Some(expected.to_string().as_str()) {
                return Err(Status::failed_precondition("process replay state expired"));
            }
        }
        Ok(Response::new(replay_process_event_stream(replay)))
    }
}

#[tonic::async_trait]
impl Process for ExecutionService {
    type StartStream =
        std::pin::Pin<Box<dyn futures_util::Stream<Item = Result<ProcessEvent, Status>> + Send>>;
    type ConnectProcessStream =
        std::pin::Pin<Box<dyn futures_util::Stream<Item = Result<ProcessEvent, Status>> + Send>>;
    type ListProcessesStream =
        std::pin::Pin<Box<dyn futures_util::Stream<Item = Result<ProcessInfo, Status>> + Send>>;

    async fn start(
        &self,
        mut request: Request<StartRequest>,
    ) -> Result<Response<Self::StartStream>, Status> {
        let _operation = self.inner.lifecycle.admit()?;
        crate::payload::check_message(
            request.get_ref(),
            pplx_space_spaced_guest_protocol::process_limits::MAX_MESSAGE_BYTES,
        )?;
        let received_at = Instant::now();
        let ingress = request
            .extensions_mut()
            .remove::<pplx_space_common_grpc::RequestPermit>();
        let mut request = request.into_inner();
        // Legacy validation and identity-collision precedence remain unchanged.
        // Recovery binding precedes account lookup and mutable launch defaults.
        let mut ingress = ingress;
        let recovery = if request.recovery.is_some() {
            // A queued activation update must not turn requests into payload-holding waiters.
            let activation = self
                .inner
                .recovery
                .launches
                .clone()
                .try_read_owned()
                .map_err(|_| pplx_space_spaced_guest_protocol::recovery::RecoveryReason::TemporarilyUnavailable.into_status())?;
            let permit = crate::payload::acquire(
                &self.inner.process_payload_slots,
                pplx_space_spaced_guest_protocol::process_limits::START_PAYLOAD_SLOTS,
                pplx_space_metrics::payload::Budget::GuestProcess,
                ingress.take(),
            )?;
            let retained = permit.clone();
            let (returned, fence, binding) = self
                .inner
                .process_tasks
                .spawn_blocking(move || {
                    let _permit = retained;
                    let fence =
                        pplx_space_spaced_guest_protocol::recovery::validate_start(&request)?
                            .expect("recovery envelope present");
                    validate_process_tag(request.tag.clone())?;
                    if let Some(pty) = &request.pty {
                        pty_winsize(pty.size)?;
                    }
                    let binding = crate::recovery::binding(&request);
                    Ok::<_, Status>((request, fence, binding))
                })
                .await
                .map_err(|_| Status::internal("recovery start validation task failed"))??;
            request = returned;
            ingress = Some(permit);
            self.inner.recovery.check(fence)?;
            let key = require_idempotency_key(request.idempotency_key.clone(), "process_start")?;
            let lookup = self.inner.idempotency.reserve_bound(key, binding).await?;
            self.inner.recovery.check(fence)?;
            let reservation = match lookup {
                IdempotencyLookup::Reserved(reservation) => reservation,
                IdempotencyLookup::Completed(result) => return self.replay_started(result),
            };
            request.command = std::mem::take(
                &mut request
                    .recovery
                    .as_mut()
                    .expect("validated envelope")
                    .command,
            );
            Some(RecoveryStartAdmission {
                reservation,
                launch: RecoveryLaunch {
                    fence,
                    _activation: activation,
                },
            })
        } else {
            None
        };
        let process_command =
            ProcessCommand::new(request.command).map_err(invalid_argument_status)?;
        let requested_cwd = request.cwd;
        let requested_env = request.env;
        let user = request
            .user
            .map(GuestUserName::parse)
            .transpose()
            .map_err(invalid_argument_status)?;
        let listed_user = user.as_ref().map(|user| user.as_str().to_string());
        let listed_tag = validate_process_tag(request.tag)?;

        // A caller-assigned id lets the control plane name the process before it
        // starts. A minted id cannot collide, so both cases share one identity
        // and one uniqueness check, taken again below under the start gate.
        let process_id = request
            .process_id
            .map(ProcessId::parse)
            .transpose()
            .map_err(invalid_argument_status)?
            .unwrap_or_else(ProcessId::new);
        // An assigned id that is already in use is a definitive answer, so give
        // it precedence over every later rejection and wait in this handler.
        // The data plane reconnects only on `AlreadyExists`, and a reconnect
        // re-sends Start whenever the first attach dropped before binding its
        // session, which is what a timed-out start leaves behind. Every later
        // step can fail or stall for such a retry of a still-running process:
        // the account lookup or PTY setup hits the descriptor limit,
        // `reserve_or_get` fails an idempotency key past its window, and
        // `reserve` reports exhaustion when every record slot holds a running
        // process. Answering here also keeps the
        // outcome independent of the idempotency record's lifetime. Payload
        // admission in front of this handler, here and in spaced-host, still
        // applies: it runs before the request is decoded, so while every start
        // slot is held by a stalled spawn, a reconnect waits or is refused there.
        // Only a published process can be reconnected to. A same-key retry of
        // a start that is still publishing goes on to wait for its outcome.
        if self.inner.processes.is_published(&process_id) {
            return Err(process_id_in_use());
        }
        let account_name = user.unwrap_or_else(|| self.inner.default_operation_user.clone());
        let (account, _start_admission) = resolve_process_account(
            self.inner.process_tasks.clone(),
            self.inner.process_payload_slots.clone(),
            ingress,
            move || {
                GuestAccount::resolve(account_name)
                    .map_err(crate::identity::ResolveGuestAccountError::into_status)
            },
        )
        .await?;
        let account_lookup = received_at.elapsed();
        let listed_command = process_command.argv().to_vec();
        let env = parse_env(requested_env);
        let listed_env = proto_env(&env);
        let default_cwd = self
            .inner
            .default_working_dir
            .as_deref()
            .map(|path| {
                path.to_str()
                    .ok_or_else(|| Status::internal("image working directory is not UTF-8"))
            })
            .transpose()?;
        // A command's explicit cwd wins over the image's WORKDIR.
        let cwd = requested_cwd
            .as_deref()
            .or(default_cwd)
            .map(|cwd| guest_path::normalize_for_account(cwd, &account))
            .transpose()?;
        let listed_cwd = cwd.as_ref().map(|cwd| cwd.expose().to_string());
        let timeout = validate_optional_timeout(request.timeout)?;
        let background = request.background;
        let stdin_enabled = request.stdin;
        let pty_config = request.pty;
        let clean_env = request.clean_env;
        let idempotency_key =
            require_idempotency_key(request.idempotency_key.clone(), "process_start")?;

        let process_command = Pii::new(&process_command);
        let argv = process_command.expose_pii().argv();
        let Some((program, args)) = argv.split_first() else {
            return Err(Status::invalid_argument("command argv must not be empty"));
        };
        let mut command = Command::new(program);
        command.args(args);
        command.env_clear();
        command.env("PATH", DEFAULT_LOGIN_PATH);
        command.env("HOME", account.home());
        command.env("USER", account.name().as_str());
        command.env("LOGNAME", account.name().as_str());
        command.env("SHELL", account.shell());
        if !clean_env {
            let process_env = self.inner.process_env.read().await.clone();
            if !process_env.as_map().is_empty() {
                command.envs(
                    process_env
                        .as_map()
                        .iter()
                        .map(|(key, value)| (key.as_str(), Pii::new(value).expose_pii().expose())),
                );
            }
        }
        if !env.as_map().is_empty() {
            command.envs(
                env.as_map()
                    .iter()
                    .map(|(key, value)| (key.as_str(), Pii::new(value).expose_pii().expose())),
            );
        }
        // Place the workload in its `workloads/` cgroup before exec, and
        // before dropping credentials: the move writes a root-owned
        // `cgroup.procs`, and these pre_exec hooks run in registration order.
        if let Some(cgroups) = self.inner.guest_cgroups.as_ref() {
            cgroups.place_spawned_workload(&mut command);
        }
        // spaced runs with a protective OOM score (the unit's
        // `OOMScoreAdjust`), and children inherit it: reset the workload to
        // the default so the OOM killer prefers it over spaced, not the
        // reverse. Also before dropping credentials.
        reset_workload_oom_score(&mut command);
        let uses_pty = pty_config.is_some();
        if !uses_pty {
            command.process_group(0);
        }
        account.configure_child_credentials(
            &mut command,
            self.inner.identity_policy,
            cwd.as_ref()
                .map(|cwd| std::path::Path::new(cwd.expose()))
                .unwrap_or(account.home()),
        )?;
        let mut prepared_pty = pty_config
            .map(|pty_config| prepare_pty(pty_config.size))
            .transpose()?;
        if let Some(pty) = prepared_pty.as_mut() {
            let control_fd = pty.control_fd.as_raw_fd();
            // SAFETY: the pre_exec hook calls libc `login_tty` and captures a
            // pty slave fd kept open until `spawn` completes.
            unsafe {
                command.pre_exec(move || configure_child_pty(control_fd));
            }
        } else {
            if stdin_enabled {
                command.stdin(std::process::Stdio::piped());
            } else {
                command.stdin(std::process::Stdio::null());
            }
            command.stdout(std::process::Stdio::piped());
            command.stderr(std::process::Stdio::piped());
        }

        let (reservation, recovery_launch) = match recovery {
            Some(admission) => (admission.reservation, Some(admission.launch)),
            None => match self
                .inner
                .idempotency
                .reserve_or_get(idempotency_key)
                .await?
            {
                IdempotencyLookup::Reserved(reservation) => (reservation, None),
                IdempotencyLookup::Completed(result) => return self.replay_started(result),
            },
        };

        let process_reservation = self.inner.processes.reserve()?;
        // Before the start begins: a refused thread ends this start with its
        // key released, so a retry with the same key can succeed.
        let spawn_thread = SpawnThread::start().map_err(|error| {
            crate::refused(Status::unavailable(format!(
                "cannot start a process spawn thread: {error}"
            )))
        })?;

        // Cancellation while waiting for admission has no effect to replay:
        // the unstarted reservation releases its key on Drop. From `begin` until
        // the owner below is tracked there are no suspension points, and from
        // then on the owner, not this request, carries the start through
        // publication and supervisor handoff.
        let start_gate = self
            .inner
            .start_gate
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        crate::execution::require_running(&self.inner.shutdown)?;
        if let Some(recovery) = &recovery_launch {
            self.inner.recovery.check(recovery.fence)?;
        }
        // Re-claim the identity inside the gate. The check at the top of this
        // handler is reachable across suspension points, so on its own it would
        // let two starts carrying the same assigned id both pass and have the
        // second publication replace the first, leaving its live process
        // unreachable. The claim is authoritative until the record is
        // published, and it is taken before `spawn` so a refused start never
        // leaves a child behind.
        let process_id_claim = match self.inner.processes.claim(&process_id) {
            Ok(claim) => claim,
            Err(ProcessIdTaken::Published) => return Err(process_id_in_use()),
            // `AlreadyExists` would send the caller to reconnect, which cannot
            // succeed until the other start publishes its process.
            Err(ProcessIdTaken::Starting) => {
                return Err(Status::unavailable(
                    "process id is held by a start that has not published its process yet; retry",
                ));
            }
        };
        let operation = reservation.begin()?;
        let request_lifetime = _start_admission.request_lifetime();
        let launch = ProcessLaunch {
            command,
            recovery_launch,
            prepared_pty,
            stdin_enabled,
            info: ProcessInfo {
                process_id: process_id.as_str().to_string(),
                pid: 0,
                command: listed_command,
                cwd: listed_cwd,
                env: listed_env,
                user: listed_user,
                tag: listed_tag,
                pty: Some(uses_pty),
                exit: None,
                ended_at: None,
            },
            timeout,
            process_id: process_id.clone(),
            spawn_thread,
            claim: process_id_claim,
            process_reservation,
            start_admission: _start_admission,
            account_lookup,
            inner: Arc::clone(&self.inner),
        };
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        self.inner.process_tasks.spawn(async move {
            // The request may be gone. The process stays supervised either way,
            // and a same-key retry replays it.
            match launch.run(operation).await {
                Ok(LaunchedProcess {
                    recovery_incarnation: _,
                    pid,
                    started,
                    cursor,
                    supervision,
                }) => {
                    let _ = started_tx.send(Ok((pid, started, cursor)));
                    supervision.run().await;
                }
                Err(status) => {
                    let _ = started_tx.send(Err(status));
                }
            }
        });
        drop(start_gate);

        let (pid, started, cursor) = request_lifetime
            .observe(started_rx)
            .await
            .map_err(|_| Status::internal("process start ended without a result"))??;
        tracing::debug!(
            command = "bridge_guest_process_start",
            process_id = %process_id,
            pid,
            background,
            "spaced bridge guest command succeeded"
        );
        Ok(Response::new(
            process_event_stream_with_prefix(vec![started], Some(cursor)) as Self::StartStream,
        ))
    }

    async fn connect_process(
        &self,
        request: Request<ConnectProcessRequest>,
    ) -> Result<Response<Self::ConnectProcessStream>, Status> {
        let _operation = self.inner.lifecycle.admit()?;
        crate::payload::check_message(
            request.get_ref(),
            pplx_space_spaced_guest_protocol::process_limits::MAX_MESSAGE_BYTES,
        )?;
        let request = request.into_inner();
        let address = ProcessAddress::parse(request.process_id, request.recovery)?;
        let resume_after_output_gap = request.resume_after_output_gap.unwrap_or(false);
        // Validate the field for wire compatibility, but do not turn it into a
        // total stream-lifetime deadline here. ECW and native callers own
        // their outer stream budgets; enforcing the same value again in the
        // guest can truncate a healthy, continuously producing process.
        validate_optional_timeout(request.timeout)?;
        let target = self.select_process(address, TargetState::Retained).await?;
        let process_id = &target.id;
        let replay = target.record.replay(
            request.output_cursor,
            request.resume_from_oldest_retained,
            request.resume_after_output_gap.unwrap_or(false),
        );
        let pid = match &replay {
            ProcessReplay::Running { started, .. } | ProcessReplay::Completed { started, .. } => {
                started.pid
            }
        };
        let stream = match replay {
            ProcessReplay::Running { cursor, .. } => {
                let output_cursor = request.output_cursor.map(|_| cursor.next_sequence);
                process_event_stream_with_prefix(
                    vec![process_connected_event(
                        process_id,
                        pid,
                        output_cursor,
                        resume_after_output_gap,
                    )],
                    Some(cursor),
                )
            }
            replay @ ProcessReplay::Completed { .. } => replay_process_event_stream(replay),
        };
        tracing::debug!(
            command = "bridge_guest_process_connect",
            process_id = %process_id,
            ?pid,
            "spaced bridge guest command succeeded"
        );
        Ok(Response::new(stream as Self::ConnectProcessStream))
    }

    async fn list_processes(
        &self,
        request: Request<ListProcessesRequest>,
    ) -> Result<Response<Self::ListProcessesStream>, Status> {
        let _operation = self.inner.lifecycle.admit()?;
        crate::payload::check_message(
            request.get_ref(),
            pplx_space_spaced_guest_protocol::process_limits::MAX_MESSAGE_BYTES,
        )?;
        let request = request.into_inner();
        let processes = self
            .inner
            .processes
            .list(request.include_exited, request.process_id.as_deref());
        tracing::debug!(
            command = "bridge_guest_process_list",
            count = processes.len(),
            "spaced bridge guest command succeeded"
        );
        Ok(Response::new(
            Box::pin(tokio_stream::iter(processes.into_iter().map(Ok)))
                as Self::ListProcessesStream,
        ))
    }

    async fn send_signal(
        &self,
        request: Request<SendSignalRequest>,
    ) -> Result<Response<SendSignalResponse>, Status> {
        let _operation = self.inner.lifecycle.admit()?;
        crate::payload::check_message(
            request.get_ref(),
            pplx_space_spaced_guest_protocol::process_limits::MAX_MESSAGE_BYTES,
        )?;
        let request = request.into_inner();
        let address = ProcessAddress::parse(request.process_id, request.recovery)?;
        let signal =
            ProcessSignal::try_from_i32(request.signal).map_err(invalid_argument_status)?;
        let target = self.select_process(address, TargetState::Running).await?;
        let process_id = &target.id;
        let handle = &target.record;
        let kill = signal.as_i32() == libc::SIGKILL;
        match handle.signal_handle.signal(signal) {
            Ok(()) => {}
            // The leader can be reaped while buffered output still awaits its reader.
            Err(error) if kill && error.code() == tonic::Code::NotFound => {}
            Err(error) => return Err(error),
        }
        if kill {
            handle.journal.release_backpressure();
        }
        tracing::debug!(
            command = "bridge_guest_process_send_signal",
            process_id = %process_id,
            pid = handle.info.pid,
            "spaced bridge guest command succeeded"
        );
        Ok(Response::new(SendSignalResponse {}))
    }

    async fn send_input(
        &self,
        mut request: Request<SendInputRequest>,
    ) -> Result<Response<SendInputResponse>, Status> {
        let _operation = self.inner.lifecycle.admit()?;
        let _admission = crate::payload::admit(
            &mut request,
            &self.inner.process_payload_slots,
            1,
            pplx_space_metrics::payload::Budget::GuestProcess,
            pplx_space_spaced_guest_protocol::process_limits::MAX_MESSAGE_BYTES,
        )?;
        let mut request = request.into_inner();
        let address = ProcessAddress::parse(
            std::mem::take(&mut request.process_id),
            request.recovery.take(),
        )?;
        let target = self.select_process(address, TargetState::Running).await?;
        let process_id = &target.id;
        let record = &target.record;
        record.write_stdin(&request.data).await?;
        tracing::debug!(
            command = "bridge_guest_process_send_input",
            process_id = %process_id,
            bytes = request.data.len(),
            "spaced bridge guest command succeeded"
        );
        Ok(Response::new(SendInputResponse {}))
    }

    async fn close_stdin(
        &self,
        request: Request<CloseStdinRequest>,
    ) -> Result<Response<CloseStdinResponse>, Status> {
        let _operation = self.inner.lifecycle.admit()?;
        crate::payload::check_message(
            request.get_ref(),
            pplx_space_spaced_guest_protocol::process_limits::MAX_MESSAGE_BYTES,
        )?;
        let request = request.into_inner();
        let address = ProcessAddress::parse(request.process_id, request.recovery)?;
        let target = self.select_process(address, TargetState::Running).await?;
        let process_id = &target.id;
        let record = &target.record;
        record.close_stdin().await?;
        tracing::debug!(
            command = "bridge_guest_process_close_stdin",
            process_id = %process_id,
            "spaced bridge guest command succeeded"
        );
        Ok(Response::new(CloseStdinResponse {}))
    }

    async fn resize_pty(
        &self,
        request: Request<ResizePtyRequest>,
    ) -> Result<Response<ResizePtyResponse>, Status> {
        let _operation = self.inner.lifecycle.admit()?;
        crate::payload::check_message(
            request.get_ref(),
            pplx_space_spaced_guest_protocol::process_limits::MAX_MESSAGE_BYTES,
        )?;
        let request = request.into_inner();
        let address = ProcessAddress::parse(request.process_id, request.recovery)?;
        let size = request
            .size
            .ok_or_else(|| Status::invalid_argument("pty size is required"))?;
        let target = self.select_process(address, TargetState::Running).await?;
        let process_id = &target.id;
        let record = &target.record;
        record.resize_pty(size)?;
        tracing::debug!(
            command = "bridge_guest_process_resize_pty",
            process_id = %process_id,
            "spaced bridge guest command succeeded"
        );
        Ok(Response::new(ResizePtyResponse {}))
    }
}

#[cfg(all(test, unix))]
mod tests;
