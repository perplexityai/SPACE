//! Owned process launch, publication and supervision.

use super::*;

/// A published process, owned by the start's tracked task until its exit is
/// recorded. The registry's reaper ends its record's retention.
pub(super) struct ProcessSupervision {
    pub(super) child: tokio::process::Child,
    pub(super) exit_observer: ExitObserver,
    pub(super) output_tasks: VecDeque<ProcessOutputTask>,
    pub(super) timeout: Option<pplx_space_common_core::types::ReadyTimeout>,
    pub(super) shutdown: tokio_util::sync::CancellationToken,
    pub(super) record: Arc<ProcessRecord>,
    pub(super) process_id: ProcessId,
    pub(super) pid: u32,
    pub(super) registry: ProcessRegistry,
    pub(super) retained_tasks: tokio_util::task::TaskTracker,
    pub(super) closed: CancellationToken,
}

impl ProcessSupervision {
    pub(super) async fn run(self) {
        let ProcessSupervision {
            mut child,
            exit_observer,
            mut output_tasks,
            timeout,
            shutdown,
            record,
            process_id,
            pid,
            registry,
            retained_tasks,
            closed,
        } = self;
        let completion = async {
            #[cfg(target_os = "linux")]
            match exit_observer.exited().await {
                // Preserve the leader's identity until descendants have
                // released its output, but stop its execution deadline now.
                Ok(()) => return Ok(None),
                Err(error) => {
                    tracing::warn!(target: LOG_TARGET, %error, pid, "pidfd observation failed; falling back to child wait")
                }
            }
            child.wait().await.map(Some)
        };
        let deadline = async {
            match timeout {
                Some(timeout) => tokio::time::sleep(timeout.as_duration()).await,
                None => std::future::pending::<()>().await,
            }
        };
        let waited = tokio::select! {
            biased;
            status = completion => Ok(status),
            _ = shutdown.cancelled() => Err(ProcessExitOutcome::Signaled(libc::SIGKILL)),
            _ = deadline => Err(ProcessExitOutcome::TimedOut(libc::SIGKILL)),
        };
        let outcome = match waited {
            Ok(Ok(None)) => {
                let drained = tokio::select! {
                    drained = drain_process_output_tasks(
                        &mut output_tasks, &process_id, pid, false,
                    ) => drained,
                    _ = shutdown.cancelled() => false,
                };
                if !drained && let Err(error) = record.signal_handle.terminate_group() {
                    tracing::warn!(target: LOG_TARGET, %error, pid, "failed to terminate descendants holding output open");
                }
                process_status_outcome(child.wait().await.ok())
            }
            Ok(status) => process_status_outcome(status.ok().flatten()),
            Err(outcome) => {
                record.journal.release_backpressure();
                if let Err(error) = record.signal_handle.terminate_group() {
                    tracing::warn!(target: LOG_TARGET, %error, pid, "failed to terminate process group");
                }
                if let Err(error) = child.wait().await {
                    tracing::warn!(target: LOG_TARGET, %error, pid, "failed to reap terminated child");
                }
                outcome
            }
        };

        if !output_tasks.is_empty() {
            tokio::select! {
                _ = drain_process_output_tasks(
                    &mut output_tasks, &process_id, pid, true,
                ) => {},
                _ = shutdown.cancelled() => {
                    record.journal.release_backpressure();
                    drain_process_output_tasks(
                        &mut output_tasks, &process_id, pid, true,
                    ).await;
                },
            }
        }
        let exit = process_exit_event(&process_id, Some(pid), outcome);
        // Completed first, so input that arrives from here on is refused as
        // for an exited process before its descriptors close. Output tasks
        // have ended, so these are the last input descriptors. The signal
        // pidfd stays: a signal racing this exit uses it safely, as a reaped
        // pidfd only reports ESRCH, and the record limit bounds it.
        record.journal.complete(exit);
        record.input.release();
        // Release supervisor ownership. Eviction can free this record's
        // permit once any in-flight stdin/control handlers also release it.
        drop(record);
        // The reaped child and its exit-observer pidfd are no longer needed.
        // Holding them through the retention wait below kept a descriptor
        // per start for that whole window, exhausting the daemon's limit at
        // sustained start rates.
        drop(child);
        #[cfg(target_os = "linux")]
        drop(exit_observer);
        #[cfg(not(target_os = "linux"))]
        let () = exit_observer;
        // The record stays for `COMPLETED_PROCESS_RETENTION`; the registry's
        // single reaper removes it. Waiting here instead kept this whole task
        // (its future, about 2 KiB) alive for the retention of every start:
        // ~48 MiB at 450 starts/s with a 60 s retention, though at most 128
        // records exist.
        drop(process_id);
        registry.ensure_reaper(closed, &retained_tasks);
    }
}

/// An admitted start from `begin` until its process is published. The start's
/// tracked owner task runs it, so it completes even if the request goes away.
pub(super) struct ProcessLaunch {
    pub(super) command: Command,
    pub(super) recovery_launch: Option<RecoveryLaunch>,
    pub(super) prepared_pty: Option<PreparedPty>,
    pub(super) stdin_enabled: bool,
    /// What `list` reports; the pid is filled in once the child exists.
    pub(super) info: ProcessInfo,
    pub(super) timeout: Option<pplx_space_common_core::types::ReadyTimeout>,
    pub(super) process_id: ProcessId,
    pub(super) spawn_thread: SpawnThread,
    pub(super) claim: ProcessIdClaim,
    pub(super) process_reservation: ProcessRecordReservation,
    /// Held until the child is published.
    pub(super) start_admission: pplx_space_common_grpc::RequestPermit,
    pub(super) account_lookup: Duration,
    pub(super) inner: Arc<crate::execution::ExecutionServiceInner>,
}

pub(super) type SpawnOutcome = (io::Result<tokio::process::Child>, Option<PreparedPty>);

/// A thread of its own for one start's `spawn`. `spawn` blocks until the
/// child execs, which can take seconds under memory or I/O pressure, so it
/// stays off the runtime threads that serve every other request. Not on the
/// blocking pool: a pool task is queued before its thread exists, and when
/// none can be created `spawn_blocking` panics with the spawn still queued,
/// to run later for a start that has already failed. The thread is created
/// before its start begins, so a guest that cannot create one refuses the
/// start with its key still free for a retry. Dropped before
/// [`SpawnThread::spawn`], the thread ends without spawning anything. Start
/// admission bounds these threads to the concurrent starts.
pub(super) struct SpawnThread {
    pub(super) handoff: tokio::sync::oneshot::Sender<(Command, Option<PreparedPty>)>,
    pub(super) outcome: tokio::sync::oneshot::Receiver<SpawnOutcome>,
}

impl SpawnThread {
    pub(super) fn start() -> io::Result<Self> {
        #[cfg(test)]
        if REFUSE_SPAWN_THREADS.get() {
            return Err(io::Error::from_raw_os_error(libc::EAGAIN));
        }
        let runtime = tokio::runtime::Handle::current();
        let (handoff, handoff_rx) = tokio::sync::oneshot::channel();
        let (outcome_tx, outcome) = tokio::sync::oneshot::channel::<SpawnOutcome>();
        std::thread::Builder::new()
            .name("spaced-process-spawn".to_string())
            .spawn(move || {
                let Ok((mut command, prepared_pty)) = handoff_rx.blocking_recv() else {
                    // The start ended before handing over its command.
                    return;
                };
                // Tokio's child handling registers with the runtime.
                let _runtime = runtime.enter();
                let child = spawn_child(&mut command);
                // The PTY slave stays open until `spawn` returns.
                if let Err((Ok(mut child), _pty)) = outcome_tx.send((child, prepared_pty)) {
                    // Its start is gone (the runtime is shutting down), so no
                    // one will supervise the child: stop it.
                    if let Err(error) = child.start_kill() {
                        tracing::warn!(target: LOG_TARGET, %error, "failed to stop an unsupervised spawned child");
                    }
                }
            })?;
        Ok(Self { handoff, outcome })
    }

    /// Spawn `command` on the thread. An error once the command is handed
    /// over leaves unknown whether it ran.
    pub(super) async fn spawn(
        self,
        command: Command,
        prepared_pty: Option<PreparedPty>,
    ) -> Result<SpawnOutcome, Status> {
        self.handoff
            .send((command, prepared_pty))
            .map_err(|_| Status::internal("process spawn thread ended before its command"))?;
        self.outcome.await.map_err(|_| {
            Status::internal(
                "process spawn thread ended without a result; whether the command started is unknown",
            )
        })
    }
}

/// A published process: what its start answers with, and its supervision.
pub(super) struct LaunchedProcess {
    pub(super) recovery_incarnation: Option<ProcessRecoveryToken>,
    pub(super) pid: u32,
    pub(super) started: ProcessEvent,
    pub(super) cursor: ProcessJournalCursor,
    pub(super) supervision: ProcessSupervision,
}

impl ProcessLaunch {
    /// Publish the process and settle the start's idempotent operation with
    /// the outcome, so a same-key retry replays it.
    pub(super) async fn run(
        mut self,
        operation: IdempotencyOperation,
    ) -> Result<LaunchedProcess, Status> {
        let process_id = self.process_id.clone();
        let recovery_launch = self.recovery_launch.take();
        let launched = self
            .publish(recovery_launch.as_ref().map(|launch| launch.fence))
            .await;
        operation.complete(match &launched {
            Ok(launched) => Ok(match launched.recovery_incarnation {
                Some(incarnation) => IdempotencyRecord::RecoveryProcessStarted {
                    process_id,
                    incarnation,
                },
                None => IdempotencyRecord::ProcessStarted { process_id },
            }),
            Err(status) => Err(status.clone()),
        });
        drop(recovery_launch);
        launched
    }

    pub(super) async fn publish(
        self,
        recovery_fence: Option<pplx_space_spaced_guest_protocol::recovery::RecoveryFence>,
    ) -> Result<LaunchedProcess, Status> {
        let ProcessLaunch {
            command,
            recovery_launch: _,
            prepared_pty,
            stdin_enabled,
            mut info,
            timeout,
            process_id,
            spawn_thread,
            claim,
            process_reservation,
            start_admission,
            account_lookup,
            inner,
        } = self;
        let _start_admission = start_admission;
        let spawn_started_at = Instant::now();
        let spawned = spawn_thread.spawn(command, prepared_pty).await;
        let spawn = spawn_started_at.elapsed();
        if [account_lookup, spawn]
            .iter()
            .any(|phase| *phase >= SLOW_START_PHASE)
        {
            tracing::warn!(target: LOG_TARGET,
                process_id = %process_id,
                pty = info.pty.unwrap_or_default(),
                account_lookup_ms = account_lookup.as_millis(),
                spawn_ms = spawn.as_millis(),
                spawned = matches!(spawned, Ok((Ok(_), _))),
                "slow guest process start"
            );
        }
        let (spawn_result, mut prepared_pty) = spawned?;
        let mut child = spawn_result.map_err(map_io_error)?;
        let Some(pid) = child.id() else {
            terminate_and_reap_failed_start(child, process_reservation, &inner.process_tasks);
            return Err(Status::internal("spawned child has no pid"));
        };
        let (signal_handle, exit_observer) = match ProcessSignalHandle::open(pid) {
            Ok(handle) => handle,
            Err(error) => {
                terminate_and_reap_failed_start(child, process_reservation, &inner.process_tasks);
                return Err(Status::internal(format!(
                    "failed to create safe handle for spawned process: {error}"
                )));
            }
        };
        let (input, pty_reader, stdout, stderr) = if let Some(pty) = prepared_pty.take() {
            (
                ProcessInput::Pty(PtyInput {
                    writer: InputDescriptor::new(Some(pty.writer)),
                    resize_fd: std::sync::Mutex::new(Some(pty.resize_fd)),
                }),
                Some(pty.reader),
                None,
                None,
            )
        } else {
            let stdin = if stdin_enabled {
                child.stdin.take()
            } else {
                None
            };
            let input = if stdin_enabled {
                ProcessInput::Pipe(InputDescriptor::new(stdin))
            } else {
                ProcessInput::Disabled
            };
            (input, None, child.stdout.take(), child.stderr.take())
        };
        // Retain independent, close-on-exec descriptors to distinguish open
        // writers from output tasks waiting only for journal delivery. Prepare
        // them before publishing the child, so allocation failure uses normal
        // failed-start cleanup.
        let sources = [
            pty_reader.as_ref().map(AsFd::as_fd),
            stdout.as_ref().map(AsFd::as_fd),
            stderr.as_ref().map(AsFd::as_fd),
        ];
        let mut output_sources = match sources
            .into_iter()
            .flatten()
            .map(|fd| fd.try_clone_to_owned())
            .collect::<io::Result<VecDeque<_>>>()
        {
            Ok(sources) => sources,
            Err(error) => {
                terminate_and_reap_failed_start(child, process_reservation, &inner.process_tasks);
                return Err(map_io_error(error));
            }
        };
        let mut started = match process_started_event(&process_id, Some(pid)) {
            Ok(started) => started,
            Err(status) => {
                terminate_and_reap_failed_start(child, process_reservation, &inner.process_tasks);
                return Err(status);
            }
        };
        let recovery_identity = recovery_fence.map(|fence| recovery::RecoveryIdentity {
            activation: fence.activation,
            incarnation: ProcessRecoveryToken::new(),
        });
        let recovery_incarnation = recovery_identity.map(|identity| identity.incarnation);
        if let Some(incarnation) = recovery_incarnation
            && let Some(
                pplx_space_spaced_guest_protocol::proto::operations::process_event::Event::Started(
                    event,
                ),
            ) = started.event.as_mut()
        {
            event.instance_id = Some(inner.recovery.instance.to_string());
            event.process_incarnation = Some(incarnation.to_string());
        }
        info.pid = pid;
        let record = Arc::new(ProcessRecord::new(
            started.clone(),
            recovery_identity,
            info,
            input,
            signal_handle,
            process_reservation,
        ));
        claim.publish(Arc::clone(&record));

        let cursor = record.journal.cursor(Some(0), false, None);
        let mut output_tasks = VecDeque::new();
        if let Some(reader) = pty_reader {
            output_tasks.push_back(spawn_process_output_reader(
                reader,
                output_sources
                    .pop_front()
                    .expect("each reader has an output descriptor"),
                Arc::clone(&record),
                process_id.clone(),
                pid,
                ProcessOutputKind::Stdout,
            ));
        }
        if let Some(stdout) = stdout {
            output_tasks.push_back(spawn_process_output_reader(
                stdout,
                output_sources
                    .pop_front()
                    .expect("each reader has an output descriptor"),
                Arc::clone(&record),
                process_id.clone(),
                pid,
                ProcessOutputKind::Stdout,
            ));
        }
        if let Some(stderr) = stderr {
            output_tasks.push_back(spawn_process_output_reader(
                stderr,
                output_sources
                    .pop_front()
                    .expect("each reader has an output descriptor"),
                Arc::clone(&record),
                process_id.clone(),
                pid,
                ProcessOutputKind::Stderr,
            ));
        }
        let supervision = ProcessSupervision {
            child,
            exit_observer,
            output_tasks,
            timeout,
            shutdown: inner.shutdown.clone(),
            closed: inner.lifecycle.closed(),
            retained_tasks: inner.process_tasks.clone(),
            record: Arc::clone(&record),
            process_id,
            pid,
            registry: inner.processes.clone(),
        };
        Ok(LaunchedProcess {
            recovery_incarnation,
            pid,
            started,
            cursor,
            supervision,
        })
    }
}
