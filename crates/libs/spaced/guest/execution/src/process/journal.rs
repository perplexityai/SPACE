//! Bounded output retention, replay cursors and process events.

use super::*;

// Charge payloads and event storage, including each repeated process ID: tiny
// writes must not turn a bounded output window into millions of allocations.
// The primary attached reader applies backpressure; detached replay stays bounded.
// The total is sized from the guest's memory (see `ProcessOutputBudget`): a
// fixed budget would be 3% of a 2 GiB guest but an eighth of a 512 MiB one.
pub(super) const PROCESS_OUTPUT_RETENTION_MAX_BYTES: usize = 64 * 1024 * 1024;

pub(super) const PROCESS_OUTPUT_RETENTION_MIN_BYTES: usize = 16 * 1024 * 1024;

// One process retains at most a quarter of the smallest total.
pub(super) const PROCESS_OUTPUT_RETENTION_BYTES: usize = PROCESS_OUTPUT_RETENTION_MIN_BYTES / 4;

/// How much process output spaced retains, sized from the guest's memory: a
/// thirty-second of it, between 16 and 64 MiB (a 2 GiB guest keeps the full
/// 64 MiB). Half is guaranteed, split evenly across the record slots, so every
/// admitted record keeps a useful stream-sized floor without competing
/// globally; larger replay windows borrow from the shared other half, and
/// exhaustion evicts only that process's older output.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ProcessOutputBudget {
    pub(super) guaranteed_per_record: usize,
    pub(super) shared: usize,
}

impl ProcessOutputBudget {
    pub(crate) fn for_guest_memory(mem_total_bytes: Option<u64>) -> Self {
        let total = mem_total_bytes
            .and_then(|bytes| usize::try_from(bytes / 32).ok())
            .map_or(PROCESS_OUTPUT_RETENTION_MAX_BYTES, |total| {
                total.clamp(
                    PROCESS_OUTPUT_RETENTION_MIN_BYTES,
                    PROCESS_OUTPUT_RETENTION_MAX_BYTES,
                )
            });
        let guaranteed_per_record = total / 2 / PROCESS_RECORD_LIMIT;
        Self {
            guaranteed_per_record,
            shared: total - guaranteed_per_record * PROCESS_RECORD_LIMIT,
        }
    }

    #[cfg(test)]
    pub(crate) fn total(self) -> usize {
        self.guaranteed_per_record * PROCESS_RECORD_LIMIT + self.shared
    }
}

// The default budget's values, for tests written against a registry or
// journal built without a guest size.
#[cfg(test)]
pub(super) const PROCESS_OUTPUT_GUARANTEED_BYTES: usize =
    PROCESS_OUTPUT_RETENTION_MAX_BYTES / 2 / PROCESS_RECORD_LIMIT;

#[cfg(test)]
pub(super) const PROCESS_OUTPUT_SHARED_BYTES: usize =
    PROCESS_OUTPUT_RETENTION_MAX_BYTES - PROCESS_RECORD_LIMIT * PROCESS_OUTPUT_GUARANTEED_BYTES;

pub(super) const PROCESS_OUTPUT_CURSOR_AHEAD_MESSAGE: &str =
    "process output cursor is past the output the process has written";

pub(super) struct ProcessEventJournal {
    pub(super) state: std::sync::Mutex<ProcessJournalState>,
    pub(super) changed: watch::Sender<ProcessJournalChange>,
    pub(super) shared_output_slots: Arc<Semaphore>,
    pub(super) budget: ProcessOutputBudget,
    pub(super) consumed: watch::Sender<()>,
}

impl ProcessEventJournal {
    #[cfg(test)]
    pub(super) fn new(shared_output_slots: Arc<Semaphore>) -> Self {
        Self::with_budget(
            shared_output_slots,
            ProcessOutputBudget::for_guest_memory(None),
        )
    }

    pub(super) fn with_budget(
        shared_output_slots: Arc<Semaphore>,
        budget: ProcessOutputBudget,
    ) -> Self {
        let (changed, _) = watch::channel(ProcessJournalChange::OutputChanged);
        Self {
            state: std::sync::Mutex::new(ProcessJournalState::default()),
            changed,
            shared_output_slots,
            budget,
            consumed: watch::channel(()).0,
        }
    }

    pub(super) fn subscribe(&self) -> watch::Receiver<ProcessJournalChange> {
        self.changed.subscribe()
    }

    #[cfg(test)]
    pub(super) fn tail_cursor(&self) -> u64 {
        self.state
            .lock()
            .expect("process output journal lock poisoned")
            .next_sequence
    }

    pub(super) fn exit(&self) -> Option<ProcessEvent> {
        self.state
            .lock()
            .expect("process output journal lock poisoned")
            .exit
            .clone()
    }

    pub(super) fn is_completed(&self) -> bool {
        self.state
            .lock()
            .expect("process output journal lock poisoned")
            .exit
            .is_some()
    }

    pub(super) fn is_expired(&self, retention: Duration) -> bool {
        self.state
            .lock()
            .expect("process output journal lock poisoned")
            .completed_at
            .is_some_and(|completed_at| completed_at.elapsed() > retention)
    }

    /// Drops the retained output of a process that completed more than
    /// the configured output window ago, keeping its exit, and says
    /// whether this call released it. A reconnect without a cursor needs just
    /// the start and the exit; one that resumes from a cursor reads the lagged
    /// error from then on.
    pub(super) fn release_output_if_due(&self, retention: Duration) -> bool {
        let mut state = self
            .state
            .lock()
            .expect("process output journal lock poisoned");
        if state.output_released
            || !state
                .completed_at
                .is_some_and(|completed_at| completed_at.elapsed() > retention)
        {
            return false;
        }
        let cleared: u64 = state
            .output
            .iter()
            .map(|event| output_data_bytes(&event.event))
            .sum();
        state.dropped_output_bytes += cleared;
        state.output.clear();
        state.retained_bytes = 0;
        state.guaranteed_bytes = 0;
        state.first_sequence = state.next_sequence;
        state.output_released = true;
        true
    }

    /// When the reaper next has work for this record: releasing its output,
    /// then ending its retention (just after each, as both are strictly past).
    pub(super) fn next_deadline(
        &self,
        retention: ProcessRetention,
    ) -> Option<tokio::time::Instant> {
        let state = self
            .state
            .lock()
            .expect("process output journal lock poisoned");
        let retained_for = if state.output_released {
            retention.completed
        } else {
            retention.output.min(retention.completed)
        };
        state
            .completed_at
            .map(|completed_at| completed_at + retained_for + Duration::from_millis(1))
    }

    /// The exit and its wall-clock time, once the process has completed.
    pub(super) fn ended(&self) -> Option<(ProcessEvent, Option<SystemTime>)> {
        let state = self
            .state
            .lock()
            .expect("process output journal lock poisoned");
        state.exit.clone().map(|exit| (exit, state.ended_at))
    }

    /// Moves the completion time back, as if `by` had passed since.
    #[cfg(test)]
    pub(super) fn backdate_completion(&self, by: Duration) {
        let mut state = self
            .state
            .lock()
            .expect("process output journal lock poisoned");
        state.completed_at = state.completed_at.map(|completed_at| completed_at - by);
    }

    pub(super) fn completed_at(&self) -> Option<tokio::time::Instant> {
        self.state
            .lock()
            .expect("process output journal lock poisoned")
            .completed_at
    }

    /// A reader from `requested`, or from the live tail when `None`. With
    /// `from_oldest_retained`, a cursor before the oldest retained output
    /// starts there instead of lagging. That start is chosen under the lock
    /// that also claims the primary reader's place, so no output written
    /// meanwhile can evict it.
    pub(super) fn cursor(
        self: &Arc<Self>,
        requested: Option<u64>,
        from_oldest_retained: bool,
        gap_resume: Option<OutputGapResume>,
    ) -> ProcessJournalCursor {
        let mut state = self
            .state
            .lock()
            .expect("process output journal lock poisoned");
        let next_sequence = match requested {
            Some(cursor) if from_oldest_retained && gap_resume.is_none() => {
                cursor.max(state.first_sequence)
            }
            Some(cursor) => cursor,
            None => state.next_sequence,
        };
        // Only one reader controls the producer. Observers cannot stall an
        // existing consumer; after disconnect a new attachment can take over.
        // A gap reader reports its original cursor, but holds the first retained
        // position now, under the same lock, so reporting the gap cannot lose it.
        let primary_sequence = if gap_resume.is_some() {
            next_sequence.max(state.first_sequence)
        } else {
            next_sequence
        };
        let primary = if !state.backpressure_disabled
            && state.primary.upgrade().is_none()
            && primary_sequence >= state.first_sequence
            && primary_sequence <= state.next_sequence
        {
            let position = Arc::new(AtomicU64::new(primary_sequence));
            state.primary = Arc::downgrade(&position);
            Some(position)
        } else {
            None
        };
        ProcessJournalCursor {
            journal: Arc::downgrade(self),
            changed: self.subscribe(),
            next_sequence,
            primary,
            gap_resume,
        }
    }

    pub(super) fn release_backpressure(&self) {
        {
            let mut state = self
                .state
                .lock()
                .expect("process output journal lock poisoned");
            state.primary = Weak::new();
            state.backpressure_disabled = true;
        }
        self.consumed.send_replace(());
    }

    pub(super) async fn publish_output(&self, mut event: ProcessEvent) {
        let mut consumed = self.consumed.subscribe();
        loop {
            match self.try_append_output(event) {
                None => return,
                Some(pending) => event = pending,
            }
            if consumed.changed().await.is_err() {
                return;
            }
        }
    }

    #[cfg(test)]
    pub(super) fn append_output(&self, event: ProcessEvent) {
        assert!(self.try_append_output(event).is_none());
    }

    pub(super) fn try_append_output(&self, event: ProcessEvent) -> Option<ProcessEvent> {
        let (output_bytes, data_bytes) = match event.event.as_ref() {
            Some(
                pplx_space_spaced_guest_protocol::proto::bridge_guest::process_event::Event::Stdout(
                    output,
                ),
            )
            | Some(
                pplx_space_spaced_guest_protocol::proto::bridge_guest::process_event::Event::Stderr(
                    output,
                ),
            ) => (
                output.data.capacity()
                    + event.process_id.capacity()
                    + std::mem::size_of::<SequencedProcessEvent>(),
                output.data.len() as u64,
            ),
            _ => unreachable!("process journal only retains output events"),
        };
        if output_bytes > PROCESS_OUTPUT_RETENTION_BYTES {
            {
                let mut state = self
                    .state
                    .lock()
                    .expect("process output journal lock poisoned");
                let cleared: u64 = state
                    .output
                    .iter()
                    .map(|event| output_data_bytes(&event.event))
                    .sum();
                state.dropped_output_bytes += cleared + data_bytes;
                state.output.clear();
                state.retained_bytes = 0;
                state.guaranteed_bytes = 0;
                state.next_sequence += 1;
                state.first_sequence = state.next_sequence;
            }
            self.changed
                .send_replace(ProcessJournalChange::OutputChanged);
            return None;
        }
        {
            let mut state = self
                .state
                .lock()
                .expect("process output journal lock poisoned");
            let sequence = state.next_sequence;
            while state.retained_bytes.saturating_add(output_bytes) > PROCESS_OUTPUT_RETENTION_BYTES
            {
                if !can_evict_process_output(&state) {
                    return Some(event);
                }
                evict_oldest_process_output(&mut state);
            }
            let shared_output_slot = loop {
                let guaranteed_bytes = output_bytes.min(
                    self.budget
                        .guaranteed_per_record
                        .saturating_sub(state.guaranteed_bytes),
                );
                let shared_bytes = output_bytes - guaranteed_bytes;
                let shared_output_slot = if shared_bytes == 0 {
                    None
                } else {
                    match self
                        .shared_output_slots
                        .clone()
                        .try_acquire_many_owned(shared_bytes as u32)
                    {
                        Ok(slot) => Some(slot),
                        Err(TryAcquireError::NoPermits | TryAcquireError::Closed)
                            if !state.output.is_empty() =>
                        {
                            if !can_evict_process_output(&state) {
                                return Some(event);
                            }
                            evict_oldest_process_output(&mut state);
                            continue;
                        }
                        Err(TryAcquireError::NoPermits | TryAcquireError::Closed) => {
                            state.first_sequence = state.next_sequence + 1;
                            state.dropped_output_bytes += data_bytes;
                            break None;
                        }
                    }
                };
                state.guaranteed_bytes += guaranteed_bytes;
                break Some((guaranteed_bytes, shared_output_slot));
            };
            state.next_sequence += 1;
            let Some((guaranteed_bytes, shared_output_slot)) = shared_output_slot else {
                drop(state);
                self.changed
                    .send_replace(ProcessJournalChange::OutputChanged);
                return None;
            };
            state.retained_bytes += output_bytes;
            state.output.push_back(SequencedProcessEvent {
                sequence,
                event,
                output_bytes,
                guaranteed_bytes,
                _shared_output_slot: shared_output_slot,
            });
        }
        self.changed
            .send_replace(ProcessJournalChange::OutputChanged);
        None
    }

    pub(super) fn complete(&self, exit: ProcessEvent) {
        let (next_sequence, output_bytes) = {
            let mut state = self
                .state
                .lock()
                .expect("process output journal lock poisoned");
            state.exit = Some(exit.clone());
            state.completed_at = Some(tokio::time::Instant::now());
            state.ended_at = Some(SystemTime::now());
            let retained: u64 = state
                .output
                .iter()
                .map(|event| output_data_bytes(&event.event))
                .sum();
            (state.next_sequence, state.dropped_output_bytes + retained)
        };
        self.changed.send_replace(ProcessJournalChange::Exit {
            event: exit,
            next_sequence,
            output_bytes,
        });
    }

    pub(super) fn read(&self, cursor: u64) -> ProcessJournalRead {
        let state = self
            .state
            .lock()
            .expect("process output journal lock poisoned");
        if cursor < state.first_sequence {
            return ProcessJournalRead::Lagged {
                first_sequence: state.first_sequence,
                output_bytes_before: state.dropped_output_bytes,
            };
        }
        // A cursor only ever advances over output it has read, so one past the
        // end came from a caller, not from this journal: waiting for output to
        // reach it would skip everything written before it.
        if cursor > state.next_sequence {
            return ProcessJournalRead::Ahead;
        }
        if cursor < state.next_sequence {
            let offset = (cursor - state.first_sequence) as usize;
            return ProcessJournalRead::Event(
                state
                    .output
                    .get(offset)
                    .expect("process cursor within retained output window")
                    .event
                    .clone(),
            );
        }
        state
            .exit
            .clone()
            .map_or(ProcessJournalRead::Pending, ProcessJournalRead::Exit)
    }
}

#[derive(Default)]
pub(super) struct ProcessJournalState {
    pub(super) primary: Weak<AtomicU64>,
    pub(super) backpressure_disabled: bool,
    pub(super) output: VecDeque<SequencedProcessEvent>,
    pub(super) first_sequence: u64,
    pub(super) next_sequence: u64,
    pub(super) retained_bytes: usize,
    pub(super) guaranteed_bytes: usize,
    /// Data bytes of every output event below `first_sequence`: evicted, or
    /// dropped on arrival.
    pub(super) dropped_output_bytes: u64,
    pub(super) exit: Option<ProcessEvent>,
    // Tokio's clock, which is the reaper's, so tests can drive it.
    pub(super) completed_at: Option<tokio::time::Instant>,
    pub(super) ended_at: Option<SystemTime>,
    pub(super) output_released: bool,
}

pub(super) struct SequencedProcessEvent {
    pub(super) sequence: u64,
    pub(super) event: ProcessEvent,
    pub(super) output_bytes: usize,
    pub(super) guaranteed_bytes: usize,
    pub(super) _shared_output_slot: Option<OwnedSemaphorePermit>,
}

pub(super) fn can_evict_process_output(state: &ProcessJournalState) -> bool {
    state
        .primary
        .upgrade()
        .is_none_or(|position| position.load(Ordering::Acquire) > state.first_sequence)
}

pub(super) fn evict_oldest_process_output(state: &mut ProcessJournalState) {
    let removed = state
        .output
        .pop_front()
        .expect("retained byte count requires a process output event");
    state.retained_bytes -= removed.output_bytes;
    state.guaranteed_bytes -= removed.guaranteed_bytes;
    state.dropped_output_bytes += output_data_bytes(&removed.event);
    state.first_sequence = removed.sequence + 1;
}

#[derive(Clone)]
pub(super) enum ProcessJournalChange {
    OutputChanged,
    Exit {
        event: ProcessEvent,
        next_sequence: u64,
        /// Stdout and stderr data bytes in every output event, retained or not.
        output_bytes: u64,
    },
}

pub(super) enum ProcessJournalRead {
    Event(ProcessEvent),
    Exit(ProcessEvent),
    Pending,
    Lagged {
        first_sequence: u64,
        output_bytes_before: u64,
    },
    Ahead,
}

#[derive(Clone, Copy)]
pub(super) enum ProcessOutputKind {
    Stdout,
    Stderr,
}

impl ProcessOutputKind {
    pub(super) const fn as_str(self) -> &'static str {
        match self {
            Self::Stdout => "stdout",
            Self::Stderr => "stderr",
        }
    }
}

pub(super) struct ProcessJournalCursor {
    pub(super) journal: Weak<ProcessEventJournal>,
    pub(super) changed: watch::Receiver<ProcessJournalChange>,
    pub(super) next_sequence: u64,
    pub(super) primary: Option<Arc<AtomicU64>>,
    /// Set when the reader asked to continue past evicted output.
    pub(super) gap_resume: Option<OutputGapResume>,
}

impl Drop for ProcessJournalCursor {
    fn drop(&mut self) {
        self.primary.take();
        if let Some(journal) = self.journal.upgrade() {
            journal.consumed.send_replace(());
        }
    }
}

pub(super) enum ProcessReplay {
    Running {
        started: ProcessEvent,
        cursor: ProcessJournalCursor,
    },
    Completed {
        started: ProcessEvent,
        exit: ProcessEvent,
    },
}

pub(super) fn replay_process_event_stream(
    replay: ProcessReplay,
) -> std::pin::Pin<Box<dyn futures_util::Stream<Item = Result<ProcessEvent, Status>> + Send>> {
    match replay {
        ProcessReplay::Running {
            started, cursor, ..
        } => process_event_stream_with_prefix(vec![started], Some(cursor)),
        ProcessReplay::Completed { started, exit } => {
            process_event_stream_with_prefix(vec![started, exit], None)
        }
    }
}

pub(super) fn process_event_stream_with_prefix(
    prefix: Vec<ProcessEvent>,
    cursor: Option<ProcessJournalCursor>,
) -> std::pin::Pin<Box<dyn futures_util::Stream<Item = Result<ProcessEvent, Status>> + Send>> {
    // Pull directly from the bounded journal: a stalled subscriber owns only
    // its cursor, without a second payload queue or a detached pump task.
    Box::pin(futures_util::stream::unfold(
        (prefix.into_iter(), cursor),
        |(mut prefix, mut cursor)| async move {
            if let Some(event) = prefix.next() {
                return Some((Ok(event), (prefix, cursor)));
            }
            let current = cursor.as_mut()?;
            let result = loop {
                let Some(journal) = current.journal.upgrade() else {
                    if let Some(gap) = current.skip_evicted_output() {
                        return Some((Ok(gap), (prefix, cursor)));
                    }
                    break evicted_process_journal_result(current);
                };
                let read = journal.read(current.next_sequence);
                drop(journal);
                match read {
                    ProcessJournalRead::Event(event) => {
                        current.advance_to(current.next_sequence + 1);
                        return Some((Ok(event), (prefix, cursor)));
                    }
                    ProcessJournalRead::Exit(exit) => break Ok(exit),
                    ProcessJournalRead::Lagged {
                        first_sequence,
                        output_bytes_before,
                    } => {
                        let Some(gap) =
                            current.skip_output_gap(first_sequence, output_bytes_before)
                        else {
                            break Err(process_output_lagged_status(Some(first_sequence)));
                        };
                        return Some((Ok(gap), (prefix, cursor)));
                    }
                    ProcessJournalRead::Ahead => break Err(process_output_cursor_ahead_status()),
                    ProcessJournalRead::Pending => {
                        if current.changed.changed().await.is_err() {
                            if let Some(gap) = current.skip_evicted_output() {
                                return Some((Ok(gap), (prefix, cursor)));
                            }
                            break evicted_process_journal_result(current);
                        }
                    }
                }
            };
            Some((result, (prefix, None)))
        },
    ))
}

pub(super) fn process_output_cursor_ahead_status() -> Status {
    let mut status = Status::out_of_range(PROCESS_OUTPUT_CURSOR_AHEAD_MESSAGE);
    status.metadata_mut().insert(
        pplx_space_common_core::spaced::ERROR_REASON_METADATA_KEY,
        tonic::metadata::MetadataValue::from_static(
            pplx_space_common_core::spaced::PROCESS_OUTPUT_CURSOR_AHEAD,
        ),
    );
    status
}

/// The status that ends a stream whose cursor is before the oldest retained
/// output, naming where that output starts, `first_sequence`, so the caller
/// can continue from there. `None` once the journal itself is gone.
pub(super) fn process_output_lagged_status(first_sequence: Option<u64>) -> Status {
    let mut status =
        Status::resource_exhausted(pplx_space_common_core::spaced::PROCESS_OUTPUT_LAGGED_MESSAGE);
    let metadata = status.metadata_mut();
    metadata.insert(
        pplx_space_common_core::spaced::ERROR_REASON_METADATA_KEY,
        tonic::metadata::MetadataValue::from_static(
            pplx_space_common_core::spaced::PROCESS_OUTPUT_LAGGED,
        ),
    );
    if let Some(first_sequence) = first_sequence {
        metadata.insert(
            pplx_space_common_core::spaced::PROCESS_OUTPUT_FIRST_SEQUENCE_METADATA_KEY,
            tonic::metadata::MetadataValue::from(first_sequence),
        );
    }
    status
}

pub(super) fn evicted_process_journal_result(
    cursor: &ProcessJournalCursor,
) -> Result<ProcessEvent, Status> {
    match cursor.changed.borrow().clone() {
        ProcessJournalChange::Exit {
            event,
            next_sequence,
            ..
        } if cursor.next_sequence >= next_sequence => Ok(event),
        ProcessJournalChange::Exit { .. } => Err(process_output_lagged_status(None)),
        ProcessJournalChange::OutputChanged => Err(Status::resource_exhausted(
            "process replay state was evicted",
        )),
    }
}

pub(super) fn process_started_event(
    process_id: &ProcessId,
    pid: Option<u32>,
) -> Result<ProcessEvent, Status> {
    let start_time = pplx_space_spaced_guest_protocol::system_time_to_proto(SystemTime::now())?;
    Ok(ProcessEvent {
        process_id: process_id.as_str().to_string(),
        pid,
        event: Some(
            pplx_space_spaced_guest_protocol::proto::bridge_guest::process_event::Event::Started(
                pplx_space_spaced_guest_protocol::proto::bridge_guest::ProcessStarted {
                    start_time: Some(start_time),
                    ..Default::default()
                },
            ),
        ),
    })
}

pub(super) fn process_connected_event(
    process_id: &ProcessId,
    pid: Option<u32>,
    output_cursor: Option<u64>,
    resume_after_output_gap: bool,
) -> ProcessEvent {
    ProcessEvent {
        process_id: process_id.as_str().to_string(),
        pid,
        event: Some(
            pplx_space_spaced_guest_protocol::proto::bridge_guest::process_event::Event::Connected(
                pplx_space_spaced_guest_protocol::proto::bridge_guest::ProcessConnected {
                    output_cursor,
                    resume_after_output_gap,
                },
            ),
        ),
    }
}

pub(super) fn process_stdout_event(
    process_id: &ProcessId,
    pid: Option<u32>,
    data: Vec<u8>,
) -> ProcessEvent {
    ProcessEvent {
        process_id: process_id.as_str().to_string(),
        pid,
        event: Some(
            pplx_space_spaced_guest_protocol::proto::bridge_guest::process_event::Event::Stdout(
                pplx_space_spaced_guest_protocol::proto::bridge_guest::ProcessOutput { data },
            ),
        ),
    }
}

pub(super) fn process_stderr_event(
    process_id: &ProcessId,
    pid: Option<u32>,
    data: Vec<u8>,
) -> ProcessEvent {
    ProcessEvent {
        process_id: process_id.as_str().to_string(),
        pid,
        event: Some(
            pplx_space_spaced_guest_protocol::proto::bridge_guest::process_event::Event::Stderr(
                pplx_space_spaced_guest_protocol::proto::bridge_guest::ProcessOutput { data },
            ),
        ),
    }
}

pub(super) fn process_exit_event(
    process_id: &ProcessId,
    pid: Option<u32>,
    outcome: ProcessExitOutcome,
) -> ProcessEvent {
    let outcome = match outcome {
        ProcessExitOutcome::Exited(exit_code) => {
            pplx_space_spaced_guest_protocol::proto::bridge_guest::process_exit::Outcome::Exited(
                pplx_space_spaced_guest_protocol::proto::bridge_guest::ProcessExited { exit_code },
            )
        }
        ProcessExitOutcome::Signaled(signal) => {
            pplx_space_spaced_guest_protocol::proto::bridge_guest::process_exit::Outcome::Signaled(
                pplx_space_spaced_guest_protocol::proto::bridge_guest::ProcessSignaled {
                    signal: Some(signal),
                },
            )
        }
        ProcessExitOutcome::TimedOut(signal) => {
            pplx_space_spaced_guest_protocol::proto::bridge_guest::process_exit::Outcome::TimedOut(
                pplx_space_spaced_guest_protocol::proto::bridge_guest::ProcessTimedOut {
                    signal: Some(signal),
                },
            )
        }
        ProcessExitOutcome::Unknown => {
            pplx_space_spaced_guest_protocol::proto::bridge_guest::process_exit::Outcome::Unknown(
                pplx_space_spaced_guest_protocol::proto::bridge_guest::ProcessUnknown {},
            )
        }
    };
    ProcessEvent {
        process_id: process_id.as_str().to_string(),
        pid,
        event: Some(
            pplx_space_spaced_guest_protocol::proto::bridge_guest::process_event::Event::Exit(
                pplx_space_spaced_guest_protocol::proto::bridge_guest::ProcessExit {
                    outcome: Some(outcome),
                },
            ),
        ),
    }
}
