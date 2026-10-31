//! Process identity claims, retained records and expiry.

use super::*;

// Process records retain command/env metadata and task-owned descriptors, so
// cap them as well as the output bytes. Completed records are evicted first
// under pressure; if every slot belongs to a running process, admission fails.
pub(super) const PROCESS_RECORD_LIMIT: usize = 128;

// Default time a completed record, and so its exit, stays connectable: a client
// that reattaches after the command ends still reads `process_exited`. Its
// output is released `COMPLETED_PROCESS_OUTPUT_RETENTION` after the exit, so
// finished processes do not hold the shared output budget. The record limit
// above still bounds what is retained.
pub(crate) const COMPLETED_PROCESS_RETENTION: Duration = Duration::from_secs(15 * 60);

// Default time a completed record keeps its output. A retried start replays it,
// so this matches the default start idempotency window. A reconnect that resumes
// from an output cursor also replays it; after this it reads the lagged error.
pub(crate) const COMPLETED_PROCESS_OUTPUT_RETENTION: Duration = IDEMPOTENCY_RECORD_TTL;

pub(super) const PROCESS_CAPACITY_MESSAGE: &str = "too many retained processes";

// TODO(spaced): persist or reconstruct the process registry if callers need
// connect/list/process-id stability across guest-svc restart. Today process IDs
// are process-local UUIDs, so a guest-svc crash loses pid→process_id mappings.
#[derive(Clone)]
pub(crate) struct ProcessRegistry {
    pub(super) records: Arc<DashMap<ProcessId, ProcessEntry>>,
    pub(super) record_slots: Arc<Semaphore>,
    pub(super) shared_output_slots: Arc<Semaphore>,
    /// Whether the retention reaper is running. It runs only while a
    /// completed record is retained, so an idle daemon has no timer.
    pub(super) reaper_running: Arc<std::sync::atomic::AtomicBool>,
    /// Wakes a running reaper to look again: a newly completed record's
    /// output can be due before the deadline it is sleeping until.
    pub(super) reaper_wake: Arc<Notify>,
    pub(super) output_budget: ProcessOutputBudget,
    pub(super) retention: ProcessRetention,
}

/// A process id in use: by an admitted start that has not published its
/// process yet, or by a published process.
pub(super) enum ProcessEntry {
    Starting,
    Published(Arc<ProcessRecord>),
}

impl ProcessEntry {
    pub(super) fn record(&self) -> Option<&Arc<ProcessRecord>> {
        match self {
            Self::Starting => None,
            Self::Published(record) => Some(record),
        }
    }
}

/// Why a process id could not be claimed.
pub(super) enum ProcessIdTaken {
    /// Another admitted start holds it and has not published its process.
    Starting,
    Published,
}

/// Exclusive use of a process id from start admission until its process is
/// published, so two starts carrying the same assigned id cannot both spawn.
/// Dropping an unpublished claim frees the id.
pub(super) struct ProcessIdClaim {
    pub(super) records: Arc<DashMap<ProcessId, ProcessEntry>>,
    pub(super) process_id: ProcessId,
    pub(super) published: bool,
}

impl ProcessIdClaim {
    pub(super) fn publish(mut self, record: Arc<ProcessRecord>) {
        self.records
            .insert(self.process_id.clone(), ProcessEntry::Published(record));
        self.published = true;
    }
}

impl Drop for ProcessIdClaim {
    fn drop(&mut self) {
        if !self.published {
            self.records.remove_if(&self.process_id, |_, entry| {
                matches!(entry, ProcessEntry::Starting)
            });
        }
    }
}

pub(super) struct ProcessRecord {
    // RPCs can retain a completed record beyond registry eviction.
    pub(super) _record_slot: OwnedSemaphorePermit,
    pub(super) started: ProcessEvent,
    /// What `list` reports, without `env`.
    pub(super) info: ProcessInfo,
    /// Up to 64 KiB, kept apart from `info` so a finished record can drop it
    /// with its output: no caller reads an exited process's environment.
    pub(super) env: std::sync::Mutex<HashMap<String, String>>,
    pub(super) input: ProcessInput,
    pub(super) signal_handle: ProcessSignalHandle,
    pub(super) journal: Arc<ProcessEventJournal>,
}

impl ProcessRecord {
    pub(super) fn new(
        started: ProcessEvent,
        mut info: ProcessInfo,
        input: ProcessInput,
        signal_handle: ProcessSignalHandle,
        reservation: ProcessRecordReservation,
    ) -> Self {
        let env = std::mem::take(&mut info.env);
        Self {
            _record_slot: reservation.record_slot,
            started,
            info,
            env: std::sync::Mutex::new(env),
            input,
            signal_handle,
            journal: Arc::new(ProcessEventJournal::with_budget(
                reservation.shared_output_slots,
                reservation.output_budget,
            )),
        }
    }

    pub(super) async fn write_stdin(&self, data: &[u8]) -> Result<(), Status> {
        self.input.write(data).await
    }

    pub(super) async fn close_stdin(&self) -> Result<(), Status> {
        self.input.close().await
    }

    pub(super) fn resize_pty(&self, size: PtySize) -> Result<(), Status> {
        self.input.resize_pty(size)
    }

    /// What `list` reports for this process.
    pub(super) fn listed_info(&self) -> ProcessInfo {
        ProcessInfo {
            env: self
                .env
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone(),
            ..self.info.clone()
        }
    }

    pub(super) fn release_env(&self) {
        std::mem::take(
            &mut *self
                .env
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        );
    }
}

impl Default for ProcessRegistry {
    /// The budget of a guest of unknown size: the full 64 MiB.
    fn default() -> Self {
        Self::new(
            ProcessOutputBudget::for_guest_memory(None),
            ProcessRetention::default(),
        )
    }
}

impl ProcessRegistry {
    pub(crate) fn new(output_budget: ProcessOutputBudget, retention: ProcessRetention) -> Self {
        Self {
            records: Arc::new(DashMap::new()),
            record_slots: Arc::new(Semaphore::new(PROCESS_RECORD_LIMIT)),
            shared_output_slots: Arc::new(Semaphore::new(output_budget.shared)),
            reaper_running: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            reaper_wake: Arc::new(Notify::new()),
            output_budget,
            retention,
        }
    }
}

pub(super) struct ProcessRecordReservation {
    pub(super) record_slot: OwnedSemaphorePermit,
    pub(super) shared_output_slots: Arc<Semaphore>,
    pub(super) output_budget: ProcessOutputBudget,
}

impl ProcessRegistry {
    pub(super) fn claim(&self, process_id: &ProcessId) -> Result<ProcessIdClaim, ProcessIdTaken> {
        match self.records.entry(process_id.clone()) {
            dashmap::Entry::Occupied(occupied) => Err(match occupied.get() {
                ProcessEntry::Starting => ProcessIdTaken::Starting,
                ProcessEntry::Published(_) => ProcessIdTaken::Published,
            }),
            dashmap::Entry::Vacant(vacant) => {
                vacant.insert(ProcessEntry::Starting);
                Ok(ProcessIdClaim {
                    records: Arc::clone(&self.records),
                    process_id: process_id.clone(),
                    published: false,
                })
            }
        }
    }

    pub(super) fn is_published(&self, process_id: &ProcessId) -> bool {
        self.records
            .get(process_id)
            .is_some_and(|entry| entry.record().is_some())
    }

    pub(super) fn reserve(&self) -> Result<ProcessRecordReservation, Status> {
        loop {
            match self.record_slots.clone().try_acquire_owned() {
                Ok(record_slot) => {
                    return Ok(ProcessRecordReservation {
                        record_slot,
                        shared_output_slots: Arc::clone(&self.shared_output_slots),
                        output_budget: self.output_budget,
                    });
                }
                Err(TryAcquireError::Closed) => {
                    return Err(crate::refused(Status::unavailable(
                        "process registry is closed",
                    )));
                }
                Err(TryAcquireError::NoPermits) => {}
            }

            let completed = self
                .records
                .iter()
                .filter_map(|entry| {
                    entry
                        .record()?
                        .journal
                        .completed_at()
                        .map(|completed_at| (entry.key().clone(), completed_at))
                })
                .min_by_key(|(_, completed_at)| *completed_at);
            let Some((process_id, _)) = completed else {
                return Err(crate::refused(Status::resource_exhausted(
                    PROCESS_CAPACITY_MESSAGE,
                )));
            };
            self.records.remove(&process_id);
        }
    }

    pub(super) fn get_record(&self, process_id: &ProcessId) -> Option<Arc<ProcessRecord>> {
        let record = self
            .records
            .get(process_id)
            .and_then(|entry| entry.record().cloned())?;
        if record.journal.is_expired(self.retention.completed) {
            self.records.remove(process_id);
            return None;
        }
        Some(record)
    }

    pub(super) fn get_running(&self, process_id: &ProcessId) -> Option<Arc<ProcessRecord>> {
        let record = self.get_record(process_id)?;
        if record.journal.is_completed() {
            return None;
        }
        Some(record)
    }

    pub(super) fn replay(
        &self,
        process_id: &ProcessId,
        output_cursor: Option<u64>,
        from_oldest_retained: bool,
    ) -> Option<ProcessReplay> {
        let record = self.get_record(process_id)?;
        if output_cursor.is_none()
            && let Some(exit) = record.journal.exit()
        {
            return Some(ProcessReplay::Completed {
                started: record.started.clone(),
                exit,
            });
        }
        Some(ProcessReplay::Running {
            started: record.started.clone(),
            cursor: record.journal.cursor(output_cursor, from_oldest_retained),
        })
    }

    /// Removes completed records whose retention has ended. One task at most:
    /// it sleeps until the earliest record expires and exits when no completed
    /// record is retained; the next process to complete starts it again, or
    /// wakes it to recompute that deadline.
    pub(super) fn ensure_reaper(
        &self,
        shutdown: tokio_util::sync::CancellationToken,
        tasks: &tokio_util::task::TaskTracker,
    ) {
        use std::sync::atomic::Ordering;
        if self.reaper_running.swap(true, Ordering::AcqRel) {
            // The permit is kept if the reaper is between sleeps, so it looks
            // again before its next one.
            self.reaper_wake.notify_one();
            return;
        }
        let registry = self.clone();
        drop(tasks.spawn(async move {
            loop {
                registry.remove_expired();
                let Some(next) = registry.next_expiry() else {
                    registry.reaper_running.store(false, Ordering::Release);
                    // A record completed after the look above may have seen the
                    // reaper as running and not started one: look again.
                    if registry.next_expiry().is_some()
                        && !registry.reaper_running.swap(true, Ordering::AcqRel)
                    {
                        continue;
                    }
                    return;
                };
                tokio::select! {
                    () = tokio::time::sleep_until(next) => {}
                    () = registry.reaper_wake.notified() => {}
                    () = shutdown.cancelled() => {
                        registry.reaper_running.store(false, Ordering::Release);
                        return;
                    }
                }
            }
        }));
    }

    pub(super) fn remove_expired(&self) {
        self.records.retain(|_, entry| {
            let Some(record) = entry.record() else {
                return true;
            };
            if record.journal.release_output_if_due(self.retention.output) {
                record.release_env();
            }
            !record.journal.is_expired(self.retention.completed)
        });
    }

    /// When the reaper next has work: releasing a completed record's output
    /// or ending its retention, whichever comes first.
    pub(super) fn next_expiry(&self) -> Option<tokio::time::Instant> {
        self.records
            .iter()
            .filter_map(|entry| entry.record()?.journal.next_deadline(self.retention))
            .min()
    }

    /// Running processes, and with `include_exited` also the retained
    /// completed ones, each with its exit. With `process_id`, only that one.
    pub(super) fn list(&self, include_exited: bool, process_id: Option<&str>) -> Vec<ProcessInfo> {
        let mut expired = Vec::new();
        let listed = self
            .records
            .iter()
            .filter(|entry| process_id.is_none_or(|process_id| entry.key().as_str() == process_id))
            .filter_map(|entry| {
                let record = entry.record()?;
                if record.journal.is_expired(self.retention.completed) {
                    expired.push(entry.key().clone());
                    return None;
                }
                let Some((exit, ended_at)) = record.journal.ended() else {
                    return Some(record.listed_info());
                };
                let Some(
                    pplx_space_spaced_guest_protocol::proto::bridge_guest::process_event::Event::Exit(
                        exit,
                    ),
                ) = exit.event
                else {
                    return None;
                };
                include_exited.then(|| ProcessInfo {
                    exit: Some(exit),
                    ended_at: ended_at.and_then(|ended_at| {
                        pplx_space_spaced_guest_protocol::system_time_to_proto(ended_at).ok()
                    }),
                    ..record.listed_info()
                })
            })
            .collect();
        for process_id in expired {
            self.records.remove(&process_id);
        }
        listed
    }
}
