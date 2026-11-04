//! Page-cache writeback pacing shared by every streamed pull: ranges, the byte budget and the
//! coordinator.

use super::*;

// Submit decoded output to the process-wide writeback worker in the same
// increments used by ranged downloads. Writers share one byte budget, so
// aggregate dirty output stays bounded independently of pull concurrency.
pub(super) const STREAM_WRITEBACK_WINDOW_BYTES: u64 = 32 * 1024 * 1024;

pub const DEFAULT_STREAM_WRITEBACK_BUDGET_BYTES: u64 = 2 * 1024 * 1024 * 1024;

pub(super) const STREAM_WRITEBACK_IDLE_DRAIN: std::time::Duration =
    std::time::Duration::from_secs(1);

pub(super) const STREAM_WRITEBACK_METRIC_INTERVAL: std::time::Duration =
    std::time::Duration::from_secs(1);

#[derive(Debug)]
pub(super) struct StreamWritebackRange {
    pub(super) file: Arc<std::fs::File>,
    pub(super) offset: u64,
    pub(super) length: u64,
    pub(super) debt_bytes: u64,
    pub(super) started: bool,
    pub(super) timing: Arc<StreamPullTiming>,
}

#[derive(Debug)]
pub(super) enum StreamWritebackCommand {
    Range(StreamWritebackRange),
    ReapToLowWatermark,
}

pub(super) trait StreamPageCacheOperations: std::fmt::Debug + Send + Sync {
    fn start(&self, file: &std::fs::File, offset: u64, length: u64) -> std::io::Result<()>;
    fn wait(&self, file: &std::fs::File, offset: u64, length: u64) -> std::io::Result<()>;
    fn advise_dontneed(
        &self,
        file: &std::fs::File,
        offset: u64,
        length: u64,
    ) -> std::io::Result<()>;
}

#[derive(Debug)]
pub(super) struct SystemStreamPageCacheOperations;

impl StreamPageCacheOperations for SystemStreamPageCacheOperations {
    fn start(&self, file: &std::fs::File, offset: u64, length: u64) -> std::io::Result<()> {
        page_cache::start_writeback_range(file, offset, length)
    }

    fn wait(&self, file: &std::fs::File, offset: u64, length: u64) -> std::io::Result<()> {
        page_cache::wait_writeback_range(file, offset, length)
    }

    fn advise_dontneed(
        &self,
        file: &std::fs::File,
        offset: u64,
        length: u64,
    ) -> std::io::Result<()> {
        page_cache::advise_range_dontneed(file, offset, length)
    }
}

#[derive(Debug, Default)]
pub(super) struct StreamWritebackBudget {
    pub(super) outstanding_bytes: std::sync::Mutex<u64>,
    pub(super) available: std::sync::Condvar,
    pub(super) failed: std::sync::atomic::AtomicBool,
}

#[derive(Debug)]
pub(super) struct StreamWritebackCoordinator {
    pub(super) sender: std::sync::mpsc::Sender<StreamWritebackCommand>,
    pub(super) budget: Arc<StreamWritebackBudget>,
    pub(super) high_watermark_bytes: u64,
    pub(super) priority_threshold: Option<JobPriority>,
    pub(super) priority_reserve_bytes: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum StreamWritebackFailureOperation {
    Start,
    Wait,
    Advice,
    Queue,
}

impl StreamWritebackFailureOperation {
    pub(super) fn as_str(self) -> &'static str {
        match self {
            Self::Start => "start",
            Self::Wait => "wait",
            Self::Advice => "advice",
            Self::Queue => "queue",
        }
    }
}

impl std::fmt::Display for StreamWritebackFailureOperation {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum StreamWritebackPriorityClass {
    Shared,
    High,
}

impl StreamWritebackPriorityClass {
    pub(super) fn as_str(self) -> &'static str {
        match self {
            Self::Shared => "shared",
            Self::High => "high",
        }
    }
}

impl std::fmt::Display for StreamWritebackPriorityClass {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

pub(super) fn fail_stream_writeback_budget(
    budget: &StreamWritebackBudget,
    operation: StreamWritebackFailureOperation,
) {
    let was_failed = budget
        .failed
        .swap(true, std::sync::atomic::Ordering::AcqRel);
    budget.available.notify_all();
    if !was_failed {
        pplx_space_metrics::volume_manager::stream_writeback_failure(operation.as_str());
    }
}

pub(super) fn release_stream_writeback_budget(budget: &StreamWritebackBudget, bytes: u64) {
    let mut outstanding = budget
        .outstanding_bytes
        .lock()
        .expect("stream writeback budget lock poisoned");
    *outstanding = outstanding.saturating_sub(bytes);
    budget.available.notify_all();
}

pub(super) fn emit_stream_writeback_outstanding_if_due(
    budget: &StreamWritebackBudget,
    last_emitted_at: &mut Instant,
    force: bool,
) {
    let outstanding = stream_writeback_outstanding_bytes(budget);
    if !should_emit_stream_writeback_outstanding(last_emitted_at.elapsed(), force) {
        return;
    }
    pplx_space_metrics::volume_manager::stream_writeback_outstanding_bytes(outstanding);
    *last_emitted_at = Instant::now();
}

pub(super) fn should_emit_stream_writeback_outstanding(
    elapsed: std::time::Duration,
    force: bool,
) -> bool {
    force || elapsed >= STREAM_WRITEBACK_METRIC_INTERVAL
}

pub(super) fn retire_stream_writeback_range(
    range: StreamWritebackRange,
    budget: &StreamWritebackBudget,
    page_cache_operations: &dyn StreamPageCacheOperations,
    writeback_enabled: &mut bool,
    cache_advice_enabled: &mut bool,
) {
    if *writeback_enabled && range.started {
        let wait_started_at = Instant::now();
        let result = page_cache_operations.wait(&range.file, range.offset, range.length);
        StreamPullTiming::add_elapsed(&range.timing.writeback_wait_us, wait_started_at);
        if let Err(error) = result {
            tracing::warn!(
                %error,
                offset = range.offset,
                length = range.length,
                "waiting for streamed output range writeback failed; closing streamed pull admission"
            );
            *writeback_enabled = false;
            fail_stream_writeback_budget(budget, StreamWritebackFailureOperation::Wait);
        }
    }
    if *writeback_enabled
        && range.started
        && *cache_advice_enabled
        && let Err(error) =
            page_cache_operations.advise_dontneed(&range.file, range.offset, range.length)
    {
        tracing::warn!(
            %error,
            offset = range.offset,
            length = range.length,
            "streamed output range cache advice failed; disabling cache advice"
        );
        pplx_space_metrics::volume_manager::stream_writeback_failure(
            StreamWritebackFailureOperation::Advice.as_str(),
        );
        *cache_advice_enabled = false;
    }
    release_stream_writeback_budget(budget, range.debt_bytes);
}

pub(super) fn stream_writeback_outstanding_bytes(budget: &StreamWritebackBudget) -> u64 {
    *budget
        .outstanding_bytes
        .lock()
        .expect("stream writeback budget lock poisoned")
}

pub(super) fn pending_stream_writeback_debt(
    pending: &std::collections::VecDeque<StreamWritebackRange>,
) -> u64 {
    pending
        .iter()
        .fold(0_u64, |total, range| total.saturating_add(range.debt_bytes))
}

pub(super) fn drain_stream_writeback_ranges_to(
    pending: &mut std::collections::VecDeque<StreamWritebackRange>,
    budget: &StreamWritebackBudget,
    target_bytes: u64,
    page_cache_operations: &dyn StreamPageCacheOperations,
    writeback_enabled: &mut bool,
    cache_advice_enabled: &mut bool,
) -> bool {
    while stream_writeback_outstanding_bytes(budget) > target_bytes {
        let Some(range) = pending.pop_front() else {
            return false;
        };
        retire_stream_writeback_range(
            range,
            budget,
            page_cache_operations,
            writeback_enabled,
            cache_advice_enabled,
        );
    }
    true
}

pub(super) fn drain_all_stream_writeback_ranges(
    pending: &mut std::collections::VecDeque<StreamWritebackRange>,
    budget: &StreamWritebackBudget,
    page_cache_operations: &dyn StreamPageCacheOperations,
    writeback_enabled: &mut bool,
    cache_advice_enabled: &mut bool,
) {
    while let Some(range) = pending.pop_front() {
        retire_stream_writeback_range(
            range,
            budget,
            page_cache_operations,
            writeback_enabled,
            cache_advice_enabled,
        );
    }
}

pub(super) fn queue_stream_writeback_range(
    pending: &mut std::collections::VecDeque<StreamWritebackRange>,
    range: StreamWritebackRange,
) {
    if let Some(previous) = pending.iter_mut().rev().find(|previous| {
        Arc::ptr_eq(&previous.file, &range.file) && Arc::ptr_eq(&previous.timing, &range.timing)
    }) {
        let can_merge = !previous.started
            && previous
                .offset
                .checked_add(previous.length)
                .is_some_and(|end| end <= range.offset)
            && previous
                .debt_bytes
                .checked_add(range.debt_bytes)
                .is_some_and(|debt| debt <= STREAM_WRITEBACK_WINDOW_BYTES);
        if can_merge
            && let (Some(length), Some(debt_bytes)) = (
                range
                    .offset
                    .checked_add(range.length)
                    .and_then(|end| end.checked_sub(previous.offset)),
                previous.debt_bytes.checked_add(range.debt_bytes),
            )
        {
            previous.length = length;
            previous.debt_bytes = debt_bytes;
            return;
        }
    }
    pending.push_back(range);
}

pub(super) fn start_pending_stream_writeback_ranges(
    pending: &mut std::collections::VecDeque<StreamWritebackRange>,
    budget: &StreamWritebackBudget,
    page_cache_operations: &dyn StreamPageCacheOperations,
    writeback_enabled: &mut bool,
    minimum_debt_bytes: u64,
) {
    for range in pending
        .iter_mut()
        .filter(|range| !range.started && range.debt_bytes >= minimum_debt_bytes)
    {
        if !*writeback_enabled {
            break;
        }
        let start_started_at = Instant::now();
        let start_cpu_started_at = current_thread_cpu_us();
        let result = page_cache_operations.start(&range.file, range.offset, range.length);
        StreamPullTiming::add_wall_and_thread_cpu(
            &range.timing.writeback_start_us,
            &range.timing.writeback_start_cpu_us,
            start_started_at,
            start_cpu_started_at,
        );
        match result {
            Ok(()) => {
                range.started = true;
                StreamPullTiming::add(&range.timing.writeback_start_calls, 1);
                StreamPullTiming::add(
                    &range.timing.writeback_submitted_logical_bytes,
                    range.length,
                );
                StreamPullTiming::add(
                    &range.timing.writeback_submitted_dirty_bytes,
                    range.debt_bytes,
                );
            }
            Err(error) => {
                tracing::warn!(
                    %error,
                    offset = range.offset,
                    length = range.length,
                    debt_bytes = range.debt_bytes,
                    "starting streamed output range writeback failed; closing streamed pull admission"
                );
                *writeback_enabled = false;
                fail_stream_writeback_budget(budget, StreamWritebackFailureOperation::Start);
            }
        }
    }
}

impl StreamWritebackCoordinator {
    pub(super) fn new(
        high_watermark_bytes: u64,
        priority_threshold: Option<JobPriority>,
    ) -> Arc<Self> {
        Self::new_with_page_cache_operations(
            high_watermark_bytes,
            priority_threshold,
            Arc::new(SystemStreamPageCacheOperations),
        )
    }

    pub(super) fn new_with_page_cache_operations(
        high_watermark_bytes: u64,
        priority_threshold: Option<JobPriority>,
        page_cache_operations: Arc<dyn StreamPageCacheOperations>,
    ) -> Arc<Self> {
        let (sender, receiver) = std::sync::mpsc::channel::<StreamWritebackCommand>();
        let budget = Arc::new(StreamWritebackBudget::default());
        let worker_budget = Arc::clone(&budget);
        let low_watermark_bytes = high_watermark_bytes / 2;
        let writeback_window_bytes = STREAM_WRITEBACK_WINDOW_BYTES.min(low_watermark_bytes);
        std::thread::Builder::new()
            .name("volmgr-writeback".to_string())
            .spawn(move || {
                let mut writeback_enabled = true;
                let mut cache_advice_enabled = true;
                let mut pending = std::collections::VecDeque::new();
                let mut drain_target = None;
                let mut pressure_active = false;
                let mut last_outstanding_metric_at = Instant::now();

                loop {
                    let mut force_outstanding_metric = false;
                    let writeback_enabled_at_start = writeback_enabled;
                    match receiver.recv_timeout(STREAM_WRITEBACK_IDLE_DRAIN) {
                        Ok(StreamWritebackCommand::Range(range)) if writeback_enabled => {
                            let activation_timing = Arc::clone(&range.timing);
                            queue_stream_writeback_range(&mut pending, range);
                            // Overlap complete dirty windows with decoding even
                            // below the pressure watermark. Keep their debt
                            // charged until the existing completion/drain path.
                            start_pending_stream_writeback_ranges(
                                &mut pending,
                                &worker_budget,
                                page_cache_operations.as_ref(),
                                &mut writeback_enabled,
                                writeback_window_bytes,
                            );
                            if pending_stream_writeback_debt(&pending) >= low_watermark_bytes {
                                let newly_activated = !pressure_active;
                                if !pressure_active {
                                    pressure_active = true;
                                    force_outstanding_metric = true;
                                    StreamPullTiming::add(
                                        &activation_timing.writeback_low_watermark_activations,
                                        1,
                                    );
                                }
                                start_pending_stream_writeback_ranges(
                                    &mut pending,
                                    &worker_budget,
                                    page_cache_operations.as_ref(),
                                    &mut writeback_enabled,
                                    if newly_activated {
                                        0
                                    } else {
                                        writeback_window_bytes
                                    },
                                );
                            }
                        }
                        Ok(StreamWritebackCommand::Range(range)) => {
                            release_stream_writeback_budget(&worker_budget, range.debt_bytes);
                        }
                        Ok(StreamWritebackCommand::ReapToLowWatermark) => {
                            force_outstanding_metric = true;
                            drain_target = Some(low_watermark_bytes);
                            start_pending_stream_writeback_ranges(
                                &mut pending,
                                &worker_budget,
                                page_cache_operations.as_ref(),
                                &mut writeback_enabled,
                                0,
                            );
                        }
                        Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                            if pending_stream_writeback_debt(&pending) >= low_watermark_bytes {
                                start_pending_stream_writeback_ranges(
                                    &mut pending,
                                    &worker_budget,
                                    page_cache_operations.as_ref(),
                                    &mut writeback_enabled,
                                    0,
                                );
                                drain_target = Some(low_watermark_bytes);
                            }
                        }
                        Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                            start_pending_stream_writeback_ranges(
                                &mut pending,
                                &worker_budget,
                                page_cache_operations.as_ref(),
                                &mut writeback_enabled,
                                0,
                            );
                            drain_all_stream_writeback_ranges(
                                &mut pending,
                                &worker_budget,
                                page_cache_operations.as_ref(),
                                &mut writeback_enabled,
                                &mut cache_advice_enabled,
                            );
                            emit_stream_writeback_outstanding_if_due(
                                &worker_budget,
                                &mut last_outstanding_metric_at,
                                true,
                            );
                            break;
                        }
                    }

                    if drain_target.is_some() {
                        start_pending_stream_writeback_ranges(
                            &mut pending,
                            &worker_budget,
                            page_cache_operations.as_ref(),
                            &mut writeback_enabled,
                            0,
                        );
                    }
                    if drain_target.is_some_and(|target| {
                        drain_stream_writeback_ranges_to(
                            &mut pending,
                            &worker_budget,
                            target,
                            page_cache_operations.as_ref(),
                            &mut writeback_enabled,
                            &mut cache_advice_enabled,
                        )
                    }) {
                        drain_target = None;
                        pressure_active = stream_writeback_outstanding_bytes(&worker_budget)
                            > low_watermark_bytes;
                        force_outstanding_metric = true;
                    }
                    if !writeback_enabled {
                        drain_all_stream_writeback_ranges(
                            &mut pending,
                            &worker_budget,
                            page_cache_operations.as_ref(),
                            &mut writeback_enabled,
                            &mut cache_advice_enabled,
                        );
                        drain_target = None;
                        pressure_active = false;
                        force_outstanding_metric |= writeback_enabled_at_start;
                    }
                    emit_stream_writeback_outstanding_if_due(
                        &worker_budget,
                        &mut last_outstanding_metric_at,
                        force_outstanding_metric,
                    );
                }
            })
            .expect("spawn volume-manager writeback worker");
        Arc::new(Self {
            sender,
            budget,
            high_watermark_bytes,
            priority_threshold,
            priority_reserve_bytes: if priority_threshold.is_some() {
                STREAM_WRITEBACK_WINDOW_BYTES.min(high_watermark_bytes / 2)
            } else {
                0
            },
        })
    }

    pub(super) fn admission_limit(&self, priority: JobPriority) -> u64 {
        if self
            .priority_threshold
            .is_some_and(|threshold| priority < threshold)
        {
            self.high_watermark_bytes
                .saturating_sub(self.priority_reserve_bytes)
        } else {
            self.high_watermark_bytes
        }
    }

    pub(super) fn try_reserve(&self, bytes: u64, priority: JobPriority) -> std::io::Result<bool> {
        if self
            .budget
            .failed
            .load(std::sync::atomic::Ordering::Acquire)
        {
            return Err(std::io::Error::other("stream writeback coordinator failed"));
        }
        let mut outstanding = self
            .budget
            .outstanding_bytes
            .lock()
            .map_err(|_| std::io::Error::other("stream writeback budget lock poisoned"))?;
        if self
            .budget
            .failed
            .load(std::sync::atomic::Ordering::Acquire)
        {
            return Err(std::io::Error::other("stream writeback coordinator failed"));
        }
        let admission_limit = self.admission_limit(priority);
        if *outstanding != 0 && outstanding.saturating_add(bytes) > admission_limit {
            return Ok(false);
        }
        *outstanding = outstanding.saturating_add(bytes);
        Ok(true)
    }

    pub(super) fn reserve(&self, bytes: u64, priority: JobPriority) -> std::io::Result<()> {
        let reserve_started_at = Instant::now();
        if self.try_reserve(bytes, priority)? {
            return Ok(());
        }
        let mut outstanding = self
            .budget
            .outstanding_bytes
            .lock()
            .map_err(|_| std::io::Error::other("stream writeback budget lock poisoned"))?;
        if self
            .budget
            .failed
            .load(std::sync::atomic::Ordering::Acquire)
        {
            return Err(std::io::Error::other("stream writeback coordinator failed"));
        }
        let admission_limit = self.admission_limit(priority);
        let priority_class = if admission_limit == self.high_watermark_bytes {
            StreamWritebackPriorityClass::High
        } else {
            StreamWritebackPriorityClass::Shared
        };
        while *outstanding != 0 && outstanding.saturating_add(bytes) > admission_limit {
            if self
                .sender
                .send(StreamWritebackCommand::ReapToLowWatermark)
                .is_err()
            {
                tracing::warn!(
                    "stream writeback worker stopped while a producer was blocked; closing streamed pull admission"
                );
                fail_stream_writeback_budget(&self.budget, StreamWritebackFailureOperation::Queue);
                return Err(std::io::Error::other("stream writeback worker stopped"));
            }
            outstanding = self.budget.available.wait(outstanding).map_err(|_| {
                std::io::Error::other("stream writeback budget lock poisoned while waiting")
            })?;
            if self
                .budget
                .failed
                .load(std::sync::atomic::Ordering::Acquire)
            {
                return Err(std::io::Error::other("stream writeback coordinator failed"));
            }
        }
        *outstanding = outstanding.saturating_add(bytes);
        pplx_space_metrics::volume_manager::stream_writeback_producer_blocked_ms(
            reserve_started_at.elapsed().as_secs_f64() * 1000.0,
            priority_class.as_str(),
        );
        Ok(())
    }

    pub(super) fn release(&self, bytes: u64) {
        let mut outstanding = self
            .budget
            .outstanding_bytes
            .lock()
            .expect("stream writeback budget lock poisoned");
        *outstanding = outstanding.saturating_sub(bytes);
        self.budget.available.notify_all();
    }

    pub(super) fn submit(
        &self,
        file: Arc<std::fs::File>,
        offset: u64,
        length: u64,
        debt_bytes: u64,
        timing: Arc<StreamPullTiming>,
    ) {
        let result = self
            .sender
            .send(StreamWritebackCommand::Range(StreamWritebackRange {
                file,
                offset,
                length,
                debt_bytes,
                started: false,
                timing,
            }))
            .map_err(|_| std::io::Error::other("stream writeback worker stopped"));
        if let Err(error) = result {
            tracing::warn!(
                %error,
                offset,
                length,
                debt_bytes,
                "streamed output range could not be queued for writeback; closing streamed pull admission"
            );
            fail_stream_writeback_budget(&self.budget, StreamWritebackFailureOperation::Queue);
            self.release(debt_bytes);
        }
    }
}
