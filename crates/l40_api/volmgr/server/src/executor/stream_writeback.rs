//! One stream's writeback state: dirty-range accumulation, reservations and flushes.

use super::*;

#[derive(Debug)]
pub(super) struct StreamWritebackAccumulator {
    pub(super) offset: u64,
    pub(super) length: u64,
    pub(super) debt_bytes: u64,
}

#[derive(Debug)]
pub(super) struct StreamWriteback {
    pub(super) coordinator: Option<Arc<StreamWritebackCoordinator>>,
    pub(super) timing: Option<Arc<StreamPullTiming>>,
    pub(super) priority: JobPriority,
    pub(super) writeback_file: Option<Arc<std::fs::File>>,
    pub(super) unclassified_bytes: u64,
    pub(super) reserved_bytes: u64,
    pub(super) deferred_hole_bytes: u64,
    pub(super) pending: Option<StreamWritebackAccumulator>,
}

impl StreamWriteback {
    pub(super) fn new(
        coordinator: Option<Arc<StreamWritebackCoordinator>>,
        timing: Option<Arc<StreamPullTiming>>,
        priority: JobPriority,
    ) -> Self {
        Self {
            coordinator,
            timing,
            priority,
            writeback_file: None,
            unclassified_bytes: 0,
            reserved_bytes: 0,
            deferred_hole_bytes: 0,
            pending: None,
        }
    }

    pub(super) fn pending_debt_bytes(&self) -> u64 {
        self.pending
            .as_ref()
            .map_or(0, |pending| pending.debt_bytes)
    }

    pub(super) fn additional_reservation_bytes(&self, bytes: u64) -> u64 {
        let Some(coordinator) = &self.coordinator else {
            return 0;
        };
        let owned_bytes = self
            .unclassified_bytes
            .saturating_add(self.pending_debt_bytes())
            .saturating_add(self.deferred_hole_bytes);
        let required_bytes = owned_bytes.saturating_add(bytes);
        if required_bytes <= self.reserved_bytes {
            return 0;
        }
        let reservation_window = STREAM_WRITEBACK_WINDOW_BYTES
            .min(coordinator.admission_limit(self.priority) / 2)
            .max(bytes);
        owned_bytes
            .saturating_add(reservation_window)
            .saturating_sub(self.reserved_bytes)
    }

    pub(super) fn reserve(&mut self, bytes: usize) -> std::io::Result<()> {
        let bytes = bytes as u64;
        if self.coordinator.is_some() {
            let mut additional = self.additional_reservation_bytes(bytes);
            if additional != 0 {
                self.release_deferred_holes();
                additional = self.additional_reservation_bytes(bytes);
                let coordinator = self
                    .coordinator
                    .as_ref()
                    .expect("coordinator checked above");
                let reserve_started_at = Instant::now();
                let reserve_cpu_started_at = current_thread_cpu_us();
                coordinator.reserve(additional, self.priority)?;
                if let Some(timing) = &self.timing {
                    StreamPullTiming::add_wall_and_thread_cpu(
                        &timing.writeback_reserve_us,
                        &timing.writeback_reserve_cpu_us,
                        reserve_started_at,
                        reserve_cpu_started_at,
                    );
                }
                self.reserved_bytes = self.reserved_bytes.saturating_add(additional);
            }
        }
        Ok(())
    }

    pub(super) fn try_reserve(&mut self, bytes: usize) -> std::io::Result<bool> {
        let bytes = bytes as u64;
        if self.coordinator.is_none() {
            return Ok(true);
        }
        let mut additional = self.additional_reservation_bytes(bytes);
        if additional == 0 {
            return Ok(true);
        }
        self.release_deferred_holes();
        additional = self.additional_reservation_bytes(bytes);
        let coordinator = self
            .coordinator
            .as_ref()
            .expect("coordinator checked above");
        let reserve_started_at = Instant::now();
        let reserve_cpu_started_at = current_thread_cpu_us();
        if !coordinator.try_reserve(additional, self.priority)? {
            return Ok(false);
        }
        if let Some(timing) = &self.timing {
            StreamPullTiming::add_wall_and_thread_cpu(
                &timing.writeback_reserve_us,
                &timing.writeback_reserve_cpu_us,
                reserve_started_at,
                reserve_cpu_started_at,
            );
        }
        self.reserved_bytes = self.reserved_bytes.saturating_add(additional);
        Ok(true)
    }

    pub(super) fn record_written(&mut self, bytes: usize) {
        if self.coordinator.is_some() {
            self.unclassified_bytes = self.unclassified_bytes.saturating_add(bytes as u64);
        }
    }

    pub(super) fn release_unwritten(&mut self, bytes: usize) {
        let bytes = bytes as u64;
        if bytes == 0 {
            return;
        }
        if let Some(coordinator) = &self.coordinator {
            self.reserved_bytes = self.reserved_bytes.saturating_sub(bytes);
            coordinator.release(bytes);
        }
    }

    pub(super) fn writeback_file(
        &mut self,
        file: &std::fs::File,
    ) -> std::io::Result<Arc<std::fs::File>> {
        if let Some(file) = &self.writeback_file {
            return Ok(Arc::clone(file));
        }
        let file = Arc::new(file.try_clone()?);
        self.writeback_file = Some(Arc::clone(&file));
        Ok(file)
    }

    pub(super) fn release_deferred_holes(&mut self) {
        let hole_bytes = std::mem::take(&mut self.deferred_hole_bytes);
        if hole_bytes == 0 {
            return;
        }
        self.reserved_bytes = self
            .reserved_bytes
            .checked_sub(hole_bytes)
            .expect("deferred holes exceed producer reservation");
        let coordinator = self
            .coordinator
            .as_ref()
            .expect("deferred holes require a coordinator");
        coordinator.release(hole_bytes);
        if let Some(timing) = &self.timing {
            StreamPullTiming::add(&timing.writeback_hole_bytes_released, hole_bytes);
        }
    }

    pub(super) fn flush_pending(&mut self, file: &std::fs::File) -> std::io::Result<()> {
        self.release_deferred_holes();
        let Some(pending) = self.pending.take() else {
            return Ok(());
        };
        let writeback_file = self.writeback_file(file)?;
        let coordinator = self
            .coordinator
            .as_ref()
            .expect("pending writeback requires a coordinator");
        let timing = self
            .timing
            .clone()
            .unwrap_or_else(|| Arc::new(StreamPullTiming::default()));
        self.reserved_bytes = self
            .reserved_bytes
            .checked_sub(pending.debt_bytes)
            .ok_or_else(|| {
                std::io::Error::other("pending writeback exceeds producer reservation")
            })?;
        coordinator.submit(
            writeback_file,
            pending.offset,
            pending.length,
            pending.debt_bytes,
            timing,
        );
        Ok(())
    }

    pub(super) fn accumulate_dirty_range(
        &mut self,
        file: &std::fs::File,
        mut offset: u64,
        mut length: u64,
    ) -> std::io::Result<()> {
        while length != 0 {
            if self.pending_debt_bytes() == STREAM_WRITEBACK_WINDOW_BYTES {
                self.flush_pending(file)?;
            }
            let available = STREAM_WRITEBACK_WINDOW_BYTES - self.pending_debt_bytes();
            let debt_bytes = length.min(available);
            let end = offset
                .checked_add(debt_bytes)
                .ok_or_else(|| std::io::Error::other("streamed output range exceeds u64::MAX"))?;
            if let Some(pending) = &mut self.pending {
                let pending_end = pending.offset.checked_add(pending.length).ok_or_else(|| {
                    std::io::Error::other("pending streamed output range exceeds u64::MAX")
                })?;
                if offset < pending_end {
                    return Err(std::io::Error::other(
                        "streamed dirty output ranges are not ordered",
                    ));
                }
                pending.length = end.checked_sub(pending.offset).ok_or_else(|| {
                    std::io::Error::other("streamed output range precedes pending range")
                })?;
                pending.debt_bytes =
                    pending.debt_bytes.checked_add(debt_bytes).ok_or_else(|| {
                        std::io::Error::other("pending streamed dirty debt exceeds u64::MAX")
                    })?;
            } else {
                self.pending = Some(StreamWritebackAccumulator {
                    offset,
                    length: debt_bytes,
                    debt_bytes,
                });
            }
            offset = end;
            length -= debt_bytes;
        }
        if self.pending_debt_bytes() == STREAM_WRITEBACK_WINDOW_BYTES {
            self.flush_pending(file)?;
        }
        Ok(())
    }

    pub(super) fn classify(
        &mut self,
        file: &std::fs::File,
        materialization: SparseMaterialization,
    ) -> std::io::Result<()> {
        let (data_ranges, hole_bytes) = materialization.into_parts();
        let dirty_bytes = data_ranges
            .iter()
            .try_fold(0_u64, |total, range| total.checked_add(range.length()))
            .ok_or_else(|| std::io::Error::other("streamed dirty output exceeds u64::MAX"))?;
        let classified_bytes = dirty_bytes
            .checked_add(hole_bytes)
            .ok_or_else(|| std::io::Error::other("streamed classified output exceeds u64::MAX"))?;
        if classified_bytes == 0 || self.coordinator.is_none() {
            return Ok(());
        }
        if classified_bytes > self.unclassified_bytes || classified_bytes > self.reserved_bytes {
            return Err(std::io::Error::other(
                "sparse writer classified more bytes than writeback admission reserved",
            ));
        }
        self.unclassified_bytes -= classified_bytes;
        if hole_bytes != 0 {
            self.deferred_hole_bytes = self
                .deferred_hole_bytes
                .checked_add(hole_bytes)
                .ok_or_else(|| std::io::Error::other("deferred sparse holes exceed u64::MAX"))?;
            if self.deferred_hole_bytes >= STREAM_WRITEBACK_WINDOW_BYTES {
                self.release_deferred_holes();
            }
        }
        for range in data_ranges {
            self.accumulate_dirty_range(file, range.offset(), range.length())?;
        }
        Ok(())
    }

    pub(super) fn classify_dense(
        &mut self,
        file: &std::fs::File,
        offset: u64,
        length: u64,
    ) -> std::io::Result<()> {
        self.classify(
            file,
            SparseMaterialization::from(SparseDataRange::new(offset, length)),
        )
    }
}

impl Drop for StreamWriteback {
    fn drop(&mut self) {
        self.release_deferred_holes();
        if self.reserved_bytes != 0
            && let Some(coordinator) = &self.coordinator
        {
            coordinator.release(self.reserved_bytes);
        }
    }
}
