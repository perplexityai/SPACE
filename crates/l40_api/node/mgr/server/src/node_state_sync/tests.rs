use std::sync::atomic::{AtomicUsize, Ordering};

/// Counts node-status marks in the process instead of notifying node-ctl.
#[derive(Debug, Default)]
pub(crate) struct RecordingNodeStatusDirtySink {
    marks: AtomicUsize,
}

impl RecordingNodeStatusDirtySink {
    /// Marks recorded since the last call.
    pub(crate) fn take(&self) -> usize {
        self.marks.swap(0, Ordering::AcqRel)
    }
}

impl super::NodeStatusDirtySink for RecordingNodeStatusDirtySink {
    fn mark_node_status_dirty(&self) {
        self.marks.fetch_add(1, Ordering::AcqRel);
    }
}
