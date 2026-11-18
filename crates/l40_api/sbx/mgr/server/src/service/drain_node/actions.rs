//! The drain's work: the suspends, disk publications and uploads it starts
//! as detached state writers, and what it remembers of them between calls.

/// How many of the drain's suspends and publications may be in flight on the
/// node at once, the uploads they leave pending included, unless
/// [`DRAIN_NODE_WINDOW_ENV`] sets another positive number. Beside the idle
/// auto-suspend scheduler's 8, at most 16 suspends from these two callers
/// reach node-manager's Filesystem lane at once, below the 20 arrivals (4
/// permits and 16 waiting, its default) past which the lane refuses one,
/// which would roll that suspend back. The lane is shared: a caller's
/// suspend or snapshot, a snapshot rollback or restore, the rootfs sends and
/// receives, a sandbox instance's creation and a guest sync arrive there
/// too, so this keeps the drain and the scheduler from filling it, not every
/// caller. It is also vol-mgr's push concurrency per kind, so a VM the drain
/// has paused uploads rather than waiting behind the node's backlog.
pub(super) const DEFAULT_DRAIN_NODE_WINDOW: usize = 8;
