//! `DrainNode`: the node's own half of a drain, forced or gentle.
//!
//! The control plane names an epoch; the node seals itself under it (see
//! [`crate::activation_seal`]) and, on every call, makes each of its
//! residents durable elsewhere and reports what it has done. It acts only
//! through the paths the node already owns: the suspend of a started or paused
//! VM is the idle auto-suspend's (a full snapshot published and uploaded under
//! node-local durability), a stopped sandbox's disk is published through
//! `EnqueueArtifactBackup`, so is the snapshot of a suspended sandbox whose
//! row pins no artifact (its lifecycle untouched, and never reported
//! evacuated), and the upload of every artifact without a durable copy, named
//! snapshots included, is finished the same way. Each runs as a detached
//! state writer and checks the seal again first. The control plane learns of
//! each artifact as it always does, through the backup it registers; the
//! drain only reports the artifact, the row's durable pin, once it is
//! durable, with the lifecycle the sandbox had.
//! A drain suspend records that lifecycle (`drain_evacuations`) under the
//! sandbox's wake lock, while the node is still sealed under the drain's
//! epoch, so it survives a restart; a stopped sandbox's row keeps its own.
//! A cancel keeps the records and hands each sandbox back instead
//! ([`planner::plan_cancel`]): the control plane restores it, then names it for the
//! node to forget, which only a cancel still under the node's current epoch
//! does. A started sandbox the drain left paused stays blocked until then.
//!
//! A call never waits for that work. It plans from the rows as they are,
//! starts what is not already running, and answers. It paces what it starts:
//! at most a window of suspends and publications is in flight at once,
//! uploads they leave pending included, and a suspend starts only while the
//! cache disk covers its snapshot; the rest wait, `pending`, for a later
//! call. An upload whose job is still queued or running is left to it.
//! [`planner::DrainPlan`] is that decision, a pure function of what the call read, and `done` is true only
//! when nothing is left: no VM, nothing pending or blocked, every scan that
//! can reveal a resident or an artifact succeeded, and the seal held, under
//! the call's epoch, from before the walk to its end, with no activation in
//! flight at its first read. Every row that keeps `done` false says why: work
//! that keeps failing, and a row stuck in a transition the drain doesn't
//! drive, turn from `pending` into `blocked` after a bound.
//!
//! A gentle call (`gentle_epoch`) plans the same way, except that it holds a
//! sandbox in `started` the node can make durable: it reports it `pending`
//! ([`planner::HOLD`]) and suspends nothing for it, so it keeps running until
//! the node's idle auto-lifecycle, or its user, moves it on. One the node
//! can't make durable is reported `blocked` with why, and one whose drain
//! suspend is already running is waited for, as under a forced call. The seal refuses activations only, so that
//! suspend, stop or delete goes ahead. A sandbox that suspends on its own has
//! no drain record and is never reported evacuated; its upload is finished
//! like any artifact without a durable copy. A gentle drain's suspend
//! suspends a VM only from `paused`: it checks again under the wake lock, so
//! a resume that was already in flight at the seal and finished `started`
//! before the suspend ran is held, not suspended. A forced call under the
//! same epoch continues the same drain and holds nothing.

mod actions;
mod planner;
mod rpc;

pub(super) use actions::{DrainActions, drain_node_window_from_env};

#[cfg(test)]
mod tests;
