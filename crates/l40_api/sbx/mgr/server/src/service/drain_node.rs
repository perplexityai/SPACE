//! `DrainNode`: the node's own half of a drain.
//!
//! The control plane names a generation, one drain of this node, and says
//! to drain or to cancel. A drain call seals the node under the generation
//! (see [`crate::activation_seal`]) and, on every call, makes each of its
//! residents durable elsewhere and reports the node's state. The calls are
//! level-triggered: each states the desired drain, and repeating one is
//! safe. It acts only through
//! the paths the node already owns: the suspend of a started or paused VM is
//! the idle auto-suspend's (a full snapshot published and uploaded under
//! node-local durability), a stopped sandbox's disk is published through
//! `EnqueueArtifactBackup`, so is the snapshot of a suspended sandbox whose
//! row pins no artifact (its lifecycle untouched), and the upload of every
//! artifact without a durable copy, named snapshots included, is finished
//! the same way. Each runs as a detached state writer and checks the seal
//! again first. The control plane learns of each artifact as it always
//! does, through the backup it registers.
//!
//! A call that doesn't say to evacuate holds: a started sandbox the node can
//! make durable is reported `pending` ([`planner::Wait::Hold`]) and nothing
//! is suspended for it, so it keeps running until the drain evacuates, or
//! until its idle auto-lifecycle or its user moves it on. A started sandbox
//! whose drain suspend is already running is waited for. A suspend planned
//! while holding suspends a VM only from `paused`, checked again under the
//! wake lock, so a resume that was already in flight at the seal and
//! finished `started` is held, not suspended. A call that says to evacuate
//! starts evacuating: from that call on, everything still running is
//! suspended too, within the window, and the drain keeps evacuating under
//! the generation until a cancel, whatever a later call under it says. It
//! keeps that in memory only: after a restart it holds again until a call
//! says to evacuate. The node keeps no deadline of its own.
//!
//! The report is rebuilt from the rows on every call, so it survives a
//! restart: a suspended sandbox whose pinned full snapshot is durable, and a
//! stopped one whose pinned disk is, are `evacuated` with that artifact,
//! whatever suspended or stopped them. Every reason the node isn't finished
//! is an entry, so the drain is finished exactly when `pending` and
//! `blocked` are both empty. Each pending entry names its kind (held, being
//! moved by the drain, or waiting on work the drain doesn't drive), whether
//! its row was a started VM at the planning read (`running`), the detail in
//! its reason, and when its row entered its state (`since`); the drain
//! reports no sandbox `blocked`, and one it can't move is pending with why.
//! `blocked` holds node-wide problems only. A cancel unseals, keeps its
//! generation as the tombstone, and answers at once: nothing is started,
//! stopped or handed back.
//!
//! A call never waits for that work. It plans from the rows as they are,
//! starts what is not already running, and answers. It paces what it
//! starts: at most a window of suspends and publications is in flight at
//! once, uploads they leave pending included, and a suspend starts only
//! while the cache disk covers its snapshot; the rest wait, `pending`, for a
//! later call. An upload whose job is still queued or running is left to
//! it. [`planner::DrainPlan`] is that decision, a pure function of what the
//! pass read. Beside it, the report blocks unless every scan that can reveal
//! a resident or an artifact succeeded and the seal held, under the call's
//! generation, from before the walk to its end, and lists the activations
//! and the artifact producers in flight at its first read as pending.
//!
//! One `DrainNode` call runs at a time, drain and cancel alike, its scans
//! included: a call that arrives while another is in progress fails at once
//! with `Aborted` and
//! [`pplx_space_rpc_contracts::drain_node_signals::DRAIN_CALL_IN_PROGRESS_MARKER`]
//! and changes nothing. The call's guard is released when the call ends,
//! however it ends, and the call is bounded by a whole-call timeout, after
//! which it answers `DeadlineExceeded`.
//!
//! A node-local artifact producer (a snapshot, an import, a publication)
//! asks the seal too, once it holds the sandbox's artifact lock
//! ([`crate::activation_seal::ActivationSeal::admit_producer`]): sealed, one
//! for a sandbox without a VM is refused, so nothing appears after a pass
//! found the node finished. One for a started or paused sandbox runs and is
//! `pending` ([`planner::Wait::Snapshot`]) until it ends; the drain's suspend
//! of that VM waits for the same lock, so a producer queued behind the
//! suspend reads the row after it, finds no VM and is refused. Its artifact
//! is then uploaded like any other before the drain is finished.
//!
//! `PreviewDrainNode` ([`crate::service::SandboxManagerService::finish_preview`])
//! reports what an evacuating drain would find on this unsealed node now,
//! without being one: it runs a call's reads, its planner and its cache
//! disk measurement, but takes no seal, starts no work and writes nothing,
//! the drain's memory of earlier calls included. It plans as an evacuating
//! drain would, so it lists a started sandbox with the work an evacuating
//! drain would start for it, never held, and answers the report a drain call
//! would. It doesn't read node-manager's orphan inventory, whose few scans
//! every caller on the node shares, so a drain may find more node-wide
//! blockers. A sealed node refuses it with `FailedPrecondition`, naming the
//! generation it is sealed under, and one preview runs at a time: another
//! fails at once with `Aborted` and the same marker as a `DrainNode` call.

mod actions;
mod planner;
mod rpc;

pub(super) use actions::{DrainActions, drain_node_window_from_env};

#[cfg(test)]
mod producer_tests;
#[cfg(test)]
mod tests;
