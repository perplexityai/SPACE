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
//! node-local durability), the snapshot of a suspended sandbox whose row
//! pins no artifact is published through `EnqueueArtifactBackup` (its
//! lifecycle untouched), and the upload of every artifact without a durable
//! copy, named snapshots included, is finished the same way. A v2 drain
//! doesn't move a stopped sandbox: it is pending, waiting on what the drain
//! doesn't drive ([`planner::STOPPED_REASON`]), and nothing is started for
//! it, so a node that holds one is never finished. Each runs as a detached state writer and checks the seal
//! again first. The control plane learns of each artifact as it always
//! does, through the backup it registers.
//!
//! A call that doesn't say to evacuate holds: a started sandbox the node can
//! make durable is reported `pending` ([`planner::Wait::Hold`]) and nothing
//! is suspended for it, so it keeps running until the drain evacuates, or
//! until its idle auto-lifecycle or its user moves it on, and is reported
//! held whatever its pin's upload does, that upload finished meanwhile and
//! the held entry's reason saying when it holds a window slot and how its
//! last attempt failed, if it did. A
//! started sandbox whose drain suspend is already running is waited for. A
//! suspend planned
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
//! restart: a suspended sandbox whose pinned full snapshot is durable is
//! `evacuated` with that artifact, whatever suspended it. Every reason the node isn't finished
//! is an entry, so the drain is finished exactly when `pending` and
//! `blocked` are both empty. Each pending entry names its kind (held, being
//! moved by the drain, or waiting on work the drain doesn't drive), whether
//! its row was a started VM at the planning read (`running`), the detail in
//! its reason, and when its row entered its state (`since`); the drain
//! reports no sandbox `blocked`, and one it can't move is pending with why,
//! a VM whose suspend the node refuses waiting on what the drain doesn't
//! drive. Such a VM's pin, holding or evacuating, still has its upload
//! finished: no suspend of the drain's will replace it, as none replaces a
//! held VM's. Its entry's reason says when that upload holds a window
//! slot.
//! `blocked` holds node-wide problems only. A cancel unseals, keeps its
//! generation as the tombstone, and answers at once: nothing is started,
//! stopped or handed back.
//!
//! A call never waits for that work. It plans from the rows as they are,
//! starts what is not already running, and answers. Each sandbox has one
//! stage in a pass ([`planner::Stage`]), a pure function of its row, the
//! drain's action running for it, its pin's upload and the hold, and the
//! pass's window, disk reservation, action and reported wait all come from
//! it. It paces what it starts: a sandbox holds one of a window of slots
//! from its suspend or publication's start until the upload of the pin it
//! leaves ends, a pin upload's retry needs a slot too, after the VMs' work,
//! paused VMs then started ones, beside the publications, and a suspend starts
//! only while the cache disk covers its snapshot beside what the running
//! suspends reserve, the bytes sized when each started; the rest wait,
//! `pending`, for a later call. It starts at most one action per sandbox:
//! its suspend or publication, then its pin's upload, then the upload of
//! another of its artifacts, which takes no slot. An upload whose job is
//! still queued or running is left to it. The call reads the drain's own
//! work first, then the cache disk, then the rows, so work that ends
//! between those reads is counted twice, never not at all.
//! [`planner::DrainPlan`] is the pass's decision. Beside it, the report blocks unless every scan that can reveal
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
