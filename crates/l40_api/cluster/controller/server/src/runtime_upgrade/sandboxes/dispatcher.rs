//! What the sandbox drivers ask of the substrate, and how they read a
//! sandbox's lifecycle into a step.

use std::future::Future;

use pplx_space_execution_spec::EnvironmentReference;
use pplx_space_k8s_ids::PodUid;
use pplx_space_runtime_upgrade_campaign_store::LeasedItem;
use pplx_space_sandbox_model::{SandboxId, SandboxLifecycle};
use uuid::Uuid;

/// How the node-side upgrade of a stopped sandbox ended.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StoppedUpgrade {
    /// Booted under the target and stopped again.
    Upgraded,
    /// Already on the target; nothing was booted.
    AlreadyOnTarget,
    /// Held without moving the sandbox (for example, `item`'s active lease was
    /// lost before claiming the upgrade); returns [`ItemOutcome::Release`]
    /// without spending an attempt.
    Held(String),
    /// The upgrade's own decision moved nothing, for the reason given: the
    /// policy or catalog changed since the item was found, for example.
    NotMoved(String),
}

/// Active Kubernetes execution row `(generation, pod_uid)` that hosted the
/// sandbox when a campaign stop settled.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StoppedK8sExecution {
    pub generation: i64,
    pub pod_uid: PodUid,
}

/// Timestamps and execution identity fencing one campaign-owned stop episode.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StopFence {
    /// `LEAST(NOW(), pre_stop_lifecycle_changed_at)` captured on the database
    /// clock before dispatching the node stop RPC, where
    /// `pre_stop_lifecycle_changed_at` is `sandboxes.lifecycle_changed_at` on
    /// the verified `'started'` or `'paused'` row. Passed as `initiated_at` to
    /// [`pplx_space_runtime_upgrade_campaign_store::external_stop_since`] so an
    /// external stop, kill, delete, or rollback that is in flight (`Release`
    /// while preserving stop ownership) or completed after the verified running
    /// episode began (while `sandboxes` was still stale `'started'`/`'paused'`
    /// ahead of node-status reconcile) fences this campaign's stop.
    pub initiated_at: jiff::Timestamp,
    /// `NOW()` captured on the database clock when the `'started'` or
    /// `'paused'` row was verified before dispatching the node stop RPC. Passed
    /// as `dispatched_at` to
    /// [`pplx_space_runtime_upgrade_campaign_store::external_stop_since`] so an
    /// already-settled stop or `auto_restart: true` rollback that completed
    /// between `initiated_at` and `dispatched_at` does not retroactively veto a
    /// stop dispatched later on the still-running sandbox.
    pub dispatched_at: jiff::Timestamp,
    /// Lower bound on when this campaign's stop settled `'stopped'` on the
    /// database clock. Passed as `since` to
    /// [`pplx_space_runtime_upgrade_campaign_store::external_activation_since`]
    /// and as `stopped_at` to
    /// [`pplx_space_runtime_upgrade_campaign_store::stopped_execution_ownership`]
    /// so external activations and Kubernetes execution retirements in
    /// `[stopped_at, settled_stopped_at)` remain visible even if an
    /// undispatched `'starting' -> 'stopped'` rollback refreshes
    /// [`Self::settled_stopped_at`].
    pub stopped_at: jiff::Timestamp,
    /// Latest controller-settled `'stopped'` `sandboxes.lifecycle_changed_at`
    /// for this stop episode (equal to [`Self::stopped_at`] initially, or
    /// refreshed when rolling back an undispatched `'starting' -> 'stopped'`
    /// claim or adopting a converged [`InitiatedStop`]).
    pub settled_stopped_at: jiff::Timestamp,
    /// On the Kubernetes substrate, the active `sandbox_k8s_executions`
    /// `(generation, pod_uid)` that settled `'stopped'` at [`Self::stopped_at`],
    /// verified by
    /// [`pplx_space_runtime_upgrade_campaign_store::stopped_execution_ownership`]
    /// across pod replacements, `node_drain` stopped disk evacuations, stopped
    /// re-placements, and implicit wakes. `None` on Direct (which does not
    /// track Kubernetes executions or drain stopped disks across nodes).
    pub k8s_stopped_execution: Option<StoppedK8sExecution>,
}

impl StopFence {
    /// A fence whose settled `'stopped'` timestamp has not been refreshed by a
    /// subsequent rollback.
    pub const fn new(
        initiated_at: jiff::Timestamp,
        dispatched_at: jiff::Timestamp,
        stopped_at: jiff::Timestamp,
        k8s_stopped_execution: Option<StoppedK8sExecution>,
    ) -> Self {
        Self {
            initiated_at,
            dispatched_at,
            stopped_at,
            settled_stopped_at: stopped_at,
            k8s_stopped_execution,
        }
    }

    /// A fence whose stop initiation, dispatch, and `'stopped'` settlement
    /// share one timestamp, with no Kubernetes stopped-execution identity
    /// attached.
    pub const fn at(stopped_at: jiff::Timestamp) -> Self {
        Self::new(stopped_at, stopped_at, stopped_at, None)
    }
}

/// How a guarded stop ended.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum GuardedStop {
    /// The sandbox was started or paused on the source and is stopped now,
    /// fenced by `0`.
    Stopped(StopFence),
    /// A temporary hold (policy, memory retirement, catalog, or a lost active
    /// campaign item lease) prevented stopping the sandbox right now; left as
    /// it was so the item is released without spending an attempt or skipping a
    /// running source sandbox.
    Held(String),
    /// The sandbox was no longer started or paused on the source (or was
    /// already powered off on the node before this stop call), for the reason
    /// given; left as it was.
    Declined(String),
}

/// How starting a stopped sandbox onto a campaign's target ended.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TargetStart {
    /// Cold-booted under the target and started now.
    Started,
    /// Booted from the fenced `StopFence::stopped_at` state onto the target
    /// without moving environments (for example, after a coexisting `sandboxes`
    /// campaign upgraded its recorded execution to the target while it remained
    /// stopped at `stopped_at`).
    AlreadyOnTarget,
    /// Still stopped under this item's owned stop without booting, for the
    /// reason given: the policy, memory retirement, or catalog changed since the
    /// driver checked it so starting now wouldn't be an authorized cold-boot
    /// onto the target, for example. Stop ownership stays intact so the item is
    /// released and cold-booted once the hold clears.
    HeldStopped(String),
    /// Starting this campaign-owned stopped sandbox onto the target was
    /// permanently refused (for example, its template release is permanently
    /// incompatible or its recorded execution moved to neither end of the
    /// campaign), for the reason given. Stop ownership is cleared and the item
    /// is skipped rather than spinning in [`ItemOutcome::Release`].
    Declined(String),
    /// Left the owned stop state before `start` claimed the row (for example, a
    /// prior timed-out `start` or concurrent user wake already moved it — the
    /// driver re-inspects the sandbox before deciding whether stop ownership is
    /// still intact, that transition reached the target, is still
    /// mid-transition, or superseded the stop) or saw an external stop/kill
    /// since `StopFence::initiated_at`, for the reason given. Policy, memory,
    /// or catalog holds while the campaign still owns the stop return
    /// [`Self::HeldStopped`] instead, and permanent start refusals while the
    /// stop remains owned return [`Self::Declined`].
    NotOnTarget(String),
}

/// Claim metadata recorded when [`SandboxUpgradeDispatcher::start`] moves a
/// campaign-owned `'stopped'` sandbox to `'starting'` before dialing the node
/// start RPC.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StartingClaim {
    /// The owned [`StopFence`] from which `'starting'` was claimed.
    pub stop: StopFence,
    /// `sandboxes.lifecycle_changed_at` returned by the `'stopped' -> 'starting'`
    /// claim (`None` only in non-persisting unit fakes), guarding any
    /// undispatched `'starting' -> 'stopped'` recovery against a newer
    /// lifecycle episode.
    pub starting_at: Option<jiff::Timestamp>,
    /// `sandboxes.placement` prior to claiming `'starting'`, restored if an
    /// undispatched claim rolls `'starting'` back to `'stopped'`.
    pub prev_placement: Option<String>,
    /// `sandboxes.active_node_id` prior to claiming `'starting'`, restored if
    /// an undispatched claim rolls `'starting'` back to `'stopped'`.
    pub prev_active_node_id: Option<Uuid>,
}

impl StartingClaim {
    /// A claim recording only `stop` without persisted row metadata.
    pub const fn from_stop(stop: StopFence) -> Self {
        Self {
            stop,
            starting_at: None,
            prev_placement: None,
            prev_active_node_id: None,
        }
    }
}

/// How recovery of a `'starting'` sandbox under
/// [`RecordedStop::StartingOnTarget`] ended on retry.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StartingRecovery {
    /// The node never received a start for the recorded [`StartingClaim`] (for
    /// example, the controller exited or cancelled the call between claiming
    /// `'starting'` in Postgres and dispatching the node start RPC); rolled
    /// `'starting'` back to `'stopped'` (`RETURNING lifecycle_changed_at`) and
    /// restored [`RecordedStop::Stopped`] with `0`.
    RestoredStopped(StopFence),
    /// The node already received a start for the recorded [`StartingClaim`] (or
    /// `item` lost its active lease before recovery could act), so the sandbox
    /// stays `'starting'` until node-status publish settles it.
    StillStarting,
    /// `sandboxes.lifecycle` left the recorded [`StartingClaim`]'s `'starting'`
    /// episode before recovery acted, so the caller should re-read the settled
    /// row.
    Settled,
}

/// Pre-dispatch fence persisted before dialing the node stop RPC for a leased
/// `sandbox_force_restarts` item, so a controller crash or deadline timeout
/// after the node admits the stop can reconcile against a concurrent
/// `converge_published_stopped` node-status publish without adopting an
/// unadmitted guest exit or external stop.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InitiatedStop {
    /// `LEAST(NOW(), pre_stop_lifecycle_changed_at)` captured on the database
    /// clock before dispatching the node stop RPC, passed to
    /// [`pplx_space_runtime_upgrade_campaign_store::external_stop_since`].
    pub initiated_at: jiff::Timestamp,
    /// `NOW()` captured on the database clock when the `'started'` or
    /// `'paused'` row was verified before dispatching the node stop RPC,
    /// passed to
    /// [`pplx_space_runtime_upgrade_campaign_store::external_activation_since`].
    pub dispatched_at: jiff::Timestamp,
    /// On the Kubernetes substrate, the active `sandbox_k8s_executions`
    /// `(generation, pod_uid)` hosting the sandbox when the stop was initiated;
    /// `None` on Direct.
    pub k8s_stopped_execution: Option<StoppedK8sExecution>,
}

/// Stop-and-start progress recorded for a leased `sandbox_force_restarts` item.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RecordedStop {
    /// An earlier attempt in this run recorded `0` before dialing the node stop
    /// RPC, and hasn't yet recorded a settled [`StopFence`] (for example, the
    /// stop call timed out or the controller restarted while the node stop was
    /// in flight).
    Initiated(InitiatedStop),
    /// An earlier attempt of this item stopped the sandbox under `0`, and this
    /// item's start onto the campaign's target hasn't claimed `'starting'` yet.
    Stopped(StopFence),
    /// This item claimed `'stopped' -> 'starting'` from `0.stop` in Postgres
    /// before dialing the node start RPC, and hasn't yet recorded
    /// [`Self::StartedOnTarget`] (for example, the start RPC timed out, the
    /// worker's item deadline cancelled the call, or the controller restarted
    /// before or while the node start was in flight).
    StartingOnTarget(StartingClaim),
    /// This item stopped the sandbox under `0` and its own
    /// [`SandboxUpgradeDispatcher::start`] settled the sandbox onto the
    /// campaign's target.
    StartedOnTarget(StopFence),
}

impl RecordedStop {
    /// Borrow the settled [`StopFence`] recorded when this item stopped the
    /// sandbox, or `None` while the stop is only [`Self::Initiated`].
    pub const fn as_fence(&self) -> Option<&StopFence> {
        match self {
            Self::Initiated(_) => None,
            Self::Stopped(fence) | Self::StartedOnTarget(fence) => Some(fence),
            Self::StartingOnTarget(claim) => Some(&claim.stop),
        }
    }

    /// Extract the settled [`StopFence`] recorded when this item stopped the
    /// sandbox, or `None` while the stop is only [`Self::Initiated`].
    pub fn into_fence(self) -> Option<StopFence> {
        match self {
            Self::Initiated(_) => None,
            Self::Stopped(fence) | Self::StartedOnTarget(fence) => Some(fence),
            Self::StartingOnTarget(claim) => Some(claim.stop),
        }
    }
}

/// A dispatch that didn't finish.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum DispatchError {
    /// Worth another lease: the sandbox is busy, the node is unavailable, or
    /// a deadline of the dispatcher's own passed. The sandbox is left as it
    /// was. The worker's item deadline isn't one of these: past it, the item
    /// fails.
    #[error("{0}")]
    Retryable(String),
    /// The call ran and failed; counts against the failure budget. A failed
    /// upgrade leaves the sandbox stopped on its old environment; a failed
    /// stop leaves it as the substrate's stop did.
    #[error("{0}")]
    Failed(String),
}

/// What the sandbox drivers ask of the substrate for one sandbox. Every call
/// is idempotent and safe to retry after any failure, including one that
/// left the caller unsure whether it ran.
///
/// The loop drops a call's future when the item's lease is lost or its
/// deadline passes ([`super::super::CampaignDriver::upgrade`]), so the
/// effects of a dropped call must be fenced where they land.
/// `runtime_upgrade_items` remains the single authority for item phase,
/// attempts, continuation ([`pplx_space_runtime_upgrade_campaign_store::continuing_run`]),
/// and outcomes.
pub trait SandboxUpgradeDispatcher: Send + Sync + 'static {
    /// Upgrade a stopped sandbox on `source` to `target`, the campaign's,
    /// leaving it stopped. The upgrade decides afresh where it acts, as a
    /// start would, from the sandbox's recorded environment and the policy
    /// then, and compares the sandbox's lifecycle and recorded environment
    /// where it decides, as a guarded transition does: a sandbox no longer
    /// stopped (a user started it since the driver read it) is
    /// [`StoppedUpgrade::NotMoved`] and is left running, a sandbox on
    /// `target` already is
    /// [`StoppedUpgrade::AlreadyOnTarget`] (an earlier lease's upgrade that
    /// published it but didn't finish its backup or retire is finished), and
    /// one on anything but `source` or `target` (started and stopped again
    /// under another environment since the driver read it) is
    /// [`StoppedUpgrade::NotMoved`]. So is a move to anything but `target`
    /// (the default changed since the lease), so it isn't recorded as this
    /// campaign's. Before admitting a new upgrade off `source`, verifies that
    /// `item` still holds its active `in_flight` lease on a `canary` or
    /// `rolling` campaign, returning [`StoppedUpgrade::Held`] without moving
    /// the sandbox if the lease was lost (while still allowing
    /// [`StoppedUpgrade::AlreadyOnTarget`] to finish an earlier lease's
    /// published target upgrade). Just before the call the driver checks only
    /// that the sandbox is stopped on `source` or `target`; whether it still
    /// is, and whether the policy still moves it to `target`, is this call's
    /// decision.
    fn upgrade_stopped(
        &self,
        item: &LeasedItem,
        sandbox_id: SandboxId,
        source: EnvironmentReference,
        target: EnvironmentReference,
    ) -> impl Future<Output = Result<StoppedUpgrade, DispatchError>> + Send;

    /// Stop a started or paused sandbox on `source`, so it can take the
    /// stopped or cold-boot start path to `target`. Before dialing the node
    /// stop RPC, verifies that `item` still holds its active `in_flight` lease
    /// under `item.lease_token` on a `canary` or `rolling` campaign, the
    /// sandbox's `started` or `paused` lifecycle, `source` attribution, and
    /// current campaign authorization for moving `source` to `target` (a
    /// certified upgrade edge to `target`, plus either a block of `source` for
    /// `sandboxes` campaigns or `source` memory retirement for
    /// `sandbox_force_restarts`) and captures
    /// `LEAST(NOW(), pre_stop_lifecycle_changed_at)` on the database clock as
    /// [`StopFence::initiated_at`]: a lost lease or policy/memory/catalog hold
    /// returns [`GuardedStop::Held`] without touching the sandbox, and a
    /// sandbox no longer started/paused on `source` returns
    /// [`GuardedStop::Declined`]. Implementations do not write an intermediate
    /// `'stopping'` lifecycle to `sandboxes` (matching
    /// `pplx_space_sandbox_store::action_stamp`'s public-stop concurrency
    /// contract).
    ///
    /// When no earlier `stop` attempt in this run recorded `stop_initiated_at`
    /// before dialing the node RPC, if the node stop RPC reports that the
    /// runtime was already stopped on the node before this call (for example,
    /// after a guest poweroff or external stop ahead of node-status reconcile),
    /// declines ([`GuardedStop::Declined`]) without adopting that stop as
    /// campaign-owned; only a retry after this run already recorded
    /// `stop_initiated_at` may adopt an already-stopped node response when no
    /// external stop occurred since `stop_initiated_at`.
    ///
    /// When `record_stop` is `true` (a `sandbox_force_restarts` stop), records
    /// [`RecordedStop::Initiated`] before dialing the node stop RPC (clearing
    /// it if a pre-admission failure or already-stopped response proves the
    /// node never admitted a campaign stop for this run). When the node admits
    /// and completes the campaign stop, stamps `sandboxes.lifecycle = 'stopped'`
    /// or — if `pplx_space_sandbox_store::ingest::converge_published_stopped`
    /// already converged `sandboxes.lifecycle` to `'stopped'` at
    /// `lifecycle_changed_at >= initiated_at` while the RPC was in flight (or
    /// across a retry of a continuing run whose `Initiated` stop the node
    /// confirms it admitted), with no external stop since `initiated_at`, no
    /// external activation since `dispatched_at`, and intact execution
    /// ownership — adopts that `'stopped'` timestamp (`ActionStamp::Stop`) and
    /// promotes the item's state to [`RecordedStop::Stopped`] with the
    /// resulting [`StopFence`] (`initiated_at` and `stopped_at`), gated on
    /// `item`'s row in `runtime_upgrade_items` still being `pending`,
    /// `in_flight`, or `failed` with its recorded initiation matching the
    /// admitted [`InitiatedStop`] (never `skipped` or `done`, so late stop
    /// completion after lease loss still promotes the matching run while a
    /// cleared or replaced episode cannot be overwritten; `insert_pending` and
    /// `finish_item` also clear the stop-progress columns atomically once the
    /// standalone schema migration adds them). If
    /// the node reports on retry of [`RecordedStop::Initiated`] that the
    /// runtime stopped without an admitted campaign stop (for example, the
    /// earlier attempt crashed before reaching the node and the guest later
    /// powered off on its own), clears [`RecordedStop::Initiated`] and returns
    /// [`GuardedStop::Declined`]. Because this is an internal
    /// controller/substrate transition rather than a public or admin API
    /// operation, implementations must not insert a `public_operations`
    /// `sandbox.stop` or `sandbox.kill` row, which
    /// [`pplx_space_runtime_upgrade_campaign_store::external_stop_since`]
    /// treats as external supersession.
    fn stop(
        &self,
        item: &LeasedItem,
        record_stop: bool,
        sandbox_id: SandboxId,
        source: EnvironmentReference,
        target: EnvironmentReference,
    ) -> impl Future<Output = Result<GuardedStop, DispatchError>> + Send;

    /// Start a stopped sandbox for `item` from the stopped state owned under
    /// `stop`, leaving it started on `target`, the campaign's. The start claims
    /// the transition only when `item` still holds an active `in_flight` lease
    /// (`item.lease_token` on a `canary` or `rolling` campaign), the sandbox is
    /// `'stopped'`,
    /// [`pplx_space_runtime_upgrade_campaign_store::external_stop_since`]`(stop.initiated_at)`
    /// is [`pplx_space_runtime_upgrade_campaign_store::ExternalOperation::None`],
    /// [`pplx_space_runtime_upgrade_campaign_store::external_activation_since`]`(stop.stopped_at)`
    /// is [`pplx_space_runtime_upgrade_campaign_store::ExternalOperation::None`],
    /// and either `stop.k8s_stopped_execution` is
    /// [`pplx_space_runtime_upgrade_campaign_store::StoppedExecutionOwnership::Owned`]
    /// via
    /// [`pplx_space_runtime_upgrade_campaign_store::stopped_execution_ownership`]
    /// or (`stop.k8s_stopped_execution` is `None` and)
    /// `sandboxes.lifecycle_changed_at == stop.stopped_at` with no pinned stop
    /// disk. An inactive item lease or an `InFlight` external operation or
    /// execution ownership state returns [`TargetStart::HeldStopped`] so stop
    /// ownership stays intact until the lease or operation settles; a
    /// `Succeeded` external operation or unowned stop state declines with
    /// [`TargetStart::NotOnTarget`].
    ///
    /// Revalidates policy, catalog, and `source` memory-retirement
    /// authorization when claiming the transition: if `target` is no longer the
    /// default, `source`'s memory retirement was revoked, or policy/catalog
    /// changed so starting now wouldn't land on `target` while the campaign
    /// still owns the stop, leaves it stopped without disturbing that stop
    /// ownership and returns [`TargetStart::HeldStopped`]; cold-booting onto
    /// `target` returns [`TargetStart::Started`]; booting an owned stop whose
    /// recorded execution is already `target` (for example, after a coexisting
    /// stopped-sandbox upgrade) returns [`TargetStart::AlreadyOnTarget`]; and
    /// leaving the owned stop state or seeing an external stop/kill since
    /// `stop.initiated_at` returns [`TargetStart::NotOnTarget`]; and a permanent
    /// substrate or plan refusal while still owning the stop (such as moving to
    /// an environment other than `source` or `target`) returns
    /// [`TargetStart::Declined`].
    ///
    /// In the Postgres transaction that locks the sandbox row (`FOR UPDATE`)
    /// and then `item`'s `runtime_upgrade_items` row (`FOR UPDATE` — every
    /// transaction touching both tables locks `sandboxes` before
    /// `runtime_upgrade_items`), verifies `item`'s active lease and `stop`
    /// ownership, and claims `'stopped' -> 'starting'`, transitions `item`'s
    /// recorded progress to [`RecordedStop::StartingOnTarget`]`(claim)`
    /// (recording
    /// [`pplx_space_runtime_upgrade_campaign_store::mark_leased_starting_on_target`]
    /// in the same transaction — rolling back and returning
    /// [`TargetStart::HeldStopped`] if it returns `false` — so stranded-mode
    /// selection still finds an undispatched `'starting'` claim if the catalog
    /// upgrade edge is removed) before dialing the node start
    /// RPC (if an un-dispatched error rolls `'starting'` back to `'stopped'`
    /// via `RETURNING lifecycle_changed_at`, clears that starting marker via
    /// [`pplx_space_runtime_upgrade_campaign_store::clear_leased_starting_on_target`]
    /// under the same `sandboxes`-then-`runtime_upgrade_items` lock order
    /// and restores
    /// [`RecordedStop::Stopped`] with `initiated_at: stop.initiated_at`,
    /// `stopped_at: stop.stopped_at`, `settled_stopped_at` refreshed to that
    /// rolled-back `'stopped'` `lifecycle_changed_at`, and
    /// `k8s_stopped_execution` preserved, so retry keeps ownership across
    /// Direct and Kubernetes while preserving the full `[stopped_at, ..)`
    /// external-activation and execution-retirement window), and promotes it to
    /// [`RecordedStop::StartedOnTarget`]`(stop)` (recording
    /// [`pplx_space_runtime_upgrade_campaign_store::mark_started_on_target`]
    /// durably under `item`'s row lock while `claim` still matches) when the
    /// controller settles `'starting'` to `'started'` on `target` (gated on
    /// `item`'s row still being `pending`, `in_flight`, or `failed` with `claim`
    /// matching). If the controller exits or cancels `start` before the node
    /// receives the boot, [`SandboxUpgradeDispatcher::recover_undispatched_start`]
    /// rolls `'starting'` back to `'stopped'` on the retry lease without
    /// touching an already-dispatched node start. If the start RPC times out,
    /// the worker's item deadline cancels the call, or the controller restarts
    /// while the node finishes booting onto `target` via `internal-api`
    /// node-status publish, [`RecordedStop::StartingOnTarget`] lets the retry
    /// lease certify `Done` once `sandboxes` settles `'started'` (or `'paused'`
    /// in memory after booting) on `target` with no intervening external stop
    /// or activation, without mistaking an unclaimed external user wake onto
    /// `target` for a campaign-completed upgrade.
    fn start(
        &self,
        item: &LeasedItem,
        sandbox_id: SandboxId,
        stop: StopFence,
        source: EnvironmentReference,
        target: EnvironmentReference,
    ) -> impl Future<Output = Result<TargetStart, DispatchError>> + Send;

    /// Recover a sandbox still in `'starting'` under
    /// [`RecordedStop::StartingOnTarget`]`(claim)` when an earlier attempt
    /// exited or was cancelled between claiming `'stopped' -> 'starting'` in
    /// Postgres and dispatching the node start RPC.
    ///
    /// When the node never received a start for `claim` and `sandboxes` is
    /// still in `claim`'s `'starting'` episode, locks `sandboxes` then `item`'s
    /// `runtime_upgrade_items` row (`FOR UPDATE`), rolls `'starting'` back to
    /// `'stopped'` (`RETURNING lifecycle_changed_at`), restores
    /// `claim.prev_placement` and `claim.prev_active_node_id`, clears
    /// [`pplx_space_runtime_upgrade_campaign_store::clear_leased_starting_on_target`]
    /// in the same transaction, replaces
    /// [`RecordedStop::StartingOnTarget`] with [`RecordedStop::Stopped`]
    /// carrying `settled_stopped_at = rolled_back`, and returns
    /// [`StartingRecovery::RestoredStopped`]. When the node already received a
    /// start for `claim` (or `item`'s lease is no longer active), returns
    /// [`StartingRecovery::StillStarting`] without mutating `sandboxes` so an
    /// in-flight node boot is never rolled back.
    fn recover_undispatched_start(
        &self,
        item: &LeasedItem,
        sandbox_id: SandboxId,
        claim: StartingClaim,
    ) -> impl Future<Output = Result<StartingRecovery, DispatchError>> + Send;

    /// The stop-and-start progress recorded when an earlier attempt of `item`
    /// stopped (and optionally claimed or finished starting) the sandbox in
    /// Postgres.
    ///
    /// Kept behind this read until a standalone schema-migration change adds
    /// `stop_initiated_at`, `stopped_at`, `starting_on_target`, and
    /// `started_on_target` columns directly to `runtime_upgrade_items` (DDL
    /// migrations are isolated from runtime crate changes in CI).
    fn item_stop(
        &self,
        item: &LeasedItem,
    ) -> impl Future<Output = Result<Option<RecordedStop>, DispatchError>> + Send;

    /// Clear `item`'s recorded stop when `item` is still `in_flight` under
    /// `item.lease_token` and either `item` hasn't started its current run yet
    /// (`!pplx_space_runtime_upgrade_campaign_store::continuing_run`, clearing
    /// any stale pre-run [`RecordedStop`] matching `expected`) or its recorded
    /// stop on the continuing run is still [`RecordedStop::Initiated`],
    /// [`RecordedStop::Stopped`], or [`RecordedStop::StartingOnTarget`] equal
    /// to `expected` (also clearing
    /// [`pplx_space_runtime_upgrade_campaign_store::clear_leased_starting_on_target`]
    /// when `expected` is [`RecordedStop::StartingOnTarget`]),
    /// so an earlier stop whose sandbox has since moved on, seen an external
    /// stop/kill, or reopened from `skipped`/`done` can't certify a later
    /// external upgrade as `done`, while a worker that lost its lease can't
    /// delete a newer lease's stop state, clearing [`RecordedStop::Initiated`]
    /// can't clobber a concurrent [`RecordedStop::Stopped`], clearing
    /// [`RecordedStop::Stopped`] can't clobber a concurrent
    /// [`RecordedStop::StartingOnTarget`], and a continuing run's completed
    /// [`RecordedStop::StartedOnTarget`] isn't erased. No-op when `item`'s
    /// lease is no longer active, `item` has no matching recorded stop, or a
    /// newer stop state replaced `expected`.
    fn clear_stopped_at(
        &self,
        item: &LeasedItem,
        expected: RecordedStop,
    ) -> impl Future<Output = Result<(), DispatchError>> + Send;
}

/// Lifecycles representing an active transition rather than a settled state.
pub(super) const MID_TRANSITION_LIFECYCLES: &[SandboxLifecycle] = &[
    SandboxLifecycle::Creating,
    SandboxLifecycle::Starting,
    SandboxLifecycle::Restoring,
    SandboxLifecycle::Pausing,
    SandboxLifecycle::Resuming,
    SandboxLifecycle::Suspending,
    SandboxLifecycle::Stopping,
    SandboxLifecycle::Checkpointing,
];

/// Whether `lifecycle` is an active transition rather than a settled state.
pub(super) const fn is_mid_transition(lifecycle: SandboxLifecycle) -> bool {
    match lifecycle {
        SandboxLifecycle::Creating
        | SandboxLifecycle::Starting
        | SandboxLifecycle::Restoring
        | SandboxLifecycle::Pausing
        | SandboxLifecycle::Resuming
        | SandboxLifecycle::Suspending
        | SandboxLifecycle::Stopping
        | SandboxLifecycle::Checkpointing => true,
        SandboxLifecycle::Created
        | SandboxLifecycle::Started
        | SandboxLifecycle::Paused
        | SandboxLifecycle::Suspended
        | SandboxLifecycle::Stopped
        | SandboxLifecycle::Deleting
        | SandboxLifecycle::Quarantined
        | SandboxLifecycle::Failed => false,
    }
}

/// What to do with a sandbox in a lifecycle.
#[derive(Debug, Eq, PartialEq)]
pub(super) enum Step {
    /// Upgrade it as it is: stopped.
    Upgrade,
    /// Stop it, then upgrade it.
    StopThenUpgrade,
    /// Mid-transition; decided by where it is (see `SandboxDriver::upgrade`).
    Settling(String),
    /// Not this campaign's to move.
    Skip(String),
}

/// The step for a sandbox in `lifecycle` (the stored text), for a campaign
/// that does or doesn't take running sandboxes.
pub(super) fn step(lifecycle: &str, include_running: bool) -> Step {
    // A deleted row is kept as `deleted` for a while; it isn't a lifecycle.
    if lifecycle == "deleted" {
        return Step::Skip("the sandbox was deleted".to_owned());
    }
    let Ok(parsed) = lifecycle.parse::<SandboxLifecycle>() else {
        return Step::Skip(format!(
            "the sandbox's lifecycle {lifecycle:?} isn't upgradable"
        ));
    };
    match parsed {
        SandboxLifecycle::Creating
        | SandboxLifecycle::Starting
        | SandboxLifecycle::Restoring
        | SandboxLifecycle::Pausing
        | SandboxLifecycle::Resuming
        | SandboxLifecycle::Suspending
        | SandboxLifecycle::Stopping
        | SandboxLifecycle::Checkpointing => Step::Settling(format!("the sandbox is {lifecycle}")),
        SandboxLifecycle::Stopped => Step::Upgrade,
        SandboxLifecycle::Started | SandboxLifecycle::Paused if include_running => {
            Step::StopThenUpgrade
        }
        SandboxLifecycle::Started | SandboxLifecycle::Paused => Step::Skip(format!(
            "the sandbox is {lifecycle}, and this campaign moves only stopped sandboxes"
        )),
        SandboxLifecycle::Created
        | SandboxLifecycle::Suspended
        | SandboxLifecycle::Deleting
        | SandboxLifecycle::Quarantined
        | SandboxLifecycle::Failed => Step::Skip(format!(
            "the sandbox is {lifecycle}, which this campaign doesn't upgrade"
        )),
    }
}
