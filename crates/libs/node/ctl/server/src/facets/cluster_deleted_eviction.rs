//! Which local sandboxes the cluster has deleted may be evicted, and how fast: the per-sync budget,
//! backoff and candidate selection. The eviction loop itself runs in the sandbox desired-state
//! facet.

use super::*;

/// Cap on successful cluster-deleted evictions per sync tick.
/// Bounds both the teardown work added to one facet pass and the blast
/// radius of a bad cluster answer; the backlog drains across syncs.
pub(super) const CLUSTER_DELETED_EVICTS_PER_SYNC: usize = 25;

/// How many candidates one sync may examine while filling the eviction
/// cap. Wider than the cap so rows that defer within one sync cannot
/// monopolize the window; the per-candidate backoff below handles rows
/// that keep deferring across syncs.
pub(super) const CLUSTER_DELETED_CANDIDATE_WINDOW: usize = 100;

/// Per-candidate retry backoff: a candidate that deferred or failed is
/// skipped by selection for this long, mirroring the disk-pressure
/// eviction loop's candidate backoff, so a permanently-deferring oldest
/// prefix (e.g. malformed `updated_at` rows) rotates out of the window
/// instead of starving the drainable backlog sorted behind it. Process
/// lifetime, in-memory: a node-ctl restart merely retries earlier.
pub(super) const CLUSTER_DELETED_EVICT_BACKOFF: std::time::Duration =
    std::time::Duration::from_secs(30 * 60);

/// Slack added to a server-supplied retry horizon so the retry lands on a
/// sync *after* the horizon passes rather than racing it. Sized to the
/// deployed default poll interval as a fixed safety margin: the interval is
/// runtime-configurable, so a shorter one just skips a few more syncs and a
/// longer one still adds less than one interval of margin; neither breaks
/// the schedule, it only shifts the retry by at most one extra sync.
pub(super) const CLUSTER_DELETED_EVICT_RETRY_SLACK: std::time::Duration =
    std::time::Duration::from_secs(crate::DEFAULT_POLL_INTERVAL_SECS);

/// What one refused `EvictClusterDeletedSandbox` means for the per-id retry
/// schedule (a successful claim needs no schedule: the teardown runs
/// detached). Pure so the routing is testable without a live sbx-manager.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ClusterDeletedEvictDisposition {
    /// Retry on the very next sync, no back-off: sbx-manager found a
    /// restore staging pull live on the row at claim time (`Aborted`),
    /// which ends on the pull's own timescale.
    RetryNextSync,
    /// Skip the id for this long, then retry. Not an element failure.
    Deferred(std::time::Duration),
    /// The eviction failed outright; back off and report an element error.
    Failed(std::time::Duration),
}

/// Route a refused eviction RPC. `NotFound` (the local row vanished),
/// `FailedPrecondition` (row transitioned / inside the quiescence window /
/// refused as local-provenance or pool), `Unimplemented` (rollout skew),
/// `Unavailable` (sbx-manager draining) and `DeadlineExceeded` (teardown
/// still running detached) all defer. A refusal that carries a retry
/// horizon — sbx-manager's quiescence refusal says how long the window has
/// left — defers exactly that long plus one sync of slack, so a fresh
/// cluster-deleted row is reclaimed at quiescence end (~11 min after the
/// delete) instead of after the flat 30 min back-off that stays in place
/// for refusals with no horizon. Everything else is a failure.
pub(super) fn cluster_deleted_evict_refusal_disposition(
    status: &tonic::Status,
) -> ClusterDeletedEvictDisposition {
    match status.code() {
        Code::Aborted => ClusterDeletedEvictDisposition::RetryNextSync,
        Code::NotFound
        | Code::FailedPrecondition
        | Code::Unimplemented
        | Code::Unavailable
        | Code::DeadlineExceeded => ClusterDeletedEvictDisposition::Deferred(
            pplx_space_common_grpc::retry_after(status)
                .map(|horizon| horizon.saturating_add(CLUSTER_DELETED_EVICT_RETRY_SLACK))
                .map_or(CLUSTER_DELETED_EVICT_BACKOFF, |until| {
                    until.min(CLUSTER_DELETED_EVICT_BACKOFF)
                }),
        ),
        _ => ClusterDeletedEvictDisposition::Failed(CLUSTER_DELETED_EVICT_BACKOFF),
    }
}

pub(super) static CLUSTER_DELETED_EVICT_BACKOFF_UNTIL: std::sync::LazyLock<
    dashmap::DashMap<String, std::time::Instant>,
> = std::sync::LazyLock::new(dashmap::DashMap::new);

/// Wall-clock budget for the eviction pass inside one facet sync. The
/// whole inbound facet races a 30s cancel-by-drop timeout in the work
/// loop; stopping well inside it leaves room for the desired-state applies
/// that share the tick and carries the remainder to the next sync.
pub(super) const CLUSTER_DELETED_EVICT_SYNC_BUDGET: std::time::Duration =
    std::time::Duration::from_secs(15);

/// Per-eviction RPC deadline. The teardown is detached server-side, so
/// this bounds slot occupancy, not the teardown itself.
pub(super) const CLUSTER_DELETED_EVICT_RPC_TIMEOUT: std::time::Duration =
    std::time::Duration::from_secs(10);

/// Pick which cluster-deleted sandbox ids to evict locally this tick:
/// only ids whose local sbx-manager row is currently `suspended` (the
/// leaked wake-cache shape), `restoring` with a persisted staging
/// failure (the row a failed cross-node restore retains on the target
/// "for retry" — a retry can never come once the cluster row is gone, and
/// no other path reclaims it), or `suspending` with a permanently refused
/// teardown backup (the row a pod teardown leaves when the sandbox was
/// deleted mid-activation, whose suspend can never publish), owned by a
/// platform user (`user:<uuid>` —
/// positive evidence the cluster minted the sandbox; local-provenance
/// rows like `root@local` or `tmplmgr@local` never had a cluster row, so
/// its absence proves nothing about them), and not a pool slot. A
/// `restoring` row without the failure marker is a first-attempt restore
/// and is left alone, as is a `suspending` row without the terminal
/// teardown-backup marker, and anything else mid-transition, failed, or
/// already absent locally. Neither marker is a liveness proof — a retry of
/// the failed restore reuses the row without clearing it. sbx-manager re-checks the owner, pool and quiescence guards, takes
/// the staging lock non-blocking to prove no retry pull is live, and claims
/// the row at the exact version it read before tearing anything down — but
/// it makes no cluster call of its own: the `deleted_sandbox_ids` report
/// this selection is drawn from IS the authority that the cluster row is
/// gone, so getting that report right is this caller's responsibility, not
/// something sbx-manager can recover from.
/// Oldest first — UUIDv7 ids sort by creation time — so the largest
/// accumulated dead weight drains ahead of fresh arrivals.
pub(super) fn select_cluster_deleted_evict_candidates<'a>(
    deleted_sandbox_ids: &'a [String],
    local_states: &BTreeMap<String, SandboxInfo>,
    window: usize,
    is_backed_off: impl Fn(&str) -> bool,
) -> Vec<&'a str> {
    let mut candidates = deleted_sandbox_ids
        .iter()
        .filter(|sandbox_id| !is_backed_off(sandbox_id))
        .filter(|sandbox_id| {
            local_states.get(*sandbox_id).is_some_and(|state| {
                local_row_is_cluster_deleted_reclaimable(state)
                    && state.pool_template.is_none()
                    && state
                        .owner
                        .as_deref()
                        .and_then(|owner| SandboxOwner::parse(owner).ok())
                        .is_some_and(|owner| owner.is_platform_user())
            })
        })
        .map(String::as_str)
        .collect::<Vec<_>>();
    candidates.sort_unstable();
    candidates.dedup();
    candidates.truncate(window);
    candidates
}

/// The local row shapes a cluster-deleted report may reclaim: a
/// stopped rootfs, a suspended wake-cache copy, a failed cross-node restore's retained
/// `restoring` row (identified by any persisted `failure_reason`; a
/// whitespace-only reason is treated as no marker, matching sbx-manager),
/// or a `suspending` row whose teardown suspend had its durable backup
/// permanently refused, or a quarantined row retained for a deleted sandbox.
///
/// The `suspending` shape matches one exact reason rather than "any
/// failure", because that is the only `suspending` failure that ends the
/// row's own publication path for good. Other reasons sit on suspends
/// recovery can still finish, and offering them would spend eviction
/// budget and back-off slots on ids sbx-manager is bound to refuse.
pub(super) fn local_row_is_cluster_deleted_reclaimable(state: &SandboxInfo) -> bool {
    use pplx_space_sbx_mgr_client::proto::SandboxLifecycleState;
    match state.observed_lifecycle().ok() {
        Some(
            SandboxLifecycleState::Stopped
            | SandboxLifecycleState::Suspended
            | SandboxLifecycleState::Quarantined,
        ) => true,
        Some(SandboxLifecycleState::Restoring) => state
            .failure_reason
            .as_deref()
            .is_some_and(|reason| !reason.trim().is_empty()),
        Some(SandboxLifecycleState::Suspending) => {
            state.failure_reason.as_deref()
                == Some(
                    pplx_space_sbx_mgr_client::TEARDOWN_SUSPEND_BACKUP_PERMANENTLY_FAILED_REASON,
                )
        }
        _ => false,
    }
}
