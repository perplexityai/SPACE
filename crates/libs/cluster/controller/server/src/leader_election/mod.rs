//! Kubernetes-`Lease`-backed leader election for the cluster-controller.
//!
//! The cluster-controller hosts cluster-*singleton* reconcilers (warm-pool,
//! sweepers, node-drain, ...). Running two copies concurrently would
//! double-provision the warm pool and double-drive every CAS loop, so exactly
//! one pod may run the reconciler set at a time. That invariant used to be
//! enforced by a single-replica `Recreate` Deployment — which drops to zero
//! replicas during a rollout, leaving a window with no reconciler at all.
//! This module lets the Deployment run more than one replica under
//! `RollingUpdate` (never zero) while still guaranteeing a
//! single *active* reconciler-runner:
//!
//! * Every replica calls [`campaign`], which contends for a
//!   `coordination.k8s.io/v1` `Lease` object (see [`store`]) — the standard
//!   in-cluster lock. The winner returns a [`Leadership`] guard; losers keep
//!   retrying as standbys until they win or shutdown is requested.
//! * Only the leader runs the reconciler set, driven by [`Leadership::token`]
//!   — a child of the process shutdown token that is *also* cancelled if the
//!   lease is lost. (Every replica builds the substrate up front, before
//!   contending, so a standby is a *warm* standby — readiness means it can
//!   actually take over, and failover doesn't pay a cold start. See `main`.)
//! * A background renewal task extends the lease on an interval. If it cannot
//!   renew within the TTL, or observes another holder, it cancels the
//!   leadership token so the reconcilers stop before a standby takes over —
//!   there is never more than one active runner.
//! * On graceful shutdown the leader [`Leadership::resign`]s, clearing the
//!   lease's holder so a standby takes over within one retry interval rather
//!   than waiting out the TTL.
//!
//! While the kill-switch leaves election enabled (the default), leader
//! election runs when the process runs in a Kubernetes pod, where an apiserver
//! can host the `Lease` and a Deployment may run several replicas, whichever
//! substrate places the sandboxes; and wherever a substrate that drives the
//! Kubernetes API runs, since its client reaches the same apiserver and the
//! process may share the cluster with in-cluster replicas (see
//! [`LeaderElectionConfig::election`]). Otherwise (a Direct controller outside
//! a pod, such as on a systemd host, or the kill-switch off) `main` runs the
//! reconcilers unconditionally without contending, which assumes a single
//! instance.
//!
//! # Readiness and liveness: two different questions
//!
//! Two signals come out of the lease path, and they deliberately disagree about
//! what "unhealthy" means:
//!
//! * **Readiness** — "can this pod take over *right now*?" The marker file (see
//!   [`LeaderElectionConfig::readiness_file`]) is cleared on the first failed
//!   lease call and re-written on the first success, so a pod that loses
//!   apiserver contact goes NotReady within a few probe periods. That is what
//!   makes `maxUnavailable: 0` sound: a replacement that can't reach the lease
//!   never goes Ready, so a rollout won't tear down a working pod for one that
//!   can't take over.
//! * **Liveness** — "will this process *ever* reach the lease again?" A holder
//!   that can't renew already exits (below), but a **standby** that can't
//!   acquire has nothing to exit for: the process is healthy, so kubelet leaves
//!   it running, NotReady, retrying forever. If the reason it can't reach the
//!   lease is process-local — a half-open apiserver connection, a client wedged
//!   behind a stale token — retrying never recovers and only a restart does. So
//!   [`campaign`] tracks how long it has been unable to reach the lease for
//!   restart-fixable reasons and returns [`Campaign::LeaseUnreachable`] once
//!   those chargeable intervals span
//!   [`LeaderElectionConfig::lease_failure_grace`]; `main` exits non-zero and
//!   kubelet restarts the container with a fresh client. Only failures a restart
//!   could plausibly clear are counted: every failure is classified into a
//!   [`LeaseFailure`], and time spent receiving an overload or refusal answer is
//!   excluded from the budget (see *Failure categories* below).
//!
//! Liveness is decided here rather than by a `livenessProbe` because the
//! readiness marker is also absent when the marker *write* fails (see
//! [`reconcile_marker`], which logs and retries), so a probe can't tell "can't
//! reach the apiserver" from "can't write to /tmp". The budget is charged only
//! on restart-fixable lease failures. Grace `0` disables the exit.
//!
//! # Failure categories
//!
//! A failed lease call is classified by `LeaseError::classify` into a
//! [`LeaseFailure`], on the error variant and HTTP status only — never on message
//! text, which would reclassify silently the first time a message changed. Two
//! independent questions are asked of each category, which is why one enum
//! carries both predicates:
//!
//! | Category | Charges the budget? | Reported on sight? | Why |
//! | --- | --- | --- | --- |
//! | `Unreachable` | yes | no | Transport or handshake failure, timeout, credential load, or an answer with no usable status code. A restart rebuilds the client and re-reads the projected token — this is what the budget is for. The *config-time* TLS and proxy variants are `Refused` instead, since this build's own configuration decides them. |
//! | `Clock` | yes | yes | This replica can't reason about expiry at all. A clock may have been stepped since start, so a restart may fix it, but it also warrants a human. |
//! | `Overloaded` | no | no | 429 or 5xx: the apiserver said it is shedding load. Restarting makes it worse, because the replacement re-LISTs every Node against the same control plane. Resolves itself. |
//! | `Refused` | no | yes | 401/403/4xx, or a request this build can't form. A fresh client gets the same answer, so there is no later escalation to wait for — logged at `error` the first time it is seen. |
//!
//! Anything the classifier cannot place lands on `Unreachable`, not `Refused`.
//! That is the direction that matters: `Unreachable` is the behaviour that
//! existed before any classification, so an unrecognised failure is never
//! *quieter* than it used to be. It is not a theoretical case — `Status` marks
//! every field `#[serde(default)]` and `kube` uses a parsed error body verbatim
//! instead of overriding it with the HTTP status, so a 503 whose JSON body
//! carries no `code` arrives as `code: 0`.
//!
//! Both non-charging categories interrupt the active chargeable interval without
//! clearing time already accrued before it: a brownout interleaved with genuine
//! unreachability neither spends the budget itself nor rescues a client that is
//! really wedged. And `Refused` deliberately does not exit. A crash-loop against
//! a revoked RBAC binding restarts forever without ever fixing it, while a pod
//! that stays up, NotReady, with `acquire_failed{reason:refused}` climbing is
//! equally visible and doesn't add load to recover from.
//!
//! The categories are consumed only by [`campaign`]. [`renew_loop`] records the
//! reason on its counter but acts identically on all four: a holder that can't
//! confirm its lease must stop acting before the TTL whatever the cause, so
//! distinguishing them there would be a distinction without a behaviour.
//!
//! # What the budget doesn't cover
//!
//! * **The holder.** A holder that can't renew is forced out at `renew_deadline`
//!   — `lease_ttl - 12s`, so 48s at the fixed 60s TTL — by [`renew_loop`],
//!   independently of this budget. A restarted pod re-enters [`campaign`] as a
//!   standby and can only reach that path by *acquiring*, which a lease-path
//!   outage prevents — so during a sustained outage that exit happens at most
//!   once per pod and every later
//!   exit is a `lease_failure_grace` one. Each pod therefore restarts about once
//!   per grace window for as long as the outage lasts, paying a substrate
//!   rebuild each time. That recurring cycle is new: before this budget a
//!   standby sat NotReady indefinitely instead.
//! * **Wedges that spare the lease.** The budget observes the substrate's shared
//!   `kube::Client` only *through* the lease path, so a fault that freezes the
//!   pod/node reflectors while lease calls keep succeeding isn't caught.
//! * **Restart cost.** Exiting discards the warm reflector caches `main` builds
//!   before contending, so the replacement re-LISTs every Node against an
//!   apiserver that is by construction degraded.
//! * **A slow acquisition that lands anyway.** `acquire_or_renew` is a read
//!   followed by a write. If the pair outruns `retry_interval` but the write
//!   still lands, the apiserver records this replica as holder while [`campaign`]
//!   discards the timed-out result and keeps contending. It then *renews* that
//!   lease on later attempts (the store lets the recorded holder re-take), so
//!   peers get a valid denial, report Ready and reset their budgets — a fleet
//!   that is Ready with no reconciler running, until this replica's own budget
//!   ends it. Pre-existing in the acquisition path; the budget is what eventually
//!   breaks the tie, which it did not before.
//! * **A latency brownout, which is unreachability at the client.** The
//!   categories above separate the failures the apiserver *answers*, and a
//!   control plane that answers 429 is spared a restart. But because each attempt
//!   is bounded by `retry_interval`, a brownout that pushes the lease round-trip
//!   past that bound (control-plane saturation, APF queueing behind a slow etcd,
//!   an etcd defrag) produces no answer to classify: the client sees a timeout,
//!   which is `Unreachable` by construction and indistinguishable from a dead
//!   connection. So that shape still spends the budget and still restarts, and
//!   the restart still can't fix it. The fix is the control plane. Two things
//!   narrow the window rather than close it: a *fast*-rejecting brownout (APF
//!   returning 429 promptly) is now correctly spared, and decoupling the request
//!   timeout from the polling interval — tracked separately — would let more slow
//!   answers arrive as answers instead of timing out.
//!
//! The outcome contract — an `Option` budget where `None` waits forever,
//! shutdown outranking the budget, and exiting rather than sitting indefinitely
//! passive while health reports OK — is the cluster-scoped twin of
//! `pplx_space_common_svc::service_mutex`'s node-local one.
//!
//! # Safety model and its limits
//!
//! Mutual exclusion is *cooperative*, like Kubernetes' own client-go
//! leader election: the holder voluntarily stops acting before the lease
//! expires, using a renew deadline (`lease_ttl - RENEW_DEADLINE_MARGIN`) whose
//! margin exceeds the reconciler drain window, so its ticks finish before any
//! standby can acquire. One documented exception, accepted because the write
//! is idempotent: `node_group_mirror`'s bulk UPDATE can still commit after the
//! lease changes hands on a slow connection (see its `WRITE_BACKSTOP`). It
//! does **not** hard-fence: if a holder's process is
//! frozen past the TTL (e.g. `SIGSTOP`, VM suspension, total runtime
//! starvation) a standby will acquire and, when the frozen process resumes, an
//! already-in-flight reconciler tick could run once more before the renewal
//! task is next polled and cancels the reconciler token. True fencing would
//! require a fencing token threaded through every reconciler write; the
//! reconcilers are instead written to be CAS-safe / idempotent, which bounds
//! the damage of such a rare overlap to the same window a crash-and-restart
//! already produces. This matches client-go's documented behavior.

mod store;
#[cfg(test)]
mod tests;

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use pplx_space_api::controllers::substrate::SubstrateCapabilities;
use pplx_space_common_core::types::PositiveDuration;
use tokio::sync::watch;
use tokio::time::{Instant, MissedTickBehavior, interval, sleep_until, timeout};
use tokio_util::sync::CancellationToken;

pub use store::{KubeLeaseStore, LeaseFailure, LeaseStore};

/// The single logical lease every cluster-controller replica contends for.
const LEASE_NAME: &str = "cluster-controller";

const ENABLED_ENV: &str = "SPACE_CLUSTER_CONTROLLER_LEADER_ELECTION_ENABLED";

/// How long a freshly acquired/renewed lease stays valid (the `Lease`'s
/// `leaseDurationSeconds`) — a standby may take it over only this long after the
/// holder's last renewal. Bounds crash-failover time.
const DEFAULT_LEASE_TTL: Duration = Duration::from_secs(60);
/// How often a standby retries acquisition. Bounds the handoff gap after a
/// graceful release.
const DEFAULT_RETRY_INTERVAL: Duration = Duration::from_secs(8);
/// How much restart-fixable lease unreachability a replica may accumulate before
/// it stops treating the failure as transient and exits so Kubernetes restarts
/// it — the liveness budget (see the module's liveness section).
///
/// At the default retry cadence this budget spans hundreds of consecutive
/// restart-fixable failures, allowing transient apiserver unavailability to
/// recover without a restart loop. Both immediate errors and timeouts use
/// the same cadence:
/// `MissedTickBehavior::Delay` leaves the ticker already due when a timed-out
/// call returns, so the next attempt starts immediately.
const DEFAULT_LEASE_FAILURE_GRACE: Duration = Duration::from_secs(600);
/// Test hook (`pplx_space_common_core::env::test_hook`) for the budget
/// [`Timings::derive`] is asked for, in whole seconds; it is bounded and
/// spread like the default. Lets a Kind scenario wait out a budget at its
/// floor instead of this default. Inert unless the `test-hooks` feature is
/// compiled in.
const LEASE_FAILURE_GRACE_TEST_ENV: &str =
    "SPACE_CLUSTER_CONTROLLER_LEADER_ELECTION_LEASE_FAILURE_GRACE_SECS";
/// Test hook (`pplx_space_common_core::env::test_hook`) for the lease TTL
/// [`Timings::derive`] is asked for, in whole seconds; it is clamped into
/// `MIN_LEASE_TTL..MAX_LEASE_TTL` like the default, and the retry interval
/// and the budget's floor follow the resolved TTL. Lets a Kind scenario run
/// the election at the TTL floor, where the budget's floor is 72 s instead of
/// 240 s. Inert unless the `test-hooks` feature is compiled in.
const LEASE_TTL_TEST_ENV: &str = "SPACE_CLUSTER_CONTROLLER_LEADER_ELECTION_LEASE_TTL_SECS";

/// Floor for the liveness budget as a multiple of the resolved lease TTL: a
/// budget near the lease timings would restart the pod on exactly the transients
/// readiness exists to absorb, which is the restart storm this mechanism exists
/// to avoid. An operator-supplied budget therefore gets the same treatment
/// [`MIN_LEASE_TTL`] gives the TTL — floored with a warning — rather than being
/// trusted.
const MIN_GRACE_LEASE_TTL_MULTIPLE: u32 = 4;

/// Floor for the liveness budget as a multiple of the retry interval, because
/// the budget is spent by *sampling* the lease and the TTL says nothing about
/// how often we sample. Without it a wide `retry_interval` decides from too few
/// points: an apiserver that is healthy except at the sampling instants would
/// restart the pod having inferred continuous failure from a handful of them.
///
/// The retry interval is clamped before this applies, which narrows what this
/// floor still has to catch. The floor is this multiple of the **resolved** retry
/// interval: for example, 60s at `2s` and 240s at `8s`. The retry ceiling bounds
/// the worst it can reach — 300s at a 30s TTL, 900s at a 60s TTL — which keeps a
/// wide retry interval from floating the floor out of reach.
/// Counted in intervals throughout, since the attempt count depends on the
/// failure shape.
const MIN_GRACE_RETRY_INTERVALS: u32 = 30;

/// Slack reserved between a winning acquire returning and the renewal loop's
/// first tick, when sizing the retry-interval ceiling.
///
/// The first renew deadline is anchored on the instant *before* the acquire, but
/// the first renewal tick can only happen once `Leadership::spawn` has run and
/// the renewal task has been polled. Without room for that hop an acquire that
/// consumed its whole timeout can return with its deadline already past, and the
/// holder resigns having attempted no renewal at all. Two seconds is far more
/// than the hop costs; the surplus is what makes the tick land *strictly* inside
/// the deadline rather than tying with it.
const FIRST_RENEWAL_HEADROOM: Duration = Duration::from_secs(2);

/// Absolute ceiling on the retry interval, on top of the deadline-room bound in
/// [`Timings::derive`]. What it still buys, now that the deadline-room bound
/// exists: at a 300s TTL that bound alone would allow ~190s, putting the cadence
/// floor near 5700s — over an hour and a half, which most cluster-controller pods
/// would not live to spend, so the exit would be quietly unreachable. 30s holds
/// the floor at 900s and keeps the handoff gap this knob is documented to bound
/// inside a sane range.
const MAX_RETRY_INTERVAL: Duration = Duration::from_secs(30);

/// Ceiling on the budget *before* the replica spread is added, so the resolved
/// value reaches this plus a quarter. Every other bound here raises a too-
/// small value; this is the only one that catches a too-*large* one, and a
/// too-large budget fails silently: the pod is redeployed long before it could
/// ever be spent, so the exit never fires and nothing says so. The realistic
/// trigger is a seconds-for-millis slip on the documented default — `600000`
/// instead of `600` — which is ~6.9 days.
///
/// An hour is comfortably above every floor this module can produce
/// (`MAX_GRACE_FLOOR`, asserted below), so the two can never contradict each
/// other, and comfortably below the lifetime of a cluster-controller pod.
const MAX_LEASE_FAILURE_GRACE: Duration = Duration::from_secs(3600);

/// The largest floor the two rules above can produce, given the clamped TTL and
/// retry interval. Kept as a named constant so the no-contradiction property is
/// checked at compile time rather than argued in prose.
const MAX_GRACE_FLOOR: Duration = {
    let by_ttl = MAX_LEASE_TTL.as_secs() * MIN_GRACE_LEASE_TTL_MULTIPLE as u64;
    let by_cadence = MAX_RETRY_INTERVAL.as_secs() * MIN_GRACE_RETRY_INTERVALS as u64;
    Duration::from_secs(if by_ttl > by_cadence {
        by_ttl
    } else {
        by_cadence
    })
};

// A ceiling at or below the highest reachable floor would make the two rules
// fight, with whichever applies last silently winning — and at *equality* every
// budget resolves to the same value with no room to honour anything an operator
// asked for. Strict, so raising a ceiling into that state fails the build.
const _: () = assert!(
    MAX_GRACE_FLOOR.as_secs() < MAX_LEASE_FAILURE_GRACE.as_secs(),
    "the liveness-budget ceiling must be strictly above every derivable floor"
);
// The bounds above are only meaningful at their intended magnitudes, and every
// assertion about them elsewhere is written in terms of the constants — so a
// drift would be self-consistent and invisible. Pin the absolute values here.
const _: () = assert!(MAX_LEASE_TTL.as_secs() == 300);
// The margin and floor constrain the chart's accepted TTL range. Every
// relative assertion about them is written in terms of the constants, so
// `DRAIN + 2 -> DRAIN + 1` reduces to
// `2 > 0` and passes — verified. Pin them absolutely, and pin the drain window
// they derive from, so a change there fails the build rather than the prose.
const _: () = assert!(crate::reconcilers::SHUTDOWN_DRAIN_TIMEOUT.as_secs() == 10);
const _: () = assert!(RENEW_DEADLINE_MARGIN.as_secs() == 12);
const _: () = assert!(MIN_LEASE_TTL.as_secs() == 18);
// The marker I/O bound must stay under the tightest renewal cadence, which is
// `(MIN_LEASE_TTL - RENEW_DEADLINE_MARGIN) / 3` = 2s.
const _: () = assert!(
    FIRST_RENEWAL_HEADROOM.as_secs()
        < (MIN_LEASE_TTL.as_secs() - RENEW_DEADLINE_MARGIN.as_secs()) / 3 + 1
);
// And at the tightest TTL the retry ceiling must still clear the 1s floor the
// clamp uses, or that clamp would panic with min > max. At MIN_LEASE_TTL the
// deadline is 6s, the renewal cadence 2s and the marker bound 1s, leaving 2s.
const _: () = {
    let deadline = MIN_LEASE_TTL.as_secs() - RENEW_DEADLINE_MARGIN.as_secs();
    let interval = deadline / 3;
    assert!(deadline - interval - FIRST_RENEWAL_HEADROOM.as_secs() >= 1);
};
// The defaults are quoted numerically too ("~300 consecutive failed attempts at
// the 2s cadence", "120s at the defaults"), and every assertion about them is a
// *ratio* among the three, so (30, 2, 600), (60, 6, 600) and (300, 20, 6000) all
// satisfy it. Pin them for the same reason as the ceilings above.
const _: () = assert!(DEFAULT_LEASE_TTL.as_secs() == 60);
const _: () = assert!(DEFAULT_RETRY_INTERVAL.as_secs() == 8);
const _: () = assert!(DEFAULT_LEASE_FAILURE_GRACE.as_secs() == 600);
const _: () = assert!(MAX_RETRY_INTERVAL.as_secs() == 30);
const _: () = assert!(MAX_LEASE_FAILURE_GRACE.as_secs() == 3600);
// The renew-deadline margin must *exceed* the drain window, not merely equal it:
// at equality the old leader's last tick finishes exactly as a standby may
// acquire, with no room for scheduling or clock skew.
const _: () =
    assert!(RENEW_DEADLINE_MARGIN.as_secs() > crate::reconcilers::SHUTDOWN_DRAIN_TIMEOUT.as_secs());
// The TTL clamps must not cross, or `clamp` panics.
const _: () = assert!(MIN_LEASE_TTL.as_secs() <= MAX_LEASE_TTL.as_secs());
// `lease_ttl - RENEW_DEADLINE_MARGIN` would underflow and panic below this.
const _: () = assert!(MIN_LEASE_TTL.as_secs() > RENEW_DEADLINE_MARGIN.as_secs());
// And the smallest renew deadline must leave room for `deadline / 3` to stay
// strictly under it, or `renew_interval < renew_deadline` breaks. Both hold
// because MIN_LEASE_TTL is the margin plus 6; changing that 6 fails the absolute
// pin on MIN_LEASE_TTL below, which is what actually fences it — this guard only
// states the relationship the derivation needs.
const _: () = assert!(MIN_LEASE_TTL.as_secs() - RENEW_DEADLINE_MARGIN.as_secs() > 3);

/// The liveness budget is spread across replicas by up to `1/this` of itself,
/// keyed on replica identity. Replicas lose the lease path at the same instant,
/// so an unspread budget makes every standby give up in the same second, restart
/// in lockstep and back off in lockstep — and if the outage ends inside a shared
/// backoff window there is *no* running replica to acquire the lease. A gap with
/// no reconciler at all is the failure this design line exists to remove, so the
/// liveness exit must not reintroduce it.
///
/// **This reduces the correlation, it does not remove it.** The draw is uniform
/// over the band and taken once per pod name, so with two replicas roughly a
/// fifth of ReplicaSets land within 15s of each other and stay there for the
/// ReplicaSet's life (a kubelet restart keeps the pod name, so the value is
/// never re-drawn). Full decorrelation would need the replicas to coordinate,
/// which is what the lease they can't reach is for. What this does buy: during a
/// sustained outage the pods' restart cycles are usually out of phase, so the
/// window in which *neither* is running shrinks from "always" to "sometimes",
/// and any resulting reconciler gap is bounded by one restart backoff rather
/// than lasting the outage.
///
/// Only ever *adds* time, so every floor applied before it still holds.
const GRACE_SPREAD_DIVISOR: u32 = 4;

/// Upper bound on the graceful lease release during shutdown. Comfortably under
/// Kubernetes' default 30s termination grace period, so a stalled apiserver
/// can't hold the pod open until it's killed mid-release; on timeout the lease
/// simply expires at its TTL.
const RELEASE_TIMEOUT: Duration = Duration::from_secs(5);

/// Safety margin between when the holder **stops acting** (its renew deadline)
/// and when the lease actually **expires** (and a standby may acquire). It must
/// exceed the reconciler drain window
/// ([`crate::reconcilers::SHUTDOWN_DRAIN_TIMEOUT`]) so the old leader's
/// in-flight reconciler ticks finish before any standby starts its own — the
/// no-two-concurrent-runners guarantee, with the one documented exception
/// the module doc names. The extra buffer covers scheduling and clock skew.
/// `renew_deadline = lease_ttl - RENEW_DEADLINE_MARGIN`.
const RENEW_DEADLINE_MARGIN: Duration =
    Duration::from_secs(crate::reconcilers::SHUTDOWN_DRAIN_TIMEOUT.as_secs() + 2);

/// Ceiling for the lease TTL. The TTL bounds crash-failover time, so a value
/// beyond a few minutes is already a misconfiguration — but without a ceiling it
/// is also the one knob that can defeat the liveness budget from above: at
/// a 30000s TTL (a plausible seconds-for-millis slip) the TTL-based grace floor
/// becomes ~33 hours, so the budget is floored to a value it can never reach
/// and the exit is silently dead. Clamping here keeps [`Timings`]'s bounds
/// consistent for any TTL it is given.
const MAX_LEASE_TTL: Duration = Duration::from_secs(300);

/// Floor for the lease TTL: it must leave a renew deadline
/// (`lease_ttl - RENEW_DEADLINE_MARGIN`) big enough for at least a few renewal
/// attempts. Requests below this are bumped up with a warning.
const MIN_LEASE_TTL: Duration = Duration::from_secs(RENEW_DEADLINE_MARGIN.as_secs() + 6);

/// The election's settings, resolved once from the environment, which supplies
/// the kill-switch and the identity (`HOSTNAME`). [`Self::lease_ttl`],
/// [`Self::retry_interval`] and [`Self::lease_failure_grace`] are this module's
/// defaults; the TTL and the budget have test-hook overrides. The renewal cadence and deadline are
/// *derived* from the TTL, and the other two are bounded against it (see
/// [`Timings`]), so the safety relationships hold whatever is requested.
#[derive(Debug, Clone)]
pub struct LeaderElectionConfig {
    /// Stable per-replica identity written into the lease's `holder_id`
    /// (the Kubernetes pod name in production).
    pub identity: String,
    /// Kill-switch. When `false` the pod skips the lease entirely and runs the
    /// reconcilers unconditionally — only safe with a single replica. Exists so
    /// operators can fall back to the old single-replica behavior if the lease
    /// path ever misbehaves.
    pub enabled: bool,
    /// Database lease validity; a standby may acquire this long after the
    /// holder's last renewal.
    pub lease_ttl: Duration,
    /// The holder stops acting (cancels the reconcilers) if it can't confirm a
    /// renewal within this of its last success. Always `lease_ttl` minus a
    /// drain margin, so reconcilers finish draining before the lease expires.
    pub renew_deadline: Duration,
    /// How often the holder renews — derived to fit several attempts inside the
    /// renew deadline.
    pub renew_interval: Duration,
    /// How often a standby retries acquisition.
    pub retry_interval: Duration,
    /// Readiness marker file. Touched once this replica has made contact with
    /// the lease table (won it, or been validly denied by a peer), so a
    /// readiness probe can gate `maxUnavailable: 0`: a replacement that can't
    /// reach the lease never becomes Ready, so a rollout won't tear down a
    /// working pod for one that can't take over. `None` disables the signal
    /// (local/dev with no probe).
    pub readiness_file: Option<PathBuf>,
    /// Liveness budget for the lease path: how much restart-fixable
    /// unreachability this replica may accumulate before it stops treating the
    /// failure as transient and exits non-zero so kubelet restarts it. Answered
    /// failures don't spend the budget; a successful call resets it.
    /// Deliberately far longer than the readiness window — readiness answers
    /// "can this pod take over right now", liveness answers "will this process
    /// ever reach the lease again" (see the module's liveness section). `None`
    /// disables the exit, leaving a wedged replica to retry forever with nothing
    /// restarting it.
    pub lease_failure_grace: Option<PositiveDuration>,
}

impl LeaderElectionConfig {
    /// Whether this replica contends for the leader lease before running the
    /// reconcilers: only while the kill-switch leaves election enabled, and
    /// then when the process runs in a Kubernetes pod, whatever its substrate
    /// (a Direct controller in a multi-replica Deployment needs the lease as
    /// much as a Kubernetes one), or its substrate
    /// [`uses_kubernetes_api`](SubstrateCapabilities::uses_kubernetes_api),
    /// wherever it runs (a kubeconfig reaches the apiserver that in-cluster
    /// replicas contend on).
    pub fn election(&self, host: ProcessHost, capabilities: SubstrateCapabilities) -> Election {
        if !self.enabled {
            return Election::Skip(ElectionSkip::Disabled);
        }
        match host {
            ProcessHost::Kubernetes => Election::Contend,
            ProcessHost::OutsideKubernetes if capabilities.uses_kubernetes_api => Election::Contend,
            ProcessHost::OutsideKubernetes => Election::Skip(ElectionSkip::OutsideKubernetes),
        }
    }

    pub fn from_env() -> Self {
        Self::from_environment(&pplx_space_common_core::env::ProcessEnvironment)
    }

    pub(crate) fn from_environment(env: &dyn pplx_space_common_core::env::Environment) -> Self {
        let config = crate::env_config::EnvConfig(env);
        let identity = election_identity(env);
        let requested_ttl = config.test_duration_secs(LEASE_TTL_TEST_ENV, DEFAULT_LEASE_TTL);
        let requested_grace =
            config.test_duration_secs(LEASE_FAILURE_GRACE_TEST_ENV, DEFAULT_LEASE_FAILURE_GRACE);
        let timings = Timings::derive(
            requested_ttl,
            DEFAULT_RETRY_INTERVAL,
            Some(PositiveDuration::new_panic(requested_grace)),
            &identity,
        );
        Self {
            identity,
            enabled: config.truthy_default(ENABLED_ENV, true),
            lease_ttl: timings.lease_ttl,
            renew_deadline: timings.renew_deadline,
            renew_interval: timings.renew_interval,
            retry_interval: timings.retry_interval,
            readiness_file: Some(PathBuf::from("/tmp/space-cluster-controller-ready")),
            lease_failure_grace: timings.lease_failure_grace,
        }
    }
}

/// Where this process runs, as far as the leader `Lease` is concerned.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProcessHost {
    /// A Kubernetes pod: an apiserver can host the `Lease`, and the Deployment
    /// may run several replicas.
    Kubernetes,
    /// Anywhere else, such as a systemd host or a workstation with a
    /// kubeconfig. Whether such a process contends depends on its substrate;
    /// see [`LeaderElectionConfig::election`].
    OutsideKubernetes,
}

impl ProcessHost {
    pub fn from_env() -> Self {
        Self::from_environment(&pplx_space_common_core::env::ProcessEnvironment)
    }

    /// A pod is recognised by the two service variables the kubelet injects
    /// into every container. Both must be set, the rule the kube client (after
    /// client-go) applies before it builds an in-cluster config. A kubeconfig
    /// does not count: it reaches an apiserver from anywhere, a workstation
    /// included, and says nothing about how many replicas run. (A controller
    /// on a substrate that drives the Kubernetes API contends outside a pod
    /// anyway; [`LeaderElectionConfig::election`] decides that, not this.)
    pub(crate) fn from_environment(env: &dyn pplx_space_common_core::env::Environment) -> Self {
        if env.var_os("KUBERNETES_SERVICE_HOST").is_some()
            && env.var_os("KUBERNETES_SERVICE_PORT").is_some()
        {
            Self::Kubernetes
        } else {
            Self::OutsideKubernetes
        }
    }
}

/// What [`LeaderElectionConfig::election`] decided for this replica.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Election {
    /// Contend for the lease; only the holder runs the reconcilers.
    Contend,
    /// Run the reconcilers without the lease, which is only safe at one
    /// replica.
    Skip(ElectionSkip),
}

/// Why a replica runs the reconcilers without leader election.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ElectionSkip {
    /// The kill-switch turned election off.
    Disabled,
    /// The process does not run in a Kubernetes pod, and its substrate does
    /// not drive the Kubernetes API.
    OutsideKubernetes,
}

impl ElectionSkip {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Disabled => "leader election disabled",
            Self::OutsideKubernetes => "not running in Kubernetes or driving its API",
        }
    }
}

impl std::fmt::Display for ElectionSkip {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// The resolved lease timings. Three are requested — the TTL, the retry
/// interval and the liveness budget — and each is bounded here against the
/// *resolved* values of the ones it interacts with, rather than trusted:
///
/// * `lease_ttl` into `MIN_LEASE_TTL..MAX_LEASE_TTL`;
/// * `retry_interval` into `1s..min(lease_ttl, MAX_RETRY_INTERVAL, renew_deadline
///   - renew_interval)`;
/// * `lease_failure_grace` into `max(4x ttl, 30x retry)..MAX_LEASE_FAILURE_GRACE`.
///
/// The renew deadline and interval are purely derived, giving
/// `renew_interval < renew_deadline < lease_ttl` by construction — the property
/// that keeps two replicas from running reconcilers at once.
///
/// Stated as a list of bounds rather than as "no combination can be unsafe":
/// that stronger claim was made here three times and falsified three times, each
/// by a knob interacting with one it wasn't bounded against. A new knob, or a new
/// use of an existing one, needs its own bound added — not an assumption that
/// this list already covers it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Timings {
    lease_ttl: Duration,
    renew_deadline: Duration,
    renew_interval: Duration,
    retry_interval: Duration,
    lease_failure_grace: Option<PositiveDuration>,
}

impl Timings {
    fn derive(
        requested_ttl: Duration,
        requested_retry: Duration,
        requested_grace: Option<PositiveDuration>,
        identity: &str,
    ) -> Self {
        let lease_ttl = requested_ttl.clamp(MIN_LEASE_TTL, MAX_LEASE_TTL);
        if lease_ttl != requested_ttl {
            tracing::warn!(
                requested_secs = requested_ttl.as_secs_f64(),
                resolved_secs = lease_ttl.as_secs_f64(),
                "leader-election lease TTL outside the safe range; below the floor it leaves no \
                 room for a renew deadline, above the ceiling it leaves a crashed leader \
                 unreplaced and floors the liveness budget out of reach; clamping"
            );
        }
        let renew_deadline = lease_ttl - RENEW_DEADLINE_MARGIN;
        // Aim for ~3 renewal attempts before the deadline; never below 1s.
        let renew_interval = (renew_deadline / 3).max(Duration::from_secs(1));
        // The retry interval is capped three ways, because it is doing three jobs.
        //
        // 1. `MAX_RETRY_INTERVAL`, which holds the cadence-based grace floor to
        //    900s. Bound 3 below already caps a typo'd retry interval well
        //    short of absurd, so this is about the plausible-but-too-wide range
        //    it leaves: at a 300s TTL bound 3 alone permits ~190s, a floor over
        //    an hour and a half, which most pods would not live to spend.
        // 2. The `lease_ttl` term. Strictly redundant — bound 3 is always the
        //    tighter of the two, since `renew_deadline - renew_interval` is
        //    below `lease_ttl` for every TTL — and kept only because the knob's
        //    documented job is to bound the handoff gap after a graceful
        //    release, which is a statement about the TTL. Read bound 3 as the
        //    one that binds.
        // 3. And it is the per-attempt *acquire* timeout, which is the subtle
        //    one: `renew_loop`'s first deadline is anchored on the instant before
        //    that acquire, so an acquire allowed to consume more than
        //    `renew_deadline - renew_interval` can return successfully with its
        //    own deadline already in the past. The holder then resigns before its
        //    first renewal tick, releases the lease, exits non-zero, re-acquires
        //    and repeats — a crash-loop with no reconciler running that the
        //    liveness budget never even sees, because every call *succeeded*.
        //    Reachable at a 30s TTL with a 25s retry interval, which the TTL
        //    cap alone accepts. Leaving room for one renewal attempt inside the
        //    deadline, plus `FIRST_RENEWAL_HEADROOM`, rules it out.
        //
        // Floored at 1s as well: `env_config::duration_secs` already rejects 0,
        // but `interval(Duration::ZERO)` panics, so the precondition belongs next
        // to the use rather than in another module's parser.
        // The room the first renewal needs is the deadline minus the renewal
        // cadence, minus the readiness write that sits between the winning
        // acquire and the renewal loop's first tick, minus a second so the tick
        // lands *strictly* inside the deadline rather than tying with it (a tie
        // is a coin flip in `renew_loop`'s `select!`). Without the marker term
        // the budget is short by the spawn hop, which at a 30s TTL is enough to
        // resign with zero renewal attempts.
        let first_renewal_room = renew_deadline
            .saturating_sub(renew_interval)
            .saturating_sub(FIRST_RENEWAL_HEADROOM);
        let retry_ceiling = lease_ttl.min(MAX_RETRY_INTERVAL).min(first_renewal_room);
        // `clamp` can't panic: at the smallest TTL (18s) the deadline is 6s, the
        // renew interval 2s and the marker bound 1s, leaving a 2s ceiling — at
        // the 1s floor or above, asserted at the constants.
        let retry_interval = requested_retry.clamp(Duration::from_secs(1), retry_ceiling);
        if retry_interval != requested_retry {
            tracing::warn!(
                requested_secs = requested_retry.as_secs_f64(),
                resolved_secs = retry_interval.as_secs_f64(),
                "leader-election retry interval too large: a standby would sample less often \
                 than the lease expires, and it would push the liveness-budget floor out of \
                 reach; clamping"
            );
        }
        // The liveness budget is floored against the *resolved* TTL and the
        // *resolved* retry cadence, so both relationships hold however those
        // were configured. `None` (the kill-switch) stays `None` — a disabled
        // budget has no floor to clear, and computing the floor inside the `map`
        // keeps a nonsense TTL from mattering at all when the budget is off.
        // `saturating_mul` is belt-and-braces: both operands are clamped above,
        // so neither product can overflow today. It stays so a future change to
        // either ceiling can't turn this into a panic.
        let lease_failure_grace = requested_grace.map(|requested| {
            let grace_floor = lease_ttl
                .saturating_mul(MIN_GRACE_LEASE_TTL_MULTIPLE)
                .max(retry_interval.saturating_mul(MIN_GRACE_RETRY_INTERVALS));
            // `MAX_GRACE_FLOOR <= MAX_LEASE_FAILURE_GRACE` is asserted at compile
            // time, so the floor and the ceiling can never fight over a value.
            let bounded = requested
                .get()
                .clamp(grace_floor, MAX_LEASE_FAILURE_GRACE.max(grace_floor));
            if bounded != requested.get() {
                tracing::warn!(
                    requested_secs = requested.get().as_secs_f64(),
                    resolved_secs = bounded.as_secs_f64(),
                    floor_secs = grace_floor.as_secs_f64(),
                    ceiling_secs = MAX_LEASE_FAILURE_GRACE.as_secs_f64(),
                    "leader-election liveness budget outside the usable range; too low it \
                     restarts on transients readiness absorbs, too high it is never reached \
                     before the pod is redeployed and the exit is silently dead; clamping"
                );
            }
            spread_across_replicas(PositiveDuration::new_panic(bounded), identity)
        });
        Self {
            lease_ttl,
            renew_deadline,
            renew_interval,
            retry_interval,
            lease_failure_grace,
        }
    }
}

/// Add up to `1/`[`GRACE_SPREAD_DIVISOR`] of the budget to itself, keyed on this
/// replica's identity, so replicas that started failing together don't give up
/// together.
///
/// Deterministic per identity rather than random: the value must be stable
/// across the process's life so the log line above describes what actually
/// happens, and it needs to *differ* between pods, which the pod name already
/// does. Only ever adds, so every floor the caller applied still holds.
fn spread_across_replicas(grace: PositiveDuration, identity: &str) -> PositiveDuration {
    // FNV-1a, spelled out rather than using `DefaultHasher`, whose output is
    // explicitly not stable across releases — the spread would then change under
    // the pod on a toolchain bump.
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in identity.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    // Divide the `Duration` directly rather than in a `u64` millisecond domain:
    // still truncating (nanos, then millis), but by sub-millisecond amounts
    // instead of the ~100ms a `/100 * percent` form loses.
    // The zero guard is load-bearing rather than defensive — `% 0` panics — even
    // though the floors keep a real budget far above it.
    // `unwrap_or(0)`, not `u64::MAX`: if a future clamp change ever let the span
    // exceed `u64` milliseconds, falling back to "no spread" degrades to the
    // pre-spread behavior, whereas `u64::MAX` would add ~584 million years and put
    // the budget permanently out of reach — the failure the ceilings exist to
    // prevent.
    let span_ms = u64::try_from((grace.get() / GRACE_SPREAD_DIVISOR).as_millis()).unwrap_or(0);
    // Zero span means no spread. Unreachable while the floors keep a real budget
    // at 72s or more; kept because `% 0` panics.
    let added = if span_ms == 0 {
        Duration::ZERO
    } else {
        Duration::from_millis(hash % span_ms)
    };
    PositiveDuration::new_panic(grace.get().saturating_add(added))
}

/// Resolve the election identity from the pod's `HOSTNAME` (Kubernetes sets it
/// to the pod name), else a fresh UUID so two
/// replicas can never accidentally share an identity and both believe they
/// hold the lease.
fn election_identity(env: &dyn pplx_space_common_core::env::Environment) -> String {
    if let Ok(value) = env.var("HOSTNAME") {
        let trimmed = value.trim();
        if !trimmed.is_empty() {
            return trimmed.to_string();
        }
    }
    uuid::Uuid::now_v7().to_string()
}

/// Outcome of [`campaign`].
pub enum Campaign<S: LeaseStore + Send + Sync + 'static> {
    /// This replica won the lease. Hold the returned guard for the lifetime of
    /// the reconciler set, then [`Leadership::resign`] it.
    Elected(Leadership<S>),
    /// Shutdown was requested before this replica ever won — a standby that
    /// never got its turn. The process should exit cleanly.
    ShutdownBeforeElected,
    /// Every lease call has failed for the whole
    /// [`LeaderElectionConfig::lease_failure_grace`]. The caller must exit
    /// **non-zero** so kubelet restarts the container — the liveness signal
    /// described in the module's liveness section. Do not treat this as a clean
    /// shutdown: the point is the restart.
    LeaseUnreachable,
}

/// What the process must do with a [`Campaign`] outcome.
///
/// Exists so the decision is made once, in a tested function, rather than in
/// `main`'s match arms where nothing observes it: routing
/// [`Campaign::LeaseUnreachable`] to a clean exit — or worse, onto the
/// run-unconditionally path — type-checks and reads plausibly.
pub enum CampaignAction<S: LeaseStore + Send + Sync + 'static> {
    /// Hold the lease; run the reconcilers under this guard.
    Lead(Leadership<S>),
    /// Shutdown was requested before this replica won. Exit zero.
    ExitZero,
    /// The lease is unreachable and retrying in-process won't fix it. Exit
    /// **non-zero** so kubelet restarts the container with a fresh client.
    ExitNonZero,
}

impl<S: LeaseStore + Send + Sync + 'static> Campaign<S> {
    /// Resolve this outcome into the process's next move. Exhaustive, so a new
    /// `Campaign` variant forces a decision here rather than at the call site.
    pub fn resolve(self) -> CampaignAction<S> {
        match self {
            Self::Elected(leadership) => CampaignAction::Lead(leadership),
            Self::ShutdownBeforeElected => CampaignAction::ExitZero,
            Self::LeaseUnreachable => CampaignAction::ExitNonZero,
        }
    }
}

/// Who is authorised to run the cluster-singleton reconcilers.
///
/// This exists instead of an `Option<Leadership>` because `Option` conflates the
/// two reasons a replica might hold no lease: "election is switched off here, so
/// run unconditionally" and "election ran and did not give us the lease". Under
/// `Option`, mapping [`Campaign::LeaseUnreachable`] to `None` type-checks and
/// reads plausibly, and the result is a replica running the full reconciler set
/// *without* the lease, concurrently with the real leader — double-provisioning
/// the warm pool and double-driving every CAS loop. Naming the disabled case
/// makes that mistake something you have to write on purpose.
pub enum ReconcilerAuthority<S: LeaseStore + Send + Sync + 'static> {
    /// This replica holds the lease. Drive the reconcilers off
    /// [`Leadership::token`] and resign when they stop.
    Leader(Leadership<S>),
    /// Election does not apply here — the kill-switch, or a Direct process
    /// outside a pod — so the reconcilers run off the process shutdown token.
    /// Only safe at one replica, which is what the chart enforces for this case.
    ElectionDisabled,
}

impl<S: LeaseStore + Send + Sync + 'static> ReconcilerAuthority<S> {
    /// The token the reconcilers should run under: the leadership token when we
    /// hold the lease (cancelled on lease loss *and* process shutdown), else the
    /// process shutdown token.
    pub fn reconciler_token(&self, shutdown: &CancellationToken) -> CancellationToken {
        match self {
            Self::Leader(leadership) => leadership.token(),
            Self::ElectionDisabled => shutdown.clone(),
        }
    }

    /// The leadership guard, if any, to resign or abandon once the reconcilers
    /// have stopped.
    pub fn into_leadership(self) -> Option<Leadership<S>> {
        match self {
            Self::Leader(leadership) => Some(leadership),
            Self::ElectionDisabled => None,
        }
    }
}

/// Contend for leadership, returning once this replica has won (or shutdown
/// fires first). While waiting as a standby, retries acquisition every
/// [`LeaderElectionConfig::retry_interval`]. Call only when
/// [`LeaderElectionConfig::election`] returns [`Election::Contend`]; `main`
/// decides.
pub async fn campaign<S>(
    store: Arc<S>,
    config: LeaderElectionConfig,
    shutdown: CancellationToken,
) -> Campaign<S>
where
    S: LeaseStore + Send + Sync + 'static,
{
    let mut ticker = interval(config.retry_interval);
    // Retry on a fixed cadence even if a slow acquisition query overran a tick;
    // never fire a burst of catch-up attempts.
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    // The liveness budget, or `None` when it's kill-switched off — in which case
    // the loop below simply never has one to charge.
    let marker = ReadinessMarker::spawn(config.readiness_file.clone());
    let mut budget = config.lease_failure_grace.map(FailureBudget::new);
    // The resolved timings, once, before the first attempt. Every one of these
    // can differ from what the Deployment sets — clamped into a usable range, and
    // the budget additionally carries a per-identity spread — so the rendered
    // pod spec is not a reliable answer to "what is this replica actually doing".
    // This line is.
    tracing::info!(
        identity = %config.identity,
        lease_ttl_secs = config.lease_ttl.as_secs_f64(),
        renew_deadline_secs = config.renew_deadline.as_secs_f64(),
        renew_interval_secs = config.renew_interval.as_secs_f64(),
        retry_interval_secs = config.retry_interval.as_secs_f64(),
        lease_failure_grace_secs = config
            .lease_failure_grace
            .map_or(0.0, |grace| grace.get().as_secs_f64()),
        "contending for cluster-controller leadership with resolved lease timings"
    );
    // The failure category of the previous failed attempt, cleared by any
    // successful one. Only used to decide whether a failure is *new*, so an
    // on-sight error log fires once per run instead of once per retry. Kept
    // outside the budget because the budget can be switched off and an
    // unfixable failure still has to be reported.
    let mut last_failure: Option<LeaseFailure> = None;
    loop {
        tokio::select! {
            // Biased so shutdown is checked before the tick, rather than the two
            // racing when both are ready. Unbiased, a token cancelled before the
            // first poll still had a coin-flip chance of running one more lease
            // call — harmless in itself, but it made the contract emergent from
            // tokio's timer internals instead of stated here.
            biased;
            _ = shutdown.cancelled() => return Campaign::ShutdownBeforeElected,
            // `interval`'s first tick is immediate, so the first acquisition
            // attempt happens without waiting a retry interval.
            _ = ticker.tick() => {
                // Anchor the renewal deadline on the instant *before* the call:
                // the lease's `renewTime` is stamped during the call, at or
                // after this instant, so a deadline of `issued_at +
                // renew_deadline` is always conservative vs. the real expiry —
                // even if the apiserver response (or readiness write below) is
                // slow.
                let issued_at = Instant::now();
                // Bound the call so a half-open connection can't wedge a
                // standby's acquisition loop indefinitely.
                let attempt = timeout(
                    config.retry_interval,
                    store.acquire_or_renew(LEASE_NAME, &config.identity, config.lease_ttl),
                )
                .await;
                // Every failure shape collapses to one `LeaseFailure`, and the
                // single tail below dispatches on it. One tail, because two is
                // how the readiness clear, the counter and the liveness charge
                // end up applied to one shape and not the other.
                let outcome: Option<LeaseFailure> = match attempt {
                    Ok(Ok(true)) => {
                        leadership_change_span(&config.identity, "acquired", "lease acquired");
                        tracing::info!(
                            identity = %config.identity,
                            "won cluster-controller leadership lease"
                        );
                        marker.set(true);
                        pplx_space_metrics::leader_election::acquired(&config.identity);
                        pplx_space_metrics::leader_election::is_leader(&config.identity, true);
                        return Campaign::Elected(Leadership::spawn(
                            store,
                            config,
                            issued_at,
                            marker.clone(),
                            shutdown,
                        ));
                    }
                    Ok(Ok(false)) => {
                        // A peer holds a valid lease; keep standing by. Reaching
                        // the lease path proves we can participate, so we're a
                        // healthy standby — signal readiness.
                        marker.set(true);
                        pplx_space_metrics::leader_election::is_leader(&config.identity, false);
                        None
                    }
                    Ok(Err(err)) => Some(err.classify()),
                    // The call outran its own timeout, so the apiserver never
                    // answered — indistinguishable from a dead connection, and
                    // restart-fixable for the same reason.
                    Err(_elapsed) => Some(LeaseFailure::Unreachable),
                };
                let Some(failure) = outcome else {
                    // Answered, so this replica can participate: the failure run
                    // (and anything derived from it) starts over.
                    last_failure = None;
                    if let Some(budget) = budget.as_mut() {
                        budget.reset();
                    }
                    continue;
                };
                marker.set(false);
                pplx_space_metrics::leader_election::acquire_failed(failure.as_str());
                // Still campaigning, so certainly not the leader: say so, or a
                // contender refused from its first attempt never has a series.
                pplx_space_metrics::leader_election::is_leader(&config.identity, false);
                // A failure only a human can clear is reported the first time
                // it's seen, not after the grace window: for the categories that
                // don't spend the budget there is no later escalation, and
                // spending ten minutes of silence on a revoked RBAC binding is
                // exactly what this classification exists to stop. Repeats
                // within the same run stay at `warn` so a long outage doesn't
                // emit an error every retry interval.
                if failure.needs_operator_action() && last_failure != Some(failure) {
                    // `restart_may_help` rather than a claim in the message,
                    // because the two categories that reach here disagree about
                    // it: a refusal is not restart-fixable, an unusable clock may
                    // be (it can have been stepped since this process started).
                    tracing::error!(
                        identity = %config.identity,
                        reason = failure.as_str(),
                        restart_may_help = failure.charges_liveness_budget(),
                        "leader-election lease failure needs an operator; reported on sight \
                         rather than after the liveness budget, and this replica stays \
                         NotReady until the cause is fixed"
                    );
                } else {
                    // The per-attempt bound, named for what it is: it applies to
                    // every failure here, not only to the ones that timed out.
                    tracing::warn!(
                        reason = failure.as_str(),
                        attempt_timeout_secs = config.retry_interval.as_secs_f64(),
                        "leader-election acquisition failed; retrying"
                    );
                }
                last_failure = Some(failure);
                // Shutdown requested while that call was in flight: this pod is
                // going away, so a restart buys nothing and the budget has
                // nothing left to protect. Short-circuits what the next
                // `select!` poll would decide anyway, and keeps a normal
                // termination that happens to land at the end of an outage from
                // reporting a failure exit.
                if shutdown.is_cancelled() {
                    return Campaign::ShutdownBeforeElected;
                }
                // An answered failure proves this replica reached the lease, so
                // it ends the active chargeable segment. Already accrued
                // restart-fixable time remains: the answer excludes the
                // brownout/refusal interval without crediting back an earlier
                // client wedge.
                if !failure.charges_liveness_budget() {
                    if let Some(budget) = budget.as_mut() {
                        budget.interrupt();
                    }
                    continue;
                }
                if let Some(budget) = budget.as_mut()
                    && let Some(failing_for) = budget.charge(issued_at)
                {
                    tracing::error!(
                        identity = %config.identity,
                        reason = failure.as_str(),
                        failing_for_secs = failing_for.as_secs_f64(),
                        grace_secs = budget.grace.as_secs_f64(),
                        "cluster-controller has been unable to reach the leadership lease for \
                         its whole liveness budget; exiting non-zero so Kubernetes restarts \
                         this replica"
                    );
                    pplx_space_metrics::leader_election::lease_unreachable(
                        &config.identity,
                        failure.as_str(),
                    );
                    return Campaign::LeaseUnreachable;
                }
            }
        }
    }
}

/// The liveness budget for the lease path: how long this replica may accumulate
/// restart-fixable unreachability before it stops treating the failure as
/// transient (see the module's liveness section). Answered failures break the
/// active interval, so their time is excluded even when chargeable failures
/// occur on both sides.
///
/// Constructed only when a budget is configured, so the loop holding one never
/// sees the disabled case — `None` at the call site *is* the kill-switch, and
/// no layer below needs to re-check it. Owning both halves of the clock in one
/// type is what keeps the reset from being forgotten by a future success-shaped
/// branch, the way the two failure branches once each carried their own copy of
/// the readiness clear.
#[derive(Debug)]
struct FailureBudget {
    grace: Duration,
    /// Time already accumulated against the budget by earlier chargeable
    /// intervals in this run. Reset only by a successful lease call.
    charged: Duration,
    /// Attempt instant of the most recent chargeable failure, or `None` when the
    /// next chargeable failure should start a fresh segment — either because the
    /// last call succeeded ([`Self::reset`]) or because it failed in a way that
    /// must not be counted ([`Self::interrupt`]).
    segment_started: Option<Instant>,
}

impl FailureBudget {
    fn new(grace: PositiveDuration) -> Self {
        Self {
            grace: grace.get(),
            charged: Duration::ZERO,
            segment_started: None,
        }
    }

    /// End the current run of failures. Every successful lease call does this,
    /// which is what makes the budget measure *consecutive* unreachability — a
    /// single success inside a brownout returns the full budget, so isolated
    /// failures cannot accumulate into a restart.
    fn reset(&mut self) {
        self.charged = Duration::ZERO;
        self.segment_started = None;
    }

    /// An answered failure that must not be counted. Keeps whatever is already
    /// charged, but breaks the segment so the *time spent on this failure* is not
    /// billed to the budget by the next chargeable one.
    ///
    /// This is the whole reason the budget accumulates segments rather than
    /// holding one wall-clock anchor. With a single anchor, one timeout followed
    /// by a grace window of answered 429s followed by one more timeout exits
    /// immediately: the elapsed span covers the brownout, so overload time is
    /// retroactively billed as unreachability and the replica restarts — adding a
    /// re-LIST of every Node to precisely the brownout the classification exists
    /// to avoid escalating.
    fn interrupt(&mut self) {
        self.segment_started = None;
    }

    /// Charge a failed call against the budget, returning the accumulated
    /// chargeable time once it reaches the whole grace, or `None` while it is
    /// still inside it. Reporting stays at the call site; this only decides.
    ///
    /// What accumulates is the span between *adjacent* chargeable attempts, so
    /// the total answers exactly one question: how long has this replica been
    /// unable to reach the lease for reasons a restart could fix. Time spent
    /// getting answers — 429, 403 — is time it *did* reach the lease and is
    /// excluded, because [`Self::interrupt`] drops the anchor and the next charge
    /// therefore contributes nothing for its own step.
    ///
    /// `attempted_at` is the instant captured *before* the failing call, and the
    /// anchor is one too, so each segment runs from one failing attempt's start
    /// to the next's. Anchoring both ends pre-call keeps the total from being
    /// inflated by the failing calls' own latency — a timed-out call burns a
    /// whole `retry_interval` on its own.
    ///
    /// A resumed segment loses one retry interval (the step that re-anchors bills
    /// nothing), so the total under-counts slightly per interruption. That is the
    /// safe direction: it delays a restart rather than hastening one.
    fn charge(&mut self, attempted_at: Instant) -> Option<Duration> {
        if let Some(segment_started) = self.segment_started {
            self.charged = self
                .charged
                .saturating_add(attempted_at.saturating_duration_since(segment_started));
        }
        self.segment_started = Some(attempted_at);
        (self.charged >= self.grace).then_some(self.charged)
    }
}

/// Owns the readiness marker file.
///
/// The lease loops call [`Self::set`], which is synchronous and cannot fail: it
/// publishes the *desired* state and returns. A single background task performs
/// the filesystem work, awaiting each operation to completion before looking at
/// the next desired state.
///
/// Both halves of that matter, and both were learned the hard way.
///
/// * **Nothing is ever dropped mid-operation.** `tokio::fs` runs on the blocking
///   pool, so a future abandoned by a timeout can still complete afterwards. An
///   earlier design bounded the write with a timeout on the lease loop's own
///   stack, which permitted: a Ready write times out, a NotReady removal then
///   succeeds, and the abandoned write lands last — leaving a replica that cannot
///   reach the lease advertising itself Ready, which is precisely what
///   `maxUnavailable: 0` relies on not happening. One task with sequential awaits
///   makes that ordering unconstructible rather than unlikely.
/// * **The lease loops never await the filesystem.** `renew_loop` updates
///   readiness *after* the `select!` that races the renewal against
///   `sleep_until(deadline)`, so any await there could hold the leadership token
///   live past lease expiry while a standby starts its own reconcilers. Now there
///   is nothing to await. A wedged filesystem freezes the marker at its last
///   value and the lease loops carry on unaffected — the marker is stale, which a
///   probe treats conservatively, instead of mutual exclusion being at risk.
///
/// Cheap to clone; every clone publishes to the same task. `None` (no configured
/// path) makes every call a no-op, for local runs with no probe.
#[derive(Clone)]
pub struct ReadinessMarker(Option<Arc<watch::Sender<Option<bool>>>>);

impl ReadinessMarker {
    /// Start the writer task. A `None` path disables the signal entirely.
    pub fn spawn(path: Option<PathBuf>) -> Self {
        let Some(path) = path else {
            return Self(None);
        };
        // `None` is "no lease call has reported yet", so the task writes nothing
        // until the first real outcome — a pod is not Ready merely by starting.
        let (tx, rx) = watch::channel(None);
        tokio::spawn(marker_loop(rx, move |healthy| {
            let path = path.clone();
            async move { reconcile_marker(&path, healthy).await }
        }));
        Self(Some(Arc::new(tx)))
    }

    /// Publish what the latest lease call proved. Never blocks, never fails.
    pub fn set(&self, healthy: bool) {
        if let Some(tx) = &self.0 {
            tx.send_replace(Some(healthy));
        }
    }
}

/// Apply desired readiness states in order, coalescing any that arrive while an
/// operation is in flight so only the newest is applied.
///
/// Generic over the reconciler so the ordering guarantee can be tested with a
/// deliberately slow fake — the real filesystem cannot be made slow in a test
/// without hanging the runtime at drop.
pub(crate) async fn marker_loop<F, Fut>(mut rx: watch::Receiver<Option<bool>>, reconcile: F)
where
    F: Fn(bool) -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    loop {
        // `Err` means every sender is gone, i.e. the process is going away.
        if rx.changed().await.is_err() {
            return;
        }
        // Read *after* waiting, so a state superseded while the previous
        // operation was running is never applied.
        let Some(desired) = *rx.borrow_and_update() else {
            continue;
        };
        reconcile(desired).await;
    }
}

/// Make the marker file agree with what the last lease call proved: present after
/// a successful call, absent after a failed one. A readiness probe on the file
/// therefore follows *recent* lease contact rather than a one-time event.
///
/// Reads the filesystem rather than tracking what it believes the filesystem
/// holds, so an out-of-band removal is repaired on the next call.
///
/// Best-effort: a failed write leaves the pod NotReady, a failed removal leaves
/// it Ready, and either is retried on the next lease call.
async fn reconcile_marker(path: &Path, healthy: bool) {
    if healthy {
        // Skip the write when the file is already there — the common case, so a
        // steady-state standby costs one `stat` per lease call. `unwrap_or(false)`
        // so a stat error retries the write rather than assuming the marker is
        // fine. Not covered by a test: inducing a stat error needs an unreadable
        // path, and the probe is `test -f` against the same file, so a permission
        // fault there already fails readiness regardless of what this chooses.
        if tokio::fs::try_exists(path).await.unwrap_or(false) {
            return;
        }
        match tokio::fs::write(path, b"ready\n").await {
            Ok(()) => tracing::info!(
                path = %path.display(),
                "cluster-controller readiness marker written"
            ),
            Err(err) => tracing::warn!(
                error = %err,
                path = %path.display(),
                "failed to write readiness marker; will retry"
            ),
        }
        return;
    }
    match tokio::fs::remove_file(path).await {
        Ok(()) => tracing::warn!(
            path = %path.display(),
            "cleared readiness marker after a failed lease call"
        ),
        // Already absent, which is what a failed call wants.
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
        Err(err) => tracing::warn!(
            error = %err,
            path = %path.display(),
            "failed to clear readiness marker; will retry"
        ),
    }
}

/// Signal readiness directly, for the non-election path (`main` when it does
/// not contend: see [`LeaderElectionConfig::election`]): the process runs the
/// reconcilers unconditionally, so it is ready immediately.
///
/// The returned handle must be **bound**, not discarded: dropping the last one
/// stops the writer task. Bind it for the life of the reconcilers.
pub fn signal_ready(config: &LeaderElectionConfig) -> ReadinessMarker {
    let marker = ReadinessMarker::spawn(config.readiness_file.clone());
    marker.set(true);
    marker
}

/// RAII guard proving this replica holds leadership. Hand
/// [`token`](Self::token) to the reconciler set and call
/// [`resign`](Self::resign) (clean drain) or [`abandon`](Self::abandon)
/// (unclean) once it has stopped.
pub struct Leadership<S: LeaseStore + Send + Sync + 'static> {
    store: Arc<S>,
    identity: String,
    /// Cancelled when the lease is lost *or* the process shuts down (a child of
    /// the process shutdown token). Drives the reconciler set and stops the
    /// renewal task.
    leader_token: CancellationToken,
    /// `Option` only so [`Drop`] can exist: taking the handle in `resign`/
    /// `abandon` is what lets those consume `self` while the guard still has a
    /// destructor. `None` means it has already been joined.
    renew_task: Option<tokio::task::JoinHandle<()>>,
}

/// Fail closed if a `Leadership` is dropped without being given up explicitly.
///
/// The type is documented as an RAII guard but the interesting work happens in
/// [`Leadership::resign`] / [`Leadership::abandon`], which consume `self`. Drop
/// the guard without calling either and the detached renewal task keeps renewing:
/// the lease stays held by a process that is running no reconcilers, every
/// standby gets a valid denial, reports Ready and resets its liveness budget — so
/// there is no reconciler anywhere and nothing observes it, because every call
/// succeeds. `main` is correct today, but `result?` sits one line below the
/// `relinquish` call, and `into_leadership()` hands out an `Option` that a `let
/// _ =` would swallow. Cancelling the token here makes the renewal stop and the
/// lease expire at its TTL instead.
impl<S: LeaseStore + Send + Sync + 'static> Drop for Leadership<S> {
    fn drop(&mut self) {
        // A no-op after `resign`/`abandon`, which cancel it first.
        self.leader_token.cancel();
    }
}

impl<S: LeaseStore + Send + Sync + 'static> Leadership<S> {
    /// `acquire_issued_at` is the instant captured just before the winning
    /// acquire call, used to anchor the first renewal deadline (see
    /// [`renew_loop`]).
    fn spawn(
        store: Arc<S>,
        config: LeaderElectionConfig,
        acquire_issued_at: Instant,
        marker: ReadinessMarker,
        shutdown: CancellationToken,
    ) -> Self {
        let leader_token = shutdown.child_token();
        let renew_task = tokio::spawn(renew_loop(
            store.clone(),
            config.clone(),
            acquire_issued_at,
            marker,
            leader_token.clone(),
        ));
        Self {
            store,
            identity: config.identity,
            leader_token,
            renew_task: Some(renew_task),
        }
    }

    /// Token to drive the reconciler set. Cancelled on lease loss or shutdown.
    pub fn token(&self) -> CancellationToken {
        self.leader_token.clone()
    }

    /// Give up the lease the way the reconcilers' outcome requires: release it
    /// when they drained cleanly, abandon it otherwise.
    ///
    /// The choice is here rather than at the call site because getting it
    /// backwards is silent and severe. [`Self::resign`] makes the lease
    /// immediately acquirable, so taking it after a *timed-out* drain lets a
    /// standby start its reconciler set while a wedged tick of ours may still be
    /// detached and running — the two-concurrent-runners failure this module
    /// exists to prevent. `main` has no tests, so the decision lives where one
    /// can reach it.
    pub async fn relinquish(self, drained_cleanly: bool) {
        if drained_cleanly {
            self.resign().await;
        } else {
            self.abandon().await;
        }
    }

    /// Stop renewing and **release** the lease so a standby can take over
    /// promptly. Call this only after the reconciler set has *fully* drained
    /// (a clean shutdown), because release makes the lease immediately
    /// acquirable — if any reconciler tick were still running we'd let a
    /// standby overlap it.
    pub async fn resign(mut self) {
        // Cancelling the leader token stops the renewal task; the reconcilers it
        // also drives have already drained by the time resign is called.
        self.leader_token.cancel();
        if let Some(renew_task) = self.renew_task.take() {
            let _ = renew_task.await;
        }
        // Bound the release so a stalled apiserver can't hold shutdown past the
        // pod's termination grace period; if it times out we just let the lease
        // expire at its TTL (same as `abandon`).
        match timeout(
            RELEASE_TIMEOUT,
            self.store.release(LEASE_NAME, &self.identity),
        )
        .await
        {
            Ok(Ok(())) => {
                tracing::info!(identity = %self.identity, "released cluster-controller leadership lease")
            }
            Ok(Err(err)) => tracing::warn!(
                error = %err,
                "failed to release leadership lease on resign; a standby will take over at TTL"
            ),
            Err(_elapsed) => tracing::warn!(
                timeout_secs = RELEASE_TIMEOUT.as_secs(),
                "releasing leadership lease timed out; a standby will take over at TTL"
            ),
        }
        pplx_space_metrics::leader_election::is_leader(&self.identity, false);
    }

    /// Stop renewing but do **not** release the lease. Use this when the
    /// reconcilers could *not* be confirmed fully stopped (drain timed out on a
    /// wedged tick, or a reconciler panicked): leaving the lease to expire at
    /// its TTL means a standby only takes over after this process has exited and
    /// any detached/wedged task is dead, so no standby overlaps a still-running
    /// tick.
    pub async fn abandon(mut self) {
        self.leader_token.cancel();
        if let Some(renew_task) = self.renew_task.take() {
            let _ = renew_task.await;
        }
        tracing::warn!(
            identity = %self.identity,
            "abandoning cluster-controller leadership lease without release; \
             it will expire at its TTL after this process exits"
        );
        pplx_space_metrics::leader_election::is_leader(&self.identity, false);
    }
}

/// Renew the lease on an interval for as long as we hold it. Cancels
/// `leader_token` (stopping the reconcilers) the moment we can no longer prove
/// we hold the lease — a peer took it (`Ok(false)`) or we haven't confirmed a
/// renewal within the **renew deadline** — so the reconcilers stop with a
/// safety margin before the lease actually expires.
///
/// **The renew deadline is `lease_ttl - RENEW_DEADLINE_MARGIN`, not the full
/// TTL.** That margin (larger than the reconciler drain window) is what
/// guarantees no two replicas run reconcilers at once: when this loop hits the
/// deadline and cancels `leader_token`, the reconcilers drain for up to
/// `SHUTDOWN_DRAIN_TIMEOUT`, and the margin ensures they finish *before* the
/// lease expires and any standby can acquire it.
///
/// Two layers stop us on time even if a renewal call misbehaves:
/// * The outer `sleep_until(deadline)` fires if the deadline passes between
///   ticks.
/// * The renewal call is itself *raced* against `sleep_until(deadline)` inside
///   the tick arm, so a hung call (half-open connection, stalled apiserver)
///   can't outlive the deadline — the fixed per-call timeout it replaces could
///   overshoot when the interval didn't evenly divide the TTL.
///
/// The deadline is anchored on the instant *before* each call (`acquire_issued_at`
/// for the first, `issued_at` after): the lease's `renewTime` is stamped during
/// the call, at or after our pre-call instant, so resigning at
/// `pre_call_instant + renew_deadline` is always conservative vs. the real
/// expiry.
async fn renew_loop<S>(
    store: Arc<S>,
    config: LeaderElectionConfig,
    acquire_issued_at: Instant,
    marker: ReadinessMarker,
    leader_token: CancellationToken,
) where
    S: LeaseStore + Send + Sync + 'static,
{
    let mut ticker = interval(config.renew_interval);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    // Consume the immediate first tick — we just acquired, so the first real
    // renewal happens one interval from now.
    ticker.tick().await;
    // We must stop acting by this instant unless a renewal confirms first.
    // Anchored on the pre-acquire instant from `campaign`.
    let mut deadline = acquire_issued_at + config.renew_deadline;
    // Readiness is reconciled on every renewal outcome, not carried over from
    // `campaign`: written on a confirmed renewal, cleared on a failed one, so a
    // leader that loses apiserver contact goes NotReady before it resigns.
    loop {
        tokio::select! {
            // Biased: stop-acting outranks acting. At a deadline/tick tie the
            // unbiased form resigns only half the time, and resigning is the safe
            // side — the whole point of the deadline is to stop before the lease
            // can expire.
            biased;
            // Cancelled by the process shutting down or by `resign` — stop.
            _ = leader_token.cancelled() => return,
            // Deadline reached between ticks without a confirmed renewal.
            _ = sleep_until(deadline) => {
                resign_lost(&config.identity, &marker, &leader_token, "renew deadline passed");
                return;
            }
            _ = ticker.tick() => {
                let issued_at = Instant::now();
                // Race the query against the deadline (and cancellation) so a
                // hung query can't keep us acting past the deadline.
                let renewal = tokio::select! {
                    biased;
                    _ = leader_token.cancelled() => return,
                    _ = sleep_until(deadline) => {
                        resign_lost(
                            &config.identity,
                            &marker,
                            &leader_token,
                            "renew deadline passed mid-query",
                        );
                        return;
                    }
                    r = store.acquire_or_renew(LEASE_NAME, &config.identity, config.lease_ttl) => r,
                };
                match renewal {
                    Ok(true) => {
                        // Confirmed: extend the deadline from the pre-call
                        // instant (conservative vs. the lease's `renewTime`).
                        deadline = issued_at + config.renew_deadline;
                        marker.set(true);
                        pplx_space_metrics::leader_election::is_leader(&config.identity, true);
                    }
                    Ok(false) => {
                        resign_lost(
                            &config.identity,
                            &marker,
                            &leader_token,
                            "lease held by another replica",
                        );
                        return;
                    }
                    // Transient failure: don't move the deadline — the next
                    // ticks retry, and the deadline resigns us if none confirm.
                    // The category is recorded but not acted on: unlike a
                    // standby, a holder that can't confirm its lease must stop
                    // acting no matter why, so every category shares the one
                    // behaviour here — retry until the deadline, then resign.
                    Err(err) => {
                        // Classified once: two calls could disagree after an edit,
                        // and the counter and the log line must not.
                        let reason = err.classify().as_str();
                        marker.set(false);
                        pplx_space_metrics::leader_election::renew_failed(reason);
                        tracing::warn!(
                            error = %err,
                            reason,
                            "lease renewal failed; will retry before the deadline"
                        );
                    }
                }
            }
        }
    }
}

/// Give up leadership: clear readiness, log, count, and cancel the reconciler
/// token so the reconcilers drain (with the renew-deadline margin still ahead of
/// the lease expiry, so no standby starts before they finish).
///
/// Readiness is cleared here rather than left to the process exit, and the
/// distinction matters on the `lease_held_by_another_replica` path: that arm is
/// reached from a *successful* call, so the last thing the marker was told is
/// `true`, and nothing after this would ever tell it otherwise — `renew_loop`
/// returns immediately and its `ReadinessMarker` handle is dropped. The pod would
/// then report Ready for the whole drain while holding no lease and running no
/// reconcilers, which under `maxUnavailable: 0` is a replica a concurrent rollout
/// counts as available. Best-effort: the write is queued on the writer task and
/// the process exits shortly after, so it may not land — but it can only improve
/// on leaving a stale `true` behind.
fn resign_lost(
    identity: &str,
    marker: &ReadinessMarker,
    leader_token: &CancellationToken,
    reason: &str,
) {
    marker.set(false);
    leadership_change_span(identity, "lost", reason);
    tracing::error!(%identity, reason, "resigning cluster-controller leadership");
    pplx_space_metrics::leader_election::lease_lost(identity);
    leader_token.cancel();
}

/// Mark a tick that changed leadership. Lease calls record spans only when they
/// fail (the store's client is failure-traced), so renewals and standby polls,
/// the idle steady state, leave no trace and this span is what shows a change.
fn leadership_change_span(identity: &str, change: &'static str, reason: &str) {
    let span = tracing::info_span!(
        parent: None,
        "leader_election.change",
        otel.kind = "internal",
        "operation.name" = "leader_election.change",
        leader_election.lease = LEASE_NAME,
        leader_election.identity = %identity,
        leader_election.change = change,
        leader_election.reason = reason,
    );
    drop(span);
}
