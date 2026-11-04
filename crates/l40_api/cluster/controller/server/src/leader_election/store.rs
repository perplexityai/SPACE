//! Lease persistence for the cluster-controller leader election, backed by a
//! native Kubernetes `coordination.k8s.io/v1` `Lease` object.
//!
//! Acquisition, renewal, and takeover follow `client-go`'s `LeaseLock`
//! semantics. Two properties make it safe:
//!
//! * **Observed-time expiry (skew-immune).** A candidate does *not* compare its
//!   own wall clock to the holder's `renewTime` — those clocks can differ. It
//!   instead tracks, on its *local* monotonic clock, how long the lease's
//!   `resourceVersion` has been unchanged, and treats a peer's lease as expired
//!   only after `leaseDurationSeconds` of *local* elapsed time with no change.
//!   A live holder's renewals keep bumping `resourceVersion`, resetting every
//!   candidate's local timer, so no amount of clock skew lets a standby take
//!   over while the holder is still renewing.
//! * **Optimistic-concurrency takeover.** The take/renew write is a `replace`
//!   keyed on the observed `resourceVersion`; a losing concurrent takeover gets
//!   a `409 Conflict`, which we map to "not the leader".
//!
//! This is cooperative coordination, not a fencing/consensus guarantee (see the
//! module docs in `mod.rs`).

use std::sync::Mutex;
use std::time::{Duration, Instant};

use k8s_openapi::api::coordination::v1::{Lease, LeaseSpec};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{MicroTime, ObjectMeta};
use kube::api::PostParams;
use kube::{Api, Client};

/// A fallible lease operation. A *lost* election is not an error (it is
/// `Ok(false)`); only a transport/API failure or a bad clock is.
#[derive(Debug, thiserror::Error)]
pub enum LeaseError {
    #[error("leader-election lease API call failed: {0}")]
    Kube(#[from] kube::Error),
    #[error("system clock is before the unix epoch")]
    Clock,
}

/// What a failed lease call means for the caller, and therefore what it should
/// do about it. A closed vocabulary so the election loop dispatches on the
/// *meaning* of a failure rather than on its text, and so the failure counter
/// carries a bounded reason dimension.
///
/// The distinction that matters is whether restarting the process could plausibly
/// help. The liveness budget exists to restart a replica whose own client is
/// wedged; spending it on a failure a restart cannot fix trades a NotReady pod
/// for a NotReady pod that also re-LISTs every Node on a cycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeaseFailure {
    /// The apiserver was not reached at all, or the call outran its timeout: a
    /// transport or handshake failure (which arrive as `Service` / `HyperError`),
    /// a credential-load failure, or a response this process cannot act on. A
    /// restart rebuilds the client and re-reads the projected token, so this is
    /// what the budget is for.
    ///
    /// Note the *config-time* TLS and proxy variants — `TlsRequired`,
    /// `ProxyProtocolUnsupported`, `ProxyProtocolDisabled` — are [`Self::Refused`]
    /// instead: they are decided by this build's own configuration before any
    /// bytes move, and a fresh client reproduces them exactly.
    Unreachable,
    /// The apiserver answered, and answered that it is shedding load — 429, or
    /// 5xx. Restarting makes that worse, because the replacement re-LISTs every
    /// Node against the same overloaded control plane, so this does not spend the
    /// budget.
    Overloaded,
    /// The apiserver answered definitively and will answer the same way to the
    /// same request: authz (401/403), or a request it rejects on its merits
    /// (4xx). A fresh client changes nothing, so this does not spend the budget
    /// either — it is reported on sight instead of after a grace period of
    /// silence.
    Refused,
    /// This replica's own clock is unusable, so it cannot reason about expiry at
    /// all. Restart-fixable in the same sense as `Unreachable` (a clock may have
    /// been stepped since start), so it is treated the same way.
    Clock,
}

impl LeaseFailure {
    /// The value for the `reason` tag on the failure counter. Bounded by
    /// construction, so it is safe as a metric dimension.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Unreachable => "unreachable",
            Self::Overloaded => "overloaded",
            Self::Refused => "refused",
            Self::Clock => "clock",
        }
    }

    /// Whether this failure is one the liveness budget should spend itself on,
    /// i.e. whether a restart could plausibly clear it.
    pub fn charges_liveness_budget(self) -> bool {
        match self {
            Self::Unreachable | Self::Clock => true,
            Self::Overloaded | Self::Refused => false,
        }
    }

    /// Whether nothing this process does — retrying, restarting, waiting — can
    /// clear this failure, so a human has to change something. Such a failure is
    /// logged at `error` the first time it is seen rather than after the grace
    /// window, because for the categories that don't charge the budget there is
    /// no later escalation to wait for.
    ///
    /// Orthogonal to [`Self::charges_liveness_budget`], which is why one enum
    /// carries both: `Clock` needs an operator *and* is worth a restart (a clock
    /// may have been stepped since this process started), while `Overloaded`
    /// needs neither — it is the control plane's problem and it resolves itself.
    pub fn needs_operator_action(self) -> bool {
        match self {
            Self::Refused | Self::Clock => true,
            Self::Unreachable | Self::Overloaded => false,
        }
    }
}

impl LeaseError {
    /// Classify this failure. Errors are matched on their *variant and status
    /// code*, never on their text: `Display` output is wrapped by callers and
    /// would reclassify silently the first time a message changed.
    pub fn classify(&self) -> LeaseFailure {
        // Exhaustive on `LeaseError` on purpose: a `let ... else` would quietly
        // funnel any variant added later into `Clock`.
        let err = match self {
            Self::Clock => return LeaseFailure::Clock,
            Self::Kube(err) => err,
        };
        match err {
            // The apiserver answered. Which answer decides — but only when the
            // answer actually carries a status code, which is not guaranteed.
            // `Status` defaults every field, and `kube` uses a parsed error body
            // verbatim rather than overriding it with the HTTP status, so any
            // JSON body that happens to deserialize as a `Status` without a
            // `code` field arrives here as `code: 0` — including a 503 page from
            // a proxy in front of the apiserver. Classifying that as a refusal
            // would suppress the restart *and* log that no restart can help.
            kube::Error::Api(status) => match status.code {
                429 => LeaseFailure::Overloaded,
                // 5xx is the server saying it could not serve this now.
                500..=599 => LeaseFailure::Overloaded,
                // A real 4xx is the apiserver refusing on the merits: 401/403
                // authz, 400/404/422 request. 409 never arrives here — the store
                // maps a lost optimistic-concurrency race to `Ok(false)`.
                400..=499 => LeaseFailure::Refused,
                // No usable code: `0` when the body carried none, and anything
                // non-error is not an answer this call can act on either. Fall
                // back to the pre-classification behaviour — charge the budget
                // and allow the restart — because "we could not tell what the
                // apiserver said" must not be quieter than "it did not answer".
                _ => LeaseFailure::Unreachable,
            },
            // Deterministic and decided by *this build's own configuration*
            // before any bytes move: proxy protocol support, TLS setup, kubeconfig
            // inference, a request this build cannot form, a resource it cannot
            // discover. A fresh client reproduces every one identically, so none
            // is worth a restart.
            //
            // Deliberately NOT here: `FromUtf8` and `SerdeError`. Both are
            // decided by the *peer's* bytes — `handle_api_errors` reads the body
            // of every 4xx/5xx and returns them when it is not UTF-8 or not
            // parseable. That is the same code path that yields `Api(code: 0)`,
            // which the status match above routes to `Unreachable` on the
            // must-never-be-quieter argument; routing these the other way would
            // contradict it, and a non-UTF-8 error page from something in front
            // of the apiserver would leave the replica NotReady with nothing to
            // restart it.
            //
            // Listed explicitly rather than left to the catch-all because the
            // catch-all deliberately errs the other way — see below.
            kube::Error::ProxyProtocolUnsupported { .. }
            | kube::Error::ProxyProtocolDisabled { .. }
            | kube::Error::BuildRequest(_)
            | kube::Error::InferConfig(_)
            | kube::Error::Discovery(_)
            | kube::Error::TlsRequired
            | kube::Error::InferKubeconfig(_) => LeaseFailure::Refused,
            // Everything else: never got an answer, or got one this process
            // cannot act on. The known members are `HyperError`, `Service`,
            // `HttpError`, `ReadEvents`, `LinesCodecMaxLineLengthExceeded` and
            // `Auth` — transport and credentials, where a restart rebuilds the
            // client and re-reads the projected token, which is exactly the
            // "wedged behind a stale token" case. They are named here rather
            // than as pattern arms because clippy rejects listing variants a
            // wildcard already covers.
            //
            // The default lands on `Unreachable` rather than `Refused` because
            // `Unreachable` is the behaviour that existed before any
            // classification: it charges the budget and allows the restart. An
            // unrecognised failure must never be *quieter* than that. And it is
            // a wildcard rather than an exhaustive match because several
            // `kube::Error` variants are feature-gated, so the exhaustive arm
            // list would differ per feature set and fail to compile.
            _ => LeaseFailure::Unreachable,
        }
    }
}

/// Persistence backend for the leader-election lease. Abstracted as a trait so
/// the election loop can be unit-tested against an in-memory fake without a
/// Kubernetes apiserver; the production impl is [`KubeLeaseStore`].
///
/// Methods return `impl Future + Send` (rather than `async fn`) so the futures
/// are spawnable from the renewal task, matching the reconciler sink traits in
/// this crate.
pub trait LeaseStore {
    /// Acquire the lease if it is free/expired, or renew it if we already hold
    /// it. Returns `Ok(true)` iff we now hold a valid lease that expires `ttl`
    /// from now.
    fn acquire_or_renew(
        &self,
        lease_name: &str,
        identity: &str,
        ttl: Duration,
    ) -> impl std::future::Future<Output = Result<bool, LeaseError>> + Send;

    /// Best-effort release: mark the lease free iff we still hold it, letting a
    /// standby take over immediately instead of waiting for TTL expiry.
    fn release(
        &self,
        lease_name: &str,
        identity: &str,
    ) -> impl std::future::Future<Output = Result<(), LeaseError>> + Send;
}

/// The last-observed lease revision and the local instant we first saw it —
/// the basis for skew-immune expiry (see the module docs).
struct Observed {
    resource_version: Option<String>,
    since: Instant,
}

/// Production [`LeaseStore`] backed by a namespaced `coordination.k8s.io/v1`
/// `Lease`.
pub struct KubeLeaseStore {
    api: Api<Lease>,
    observed: Mutex<Observed>,
}

impl KubeLeaseStore {
    pub fn new(client: Client, namespace: &str) -> Self {
        Self {
            api: Api::namespaced(client, namespace),
            observed: Mutex::new(Observed {
                resource_version: None,
                since: Instant::now(),
            }),
        }
    }

    /// Record the just-observed `resourceVersion` and return how long the lease
    /// has been unchanged on our local clock. Resets the timer whenever the
    /// revision differs from what we last saw (including the first observation).
    fn observe(&self, resource_version: Option<&str>) -> Duration {
        let now = Instant::now();
        let mut observed = self.observed.lock().expect("observed lock");
        if observed.resource_version.as_deref() != resource_version {
            observed.resource_version = resource_version.map(str::to_string);
            observed.since = now;
        }
        now.duration_since(observed.since)
    }
}

impl LeaseStore for KubeLeaseStore {
    async fn acquire_or_renew(
        &self,
        lease_name: &str,
        identity: &str,
        ttl: Duration,
    ) -> Result<bool, LeaseError> {
        let existing = self.api.get_opt(lease_name).await?;
        let elapsed = self.observe(
            existing
                .as_ref()
                .and_then(|lease| lease.metadata.resource_version.as_deref()),
        );
        match existing {
            // Object absent — create it held by us. A concurrent creator wins
            // the 409 race; we back off to a standby.
            None => {
                let lease = Lease {
                    metadata: ObjectMeta {
                        name: Some(lease_name.to_string()),
                        ..Default::default()
                    },
                    spec: Some(renewed_spec(None, identity, ttl, now_unix()?)?),
                };
                match self.api.create(&PostParams::default(), &lease).await {
                    Ok(_) => Ok(true),
                    Err(err) if is_conflict(&err) => Ok(false),
                    Err(err) => Err(err.into()),
                }
            }
            // Object present — take/renew only if eligible, then `replace`
            // under the observed `resourceVersion` so a concurrent takeover
            // loses with a 409.
            Some(mut existing) => {
                let holder = existing
                    .spec
                    .as_ref()
                    .and_then(|s| s.holder_identity.as_deref());
                let lease_duration = existing
                    .spec
                    .as_ref()
                    .and_then(|s| s.lease_duration_seconds)
                    .and_then(|secs| u64::try_from(secs).ok())
                    .map_or(ttl, Duration::from_secs);
                if !takeable(holder, identity, elapsed, lease_duration) {
                    return Ok(false);
                }
                let spec = renewed_spec(existing.spec.as_ref(), identity, ttl, now_unix()?)?;
                existing.spec = Some(spec);
                match self
                    .api
                    .replace(lease_name, &PostParams::default(), &existing)
                    .await
                {
                    Ok(_) => Ok(true),
                    Err(err) if is_conflict(&err) => Ok(false),
                    Err(err) => Err(err.into()),
                }
            }
        }
    }

    async fn release(&self, lease_name: &str, identity: &str) -> Result<(), LeaseError> {
        let Some(mut existing) = self.api.get_opt(lease_name).await? else {
            return Ok(());
        };
        let held_by_us = existing
            .spec
            .as_ref()
            .and_then(|s| s.holder_identity.as_deref())
            == Some(identity);
        if !held_by_us {
            return Ok(());
        }
        // Clear the holder so a standby treats the lease as free; it acquires on
        // its next poll rather than waiting out the TTL.
        if let Some(spec) = existing.spec.as_mut() {
            spec.holder_identity = Some(String::new());
        }
        match self
            .api
            .replace(lease_name, &PostParams::default(), &existing)
            .await
        {
            Ok(_) => Ok(()),
            // A concurrent takeover already moved the lease on (409) — it is no
            // longer ours, so releasing is a benign no-op.
            Err(err) if is_conflict(&err) => Ok(()),
            // Any other failure must propagate: the caller (`resign`) would
            // otherwise log a successful handoff that didn't happen, hiding the
            // fact that the lease will now linger until its TTL.
            Err(err) => Err(err.into()),
        }
    }
}

/// Whether this identity may create/renew/take the lease. `observed_elapsed` is
/// how long the lease revision has been unchanged on *our* local clock;
/// `lease_duration` is the holder's declared validity. Pure so the takeover
/// decision is unit-testable without an apiserver.
fn takeable(
    holder: Option<&str>,
    identity: &str,
    observed_elapsed: Duration,
    lease_duration: Duration,
) -> bool {
    match holder {
        // Free (never held) or explicitly released.
        None | Some("") => true,
        // We already hold it — renew.
        Some(held) if held == identity => true,
        // Held by another — takeable only once the lease has gone unchanged for
        // its full duration on our own clock (skew-immune).
        Some(_) => observed_elapsed >= lease_duration,
    }
}

/// Build the spec to write when creating/renewing/taking the lease. Preserves
/// `acquireTime`/`leaseTransitions` on a self-renewal; bumps the transition
/// count and resets `acquireTime` on a takeover (holder change). `renewTime` is
/// written for observability/interop; expiry is decided by observed local time,
/// not by reading it back.
fn renewed_spec(
    previous: Option<&LeaseSpec>,
    identity: &str,
    ttl: Duration,
    now_unix: i64,
) -> Result<LeaseSpec, LeaseError> {
    let now = unix_to_micro_time(now_unix)?;
    let held_by_us = previous.and_then(|s| s.holder_identity.as_deref()) == Some(identity);
    let (acquire_time, lease_transitions) = if held_by_us {
        (
            previous
                .and_then(|s| s.acquire_time.clone())
                .or_else(|| Some(now.clone())),
            previous.and_then(|s| s.lease_transitions),
        )
    } else {
        let prev = previous.and_then(|s| s.lease_transitions).unwrap_or(0);
        (Some(now.clone()), Some(prev + 1))
    };
    Ok(LeaseSpec {
        holder_identity: Some(identity.to_string()),
        lease_duration_seconds: Some(i32::try_from(ttl.as_secs()).unwrap_or(i32::MAX)),
        acquire_time,
        renew_time: Some(now),
        lease_transitions,
        ..Default::default()
    })
}

fn now_unix() -> Result<i64, LeaseError> {
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|_| LeaseError::Clock)?
        .as_secs();
    i64::try_from(secs).map_err(|_| LeaseError::Clock)
}

fn unix_to_micro_time(secs: i64) -> Result<MicroTime, LeaseError> {
    let ts = k8s_openapi::jiff::Timestamp::from_second(secs).map_err(|_| LeaseError::Clock)?;
    Ok(MicroTime(ts))
}

fn is_conflict(err: &kube::Error) -> bool {
    matches!(err, kube::Error::Api(resp) if resp.code == 409)
}

#[cfg(test)]
mod tests;
