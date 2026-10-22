//! Auto-lifecycle delete-completion receipt contract.
//!
//! [`AutoLifecyclePolicyFingerprint`] is the stable identity of a normalized
//! auto-lifecycle policy that a delete-completion receipt carries. The sandbox
//! manager publishes receipts and the internal API consumes them; both use the
//! fingerprint and `AUTO_LIFECYCLE_DELETE_COMPLETION_BATCH_SIZE`. The other
//! `AUTO_LIFECYCLE_DELETE_COMPLETION_*` constants bound only the internal API.
//!
//! It is its own crate so that both sides can share the receipt contract
//! without depending on `pplx-space-common-svc`. `pplx-space-common-svc`
//! re-exports it as `types::auto_lifecycle`.

use std::{fmt, str::FromStr, time::Duration};

use pplx_space_common_core::types::{
    AutoLifecycleConditionSet, AutoLifecyclePolicy, AutoLifecycleStage, AutoLifecycleWakePolicy,
    Digest, DigestParseError,
};
use sha2::{Digest as _, Sha256};

const AUTO_LIFECYCLE_POLICY_FINGERPRINT_VERSION: &str = "v1";

/// Maximum durable auto-lifecycle delete receipts processed in one node-state
/// publication transaction.
///
/// Each processed receipt opens one savepoint, and a savepoint that takes a row
/// lock assigns a subtransaction id. PostgreSQL caches only
/// `PGPROC_MAX_CACHED_SUBXIDS` (64) subtransaction ids per backend; past that
/// the backend's snapshot is marked suboverflowed and every other backend falls
/// back to `pg_subtrans` SLRU lookups for visibility checks. Keep this well
/// under 64 so the rest of the publication transaction has headroom.
pub const AUTO_LIFECYCLE_DELETE_COMPLETION_BATCH_SIZE: usize = 16;

/// Maximum durable auto-lifecycle delete receipts read out of one
/// `PublishNodeState` request.
///
/// Only [`AUTO_LIFECYCLE_DELETE_COMPLETION_BATCH_SIZE`] receipts are evaluated
/// per publication, but parsing, metric accumulation, and the acknowledgement
/// list all scale with the repeated field itself. Receipts past this cap are
/// counted and dropped without parsing; the publisher retains them and retries.
pub const AUTO_LIFECYCLE_DELETE_COMPLETION_REQUEST_LIMIT: usize = 256;

/// How long a `Started`/`Paused` central row may keep deferring a receipt while
/// its active placement stays empty.
///
/// Ownership is normally proven by `sandboxes.active_node_id`. A live-looking
/// row with no active node is mid-transition, so the receipt defers and the
/// publisher retries. Nothing repopulates that column once the runtime is gone,
/// though, so an unbounded deferral is an unbounded retry loop. Past this grace
/// the receipt is acknowledged without deleting and the central row is left to
/// orphan reconciliation, which is the same fail-closed disposition every other
/// incomplete-evidence branch takes.
pub const AUTO_LIFECYCLE_DELETE_COMPLETION_OWNERSHIP_GRACE: Duration = Duration::from_secs(15 * 60);

/// Upper bound on how long any one receipt statement waits for a row lock.
///
/// Receipt evidence reads use `NOWAIT`, but the canonical finalize path issues
/// plain `UPDATE`/`DELETE` statements while the publication transaction already
/// holds up to [`AUTO_LIFECYCLE_DELETE_COMPLETION_BATCH_SIZE`] sandbox row
/// locks. Without a bound, one contended row blocks the whole publication.
/// PostgreSQL reports the expiry as `55P03`, the same code `NOWAIT` raises, so
/// it lands on the existing retryable-contention path.
///
/// This is applied with `set_config('lock_timeout', …, TRUE)`, which is
/// transaction-scoped: where the application role carries a sub-second
/// `lock_timeout` (see `db/README.md`) this *raises* the bound for the rest of
/// the publication rather than lowering it. Deliberate — abandoning a
/// contended row is more expensive here than waiting, because the transaction
/// is already holding the batch's locks — but it means this constant, not the
/// role setting, is the effective bound on this path. Keep it well under
/// `statement_timeout`.
pub const AUTO_LIFECYCLE_DELETE_COMPLETION_LOCK_TIMEOUT: Duration = Duration::from_secs(2);

/// Stable identity of one normalized auto-lifecycle policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct AutoLifecyclePolicyFingerprint(Digest);

impl AutoLifecyclePolicyFingerprint {
    pub fn for_policy(policy: &AutoLifecyclePolicy) -> Self {
        let AutoLifecyclePolicy {
            wake,
            condition_sets,
            stages,
        } = policy.clone().normalized();
        let AutoLifecycleWakePolicy { on: wake_on } = wake;
        // Hash a versioned semantic projection rather than the Rust struct's
        // serialized shape. Adding an unrelated/defaulted policy field must
        // not invalidate delete receipts already in flight. Exhaustive
        // destructuring makes a new policy field fail compilation until its
        // deletion-authority semantics are considered here.
        //
        // The projection is tuples and vectors only — nothing that serializes
        // as a JSON object. serde_json's map key order flips with its
        // `preserve_order` feature, which any crate in a binary's dependency
        // graph can switch on, and the publisher and the receiver of a receipt
        // are different binaries. Positional encoding is what keeps the digest
        // a property of the policy rather than of the build.
        let condition_sets = condition_sets
            .into_iter()
            .map(|(id, condition)| {
                let AutoLifecycleConditionSet {
                    kind,
                    activity_source,
                    max_guest_idle_extension,
                } = condition;
                (
                    id.as_str(),
                    kind.as_str(),
                    activity_source.map(|source| source.as_str()),
                    max_guest_idle_extension.map(|duration| {
                        let duration = duration.as_duration();
                        [duration.as_secs(), u64::from(duration.subsec_nanos())]
                    }),
                )
            })
            .collect::<Vec<_>>();
        let stages = stages
            .into_iter()
            .map(|stage| {
                let AutoLifecycleStage {
                    condition_set,
                    at,
                    action,
                } = stage;
                (condition_set.as_str(), at, action.as_str())
            })
            .collect::<Vec<_>>();
        let wake_on = wake_on
            .iter()
            .map(|source| source.as_str())
            .collect::<Vec<_>>();
        let bytes = serde_json::to_vec(&(wake_on, condition_sets, stages))
            .expect("auto-lifecycle policy fingerprint projection should serialize");
        Self(Digest::sha256(Sha256::digest(bytes).into()))
    }
}

impl fmt::Display for AutoLifecyclePolicyFingerprint {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{AUTO_LIFECYCLE_POLICY_FINGERPRINT_VERSION}:{}",
            self.0
        )
    }
}

impl FromStr for AutoLifecyclePolicyFingerprint {
    type Err = AutoLifecyclePolicyFingerprintParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let (version, digest) = value
            .split_once(':')
            .ok_or(AutoLifecyclePolicyFingerprintParseError::MissingVersion)?;
        if version != AUTO_LIFECYCLE_POLICY_FINGERPRINT_VERSION {
            return Err(
                AutoLifecyclePolicyFingerprintParseError::UnsupportedVersion(version.to_string()),
            );
        }
        digest
            .parse()
            .map(Self)
            .map_err(AutoLifecyclePolicyFingerprintParseError::Digest)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum AutoLifecyclePolicyFingerprintParseError {
    #[error("auto-lifecycle policy fingerprint has no version")]
    MissingVersion,
    #[error("unsupported auto-lifecycle policy fingerprint version '{0}'")]
    UnsupportedVersion(String),
    #[error("invalid auto-lifecycle policy fingerprint digest: {0}")]
    Digest(#[source] DigestParseError),
}

#[cfg(test)]
mod tests;
