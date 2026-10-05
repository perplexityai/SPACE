//! Dependency-light shared primitives for SPACE services.
//!
//! The crate contains dependency-light shared helpers:
//!
//! - [`types`] is the domain type catalog. Prefer it so raw strings, UUIDs,
//!   sizes, paths, and IDs are parsed once at the boundary.
//! - [`host`] and [`guest`] describe platform-specific SPACE runtime contracts.
//! - [`ids`] contains legacy sandbox ID string helpers kept for compatibility.
//!
//! # Examples
//!
//! ```rust
//! # use pplx_space_common_core::types::*;
//! let id = SandboxId::new();
//! let selector = SandboxSelector::parse(id.label().to_string())?;
//!
//! assert!(selector.matches_id(id));
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```
//!
pub mod sandbox_storage;
pub mod types;

#[cfg(feature = "archive")]
pub mod archive;

pub mod env;
pub mod guest;
pub mod host;
pub mod inventory;
pub mod operation_budget;
mod sql_locator;

/// Legacy sandbox ID string helpers.
///
/// New code should prefer [`types::SandboxId`] and [`types::SandboxSelector`].
pub mod ids {
    /// Number of lowercase hex characters in a legacy sandbox display label.
    pub const SANDBOX_LABEL_LEN: usize = 8;

    /// Error returned when a legacy sandbox ID helper receives non-hex input.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_common_core::ids::sandbox_label;
    /// assert!(sandbox_label("not a uuid").is_err());
    /// ```
    #[derive(Debug, Clone, Eq, PartialEq, thiserror::Error)]
    #[error("sandbox id must contain only hex digits and dashes")]
    pub struct InvalidSandboxId;

    fn compact_hex(value: &str) -> Option<String> {
        let mut compact = String::new();
        for c in value.chars() {
            if c == '-' {
                continue;
            }
            if !c.is_ascii_hexdigit() {
                return None;
            }
            compact.push(c.to_ascii_lowercase());
        }
        Some(compact)
    }

    /// Returns the display label for a sandbox ID string.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_common_core::ids::sandbox_label;
    /// assert_eq!(
    ///     sandbox_label("019d84bd-280d-7331-99c6-b63377f3a3a2").as_deref(),
    ///     Ok("77f3a3a2")
    /// );
    /// ```
    pub fn sandbox_label(sandbox_id: &str) -> Result<String, InvalidSandboxId> {
        let compact = compact_hex(sandbox_id).ok_or(InvalidSandboxId)?;
        if compact.len() <= SANDBOX_LABEL_LEN {
            Ok(compact)
        } else {
            let reversed_label: String = compact.chars().rev().take(SANDBOX_LABEL_LEN).collect();
            Ok(reversed_label.chars().rev().collect())
        }
    }

    /// Returns a sandbox display label, falling back to the original input.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_common_core::ids::sandbox_label_or_id;
    /// assert_eq!(sandbox_label_or_id("bad id"), "bad id");
    /// ```
    pub fn sandbox_label_or_id(sandbox_id: &str) -> String {
        let display_id = sandbox_id.strip_prefix("sbx_").unwrap_or(sandbox_id);
        sandbox_label(display_id).unwrap_or_else(|_| sandbox_id.to_string())
    }

    /// Checks whether a legacy selector matches a sandbox ID string.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_common_core::ids::sandbox_id_matches;
    /// let id = "019d84bd-280d-7331-99c6-b63377f3a3a2";
    /// assert!(sandbox_id_matches(id, "77f3"));
    /// assert!(sandbox_id_matches(id, "019d84bd-280d"));
    /// ```
    pub fn sandbox_id_matches(sandbox_id: &str, selector: &str) -> bool {
        if sandbox_id == selector {
            return true;
        }

        let Some(selector) = compact_hex(selector) else {
            return false;
        };
        if selector.is_empty() {
            return false;
        }

        let Some(compact_id) = compact_hex(sandbox_id) else {
            return false;
        };
        if selector.len() <= SANDBOX_LABEL_LEN {
            sandbox_label(sandbox_id).is_ok_and(|label| label.starts_with(&selector))
        } else {
            compact_id.starts_with(&selector)
        }
    }

    #[cfg(test)]
    mod tests;
}

/// Constants shared with spaced host and guest components.
pub mod spaced {
    /// Maximum entries sent in one write batch.
    pub const MAX_WRITE_BATCH_ENTRIES: usize = 256;

    /// gRPC metadata key naming why a call failed where its code alone is
    /// ambiguous. Hops relay a status unchanged, so the key reaches the API.
    pub const ERROR_REASON_METADATA_KEY: &str = "space-error-reason";

    /// [`ERROR_REASON_METADATA_KEY`] value of a RESOURCE_EXHAUSTED that is the
    /// sandbox's disk or quota being full (ENOSPC, EDQUOT), as opposed to the
    /// admission limits that share the code.
    pub const GUEST_STORAGE_FULL: &str = "guest-storage-full";
}

/// Stable status markers shared by the sandbox-manager wake producer and
/// cluster-side recovery callers.
pub mod wake_signals {
    /// `FailedPrecondition` message returned when an internal wake reaches a
    /// launch state without the pod network namespace required to boot it.
    pub const MISSING_RUNTIME_CONTEXT_MARKER: &str =
        "wake start context has no network namespace; re-place via restore";
}

/// Machine-readable markers node-manager attaches to `ExposePort` failures and
/// node-ctl classifies on. Producer (node-manager) and consumer (node-ctl)
/// must reference these constants, never the literals.
pub mod port_exposure_signals {
    /// gRPC status **metadata** key node-manager sets on the `ExposePort`
    /// `FailedPrecondition` status when the failure is a stale/absent pod
    /// network namespace (the CNI sandbox behind the allocation is gone).
    ///
    /// node-ctl keys its terminal-disposition on this exact metadata entry,
    /// **not** on the `FailedPrecondition` code alone: the same code is also
    /// returned for the root-namespace ingress-safety rejection, which has a
    /// live allocation the orphan reconciler will not repair and must stay
    /// non-terminal (retryable). A metadata key is used instead of a message
    /// substring so the classification is an exact machine-readable match, not
    /// a fragile phrase compare. Must be a valid lowercase ASCII gRPC metadata
    /// key.
    pub const EXPOSE_TERMINAL_REASON_METADATA_KEY: &str = "x-space-expose-terminal-reason";

    /// Metadata value carried under [`EXPOSE_TERMINAL_REASON_METADATA_KEY`] for
    /// the stale/absent pod-netns terminal reason. node-ctl completes the
    /// desired-state ref (publishing a failed observation) while the cluster
    /// orphan reconciler repairs the durable state.
    pub const EXPOSE_TERMINAL_REASON_STALE_POD_NETNS: &str = "stale_pod_netns";
}

/// Constants shared between the on-node sbx-mgr and the cluster-controller
/// for the snapshot-reupload RPC. Producers and consumers must point at the
/// same string so the consumer's classifier doesn't silently misclassify a
/// reworded producer message.
pub mod node_drain_signals {
    /// Substring the on-node sbx-mgr embeds in its
    /// `FailedPrecondition` status when a reupload cannot proceed because
    /// the local snapshot artifacts have been pruned. The cluster
    /// controller's promote handler classifies on this marker to choose
    /// the terminal `mark_sandbox_lost` path over an infinite retry. Both
    /// sides must reference this constant; never duplicate the literal.
    pub const REUPLOAD_ARTIFACT_MISSING_MARKER: &str = "required on-disk artifact missing";

    /// Substring the on-node sbx-mgr embeds in its `FailedPrecondition`
    /// status when a reupload was requested but the sandbox has no
    /// record in this node's local sqlite — i.e. the sandbox was never
    /// here, or its on-node state has already been cleaned up. Same
    /// terminal classification as `REUPLOAD_ARTIFACT_MISSING_MARKER`:
    /// the reupload can never succeed from this node, so the cluster
    /// controller flips the sandbox to `failed` and unblocks drain.
    /// Distinct from a tonic `Code::NotFound` from endpoint resolution
    /// (the substrate adapter wraps that as `SubstrateError::NotFound`
    /// with a different message), which stays retryable.
    pub const REUPLOAD_SANDBOX_UNKNOWN_MARKER: &str = "sandbox unknown to this sbx-mgr";

    /// Substring the on-node sbx-mgr embeds in the `FailedPrecondition`
    /// status it returns when a suspend is rejected because the sandbox's
    /// node-local state is already `failed` — the Firecracker guest died but
    /// the cluster SSOT still reads the sandbox as `started`/`paused`. The
    /// cluster controller's node-drain evacuation classifies on this marker to
    /// take the terminal give-up path (flip the DB row to `failed` so the node
    /// can finish draining) instead of retrying the suspend on every tick
    /// forever — the divergence never self-heals, and neither the in-flight
    /// evacuate timeout nor the stuck-transient stale-abandon sweep covers a
    /// row whose DB lifecycle is `started`/`paused`. The producer renders this
    /// exact phrase for the failed-state rejection, so the marker is a
    /// substring of the wire message; both sides reference this constant,
    /// never the literal, so a reworded producer message can't silently flip
    /// the classifier from "terminal, mark failed" to "retry forever".
    pub const SUSPEND_SANDBOX_FAILED_MARKER: &str = "sandbox is failed, must be started or paused";

    /// Substring the on-node sbx-mgr embeds in the `FailedPrecondition`
    /// status it returns when a suspend-snapshot **reupload** is rejected
    /// because the sandbox is wedged in `restoring` on the node (a stuck
    /// on-node restore) while the cluster SSOT still reads it `suspended`. The
    /// cluster controller's node-drain local-snapshot promotion classifies on
    /// this marker to take a give-up path: after a settle window, if the newest
    /// ready remote snapshot already exists, it adopts that snapshot (makes the
    /// row durable) so the node can finish draining instead of retrying the
    /// permanently-refused reupload on every tick forever.
    ///
    /// **Deliberately scoped to the `restoring` state only.** sbx-mgr refuses
    /// the reupload for *every* non-suspended state with a
    /// `"sandbox is {state}, must be suspended"` message, but the give-up is
    /// only safe for `restoring`: the VM is loading **from** the snapshot, so
    /// it holds no user state newer than the snapshot, which makes the snapshot
    /// canonical. A `started`/`paused` row (running VM) holds live state newer
    /// than any snapshot, and `stopped` may hold a modified disk — abandoning
    /// those for an older snapshot would lose work, and a settle window doesn't
    /// change that. So the marker is this exact `restoring`-state phrase (a
    /// substring of the historical producer message for `restoring`, preserving
    /// compatibility with older sandbox-manager binaries). Other states render a
    /// generic message carrying no marker and never trigger the give-up. Both
    /// sides reference this constant, never the literal, so a reworded producer
    /// message can't silently flip the
    /// classifier.
    pub const REUPLOAD_SANDBOX_RESTORING_MARKER: &str = "sandbox is restoring, must be suspended";

    /// Postgres `LISTEN`/`NOTIFY` channel that drives the immediate cohort
    /// restore of a drain-evacuated sandbox. When the cluster-api registers a
    /// suspend snapshot `ready` (the durable S3 upload finalized), it
    /// `NOTIFY`s this channel so the cluster-controller's node-drain run loop
    /// wakes and runs the restore sweep right away, instead of waiting for the
    /// next periodic tick. The periodic cluster-wide sweep remains the
    /// correctness fallback for any missed signal (controller restart, dropped
    /// notification), so the payload is advisory — the consumer always
    /// re-derives the restore-pending set from the database. Both the NOTIFY
    /// producer and the LISTEN consumer must reference this constant.
    pub const EVACUATION_RESTORE_READY_CHANNEL: &str = "space_evacuation_restore_ready";
}

/// The ownership windows the restore path's leases and guards derive from.
/// Every deriver must read these constants rather than restate the numbers:
/// a lease TTL that silently diverges from the RPC timeout breaks the
/// invariant that an in-flight node request cannot be re-driven underneath
/// its owner, and the build stays green while it happens.
pub mod restore_ownership {
    use std::time::Duration;

    /// Bounded timeout for a single API-to-node RPC. The restore lease TTL
    /// equals it: a claim's lease must outlive an in-flight node request so
    /// an active restore cannot be re-driven underneath itself.
    pub const NODE_RPC_TIMEOUT: Duration = Duration::from_secs(150);

    /// How long a fresh active execution keeps ownership of the restore
    /// gate past the lease deadline. A materialized restore execution owns
    /// downstream work for longer than any API-to-node request can remain
    /// active, so this must strictly outlive [`NODE_RPC_TIMEOUT`].
    pub const RESTORE_EXECUTION_OWNERSHIP_TTL: Duration = Duration::from_secs(300);

    const _: () = assert!(
        RESTORE_EXECUTION_OWNERSHIP_TTL.as_secs() > NODE_RPC_TIMEOUT.as_secs(),
        "execution ownership must outlive the node-RPC timeout (and with it the restore lease)",
    );
}

/// Constants shared between the on-node sbx-mgr and cluster-side failure
/// reconcilers for the artifact-backup RPC. Producers and consumers must
/// point at the same string so the consumer's classifier doesn't silently
/// misclassify a reworded producer message.
pub mod artifact_backup_signals {
    /// Substring the on-node sbx-mgr embeds in the `FailedPrecondition`
    /// status it returns when a current-instance backup is rejected because
    /// the sandbox's node-local state is `failed`. Terminal: nothing moves a
    /// `failed` local row to `stopped`/`suspended` without external action,
    /// so the execution-retire path classifies on this marker to skip the
    /// preservation step and release the pod, instead of aborting and
    /// retrying the backup on every reconciler tick forever while the
    /// leftover pod keeps feeding kubelet CreatePodSandbox retries.
    /// Node-local state stays salvageable either way — retire never sends
    /// the sbx-mgr delete. The phrase is a substring of the producer's
    /// historical wording for the `failed` state, hence byte-identical to
    /// already-deployed sbx-mgrs (no rollout-skew window); other states
    /// render a generic message carrying no marker and keep aborting the
    /// retire so a convergeable state (e.g. `stopping`) retries.
    pub const CURRENT_INSTANCE_BACKUP_SANDBOX_FAILED_MARKER: &str =
        "sandbox is failed, must be stopped or suspended";

    /// Substring the on-node sbx-mgr embeds in the `FailedPrecondition`
    /// status it returns when a current-instance backup expecting a disk
    /// artifact is rejected because the suspended instance's current
    /// artifact is a full suspend snapshot. Terminal for the retire path's
    /// fixed disk request: the full snapshot already *is* the published
    /// current artifact and its durable upload is owned by the suspend
    /// machinery, so retire classifies on this marker to skip rather than
    /// retry a kind mismatch that can never resolve. Same
    /// no-rollout-skew property as
    /// [`CURRENT_INSTANCE_BACKUP_SANDBOX_FAILED_MARKER`]: the phrase is a
    /// substring of the producer's historical wording.
    pub const CURRENT_INSTANCE_BACKUP_PUBLISHES_FULL_MARKER: &str =
        "current suspended instance publishes full";
}

#[cfg(feature = "kubernetes")]
#[path = "k8s.rs"]
pub mod kubernetes;

#[cfg(test)]
mod runtime_contract_tests;
