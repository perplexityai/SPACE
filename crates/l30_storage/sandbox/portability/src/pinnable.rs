//! Predicates for operator-pinnable suspend captures (`restore_pin`) and
//! `restoring` rows whose automatic retry refuses even though a pre-claim
//! pinnable suspend capture remains durable.

use pplx_space_snapshot_model::ArtifactOrigin;

use super::capture_order::{self, Ranks};
use super::{
    LIVE_CAPTURE, registered_before_restore_claim, restore_retry_source_snapshot_exists,
    suspend_captures, unordered_capture_order,
};

/// Whether the `capture_order` row aliased `row_alias` is a ready full suspend
/// backup with a live storage location that an operator may pin: no live
/// (`creating`, `ready`, or `deleting`) capture outranks it by capture
/// generation, and no live full suspend capture of another artifact with the
/// same rank first registered after it. Captures that tie on first
/// registration are all pinnable, and the operator chooses: neither provably
/// follows the other.
pub(crate) fn pinnable_in_capture_order(row_alias: &str) -> String {
    let suspend = ArtifactOrigin::Suspend.as_str();
    let outranked = capture_order::outranked(&format!("{row_alias}.rank"), LIVE_CAPTURE);
    format!(
        "({row_alias}.state = 'ready'\n\
          AND {row_alias}.local_artifact_id IS NOT NULL\n\
          AND {row_alias}.artifact_kind = 'full'\n\
          AND {row_alias}.artifact_origin = '{suspend}'\n\
          AND EXISTS (\n\
              SELECT 1\n\
                FROM snapshot_storage_locations pinnable_ssl\n\
               WHERE pinnable_ssl.snapshot_id = {row_alias}.id\n\
                 AND pinnable_ssl.orphaned_at IS NULL\n\
          )\n\
          AND NOT {outranked}\n\
          AND NOT EXISTS (\n\
              SELECT 1\n\
                FROM capture_order pinnable_later\n\
               WHERE (pinnable_later.artifact_origin IS NULL OR pinnable_later.artifact_origin = '{suspend}')\n\
                 AND pinnable_later.local_artifact_id IS DISTINCT FROM {row_alias}.local_artifact_id\n\
                 AND pinnable_later.rank IS NOT DISTINCT FROM {row_alias}.rank\n\
                 AND pinnable_later.state <> 'failed'\n\
                 AND pinnable_later.registered_at > {row_alias}.registered_at\n\
          ))"
    )
}

/// Returns a boolean SQL fragment: the snapshot aliased as `snapshot_alias` is
/// a ready full backup, with a live location, of a full suspend artifact whose
/// capture is one the restore selector could take once pinned: no capture
/// that's uploading or ready outranks it by capture generation (a failed one,
/// which won't land, leaves the operator the choice), and no full suspend
/// capture of another artifact with the same rank ([`super::capture_rank`]),
/// uploading or ready, first registered after it; tied captures are all
/// pinnable. In an unordered history that's the latest capture of each side.
pub fn pinnable_suspend_capture(sandbox_alias: &str, snapshot_alias: &str) -> String {
    let pinnable = pinnable_in_capture_order("me");
    let in_order =
        capture_order::for_snapshot(sandbox_alias, Ranks::Plain, snapshot_alias, &pinnable);
    format!(
        "({snapshot_alias}.sandbox_id = {sandbox_alias}.id\n\
          AND {snapshot_alias}.type = 'full'\n\
          AND {in_order})"
    )
}

/// [`pinnable_suspend_capture`] restricted to full snapshots registered before
/// the current `restoring` claim ([`registered_before_restore_claim`]), ranked
/// among pre-claim captures ([`capture_order::with_pre_claim_capture_order`]).
pub fn pinnable_pre_claim_suspend_capture(sandbox_alias: &str, snapshot_alias: &str) -> String {
    let before_claim = registered_before_restore_claim(sandbox_alias, snapshot_alias);
    let order = capture_order::with_pre_claim_capture_order(sandbox_alias);
    let pinnable = pinnable_in_capture_order("me");
    format!(
        "({snapshot_alias}.sandbox_id = {sandbox_alias}.id\n\
          AND {snapshot_alias}.type = 'full'\n\
          AND {before_claim}\n\
          AND EXISTS (\n\
              {order}\n\
              SELECT 1\n\
                FROM capture_order me\n\
               WHERE me.id = {snapshot_alias}.id\n\
                 AND {pinnable}\n\
          ))"
    )
}

/// [`super::restore_order_unordered`] restricted to full snapshots registered
/// before the current `restoring` claim.
pub fn pre_claim_restore_order_unordered(sandbox_alias: &str) -> String {
    let order = capture_order::with_pre_claim_capture_order(sandbox_alias);
    let unordered = unordered_capture_order();
    format!(
        "EXISTS (\n\
             {order}\n\
             SELECT 1\n\
              WHERE {unordered}\n\
         )"
    )
}

/// Returns a boolean SQL fragment: the `restoring` row aliased as
/// `sandbox_alias` has no active owner (its restore lease is absent or expired,
/// and no active Kubernetes execution holds a pod IP or live activation-attempt
/// lease) and satisfies [`restore_pinnable_pre_claim_capture_exists`].
///
/// Used by `restore_pin` so an unpinned `restoring` row kept out of `suspended`
/// (either to preserve `lifecycle_changed_at` against a post-claim snapshot, or
/// because `evacuation_restore_target` is set) can still receive an operator
/// pin on one of its pre-claim captures without touching `lifecycle_changed_at`.
pub fn restoring_row_accepts_operator_pin(sandbox_alias: &str) -> String {
    let pinnable_pre_claim = restore_pinnable_pre_claim_capture_exists(sandbox_alias);
    format!(
        "({sandbox_alias}.lifecycle = 'restoring'\n\
          AND ({sandbox_alias}.restore_lease_expires_at IS NULL\n\
               OR {sandbox_alias}.restore_lease_expires_at <= NOW())\n\
          AND NOT EXISTS (\n\
              SELECT 1\n\
                FROM sandbox_k8s_executions pin_exec\n\
               WHERE pin_exec.sandbox_id = {sandbox_alias}.id\n\
                 AND pin_exec.active\n\
                 AND (pin_exec.pod_ip IS NOT NULL\n\
                      OR pin_exec.activation_attempt_expires_at > NOW()\n\
                      OR (pin_exec.activation_attempt_id IS NOT NULL\n\
                          AND pin_exec.activation_attempt_expires_at IS NULL))\n\
          )\n\
          AND {pinnable_pre_claim})"
    )
}

/// Returns a boolean SQL fragment: the unpinned `restoring` row aliased as
/// `sandbox_alias` has no automatic retry source
/// ([`restore_retry_source_snapshot_exists`]), yet carries a pre-claim durable
/// suspend capture an operator could pin via `restore_pin`
/// ([`pinnable_suspend_capture`]) without an unpinned `suspended` restore
/// silently loading a stale prior-cycle snapshot.
///
/// Pre-claim captures are ranked among themselves
/// ([`capture_order::with_pre_claim_capture_order`]) so a post-claim snapshot
/// published by a failed restore branch can't hide pre-claim captures from the
/// sweeper's fail guards. Requiring `unordered OR NOT newest_suspend` rejects
/// an ordered single capture older than `placement_changed_at` when no newer
/// failed capture blocks automatic `suspended` selection: clearing
/// `placement_changed_at` on revert would otherwise let unpinned `suspended`
/// restore load pre-wake bytes without an operator pin.
pub fn restore_pinnable_pre_claim_capture_exists(sandbox_alias: &str) -> String {
    let retry_source = restore_retry_source_snapshot_exists(sandbox_alias);
    let order = capture_order::with_pre_claim_capture_order(sandbox_alias);
    let pinnable = pinnable_in_capture_order("sn");
    let unordered = unordered_capture_order();
    let newest_suspend = capture_order::newest("sn", &suspend_captures());
    format!(
        "({sandbox_alias}.current_local_artifact_id IS NULL\n\
          AND NOT {retry_source}\n\
          AND EXISTS (\n\
              {order}\n\
              SELECT 1\n\
                FROM capture_order sn\n\
               WHERE {pinnable}\n\
                 AND ({unordered} OR NOT {newest_suspend})\n\
          ))"
    )
}

/// Returns a boolean SQL fragment: [`restore_pinnable_pre_claim_capture_exists`]
/// holds for the unpinned `restoring` row aliased as `sandbox_alias`, and no
/// full snapshot was registered at or after `lifecycle_changed_at`.
///
/// Reverting `restoring -> suspended` resets `lifecycle_changed_at` and exposes
/// every full snapshot to `restore_pin` and `suspended` selection; when a
/// post-claim full snapshot exists, the sweeper spares the row from `failed`
/// via [`restore_pinnable_pre_claim_capture_exists`] while keeping it in
/// `restoring` so the claim boundary isn't lost.
pub fn restore_retry_pinnable_revert_snapshot_exists(sandbox_alias: &str) -> String {
    let pinnable_pre_claim = restore_pinnable_pre_claim_capture_exists(sandbox_alias);
    let post_before_claim = registered_before_restore_claim(sandbox_alias, "post_claim");
    format!(
        "({pinnable_pre_claim}\n\
          AND NOT EXISTS (\n\
              SELECT 1\n\
                FROM snapshots post_claim\n\
               WHERE post_claim.sandbox_id = {sandbox_alias}.id\n\
                 AND post_claim.type = 'full'\n\
                 AND NOT {post_before_claim}\n\
          ))"
    )
}
