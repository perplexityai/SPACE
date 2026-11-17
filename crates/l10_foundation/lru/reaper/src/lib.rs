//! Generic LRU eviction planner shared by the on-node template cache
//! GC and the snapshot-artifact reaper.
//!
//! The planner is a pure function: given a snapshot of all candidate
//! items plus their disk footprint, the union of currently-pinned
//! keys, the set of keys actively in use, and a disk-budget config,
//! it returns the ordered list of items to delete to bring usage back
//! below `budget_bytes * headroom_fraction`.
//!
//! ## Protected sets
//!
//! An item is **not** evictable when any of these hold:
//!
//! - Its key is in the `pinned` set (the caller's source-of-truth for
//!   "must keep regardless of age" — pinned template tags, the live
//!   `current_local_artifact_id`, an in-flight upload, etc.).
//! - Its key is in the `active` set (something is currently reading
//!   the on-disk artifact — an attached sandbox, an in-progress
//!   restore).
//! - It is still inside its grace period:
//!   `now_secs - max(created_at_secs, last_used_at_secs) < grace_secs`.
//!   The grace period defends against the create/attach race where
//!   the caller has just materialized the item but the corresponding
//!   "I am using this" record has not yet become visible to the
//!   planner's input snapshot.
//!
//! ## Why a pure function
//!
//! The eviction decision is exclusively a function of its inputs.
//! Keeping it side-effect-free lets the eviction *loop* (which has
//! its own DB, disk, and metrics integration per caller) be tested
//! separately from the *policy*, and lets each caller's `execute`
//! phase re-check pin/active membership inside its own transaction
//! to close the planner→delete race window without changing the
//! policy code.

use std::collections::HashSet;
use std::hash::Hash;

/// Input row for the LRU eviction planner. The caller computes
/// `size_bytes`, `created_at_secs`, and `last_used_at_secs` before
/// invoking the planner; the planner itself does not touch the
/// filesystem or any DB.
///
/// `key` is the caller's identifier for the item; the planner uses
/// it only for protected-set membership tests, deterministic
/// tiebreaks, and as the return key on the candidate list. Use
/// `(String, String)` for template `(name, content_hash)` pairs,
/// `LocalArtifactId` for snapshot artifacts, etc.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EvictionInput<K> {
    pub key: K,
    pub size_bytes: u64,
    pub created_at_secs: u64,
    pub last_used_at_secs: u64,
}

/// One item the planner has selected for deletion. Returned in the
/// order the caller should delete in (oldest-LRU first).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EvictionCandidate<K> {
    pub key: K,
    pub size_bytes: u64,
}

/// Planner inputs that don't come from the item set itself.
///
/// `headroom_fraction` is the fraction of `budget_bytes` the planner
/// aims to leave occupied after eviction. With `0.9` and a 100 GB
/// budget, eviction frees bytes until usage ≤ 90 GB — so the next
/// allocation doesn't immediately re-trigger GC at 100% full.
#[derive(Debug, Clone, Copy)]
pub struct EvictionConfig {
    pub budget_bytes: u64,
    pub headroom_fraction: f64,
    pub grace_secs: u64,
    pub now_secs: u64,
}

/// Decide which items to evict. Returns an empty `Vec` when usage is
/// already at or below `budget_bytes` — the caller skips the delete
/// loop. When usage is over budget but every eligible item is
/// pinned/active/in-grace, returns whatever subset can be freed
/// (possibly empty); the caller is expected to log/metric and move
/// on rather than evict a protected item.
///
/// LRU order is by `last_used_at_secs` ascending, tiebroken by the
/// natural `Ord` on `K` so two items with equal timestamps produce a
/// deterministic plan across runs — matters for metrics attribution
/// and for operators replaying eviction sequences from logs.
pub fn plan_evictions<K>(
    items: Vec<EvictionInput<K>>,
    pinned: &HashSet<K>,
    active: &HashSet<K>,
    cfg: &EvictionConfig,
) -> Vec<EvictionCandidate<K>>
where
    K: Clone + Eq + Hash + Ord,
{
    let total: u64 = items.iter().map(|b| b.size_bytes).sum();
    if total <= cfg.budget_bytes {
        return Vec::new();
    }

    let target = headroom_target_bytes(cfg.budget_bytes, cfg.headroom_fraction);
    let need_to_free = total.saturating_sub(target);
    plan_evictions_to_free(
        items,
        pinned,
        active,
        need_to_free,
        cfg.grace_secs,
        cfg.now_secs,
    )
}

fn headroom_target_bytes(budget_bytes: u64, headroom_fraction: f64) -> u64 {
    if !headroom_fraction.is_finite() || headroom_fraction <= 0.0 {
        return 0;
    }
    if headroom_fraction >= 1.0 {
        return budget_bytes;
    }

    #[expect(
        clippy::cast_sign_loss,
        reason = "headroom_fraction is finite and in 0.0..1.0, so the product is non-negative"
    )]
    let target = ((budget_bytes as f64) * headroom_fraction) as u64;
    target
}

/// [`plan_evictions`] with the bytes-to-free computed by the caller —
/// for reclaimers whose trigger is filesystem usage (statvfs
/// fractions) rather than a per-consumer byte budget. The protected
/// sets, grace rule, LRU ordering, and deterministic tiebreak are
/// identical; only the stop condition's source differs. Returns an
/// empty `Vec` when `need_to_free` is 0.
pub fn plan_evictions_to_free<K>(
    mut items: Vec<EvictionInput<K>>,
    pinned: &HashSet<K>,
    active: &HashSet<K>,
    need_to_free: u64,
    grace_secs: u64,
    now_secs: u64,
) -> Vec<EvictionCandidate<K>>
where
    K: Clone + Eq + Hash + Ord,
{
    if need_to_free == 0 {
        return Vec::new();
    }

    items.sort_by(|a, b| {
        a.last_used_at_secs
            .cmp(&b.last_used_at_secs)
            .then_with(|| a.key.cmp(&b.key))
    });

    let mut freed: u64 = 0;
    let mut out = Vec::new();
    for it in items {
        if freed >= need_to_free {
            break;
        }
        if pinned.contains(&it.key) {
            continue;
        }
        if active.contains(&it.key) {
            continue;
        }
        let last_touch = it.created_at_secs.max(it.last_used_at_secs);
        if now_secs.saturating_sub(last_touch) < grace_secs {
            continue;
        }
        freed = freed.saturating_add(it.size_bytes);
        out.push(EvictionCandidate {
            key: it.key,
            size_bytes: it.size_bytes,
        });
    }
    out
}

#[cfg(test)]
mod tests;
