//! LRU eviction loop for the on-node template cache.
//!
//! The pure planner — the `(items, pinned, active, cfg) → candidates`
//! decision — lives in [`pplx_space_common_lru_reaper`] and is shared
//! with the snapshot-artifact reaper in sandbox-manager. This module
//! owns the *template-specific* wrapper: reading `template_builds`,
//! the UNION of `template_pinned_tags` and `template_pinned_releases`,
//! the active-sandbox set, computing per-build disk footprint, and
//! driving the per-candidate `pre-check → disk delete → finalize`
//! transactions that close the planner→delete race window.
//!
//! ## Protected sets for templates
//!
//! A build `(name, content_hash)` is **not** evictable when any of
//! these hold:
//!
//! - It is in the `pinned` set (UNION of `template_pinned_tags` and
//!   `template_pinned_releases` — the same source-of-truth tables the
//!   prefetcher writes; see [`crate::pinned_digests`] and
//!   [`crate::prefetch`]).
//! - It is in the `active` set: every build referenced by a live
//!   instance subvolume on this node, across **all** sandbox owners,
//!   from `sbx-mgr.ListBuildReferencingSandboxes()`. This deliberately
//!   avoids the plain `sbx-mgr.List()` view, which is filtered to
//!   template-manager-owned sandboxes and so hides user sandboxes and
//!   adopted pool sandboxes — the gap that let GC dangle a live user
//!   sandbox's parent build and force suspend onto the Full codec. The
//!   manual delete path
//!   (`ensure_no_dependent_sandboxes_for_template_builds`) reads the
//!   same source. sbx-mgr scopes the set to states that still hold a
//!   parent-referencing instance subvolume.
//! - It is in the `warm_pool` set: the control-plane releases warm
//!   pools currently target (`ClusterApi.ListWarmPoolTemplateReleases`,
//!   fetched once per tick). The local prefetch pins only cover
//!   hardcoded-tag + latest-N releases, and a warm-pool selected
//!   release can sit outside both — without this set such a release
//!   is evictable whenever no warm slot happens to be resident,
//!   turning the next pool refill into a full re-pull.
//! - It is still inside its grace period (planner default 1800 s).
//!   The grace period covers two race windows: freshly-downloaded
//!   builds before any sandbox attaches, and builds the GC just
//!   handed out via `GetTemplate` before sandbox-manager's row
//!   becomes visible to `sbx-mgr.List()`.
//!
//! See the crate-level docs of `pplx-space-common-lru-reaper` for the
//! full rationale on protected sets, headroom semantics, and the
//! deterministic LRU tiebreak.

use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use pplx_space_common_lru_reaper::{self as lru_reaper, EvictionInput};
use rusqlite::{Connection, params};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

pub(crate) use pplx_space_common_lru_reaper::EvictionConfig;

/// Snapshot of one `template_builds` row plus its on-disk size,
/// supplied to the planner. The planner does not read the DB itself.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BuildSnapshot {
    pub(crate) name: String,
    pub(crate) content_hash: String,
    /// Sum of the on-disk artifacts for this build (rootfs + zstd +
    /// metadata + manifest + any per-`(vcpu, memory_mib)` snapshot
    /// pairs). Computed by the caller before invoking the planner.
    pub(crate) size_bytes: u64,
    /// Unix-epoch seconds when the build row was first inserted
    /// (`template_builds.created_at`).
    pub(crate) created_at_secs: u64,
    /// Unix-epoch seconds of the most recent access. Updated whenever
    /// a caller resolves the build (e.g. `GetTemplate`).
    pub(crate) last_used_at_secs: u64,
}

/// One build the planner has selected for deletion. Returned in the
/// order the caller should delete in (oldest-LRU first).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct EvictionCandidate {
    pub(crate) name: String,
    pub(crate) content_hash: String,
    pub(crate) size_bytes: u64,
}

/// Template-side wrapper around [`lru_reaper::plan_evictions`]: maps
/// `BuildSnapshot` → generic input and the generic candidate back to
/// the template-specific shape. Policy lives in the common crate; the
/// caller-specific names live here.
///
/// The `warm_pool` set is unioned into `pinned` before the generic
/// planner runs — both express the same thing at plan time ("an
/// external authority wants this digest to stay materialized"), and
/// keeping the union here means the shared crate stays a two-set
/// planner.
pub(crate) fn plan_evictions(
    builds: Vec<BuildSnapshot>,
    pinned: &HashSet<(String, String)>,
    active: &HashSet<(String, String)>,
    warm_pool: &HashSet<(String, String)>,
    cfg: &EvictionConfig,
) -> Vec<EvictionCandidate> {
    let inputs: Vec<EvictionInput<(String, String)>> = builds
        .into_iter()
        .map(|b| EvictionInput {
            key: (b.name, b.content_hash),
            size_bytes: b.size_bytes,
            created_at_secs: b.created_at_secs,
            last_used_at_secs: b.last_used_at_secs,
        })
        .collect();
    let pinned_union: HashSet<(String, String)> = pinned.union(warm_pool).cloned().collect();
    lru_reaper::plan_evictions(inputs, &pinned_union, active, cfg)
        .into_iter()
        .map(|c| EvictionCandidate {
            name: c.key.0,
            content_hash: c.key.1,
            size_bytes: c.size_bytes,
        })
        .collect()
}

/// Output of [`plan_from_db`]: the candidate list plus the bytes
/// accounting we logged before deciding to evict, so the GC loop can
/// surface a useful "would-have-freed" metric when nothing actually
/// gets deleted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct EvictionPlan {
    pub(crate) candidates: Vec<EvictionCandidate>,
    pub(crate) total_bytes_before: u64,
    pub(crate) projected_bytes_freed: u64,
}

/// Compose the eviction plan from the on-node DB and an externally
/// supplied active-sandbox set. Reads from `template_builds`,
/// `template_pinned_tags`, `template_pinned_releases`, then defers to
/// [`plan_evictions`].
///
/// `disk_size_for` lets the caller charge a build's full on-disk
/// footprint (rootfs + zstd + per-`(vcpu, memory_mib)` snapshot
/// files) instead of only `artifact_size_bytes`. The tests pass an
/// identity function to keep their inputs explicit; production code
/// will hand in the `ArtifactStore::build_disk_usage` helper.
/// Snapshot of one build's DB-side state, gathered under the
/// connection lock and used as the input to the FS-only sizing
/// step. The lock is released before `disk_size_for` is called.
struct PlanDbSnapshot {
    builds: Vec<crate::db::BuildEvictionRow>,
    snapshot_dims: std::collections::HashMap<(String, String), Vec<(u32, u32)>>,
    pinned: HashSet<(String, String)>,
}

/// Read every DB-side input the planner needs in a single locked
/// region: builds, per-build snapshot dimensions (vcpu / memory_mib
/// pairs from `template_snapshots`), and the pinned-keys union. The
/// connection lock is held for the duration of this call and is
/// released as soon as it returns — `disk_size_for` runs under the
/// caller's stackframe, NOT under the lock.
fn read_plan_db_snapshot(conn: &Connection) -> rusqlite::Result<PlanDbSnapshot> {
    let builds = crate::db::list_eviction_candidates(conn)?;
    let mut snapshot_dims: std::collections::HashMap<(String, String), Vec<(u32, u32)>> =
        std::collections::HashMap::with_capacity(builds.len());
    for b in &builds {
        let dims: Vec<(u32, u32)> = crate::db::list_snapshots(conn, &b.name, &b.content_hash)?
            .into_iter()
            .map(|s| (s.vcpu, s.memory_mib))
            .collect();
        snapshot_dims.insert((b.name.clone(), b.content_hash.clone()), dims);
    }
    let pinned: HashSet<(String, String)> = crate::db::list_pinned_build_keys(conn)?
        .into_iter()
        .collect();
    Ok(PlanDbSnapshot {
        builds,
        snapshot_dims,
        pinned,
    })
}

/// **Test-only one-call wrapper** for the existing tests that drive
/// the planner against an in-memory DB without splitting the read
/// from the size computation. Production goes through
/// `read_plan_db_snapshot` then `plan_from_snapshot` so the lock
/// can be released between them — `disk_size_for` may take the
/// same mutex in production. Tests pass identity closures so the
/// single-call form stays sound for them.
#[cfg(test)]
pub(crate) fn plan_from_db(
    conn: &Connection,
    active: &HashSet<(String, String)>,
    warm_pool: &HashSet<(String, String)>,
    cfg: &EvictionConfig,
    disk_size_for: impl Fn(&str, &str, u64, &[(u32, u32)]) -> u64,
) -> rusqlite::Result<EvictionPlan> {
    let db_snapshot = read_plan_db_snapshot(conn)?;
    plan_from_snapshot(db_snapshot, active, warm_pool, cfg, disk_size_for)
}

/// Compose the eviction plan from a previously-read
/// [`PlanDbSnapshot`] (no DB access). Split out from `plan_from_db`
/// so callers that need to release the DB lock before computing
/// sizes can do so explicitly — `gc_tick` is the production caller
/// that follows that path.
fn plan_from_snapshot(
    db_snapshot: PlanDbSnapshot,
    active: &HashSet<(String, String)>,
    warm_pool: &HashSet<(String, String)>,
    cfg: &EvictionConfig,
    disk_size_for: impl Fn(&str, &str, u64, &[(u32, u32)]) -> u64,
) -> rusqlite::Result<EvictionPlan> {
    let PlanDbSnapshot {
        builds: builds_rows,
        snapshot_dims,
        pinned,
    } = db_snapshot;

    let builds: Vec<BuildSnapshot> = builds_rows
        .into_iter()
        .map(|r| {
            let key = (r.name.clone(), r.content_hash.clone());
            let dims = snapshot_dims.get(&key).map(Vec::as_slice).unwrap_or(&[]);
            let size = disk_size_for(&r.name, &r.content_hash, r.artifact_size_bytes, dims);
            BuildSnapshot {
                name: r.name,
                content_hash: r.content_hash,
                size_bytes: size,
                created_at_secs: r.created_at_secs,
                last_used_at_secs: r.last_used_at_secs,
            }
        })
        .collect();
    let total_bytes_before: u64 = builds.iter().map(|b| b.size_bytes).sum();

    let candidates = plan_evictions(builds, &pinned, active, warm_pool, cfg);
    let projected_bytes_freed = candidates.iter().map(|c| c.size_bytes).sum();

    Ok(EvictionPlan {
        candidates,
        total_bytes_before,
        projected_bytes_freed,
    })
}

/// Why a single eviction did or didn't happen. The loop's metrics tag
/// every per-candidate outcome with one of these labels.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum EvictionOutcome {
    /// Disk artifacts were removed and the DB row was deleted.
    Deleted,
    /// A pin row appeared between plan and execute. The candidate is
    /// dropped; no disk or DB mutation occurred.
    SkippedPinnedAtExecute,
    /// The active-sandbox snapshot now contains this build. The
    /// candidate is dropped; no disk or DB mutation occurred.
    SkippedActiveAtExecute,
    /// The warm-pool protected set (control-plane releases warm pools
    /// target, snapshotted at tick start) contains this build. The
    /// candidate is dropped; no disk or DB mutation occurred. Only
    /// reachable when the set changed between plan and execute within
    /// one tick — plan-time filtering uses the same snapshot.
    SkippedWarmPoolAtExecute,
    /// The build row was already gone by the time we tried to delete
    /// it (e.g. a manual `DeleteTemplateBuild` raced us). No work to
    /// do; not an error.
    SkippedRowMissing,
}

/// Outcome of the pre-check phase of `execute_one_async`. Either
/// "proceed to disk delete" or one of the early-exit reasons.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PrecheckOutcome {
    Proceed,
    SkipPinned,
    SkipActive,
    SkipWarmPool,
    SkipRowMissing,
}

/// Pre-check phase: active-set + pin check, then atomically transition
/// `state='ready' → 'deleting'` on the build row. Runs in an IMMEDIATE
/// txn so:
///   1. A concurrent pin write is serialized against us (re-checked
///      via SQL).
///   2. The state transition is a compare-and-swap: if the row was
///      already deleted (concurrent manual `DeleteTemplateBuild`) or
///      already marked deleting (concurrent eviction tick we lost
///      a race to), the UPDATE affects 0 rows and we skip.
///   3. After this txn commits, any concurrent `resolve_template`
///      reader sees `state='deleting'` and treats the build as
///      not-resolvable — closing the GC-vs-sandbox-create race
///      where a sandbox could be issued against a build that was
///      already mid-eviction.
///
/// With `touched_before_secs`, the swap also requires that the build was
/// neither created nor used after that bound, so a `GetTemplate` that handed
/// the build to a sandbox create after the caller's own check keeps it.
fn execute_precheck(
    conn: &Connection,
    active: &HashSet<(String, String)>,
    warm_pool: &HashSet<(String, String)>,
    candidate: &EvictionCandidate,
    touched_before_secs: Option<u64>,
) -> rusqlite::Result<PrecheckOutcome> {
    let key = (candidate.name.clone(), candidate.content_hash.clone());
    if active.contains(&key) {
        return Ok(PrecheckOutcome::SkipActive);
    }
    if warm_pool.contains(&key) {
        return Ok(PrecheckOutcome::SkipWarmPool);
    }
    let tx = rusqlite::Transaction::new_unchecked(conn, rusqlite::TransactionBehavior::Immediate)?;
    let pinned: i64 = tx.query_row(
        "SELECT EXISTS(
             SELECT 1 FROM template_pinned_tags WHERE name = ?1 AND content_hash = ?2
             UNION ALL
             SELECT 1 FROM template_pinned_releases WHERE name = ?1 AND content_hash = ?2
         )",
        params![candidate.name, candidate.content_hash],
        |row| row.get(0),
    )?;
    if pinned != 0 {
        return Ok(PrecheckOutcome::SkipPinned);
    }

    // Compare-and-swap: only proceed when the row is still
    // `state='ready'`. A zero-row UPDATE means the row was deleted
    // out from under us OR another path (manual delete, parallel
    // eviction) already grabbed it.
    let updated = tx.execute(
        "UPDATE template_builds SET state = 'deleting'
         WHERE name = ?1 AND content_hash = ?2 AND state = 'ready'
           AND (?3 IS NULL
                OR MAX(CAST(created_at AS INTEGER), CAST(last_used_at AS INTEGER)) <= ?3)",
        params![
            candidate.name,
            candidate.content_hash,
            touched_before_secs.map(|secs| i64::try_from(secs).unwrap_or(i64::MAX)),
        ],
    )?;
    if updated == 0 {
        return Ok(PrecheckOutcome::SkipRowMissing);
    }

    tx.commit()?;
    Ok(PrecheckOutcome::Proceed)
}

/// Finalize phase: delete the snapshot rows and the build row in a
/// short IMMEDIATE txn. The disk delete has already completed; if a
/// pin landed between phases the disk is gone and the next pull
/// will re-materialize (pin's job is to keep the digest available,
/// not to keep this exact row). Mirror the existing `delete_build`
/// ordering: snapshots first (FK-bound) then the build row.
fn execute_finalize(conn: &Connection, candidate: &EvictionCandidate) -> rusqlite::Result<()> {
    let tx = rusqlite::Transaction::new_unchecked(conn, rusqlite::TransactionBehavior::Immediate)?;
    tx.execute(
        "DELETE FROM template_snapshots WHERE name = ?1 AND content_hash = ?2",
        params![candidate.name, candidate.content_hash],
    )?;
    tx.execute(
        "DELETE FROM template_builds WHERE name = ?1 AND content_hash = ?2",
        params![candidate.name, candidate.content_hash],
    )?;
    tx.commit()
}

/// **Test-only synchronous shim** kept for the existing unit tests
/// that drive `execute_one` against an in-memory DB without going
/// through `gc_tick`. The async production path uses
/// `execute_one_async`, which releases the DB lock around the disk
/// delete. New
/// tests should target `execute_one_async`; this shim covers the
/// race-recheck cases that don't need the async release.
#[cfg(test)]
pub(crate) fn execute_one(
    conn: &Connection,
    active: &HashSet<(String, String)>,
    warm_pool: &HashSet<(String, String)>,
    candidate: &EvictionCandidate,
    delete_disk: impl FnOnce(&str, &str) -> rusqlite::Result<()>,
) -> rusqlite::Result<EvictionOutcome> {
    match execute_precheck(conn, active, warm_pool, candidate, None)? {
        PrecheckOutcome::SkipPinned => return Ok(EvictionOutcome::SkippedPinnedAtExecute),
        PrecheckOutcome::SkipActive => return Ok(EvictionOutcome::SkippedActiveAtExecute),
        PrecheckOutcome::SkipWarmPool => return Ok(EvictionOutcome::SkippedWarmPoolAtExecute),
        PrecheckOutcome::SkipRowMissing => return Ok(EvictionOutcome::SkippedRowMissing),
        PrecheckOutcome::Proceed => {}
    }
    delete_disk(&candidate.name, &candidate.content_hash)?;
    execute_finalize(conn, candidate)?;
    Ok(EvictionOutcome::Deleted)
}

/// Production execution path: pre-check txn → async disk delete
/// (DB lock released) → finalize txn. The disk delete may take
/// seconds on multi-GB rootfs+zstd; releasing the lock around it
/// means other tokio tasks (prefetch writes, `resolve_template` on
/// the sandbox-create hot path) keep making progress while we
/// delete.
///
/// Race window: between the pre-check and the finalize a pin could
/// land. The disk is already gone at finalize time, so we DO delete
/// the row anyway — the build is unrecoverable from disk and the
/// pin's intent ("digest should be available") is honored by the
/// next prefetch tick re-pulling. The row delete is monotonic;
/// leaving it would orphan a row with no rootfs.
pub(crate) async fn execute_one_async(
    db: Arc<Mutex<Connection>>,
    active: HashSet<(String, String)>,
    warm_pool: HashSet<(String, String)>,
    candidate: EvictionCandidate,
    delete_disk: DeleteDiskFn,
) -> rusqlite::Result<EvictionOutcome> {
    execute_one_async_touched_before(db, active, warm_pool, candidate, delete_disk, None).await
}

/// [`execute_one_async`], with the pre-check's optional touch bound.
async fn execute_one_async_touched_before(
    db: Arc<Mutex<Connection>>,
    active: HashSet<(String, String)>,
    warm_pool: HashSet<(String, String)>,
    candidate: EvictionCandidate,
    delete_disk: DeleteDiskFn,
    touched_before_secs: Option<u64>,
) -> rusqlite::Result<EvictionOutcome> {
    // Phase 1: pre-check in a short locked region.
    let cand_for_precheck = candidate.clone();
    let active_for_precheck = active.clone();
    let warm_pool_for_precheck = warm_pool.clone();
    let db_for_precheck = Arc::clone(&db);
    let precheck = tokio::task::spawn_blocking(move || {
        let conn = db_for_precheck
            .lock()
            .map_err(|_| crate::db::mutex_poison_error("template GC precheck"))?;
        execute_precheck(
            &conn,
            &active_for_precheck,
            &warm_pool_for_precheck,
            &cand_for_precheck,
            touched_before_secs,
        )
    })
    .await
    .map_err(|e| {
        rusqlite::Error::ToSqlConversionFailure(Box::new(std::io::Error::other(format!(
            "precheck spawn_blocking: {e}"
        ))))
    })??;

    match precheck {
        PrecheckOutcome::SkipPinned => return Ok(EvictionOutcome::SkippedPinnedAtExecute),
        PrecheckOutcome::SkipActive => return Ok(EvictionOutcome::SkippedActiveAtExecute),
        PrecheckOutcome::SkipWarmPool => return Ok(EvictionOutcome::SkippedWarmPoolAtExecute),
        PrecheckOutcome::SkipRowMissing => return Ok(EvictionOutcome::SkippedRowMissing),
        PrecheckOutcome::Proceed => {}
    }

    // Phase 2: disk delete with NO DB lock held. This is the
    // potentially-slow step (multi-GB delete on EBS-backed storage)
    // — keeping the lock around it would block prefetch, sandbox-
    // create, and every other DB consumer for the duration.
    delete_disk(&candidate.name, &candidate.content_hash).await?;

    // Phase 3: finalize in a short locked region.
    let cand_for_finalize = candidate.clone();
    let db_for_finalize = Arc::clone(&db);
    tokio::task::spawn_blocking(move || {
        let conn = db_for_finalize
            .lock()
            .map_err(|_| crate::db::mutex_poison_error("template GC finalize"))?;
        execute_finalize(&conn, &cand_for_finalize)
    })
    .await
    .map_err(|e| {
        rusqlite::Error::ToSqlConversionFailure(Box::new(std::io::Error::other(format!(
            "finalize spawn_blocking: {e}"
        ))))
    })??;

    Ok(EvictionOutcome::Deleted)
}

/// Serializes evictions that run outside the GC loop with the passes that
/// repair rows an interrupted eviction left in `deleting`. Those passes (the
/// GC tick's [`reap_stuck_deleting`] and startup recovery) put a `deleting`
/// row whose artifacts are intact back to `ready`, which is right only when
/// no eviction is still deleting it. The GC loop holds the gate from before
/// it fetches its protected sets to the end of each tick, so its own
/// evictions never overlap its repair pass, and a wait for the gate cannot
/// age those sets.
#[derive(Clone, Debug, Default)]
pub struct EvictionGate(Arc<tokio::sync::Mutex<()>>);

impl EvictionGate {
    pub(crate) async fn hold(&self) -> tokio::sync::MutexGuard<'_, ()> {
        self.0.lock().await
    }

    fn try_hold(&self) -> Option<tokio::sync::MutexGuard<'_, ()>> {
        self.0.try_lock().ok()
    }
}

/// The disk side of an eviction outside the GC loop: how to delete a build's
/// files, and how a repair pass tells whether they are still there.
#[derive(Clone)]
pub(crate) struct EvictionDisk {
    pub(crate) delete: DeleteDiskFn,
    pub(crate) probe: ReaperDiskProbeFn,
}

/// What [`evict_idle_template`] did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum IdleTemplateEviction {
    /// Every ready build was evicted, then the template's remaining rows.
    /// A build recorded meanwhile, by a pull, makes this `Kept` instead.
    Evicted,
    /// The template has no ready build.
    NotFound,
    /// Another eviction or repair pass holds the gate; nothing was read.
    Busy,
    /// A build stayed. Builds evicted before it stay evicted.
    Kept {
        evicted: usize,
        reason: IdleTemplateKept,
    },
}

/// Why [`evict_idle_template`] kept a build.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum IdleTemplateKept {
    /// It was created or used after the bound.
    Recent,
    Pinned,
    /// A local sandbox references it.
    Active,
    /// A warm pool targets it.
    WarmPool,
    /// Its row was used, removed or claimed by another delete after the
    /// check, or a pull recorded a new build of the template meanwhile.
    Changed,
}

impl std::fmt::Display for IdleTemplateKept {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Recent => "a build was created or used within the idle period",
            Self::Pinned => "a build is pinned",
            Self::Active => "a local sandbox uses a build",
            Self::WarmPool => "a warm pool targets a build",
            Self::Changed => "a build changed during the delete",
        })
    }
}

/// Evict every ready build of `name` the way the GC evicts one, but only
/// while none was created or used after `touched_before_secs`. Each build's
/// compare-and-swap checks the bound again, which closes the window between a
/// `GetTemplate` handing the build to a sandbox create and sandbox-manager
/// recording that sandbox: the touch keeps the build. Once no build is left,
/// the template's pool, settings and snapshot rows go too, as
/// `DeleteTemplate` removes them.
///
/// Returns [`IdleTemplateEviction::Busy`] instead of waiting while the GC loop
/// or another eviction holds `gate`. A build whose disk delete or final row
/// delete fails is repaired before returning, as the GC's next tick would:
/// back to `ready` when its artifacts are intact, dropped otherwise.
pub(crate) async fn evict_idle_template(
    db: Arc<Mutex<Connection>>,
    gate: &EvictionGate,
    name: &str,
    touched_before_secs: u64,
    active: HashSet<(String, String)>,
    warm_pool: HashSet<(String, String)>,
    disk: EvictionDisk,
) -> rusqlite::Result<IdleTemplateEviction> {
    // Held until the last build is finalized, so no repair pass sees this
    // eviction's `deleting` rows as abandoned.
    let Some(_evicting) = gate.try_hold() else {
        return Ok(IdleTemplateEviction::Busy);
    };
    let builds = {
        let db = Arc::clone(&db);
        let name = name.to_string();
        tokio::task::spawn_blocking(move || {
            let conn = db
                .lock()
                .map_err(|_| crate::db::mutex_poison_error("idle template eviction"))?;
            Ok::<_, rusqlite::Error>(
                crate::db::list_eviction_candidates(&conn)?
                    .into_iter()
                    .filter(|build| build.name == name)
                    .collect::<Vec<_>>(),
            )
        })
        .await
        .map_err(|e| {
            rusqlite::Error::ToSqlConversionFailure(Box::new(std::io::Error::other(format!(
                "idle template eviction spawn_blocking: {e}"
            ))))
        })??
    };
    if builds.is_empty() {
        return Ok(IdleTemplateEviction::NotFound);
    }
    if builds
        .iter()
        .any(|build| build.created_at_secs.max(build.last_used_at_secs) > touched_before_secs)
    {
        return Ok(IdleTemplateEviction::Kept {
            evicted: 0,
            reason: IdleTemplateKept::Recent,
        });
    }

    let mut evicted = 0;
    for build in builds {
        let candidate = EvictionCandidate {
            name: build.name,
            content_hash: build.content_hash,
            size_bytes: build.artifact_size_bytes,
        };
        let outcome = execute_one_async_touched_before(
            Arc::clone(&db),
            active.clone(),
            warm_pool.clone(),
            candidate,
            Arc::clone(&disk.delete),
            Some(touched_before_secs),
        )
        .await;
        let outcome = match outcome {
            Ok(outcome) => outcome,
            Err(error) => {
                // Still holding the gate, so every `deleting` row is ours
                // or abandoned. Don't leave this one for a GC loop that may
                // be disabled.
                reap_stuck_deleting(Arc::clone(&db), &disk.probe).await;
                return Err(error);
            }
        };
        let reason = match outcome {
            EvictionOutcome::Deleted => {
                evicted += 1;
                continue;
            }
            EvictionOutcome::SkippedPinnedAtExecute => IdleTemplateKept::Pinned,
            EvictionOutcome::SkippedActiveAtExecute => IdleTemplateKept::Active,
            EvictionOutcome::SkippedWarmPoolAtExecute => IdleTemplateKept::WarmPool,
            EvictionOutcome::SkippedRowMissing => IdleTemplateKept::Changed,
        };
        return Ok(IdleTemplateEviction::Kept { evicted, reason });
    }

    let name = name.to_string();
    let removed = tokio::task::spawn_blocking(move || {
        let conn = db
            .lock()
            .map_err(|_| crate::db::mutex_poison_error("idle template eviction"))?;
        crate::db::delete_template_local_state_without_builds(&conn, &name)
    })
    .await
    .map_err(|e| {
        rusqlite::Error::ToSqlConversionFailure(Box::new(std::io::Error::other(format!(
            "idle template eviction spawn_blocking: {e}"
        ))))
    })??;
    Ok(if removed {
        IdleTemplateEviction::Evicted
    } else {
        IdleTemplateEviction::Kept {
            evicted,
            reason: IdleTemplateKept::Changed,
        }
    })
}

/// Charge a build's full disk footprint (digest dir + snapshots).
/// The third argument is the stored `artifact_size_bytes` (used only
/// as a fallback when on-disk reads fail); the fourth is the list of
/// `(vcpu, memory_mib)` snapshot pairs the planner already read from
/// `template_snapshots` — passed in so the closure does NOT need to
/// hit the DB itself. Hitting the DB here would deadlock the
/// non-reentrant `Arc<Mutex<Connection>>` the surrounding
/// `spawn_blocking` already holds.
pub(crate) type DiskSizeFn = Arc<dyn Fn(&str, &str, u64, &[(u32, u32)]) -> u64 + Send + Sync>;

/// Remove a build's on-disk artifacts. Returns a `Future` so the
/// production closure (which RPCs to node-mgr for the btrfs subvol
/// delete) can be called from `execute_one_async` *without* the DB
/// lock held — that's the structural property the DB-lock-release
/// fix depends on. The returned `rusqlite::Result` is a
/// convenience carrier so the future plugs cleanly into
/// `execute_one_async`; production wraps the underlying io::Error
/// or RPC error inside a synthetic SQLITE_IOERR.
pub(crate) type DeleteDiskFn = Arc<
    dyn Fn(
            &str,
            &str,
        )
            -> std::pin::Pin<Box<dyn std::future::Future<Output = rusqlite::Result<()>> + Send>>
        + Send
        + Sync,
>;

/// Snapshot the set of `(template, content_hash)` pairs currently in
/// use by any sandbox on this node. Returns `Err(_)` to short-circuit
/// the GC tick — without a live active set we cannot safely evict.
pub type FetchActiveFn = Arc<
    dyn Fn() -> std::pin::Pin<
            Box<dyn std::future::Future<Output = Result<HashSet<(String, String)>, String>> + Send>,
        > + Send
        + Sync,
>;

/// Snapshot the `(template, content_hash)` releases warm pools
/// currently target, from the control plane
/// (`ClusterApi.ListWarmPoolTemplateReleases`). Same fail-closed
/// contract as [`FetchActiveFn`]: `Err(_)` short-circuits the tick,
/// because evicting without this set can delete a release the next
/// pool refill depends on.
pub type FetchWarmPoolFn = FetchActiveFn;

/// What [`reap_stuck_deleting`] observed on disk for a row stuck in
/// `state='deleting'` from a prior tick. Drives the recovery choice:
/// reset to `'ready'` (bytes intact, build is usable) or drop the row
/// (bytes gone, build is unrecoverable and `do_pull` will re-fetch on
/// the next access).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReaperDiskState {
    /// The digest dir is present with the manifest-declared
    /// `rootfs.ext4` still on disk. Phase 2 errored before any bytes
    /// were wiped (nodemgr RPC unreachable, gRPC deadline exceeded,
    /// etc.). The row goes back to `'ready'` and the next planner
    /// pass either re-evicts it or hands it to a resolver.
    Intact,
    /// The digest dir is gone or `rootfs.ext4` is missing. Phase 2
    /// partially succeeded — contents wiped — but Phase 3 didn't
    /// commit (typically the post-cleanup `rmdir` on a btrfs subvol
    /// root failed and surfaced as Err). The row is dropped; a future
    /// `do_pull` re-fetches via the existing `digest_dir_existed`
    /// path that re-uses the leftover subvol root.
    Missing,
}

/// Probe closure used by [`reap_stuck_deleting`] to decide between
/// reset-to-ready and drop-row for each stuck row. Production wires
/// this to a `TemplateArtifactStore::build_artifacts_missing`-style
/// check; tests pin the answer.
pub(crate) type ReaperDiskProbeFn = Arc<
    dyn Fn(
            &str,
            &str,
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ReaperDiskState> + Send>>
        + Send
        + Sync,
>;

/// At the start of every tick, resolve any rows left in
/// `state='deleting'` from a prior tick that errored between Phase 1
/// (CAS commit) and Phase 3 (row delete).
///
/// The recovery choice depends on on-disk state, mirroring the
/// startup-recovery path (`prune_catalog_rows_missing_on_disk` in
/// `service/startup_recovery.rs`) so the online and offline recovery
/// stories converge on the same invariants:
///
/// - **Disk intact** ⇒ reset row to `'ready'`. The next planner pass
///   replans the build for eviction; if pins or sandboxes attached
///   while it was stuck, the planner's filters keep it alive.
/// - **Disk missing** ⇒ drop the row. `resolve_template` reports
///   NotFound; `do_pull` on the next access writes fresh bytes into
///   the leftover btrfs subvol root via its existing
///   `digest_dir_existed` adoption path.
///
/// **Race safety:** called at the start of `gc_tick`, before any
/// candidate enters `execute_precheck`. `execute_precheck`'s Phase 1 CAS
/// is the only writer of `state='deleting'`, and its callers outside the
/// tick (`evict_idle_template`) hold the [`EvictionGate`] that
/// `run_loop` holds around each tick, so any row in that state at
/// start-of-tick is from a prior tick, an interrupted eviction or a
/// crash, not from an eviction still in flight. Tests model this
/// directly by planting `'deleting'` before invoking `gc_tick`.
pub(crate) async fn reap_stuck_deleting(
    db: Arc<Mutex<Connection>>,
    reaper_probe: &ReaperDiskProbeFn,
) -> usize {
    let db_for_query = Arc::clone(&db);
    let stuck =
        match tokio::task::spawn_blocking(move || -> rusqlite::Result<Vec<(String, String)>> {
            let conn = db_for_query
                .lock()
                .map_err(|_| crate::db::mutex_poison_error("template GC reaper query"))?;
            let mut stmt = conn.prepare(
                "SELECT name, content_hash FROM template_builds WHERE state = 'deleting'",
            )?;
            let rows = stmt.query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })?;
            rows.collect()
        })
        .await
        {
            Ok(Ok(rows)) => rows,
            Ok(Err(err)) => {
                warn!(error = %err, "template GC reaper: select stuck rows failed");
                return 0;
            }
            Err(join_err) => {
                warn!(error = %join_err, "template GC reaper: select task panicked");
                return 0;
            }
        };

    let mut handled = 0usize;
    for (name, content_hash) in stuck {
        let state = reaper_probe(&name, &content_hash).await;
        let db_for_update = Arc::clone(&db);
        let n = name.clone();
        let h = content_hash.clone();
        let update_result = tokio::task::spawn_blocking(move || -> rusqlite::Result<()> {
            let conn = db_for_update
                .lock()
                .map_err(|_| crate::db::mutex_poison_error("template GC reaper update"))?;
            match state {
                ReaperDiskState::Intact => {
                    conn.execute(
                        "UPDATE template_builds SET state = 'ready'
                         WHERE name = ?1 AND content_hash = ?2 AND state = 'deleting'",
                        params![n, h],
                    )?;
                }
                ReaperDiskState::Missing => {
                    let tx = rusqlite::Transaction::new_unchecked(
                        &conn,
                        rusqlite::TransactionBehavior::Immediate,
                    )?;
                    tx.execute(
                        "DELETE FROM template_snapshots WHERE name = ?1 AND content_hash = ?2",
                        params![n, h],
                    )?;
                    tx.execute(
                        "DELETE FROM template_builds
                         WHERE name = ?1 AND content_hash = ?2 AND state = 'deleting'",
                        params![n, h],
                    )?;
                    tx.commit()?;
                }
            }
            Ok(())
        })
        .await;

        match update_result {
            Ok(Ok(())) => {
                handled += 1;
                info!(
                    template = %name,
                    content_hash = %content_hash,
                    resolution = match state {
                        ReaperDiskState::Intact => "reset_to_ready",
                        ReaperDiskState::Missing => "drop_row",
                    },
                    "template GC reaper: resolved stuck-deleting row",
                );
                pplx_space_metrics::template::gc_reaper_resolution(match state {
                    ReaperDiskState::Intact => "reset_to_ready",
                    ReaperDiskState::Missing => "drop_row",
                });
            }
            Ok(Err(err)) => {
                warn!(
                    template = %name,
                    content_hash = %content_hash,
                    error = %err,
                    "template GC reaper: update failed; row will be retried next tick",
                );
            }
            Err(join_err) => {
                warn!(
                    template = %name,
                    content_hash = %content_hash,
                    error = %join_err,
                    "template GC reaper: update task panicked",
                );
            }
        }
    }
    handled
}

/// Reaper probe used by [`gc_tick`]'s existing call sites and by tests
/// that don't exercise the reap-stuck-deleting path. Always reports
/// `Intact`, which is a no-op on rows in `state='ready'` (the existing
/// behavior) and a "reset to ready" on any `'deleting'` row a test
/// happens to plant — neither of which affects assertions that don't
/// touch `state='deleting'` row recovery.
#[cfg(test)]
pub(crate) fn noop_reaper_probe() -> ReaperDiskProbeFn {
    Arc::new(|_, _| Box::pin(async { ReaperDiskState::Intact }))
}

/// Test stand-in for the warm-pool protected-set fetcher: no warm
/// pools. Mirrors the inline `fetch_active` stubs the run_loop tests
/// build.
#[cfg(test)]
pub(crate) fn empty_warm_pool_fetch() -> FetchWarmPoolFn {
    Arc::new(|| Box::pin(async { Ok(HashSet::new()) }))
}

/// Runtime knobs for the on-node template GC loop.
///
/// Env vars are read once at daemon startup via [`GcConfig::from_env`]. The
/// loop is enabled with a [`GcConfig::DEFAULT_BUDGET_BYTES`] cache budget;
/// `SPACE_TEMPLATE_CACHE_BUDGET_BYTES` overrides the budget, and an explicit
/// `0` disables the loop (it then spawns, logs once, and returns without ever
/// reading from the DB or touching disk), as does a value that is not a byte
/// count (with a warning). The loop only reclaims once the node
/// is over budget and never within the grace period.
#[derive(Clone, Debug)]
pub struct GcConfig {
    pub enabled: bool,
    pub budget_bytes: u64,
    pub headroom_fraction: f64,
    pub interval: Duration,
    pub grace: Duration,
}

impl GcConfig {
    pub(crate) const ENV_BUDGET: &'static str = "SPACE_TEMPLATE_CACHE_BUDGET_BYTES";
    /// Test hooks (`pplx_space_common_core::env::test_hook`): compiled in only
    /// with the `test-hooks` feature, so release binaries cannot shrink the
    /// GC timers. Smoke benches use them to make eviction observable.
    pub(crate) const ENV_INTERVAL_SECS: &'static str = "SPACE_TEMPLATE_GC_INTERVAL_SECS";
    pub(crate) const ENV_GRACE_SECS: &'static str = "SPACE_TEMPLATE_GC_GRACE_SECS";

    /// 100 GiB: a ceiling that is never reached on a lightly used node, so
    /// the loop is a no-op there, while a node that accumulates builds still
    /// reclaims LRU builds instead of filling its disk.
    pub(crate) const DEFAULT_BUDGET_BYTES: u64 = 100 * 1024 * 1024 * 1024;
    const DEFAULT_INTERVAL_SECS: u64 = 300;
    /// 30 minutes. Wider than the worst-case window between
    /// `GetTemplate` returning a hash and sbx-mgr's row becoming
    /// visible to `List()`, and wide enough to keep prefetch from
    /// churning a freshly downloaded build.
    const DEFAULT_GRACE_SECS: u64 = 1800;
    /// Floor below which the daemon logs a loud warning at startup.
    /// Grace is the only correctness backstop for the GC-vs-prefetch
    /// and GC-vs-sandbox-create races; setting it below 60s in
    /// production risks evicting a build before sandbox-manager's
    /// row becomes visible to `List()`. The integration test
    /// `tmplmgr-eviction-btrfs.sh` uses 0 deliberately to make
    /// eviction deterministic — production never should.
    const SAFETY_GRACE_FLOOR_SECS: u64 = 60;
    /// Fraction of the budget a reclaim pass shrinks the cache to, so one
    /// pass buys room for new builds instead of evicting on every tick.
    const HEADROOM_FRACTION: f64 = 0.9;

    pub fn from_env() -> Self {
        Self::from_environment(&pplx_space_common_core::env::ProcessEnvironment)
    }

    pub(crate) fn from_environment(env: &dyn pplx_space_common_core::env::Environment) -> Self {
        let budget = match env.var(Self::ENV_BUDGET).ok() {
            None => Some(Self::DEFAULT_BUDGET_BYTES),
            Some(raw) => match raw.trim().parse::<u64>() {
                Ok(n) => Some(n).filter(|n| *n > 0),
                // The loop deletes cached builds, so a value that is not a
                // byte count disables it instead of silently applying the
                // default budget.
                Err(_) => {
                    tracing::warn!(
                        env_var = Self::ENV_BUDGET,
                        value = %raw,
                        "invalid template cache budget; template GC is disabled \
                         (set a byte count, or unset the variable for the default)",
                    );
                    None
                }
            },
        };
        let enabled = budget.is_some();
        let interval_secs = pplx_space_common_core::env::test_hook(env, Self::ENV_INTERVAL_SECS)
            .and_then(|s| s.parse::<u64>().ok())
            .filter(|n| *n > 0)
            .unwrap_or(Self::DEFAULT_INTERVAL_SECS);
        let grace_secs = pplx_space_common_core::env::test_hook(env, Self::ENV_GRACE_SECS)
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(Self::DEFAULT_GRACE_SECS);
        if enabled && grace_secs < Self::SAFETY_GRACE_FLOOR_SECS {
            tracing::warn!(
                grace_secs,
                floor_secs = Self::SAFETY_GRACE_FLOOR_SECS,
                env_var = Self::ENV_GRACE_SECS,
                "template GC grace period is below the safety floor — \
                 builds may be evicted before sandbox-manager's row becomes visible to List(). \
                 Use this setting only in tests; production should leave it at the default (1800s).",
            );
        }
        Self {
            enabled,
            budget_bytes: budget.unwrap_or(0),
            headroom_fraction: Self::HEADROOM_FRACTION,
            interval: Duration::from_secs(interval_secs),
            grace: Duration::from_secs(grace_secs),
        }
    }
}

/// Per-tick summary surfaced as a metric. Returned for logging /
/// future metrics emission. Not load-bearing for correctness.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct GcTickReport {
    pub(crate) candidates_planned: usize,
    pub(crate) deleted: usize,
    pub(crate) skipped_pinned: usize,
    pub(crate) skipped_active: usize,
    pub(crate) skipped_warm_pool: usize,
    pub(crate) skipped_row_missing: usize,
    pub(crate) errors: usize,
    pub(crate) bytes_before: u64,
    pub(crate) bytes_freed: u64,
}

/// Why one GC tick failed as a whole. Per-candidate failures are counted in
/// its report instead.
#[derive(Debug, thiserror::Error)]
pub(crate) enum GcTickError {
    #[error("plan spawn_blocking: {0}")]
    Task(#[source] tokio::task::JoinError),
    #[error("read_plan_db_snapshot: {0}")]
    PlanSnapshot(#[source] rusqlite::Error),
    #[error("plan_from_snapshot: {0}")]
    Plan(#[source] rusqlite::Error),
}

impl GcTickError {
    fn metric_reason(&self) -> pplx_space_metrics::template::TemplateGcTickFailure {
        use pplx_space_metrics::template::TemplateGcTickFailure as Failure;
        match self {
            Self::Task(_) => Failure::Task,
            Self::PlanSnapshot(_) => Failure::PlanSnapshot,
            Self::Plan(_) => Failure::Plan,
        }
    }
}

/// Run one tick of the GC loop. Pure with respect to its callbacks —
/// the only external side effects are whatever `disk_size_for` and
/// `delete_disk_for` perform. The DB connection is shared via
/// `Arc<Mutex<>>` so the SQL inside [`plan_from_db`] and
/// [`execute_one`] serializes against the rest of the daemon.
///
/// The blocking work (DB reads, fs `metadata()` calls inside
/// `disk_size_for`) is hopped onto a `spawn_blocking` thread so this
/// future stays cooperative. `active` is captured at tick-start by
/// the caller (typically by calling `sbx-mgr.List()` once per tick)
/// and consulted in two places:
///
/// 1. Inside [`plan_evictions`], to filter the candidate set.
/// 2. Inside [`execute_one`], as a per-candidate re-check before the
///    delete commits.
///
/// Grace-period protection (via `cfg.grace`) is the real defense
/// against the GC-vs-sandbox-create race; this snapshot is a
/// best-effort additional filter.
pub(crate) async fn gc_tick(
    db: Arc<Mutex<Connection>>,
    cfg: &GcConfig,
    active: HashSet<(String, String)>,
    warm_pool: HashSet<(String, String)>,
    disk_size_for: DiskSizeFn,
    delete_disk_for: DeleteDiskFn,
    reaper_probe: ReaperDiskProbeFn,
) -> Result<GcTickReport, GcTickError> {
    let now_secs = jiff::Timestamp::now().as_second().max(0).cast_unsigned();
    let evict_cfg = EvictionConfig {
        budget_bytes: cfg.budget_bytes,
        headroom_fraction: cfg.headroom_fraction,
        grace_secs: cfg.grace.as_secs(),
        now_secs,
    };

    // Step 0: resolve any rows stuck in `state='deleting'` from a
    // prior tick whose Phase 2 errored. Bytes-intact rows go back to
    // `'ready'` so the planner can replan them this tick; bytes-missing
    // rows are dropped so `resolve_template` stops 404'ing and
    // `do_pull` re-materializes on next access. Race-free: no Phase 1
    // CAS has run yet in this tick, so any `'deleting'` row here is
    // necessarily a leftover, not in-flight.
    let reaped = reap_stuck_deleting(Arc::clone(&db), &reaper_probe).await;
    if reaped > 0 {
        tracing::debug!(reaped, "template GC reaped stuck-deleting rows");
    }

    // Step 1: read every DB-side input (builds, snapshot dims, pin
    // union) under a single locked region. Released before
    // `disk_size_for` runs so the production closure can take the
    // same mutex without deadlocking.
    let db_for_plan = Arc::clone(&db);
    let db_snapshot = tokio::task::spawn_blocking(move || {
        let conn = db_for_plan
            .lock()
            .map_err(|_| crate::db::mutex_poison_error("template GC plan"))?;
        read_plan_db_snapshot(&conn)
    })
    .await
    .map_err(GcTickError::Task)?
    .map_err(GcTickError::PlanSnapshot)?;

    // Step 2: compose the plan from the snapshot. `disk_size_for`
    // hits the filesystem (and may take the DB lock itself in
    // production); both are now safe because the lock from step 1
    // is gone. Still inside `spawn_blocking` because the
    // `fs::metadata` calls inside the closure are sync.
    let active_for_plan = active.clone();
    let warm_pool_for_plan = warm_pool.clone();
    let disk_size_clone = Arc::clone(&disk_size_for);
    let evict_cfg_for_plan = evict_cfg;
    let plan = tokio::task::spawn_blocking(move || {
        plan_from_snapshot(
            db_snapshot,
            &active_for_plan,
            &warm_pool_for_plan,
            &evict_cfg_for_plan,
            |n, h, fallback, dims| disk_size_clone(n, h, fallback, dims),
        )
    })
    .await
    .map_err(GcTickError::Task)?
    .map_err(GcTickError::Plan)?;

    let mut report = GcTickReport {
        candidates_planned: plan.candidates.len(),
        bytes_before: plan.total_bytes_before,
        ..GcTickReport::default()
    };

    for candidate in plan.candidates {
        let db_for_exec = Arc::clone(&db);
        let active = active.clone();
        let warm_pool = warm_pool.clone();
        let cand = candidate.clone();
        let delete_disk = Arc::clone(&delete_disk_for);
        // `execute_one_async` releases the DB lock around the disk
        // delete. Wrap in
        // `Ok(...)` to keep the existing 3-level match shape that
        // the original `spawn_blocking` wrapper produced.
        let outcome: Result<rusqlite::Result<EvictionOutcome>, String> =
            Ok(execute_one_async(db_for_exec, active, warm_pool, cand, delete_disk).await);

        match outcome {
            Ok(Ok(EvictionOutcome::Deleted)) => {
                report.deleted += 1;
                report.bytes_freed = report.bytes_freed.saturating_add(candidate.size_bytes);
                pplx_space_metrics::template::gc_deleted(&candidate.name);
                // Surface the deletion to the cluster immediately so
                // scheduler template-presence labels don't stay stale
                // until the next periodic node-state reconcile.
                crate::node_state_sync::mark_template_dirty(&db, &candidate.name);
            }
            Ok(Ok(EvictionOutcome::SkippedPinnedAtExecute)) => {
                report.skipped_pinned += 1;
                pplx_space_metrics::template::gc_skipped("pinned_at_execute");
            }
            Ok(Ok(EvictionOutcome::SkippedActiveAtExecute)) => {
                report.skipped_active += 1;
                pplx_space_metrics::template::gc_skipped("active_at_execute");
            }
            Ok(Ok(EvictionOutcome::SkippedWarmPoolAtExecute)) => {
                report.skipped_warm_pool += 1;
                pplx_space_metrics::template::gc_skipped("warm_pool_at_execute");
            }
            Ok(Ok(EvictionOutcome::SkippedRowMissing)) => {
                report.skipped_row_missing += 1;
                pplx_space_metrics::template::gc_skipped("row_missing");
            }
            Ok(Err(err)) => {
                warn!(
                    template = %candidate.name,
                    content_hash = %candidate.content_hash,
                    error = %err,
                    "template GC: candidate delete failed"
                );
                report.errors += 1;
                pplx_space_metrics::template::gc_skipped("error");
            }
            Err(err) => {
                warn!(
                    template = %candidate.name,
                    content_hash = %candidate.content_hash,
                    error = %err,
                    "template GC: candidate delete task panicked"
                );
                report.errors += 1;
                pplx_space_metrics::template::gc_skipped("error");
            }
        }
    }

    if report.bytes_before > cfg.budget_bytes
        && report.bytes_freed.saturating_add(cfg.budget_bytes) < report.bytes_before
    {
        warn!(
            bytes_before = report.bytes_before,
            bytes_freed = report.bytes_freed,
            budget_bytes = cfg.budget_bytes,
            "template GC: cache still over budget after tick; everything else is pinned/active/in-grace",
        );
    }

    Ok(report)
}

/// Bundle of the side-effect callbacks [`run_loop`] drives each tick.
/// Grouped into one struct so the loop's signature stays stable as
/// protected-set sources grow, and so `service.rs` reads as "plug in
/// the production hooks" rather than a positional argument list.
pub struct GcHooks {
    pub fetch_active: FetchActiveFn,
    pub fetch_warm_pool: FetchWarmPoolFn,
    pub disk_size_for: DiskSizeFn,
    pub delete_disk_for: DeleteDiskFn,
    pub reaper_probe: ReaperDiskProbeFn,
}

/// Drive the GC tick on a recurring interval until `shutdown` fires.
/// The [`GcHooks`] closures handle every side effect (active-set and
/// warm-pool-set fetch, disk-size, disk-delete) so the loop itself has
/// no dependencies on `TemplateArtifactStore`, any sbx-mgr client
/// type, or the cluster API client — the orchestration in `service.rs`
/// plugs those in.
pub async fn run_loop(
    db: Arc<Mutex<Connection>>,
    cfg: GcConfig,
    shutdown: CancellationToken,
    hooks: GcHooks,
    gate: EvictionGate,
) {
    let GcHooks {
        fetch_active,
        fetch_warm_pool,
        disk_size_for,
        delete_disk_for,
        reaper_probe,
    } = hooks;
    if !cfg.enabled {
        info!(
            env_var = GcConfig::ENV_BUDGET,
            "template eviction disabled (budget is zero); loop exiting",
        );
        return;
    }

    info!(
        budget_bytes = cfg.budget_bytes,
        interval_secs = cfg.interval.as_secs(),
        grace_secs = cfg.grace.as_secs(),
        headroom_fraction = cfg.headroom_fraction,
        "template eviction loop starting",
    );

    loop {
        let tick_started = std::time::Instant::now();
        // Held from before the protected sets are fetched until the tick
        // ends, so a wait for an idle eviction cannot leave the tick acting
        // on sets older than its own grace clock. Released before every
        // interval wait.
        let evicting = tokio::select! {
            biased;
            _ = shutdown.cancelled() => return,
            guard = gate.hold() => guard,
        };
        // Fetch the active sandbox set first. A failed sbx-mgr call
        // is treated as "do not evict this tick" — without an active
        // set we cannot safely drop a build that some sandbox might
        // be holding. The grace period covers the case where the
        // failure is transient and a build hasn't aged enough yet.
        // Surfaced as `result:error` on the tick metric so dashboards
        // can split "we ran and did nothing" from "we couldn't even
        // try".
        //
        // Wrapping in `tokio::select!` so a hung sbx-mgr.list() (UDS
        // deadlock, slow traversal, peer in long GC) does not park
        // the loop past shutdown. `biased;` checks the shutdown
        // branch first so cancellation always wins even if the
        // fetcher is ready at the same poll tick.
        let active = tokio::select! {
            biased;
            _ = shutdown.cancelled() => return,
            result = fetch_active() => match result {
                Ok(a) => a,
                Err(err) => {
                    warn!(
                        error = %err,
                        "template GC: skipping tick because active-sandbox snapshot is unavailable",
                    );
                    pplx_space_metrics::template::gc_tick(
                        Err(pplx_space_metrics::template::TemplateGcTickFailure::ActiveSetUnavailable),
                        tick_started,
                        0,
                        0,
                        0,
                    );
                    drop(evicting);
                    if wait_or_shutdown(cfg.interval, &shutdown).await {
                        return;
                    }
                    continue;
                }
            }
        };

        // The warm-pool protected set follows the same fail-closed
        // contract as the active set: without it, a release the next
        // pool refill depends on could be evicted, so an unavailable
        // control plane means "do not evict this tick". The grace
        // period plus the next successful tick cover the transient
        // window.
        let warm_pool = tokio::select! {
            biased;
            _ = shutdown.cancelled() => return,
            result = fetch_warm_pool() => match result {
                Ok(w) => w,
                Err(err) => {
                    warn!(
                        error = %err,
                        "template GC: skipping tick because warm-pool protected set is unavailable",
                    );
                    pplx_space_metrics::template::gc_tick(
                        Err(pplx_space_metrics::template::TemplateGcTickFailure::WarmPoolUnavailable),
                        tick_started,
                        0,
                        0,
                        0,
                    );
                    drop(evicting);
                    if wait_or_shutdown(cfg.interval, &shutdown).await {
                        return;
                    }
                    continue;
                }
            }
        };

        // Same `select!` shape for the tick itself so an in-flight
        // `spawn_blocking` (DB read, disk delete) can't extend the
        // shutdown wait past one piece of work. The `spawn_blocking`
        // worker keeps running on its OS thread once started — we
        // just stop waiting for it. DB ops are milliseconds; disk
        // deletes are at most a few seconds per build.
        let tick_result = tokio::select! {
            biased;
            _ = shutdown.cancelled() => return,
            result = gc_tick(
                Arc::clone(&db),
                &cfg,
                active,
                warm_pool,
                Arc::clone(&disk_size_for),
                Arc::clone(&delete_disk_for),
                Arc::clone(&reaper_probe),
            ) => result,
        };
        drop(evicting);
        match tick_result {
            Ok(report) => {
                info!(
                    candidates = report.candidates_planned,
                    deleted = report.deleted,
                    skipped_pinned = report.skipped_pinned,
                    skipped_active = report.skipped_active,
                    skipped_warm_pool = report.skipped_warm_pool,
                    skipped_row_missing = report.skipped_row_missing,
                    errors = report.errors,
                    bytes_before = report.bytes_before,
                    bytes_freed = report.bytes_freed,
                    "template GC tick complete",
                );
                pplx_space_metrics::template::gc_tick(
                    Ok(()),
                    tick_started,
                    report.bytes_before,
                    report.bytes_freed,
                    u64::try_from(report.candidates_planned).unwrap_or(u64::MAX),
                );
            }
            Err(err) => {
                warn!(error = %err, "template GC tick failed");
                pplx_space_metrics::template::gc_tick(
                    Err(err.metric_reason()),
                    tick_started,
                    0,
                    0,
                    0,
                );
            }
        }

        if wait_or_shutdown(cfg.interval, &shutdown).await {
            return;
        }
    }
}

/// Wait for `interval` or until `shutdown` fires. Returns `true` if
/// the wait ended because of shutdown — the caller should exit. Used
/// instead of bare `tokio::time::sleep` so the loop is responsive to
/// SIGTERM.
async fn wait_or_shutdown(interval: Duration, shutdown: &CancellationToken) -> bool {
    tokio::select! {
        _ = shutdown.cancelled() => true,
        _ = tokio::time::sleep(interval) => false,
    }
}

#[cfg(test)]
mod tests;
