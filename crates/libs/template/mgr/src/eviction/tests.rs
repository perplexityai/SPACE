use super::*;

fn build(name: &str, hash: &str, size: u64, created: u64, used: u64) -> BuildSnapshot {
    BuildSnapshot {
        name: name.to_string(),
        content_hash: hash.to_string(),
        size_bytes: size,
        created_at_secs: created,
        last_used_at_secs: used,
    }
}

fn keys(pairs: &[(&str, &str)]) -> HashSet<(String, String)> {
    pairs
        .iter()
        .map(|(n, h)| ((*n).to_string(), (*h).to_string()))
        .collect()
}

fn cfg(budget: u64, headroom: f64, grace_secs: u64, now: u64) -> EvictionConfig {
    EvictionConfig {
        budget_bytes: budget,
        headroom_fraction: headroom,
        grace_secs,
        now_secs: now,
    }
}

// Pure-planner tests (ordering, protected-set, grace, headroom)
// live in `pplx-space-common-lru-reaper`. This module keeps the
// template-specific wrapper coverage: the per-build adapter shape,
// the `plan_from_db` → SQL UNION integration, and the
// `execute_one*` race-recheck transactions.

/// Adapter check: the template wrapper carries `name` and
/// `content_hash` through to the returned candidates in the
/// expected order. If a future refactor breaks the
/// `BuildSnapshot → EvictionInput → EvictionCandidate` round-trip
/// this fails loudly.
#[test]
fn wrapper_round_trip_preserves_name_and_hash() {
    let now = 10_000u64;
    // total=1000, budget=600, target=540, need=460.
    // 1 eviction (500) ≥ 460 → break next iter. Oldest goes first.
    let builds = vec![
        build("seed", "newer", 500, 0, now - 1000),
        build("seed", "oldest", 500, 0, now - 5000),
    ];
    let plan = plan_evictions(
        builds,
        &keys(&[]),
        &keys(&[]),
        &keys(&[]),
        &cfg(600, 0.9, 60, now),
    );
    assert_eq!(plan.len(), 1);
    assert_eq!(plan[0].name, "seed");
    assert_eq!(plan[0].content_hash, "oldest");
    assert_eq!(plan[0].size_bytes, 500);
}

// ── Integration-shaped tests for plan_from_db + execute_one ──────
//
// These use an in-memory SQLite DB so the race-recheck SQL is
// exercised end-to-end. They still avoid disk / network / sbxmgr
// by injecting the disk-size and active-set inputs directly.

use crate::db::{BuildUpsert, now, upsert_build};
use rusqlite::{Connection, params};

fn seed_build(conn: &Connection, name: &str, content_hash: &str, size: u64) {
    let upsert = BuildUpsert {
        name,
        content_hash,
        build_date: "2026-01-01T00:00:00Z",
        manifest_json: "{}",
        runtime: "fc",
        arch: "aarch64",
        artifact_size_bytes: size,
        state: "ready",
    };
    upsert_build(conn, &upsert).unwrap();
}

fn plant_pin(conn: &Connection, name: &str, content_hash: &str) {
    conn.execute(
        "INSERT INTO template_pinned_tags (name, tag, content_hash, updated_at)
         VALUES (?1, 'latest', ?2, ?3)",
        params![name, content_hash, now()],
    )
    .unwrap();
}

/// `plan_from_db` returns an empty candidate list when the
/// cumulative on-disk footprint is at or below the budget — the
/// driver loop must skip the rest of the tick entirely.
#[test]
fn plan_from_db_returns_empty_when_under_budget() {
    let conn = crate::db::test_conn();
    seed_build(&conn, "seed", "h1", 100);
    seed_build(&conn, "seed", "h2", 200);

    let plan = plan_from_db(
        &conn,
        &HashSet::new(),
        &HashSet::new(),
        &cfg(1_000, 0.9, 60, 10_000),
        |_, _, s, _| s,
    )
    .unwrap();
    assert!(plan.candidates.is_empty());
    assert_eq!(plan.total_bytes_before, 300);
    assert_eq!(plan.projected_bytes_freed, 0);
}

/// `plan_from_db` honours pin rows written via the same tables
/// the prefetcher writes — proves the SQL UNION the planner reads
/// from is wired correctly. Without this we'd still pass the
/// pure-planner tests but evict pinned builds in production.
#[test]
fn plan_from_db_respects_pinned_rows() {
    let conn = crate::db::test_conn();
    seed_build(&conn, "seed", "pinned", 1_000);
    seed_build(&conn, "seed", "evictable", 1_000);
    plant_pin(&conn, "seed", "pinned");
    crate::db::set_last_used_at_for_test(&conn, "seed", "pinned", 1).unwrap();
    crate::db::set_last_used_at_for_test(&conn, "seed", "evictable", 2).unwrap();

    let plan = plan_from_db(
        &conn,
        &HashSet::new(),
        &HashSet::new(),
        // budget 500 -> over budget, need to free; grace=0 so
        // both are eligible by age.
        &cfg(500, 0.9, 0, 10_000),
        |_, _, s, _| s,
    )
    .unwrap();
    assert_eq!(plan.candidates.len(), 1);
    assert_eq!(plan.candidates[0].content_hash, "evictable");
}

/// `execute_one` happy path: pin set is empty, active set is
/// empty, build row exists — the disk closure runs, both DB
/// tables are cleared, and the outcome is `Deleted`. We assert
/// the closure observed the right args so a future refactor that
/// drops the parameters can't silently pass.
#[test]
fn execute_one_deletes_when_no_race() {
    let conn = crate::db::test_conn();
    seed_build(&conn, "seed", "h1", 1_000);

    let cand = EvictionCandidate {
        name: "seed".to_string(),
        content_hash: "h1".to_string(),
        size_bytes: 1_000,
    };
    let observed = std::cell::RefCell::new(None);
    let outcome = execute_one(&conn, &HashSet::new(), &HashSet::new(), &cand, |n, h| {
        *observed.borrow_mut() = Some((n.to_string(), h.to_string()));
        Ok(())
    })
    .unwrap();

    assert_eq!(outcome, EvictionOutcome::Deleted);
    assert_eq!(
        observed.into_inner(),
        Some(("seed".to_string(), "h1".to_string()))
    );
    let remaining: i64 = conn
        .query_row("SELECT COUNT(*) FROM template_builds", [], |r| r.get(0))
        .unwrap();
    assert_eq!(remaining, 0);
}

/// Race-recheck: a pin lands between plan-time and execute-time
/// (here, simulated by writing the pin row before calling
/// `execute_one`). The candidate must be skipped without touching
/// disk or the DB. Captures the core race-safety guarantee — pin
/// writes win the immediate-txn write lock.
#[test]
fn execute_one_skips_pin_added_between_plan_and_execute() {
    let conn = crate::db::test_conn();
    seed_build(&conn, "seed", "h1", 1_000);

    // Pin appears between plan-time and execute-time.
    plant_pin(&conn, "seed", "h1");

    let cand = EvictionCandidate {
        name: "seed".to_string(),
        content_hash: "h1".to_string(),
        size_bytes: 1_000,
    };
    let disk_called = std::cell::Cell::new(false);
    let outcome = execute_one(&conn, &HashSet::new(), &HashSet::new(), &cand, |_, _| {
        disk_called.set(true);
        Ok(())
    })
    .unwrap();

    assert_eq!(outcome, EvictionOutcome::SkippedPinnedAtExecute);
    assert!(!disk_called.get(), "disk delete must not run when pinned");
    let row_exists: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM template_builds WHERE name = ?1 AND content_hash = ?2",
            params!["seed", "h1"],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(row_exists, 1, "DB row must survive a pin-time skip");
}

/// Race-recheck: an active sandbox appears in the supplied
/// active set. The candidate is skipped without touching disk.
/// Mirrors the pin-recheck but covers the path where sandbox-
/// manager just reported a new sandbox holding this digest.
#[test]
fn execute_one_skips_active_added_between_plan_and_execute() {
    let conn = crate::db::test_conn();
    seed_build(&conn, "seed", "h1", 1_000);

    let cand = EvictionCandidate {
        name: "seed".to_string(),
        content_hash: "h1".to_string(),
        size_bytes: 1_000,
    };
    let active = keys(&[("seed", "h1")]);
    let disk_called = std::cell::Cell::new(false);
    let outcome = execute_one(&conn, &active, &HashSet::new(), &cand, |_, _| {
        disk_called.set(true);
        Ok(())
    })
    .unwrap();

    assert_eq!(outcome, EvictionOutcome::SkippedActiveAtExecute);
    assert!(
        !disk_called.get(),
        "disk delete must not run when an active sandbox attached"
    );
}

/// Disk-delete failure must NOT clear the DB row. The build row
/// stays so the next tick can retry; an orphaned digest dir on
/// disk is the existing failure mode the recovery path already
/// handles (`try_recover_from_disk`). The opposite — DB cleared,
/// disk full — would leak bytes forever.
#[test]
fn execute_one_keeps_db_row_when_disk_delete_fails() {
    let conn = crate::db::test_conn();
    seed_build(&conn, "seed", "h1", 1_000);

    let cand = EvictionCandidate {
        name: "seed".to_string(),
        content_hash: "h1".to_string(),
        size_bytes: 1_000,
    };
    let outcome = execute_one(&conn, &HashSet::new(), &HashSet::new(), &cand, |_, _| {
        // Surface a synthetic SQLite error to model an I/O failure
        // bubbling through the disk-delete closure.
        Err(rusqlite::Error::ExecuteReturnedResults)
    });
    assert!(outcome.is_err(), "disk error must bubble to caller");

    let row_exists: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM template_builds WHERE name = ?1 AND content_hash = ?2",
            params!["seed", "h1"],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(row_exists, 1, "DB row must survive a disk-delete failure");
}

// ── gc_tick end-to-end ──────────────────────────────────────────
//
// Drives the full async orchestrator (plan + execute + report
// accounting) against an in-memory DB. The two side-effect
// closures (disk-size, disk-delete) are captured so the test can
// assert which builds were actually fingered for deletion.

/// Headline end-to-end test: one over-budget candidate flows
/// through `gc_tick` and emerges as a Deleted row with the right
/// bytes accounted in the report. Confirms the wiring between
/// `plan_from_db`, `execute_one`, and the metric counters in
/// `GcTickReport`.
#[tokio::test]
async fn gc_tick_evicts_oldest_candidate_end_to_end() {
    use std::sync::Mutex;
    let raw = crate::db::test_conn();
    seed_build(&raw, "seed", "old", 1_000);
    seed_build(&raw, "seed", "new", 500);
    // Plant LRU timestamps so "old" is clearly the LRU pick. Use
    // 1 second to keep grace math obvious.
    crate::db::set_last_used_at_for_test(&raw, "seed", "old", 1).unwrap();
    crate::db::set_last_used_at_for_test(&raw, "seed", "new", 100).unwrap();
    let db = Arc::new(Mutex::new(raw));

    let cfg = GcConfig {
        enabled: true,
        budget_bytes: 800,
        headroom_fraction: 0.9,
        interval: Duration::from_secs(60),
        grace: Duration::from_secs(0),
    };

    let deleted = Arc::new(Mutex::new(Vec::<(String, String)>::new()));
    let deleted_capture = Arc::clone(&deleted);
    let delete_disk: DeleteDiskFn = Arc::new(move |n, h| {
        let captured = Arc::clone(&deleted_capture);
        let name = n.to_string();
        let hash = h.to_string();
        Box::pin(async move {
            captured.lock().unwrap().push((name, hash));
            Ok(())
        })
    });
    // Identity disk-size: the planner is given the artifact_size
    // stored in the DB.
    let disk_size: DiskSizeFn = Arc::new(|_, _, fallback, _| fallback);

    let report = gc_tick(
        Arc::clone(&db),
        &cfg,
        HashSet::new(),
        HashSet::new(),
        disk_size,
        delete_disk,
        noop_reaper_probe(),
    )
    .await
    .unwrap();

    assert_eq!(report.deleted, 1);
    assert_eq!(report.skipped_pinned, 0);
    assert_eq!(report.skipped_active, 0);
    assert_eq!(report.errors, 0);
    assert_eq!(report.bytes_before, 1_500);
    assert_eq!(report.bytes_freed, 1_000);
    assert_eq!(
        *deleted.lock().unwrap(),
        vec![("seed".to_string(), "old".to_string())]
    );

    // DB state matches: only the newer build survives.
    let conn = db.lock().unwrap();
    let remaining: Vec<String> = conn
        .prepare("SELECT content_hash FROM template_builds ORDER BY content_hash")
        .unwrap()
        .query_map([], |r| r.get(0))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(remaining, vec!["new"]);
}

/// `run_loop` exits promptly when `shutdown.cancel()` fires. We
/// configure a long interval (10s) so the only way the future can
/// return inside the test's 2s timeout is via the cancellation
/// path inside `wait_or_shutdown`. Without that wiring the test
/// would hang and `tokio::time::timeout` would surface the bug
/// loudly. Closures are stubbed: no DB, no sbx-mgr.
#[tokio::test]
async fn run_loop_exits_cooperatively_on_shutdown() {
    use std::sync::Mutex;
    let conn = crate::db::test_conn();
    let db = Arc::new(Mutex::new(conn));

    let cfg = GcConfig {
        enabled: true,
        budget_bytes: 1,
        headroom_fraction: 0.9,
        interval: Duration::from_secs(10),
        grace: Duration::from_secs(0),
    };

    let shutdown = CancellationToken::new();
    let fetch_active: FetchActiveFn = Arc::new(|| Box::pin(async { Ok(HashSet::new()) }));
    let disk_size: DiskSizeFn = Arc::new(|_, _, fallback, _| fallback);
    let delete_disk: DeleteDiskFn = Arc::new(|_, _| Box::pin(async { Ok(()) }));

    let loop_handle = tokio::spawn(run_loop(
        Arc::clone(&db),
        cfg,
        shutdown.clone(),
        GcHooks {
            fetch_active,
            fetch_warm_pool: empty_warm_pool_fetch(),
            disk_size_for: disk_size,
            delete_disk_for: delete_disk,
            reaper_probe: noop_reaper_probe(),
        },
        EvictionGate::default(),
    ));

    // Give the loop a brief moment to complete its first tick so
    // we exercise the shutdown branch (not the no-op path before
    // any work runs).
    tokio::time::sleep(Duration::from_millis(50)).await;
    shutdown.cancel();

    let outcome = tokio::time::timeout(Duration::from_secs(2), loop_handle).await;
    assert!(
        outcome.is_ok(),
        "run_loop must exit within 2s of shutdown.cancel()"
    );
}

/// Regression test: `execute_one_async` must release the DB lock around
/// the disk delete. Before the 3-phase restructure the IMMEDIATE
/// txn + `Arc<Mutex<Connection>>` lock were both held for the
/// duration of the disk delete, blocking every other DB
/// consumer (prefetch writes, `resolve_template` on the sandbox-
/// create hot path) for seconds on a multi-GB rootfs delete.
///
/// Test shape: model a slow disk delete (sleep 500ms inside the
/// async closure), kick off the eviction in one task, and
/// concurrently try to read the DB from another task. The read
/// must complete promptly (well under the disk-delete duration);
/// before the fix it would block until disk-delete finished.
#[tokio::test]
async fn execute_one_async_releases_db_lock_during_disk_delete() {
    use std::sync::Mutex;
    let raw = crate::db::test_conn();
    seed_build(&raw, "seed", "h1", 1_000);
    let db = Arc::new(Mutex::new(raw));

    let cand = EvictionCandidate {
        name: "seed".to_string(),
        content_hash: "h1".to_string(),
        size_bytes: 1_000,
    };

    // Disk delete sleeps for 500ms — simulates a multi-GB
    // rootfs+zstd removal on slow storage.
    let delete_disk: DeleteDiskFn = Arc::new(|_, _| {
        Box::pin(async move {
            tokio::time::sleep(Duration::from_millis(500)).await;
            Ok(())
        })
    });

    // Kick off the eviction. It will block on the slow delete.
    let db_for_eviction = Arc::clone(&db);
    let eviction = tokio::spawn(async move {
        execute_one_async(
            db_for_eviction,
            HashSet::new(),
            HashSet::new(),
            cand,
            delete_disk,
        )
        .await
    });

    // Give the precheck phase a moment to finish (it's just a
    // pair of `EXISTS` queries) and the eviction to enter the
    // 500ms sleep. 50ms is enough on any sane runtime.
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Now try to read the DB from another task. Before the fix
    // this would block until the disk delete finishes (450ms+).
    // After the fix the lock is released, so the read completes
    // in microseconds.
    let db_for_read = Arc::clone(&db);
    let read_started = std::time::Instant::now();
    let read_count: i64 = tokio::task::spawn_blocking(move || {
        let conn = db_for_read.lock().unwrap();
        conn.query_row("SELECT COUNT(*) FROM template_builds", [], |row| row.get(0))
            .unwrap()
    })
    .await
    .unwrap();
    let read_elapsed = read_started.elapsed();
    assert_eq!(read_count, 1, "build row still present mid-eviction");

    // Read must complete well under the disk-delete duration.
    // The 200ms threshold leaves headroom for slow CI.
    assert!(
        read_elapsed < Duration::from_millis(200),
        "concurrent DB read blocked by disk delete: took {:?} (expected < 200ms)",
        read_elapsed,
    );

    // Eviction completes after the disk delete finishes.
    let outcome = eviction.await.unwrap().unwrap();
    assert_eq!(outcome, EvictionOutcome::Deleted);
}

/// Regression test for the GC-vs-sandbox-create race: a
/// sandbox-create that arrives mid-eviction must NOT be handed
/// a build that's about to vanish. The fix is in
/// `execute_precheck`: it atomically flips the row to
/// `state='deleting'` inside the IMMEDIATE txn, and
/// `resolve_template` filters on `state='ready'`. This test
/// gates the disk delete on a manual signal so it can observe
/// the row state DURING Phase 2 (after Phase 1 committed
/// 'deleting', before Phase 3 deletes the row).
#[tokio::test]
async fn execute_one_async_marks_deleting_before_disk_delete() {
    use std::sync::Mutex;
    use tokio::sync::oneshot;

    let raw = crate::db::test_conn();
    seed_build(&raw, "seed", "h1", 1_000);
    let db = Arc::new(Mutex::new(raw));

    let cand = EvictionCandidate {
        name: "seed".to_string(),
        content_hash: "h1".to_string(),
        size_bytes: 1_000,
    };

    // Disk delete blocks until the test signals it. While
    // blocked, we can observe the DB row state from another
    // task — and that's exactly when a concurrent
    // sandbox-create would arrive.
    let (release_tx, release_rx) = oneshot::channel::<()>();
    let release_rx = Arc::new(Mutex::new(Some(release_rx)));
    let delete_disk: DeleteDiskFn = Arc::new(move |_, _| {
        let rx_slot = Arc::clone(&release_rx);
        Box::pin(async move {
            let rx = rx_slot.lock().unwrap().take();
            if let Some(rx) = rx {
                let _ = rx.await;
            }
            Ok(())
        })
    });

    let db_for_eviction = Arc::clone(&db);
    let cand_for_eviction = cand.clone();
    let eviction = tokio::spawn(async move {
        execute_one_async(
            db_for_eviction,
            HashSet::new(),
            HashSet::new(),
            cand_for_eviction,
            delete_disk,
        )
        .await
    });

    // Wait for Phase 1 to commit (state flips to 'deleting').
    // Phase 1 is just a few SQL statements; 100ms is plenty.
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Observation: row exists in state='deleting'.
    let state: String = db
        .lock()
        .unwrap()
        .query_row(
            "SELECT state FROM template_builds WHERE name = ?1 AND content_hash = ?2",
            params!["seed", "h1"],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        state, "deleting",
        "Phase 1 must mark the row 'deleting' before the disk delete starts"
    );

    // Release Phase 2.
    let _ = release_tx.send(());

    let outcome = eviction.await.unwrap().unwrap();
    assert_eq!(outcome, EvictionOutcome::Deleted);

    // Final state: row is gone.
    let count: i64 = db
        .lock()
        .unwrap()
        .query_row("SELECT COUNT(*) FROM template_builds", [], |r| r.get(0))
        .unwrap();
    assert_eq!(count, 0);
}

/// Companion to the test above: a second eviction tick that
/// fires while another is mid-flight (rare but possible if
/// run_loop's interval is misconfigured) sees `state='deleting'`
/// and skips the candidate via the UPDATE-with-WHERE-state='ready'
/// compare-and-swap, rather than racing the disk delete.
#[tokio::test]
async fn execute_one_async_second_eviction_skips_deleting_row() {
    use std::sync::Mutex;
    let raw = crate::db::test_conn();
    seed_build(&raw, "seed", "h1", 1_000);
    // Plant `state='deleting'` directly to simulate a row
    // already mid-eviction.
    raw.execute(
        "UPDATE template_builds SET state = 'deleting'
         WHERE name = ?1 AND content_hash = ?2",
        params!["seed", "h1"],
    )
    .unwrap();
    let db = Arc::new(Mutex::new(raw));

    let cand = EvictionCandidate {
        name: "seed".to_string(),
        content_hash: "h1".to_string(),
        size_bytes: 1_000,
    };
    let disk_called = Arc::new(Mutex::new(false));
    let disk_called_capture = Arc::clone(&disk_called);
    let delete_disk: DeleteDiskFn = Arc::new(move |_, _| {
        let c = Arc::clone(&disk_called_capture);
        Box::pin(async move {
            *c.lock().unwrap() = true;
            Ok(())
        })
    });

    let outcome = execute_one_async(db, HashSet::new(), HashSet::new(), cand, delete_disk)
        .await
        .unwrap();
    assert_eq!(outcome, EvictionOutcome::SkippedRowMissing);
    assert!(
        !*disk_called.lock().unwrap(),
        "second eviction must not delete disk for a row already mid-eviction"
    );
}

/// Regression test: the production `disk_size_for` closure must not
/// re-acquire the `Arc<Mutex<Connection>>` that `gc_tick` already
/// holds via `spawn_blocking`. `std::sync::Mutex` is non-reentrant,
/// so the second `lock()` deadlocks the spawn_blocking worker
/// permanently on the first tick that finds an eviction
/// candidate. The previous tests all passed an identity
/// `DiskSizeFn` (no DB access) so this path was uncovered.
///
/// This test models the production shape with a closure that
/// re-locks the same connection. Before the fix the call hangs
/// on the inner `lock()` and the 2s timeout trips; after the
/// fix `plan_from_db` releases the lock before calling
/// `disk_size_for`, so the closure can lock the connection
/// itself without contention.
#[tokio::test]
async fn gc_tick_does_not_deadlock_when_disk_size_fn_relocks_db() {
    use std::sync::Mutex;
    let raw = crate::db::test_conn();
    seed_build(&raw, "seed", "h1", 1_000);
    crate::db::set_last_used_at_for_test(&raw, "seed", "h1", 1).unwrap();
    let db = Arc::new(Mutex::new(raw));

    let cfg = GcConfig {
        enabled: true,
        budget_bytes: 100, // over budget, so this candidate is planned
        headroom_fraction: 0.9,
        interval: Duration::from_secs(60),
        grace: Duration::from_secs(0),
    };

    // Closure that re-locks the same DB. This is the production
    // shape (`build_disk_usage_with_snapshots` calls
    // `list_snapshots` under `db.lock()` to enumerate the
    // build's snapshot pairs). On the buggy code path,
    // plan_from_db is invoked from inside a spawn_blocking that
    // already holds db.lock(), so this inner lock() deadlocks.
    let db_for_closure = Arc::clone(&db);
    let disk_size: DiskSizeFn = Arc::new(
        move |_n: &str, _h: &str, fallback: u64, _dims: &[(u32, u32)]| {
            let _guard = db_for_closure.lock().unwrap();
            fallback
        },
    );
    let delete_disk: DeleteDiskFn = Arc::new(|_, _| Box::pin(async { Ok(()) }));

    let result = tokio::time::timeout(
        Duration::from_secs(2),
        gc_tick(
            Arc::clone(&db),
            &cfg,
            HashSet::new(),
            HashSet::new(),
            disk_size,
            delete_disk,
            noop_reaper_probe(),
        ),
    )
    .await;
    assert!(
        result.is_ok(),
        "gc_tick deadlocked when disk_size_for re-acquires the DB lock",
    );
}

/// Regression test for shutdown interrupting a hung fetch:
/// `fetch_active().await` was not in a `tokio::select!` with
/// `shutdown`, so a hung `sbx-mgr.list()` would trap the loop
/// even with the cancellation token cancelled. This test models
/// a stuck active-set fetcher with `std::future::pending()` —
/// without the fix the loop blocks here forever and the 2s
/// `tokio::time::timeout` below trips. With the fix, the
/// shutdown branch wins inside the `select!` and `run_loop`
/// returns immediately.
#[tokio::test]
async fn run_loop_exits_when_active_fetch_is_stuck() {
    use std::sync::Mutex;
    let conn = crate::db::test_conn();
    let db = Arc::new(Mutex::new(conn));

    let cfg = GcConfig {
        enabled: true,
        budget_bytes: 1,
        headroom_fraction: 0.9,
        interval: Duration::from_secs(10),
        grace: Duration::from_secs(0),
    };

    // A fetcher that never completes — models sbx-mgr hung on a
    // deadlock, slow UDS, or long `List` traversal.
    let fetch_active: FetchActiveFn = Arc::new(|| {
        Box::pin(async {
            std::future::pending::<Result<HashSet<(String, String)>, String>>().await
        })
    });
    let disk_size: DiskSizeFn = Arc::new(|_, _, fallback, _| fallback);
    let delete_disk: DeleteDiskFn = Arc::new(|_, _| Box::pin(async { Ok(()) }));

    let shutdown = CancellationToken::new();
    let loop_handle = tokio::spawn(run_loop(
        Arc::clone(&db),
        cfg,
        shutdown.clone(),
        GcHooks {
            fetch_active,
            fetch_warm_pool: empty_warm_pool_fetch(),
            disk_size_for: disk_size,
            delete_disk_for: delete_disk,
            reaper_probe: noop_reaper_probe(),
        },
        EvictionGate::default(),
    ));

    // Give the loop a moment to enter the await on the stuck
    // fetcher, then cancel. Without the fix the loop is parked
    // on `fetch_active().await` with no select wrapper, so the
    // future hangs forever and `tokio::time::timeout` trips.
    tokio::time::sleep(Duration::from_millis(50)).await;
    shutdown.cancel();

    let outcome = tokio::time::timeout(Duration::from_secs(2), loop_handle).await;
    assert!(
        outcome.is_ok(),
        "run_loop must exit within 2s of shutdown.cancel() even when fetch_active is stuck",
    );
}

/// When `cfg.enabled` is false the loop logs once and returns
/// immediately, regardless of shutdown state. Cheap defensive
/// check that an operator-misconfigured budget (env unset) does
/// not stall daemon startup waiting for a tick.
#[tokio::test]
async fn run_loop_returns_immediately_when_disabled() {
    use std::sync::Mutex;
    let conn = crate::db::test_conn();
    let db = Arc::new(Mutex::new(conn));

    let cfg = GcConfig {
        enabled: false,
        budget_bytes: 0,
        headroom_fraction: 0.9,
        interval: Duration::from_secs(3600),
        grace: Duration::from_secs(0),
    };
    let shutdown = CancellationToken::new();
    let fetch_active: FetchActiveFn = Arc::new(|| Box::pin(async { Ok(HashSet::new()) }));
    let disk_size: DiskSizeFn = Arc::new(|_, _, fallback, _| fallback);
    let delete_disk: DeleteDiskFn = Arc::new(|_, _| Box::pin(async { Ok(()) }));

    let outcome = tokio::time::timeout(
        Duration::from_secs(1),
        run_loop(
            db,
            cfg,
            shutdown,
            GcHooks {
                fetch_active,
                fetch_warm_pool: empty_warm_pool_fetch(),
                disk_size_for: disk_size,
                delete_disk_for: delete_disk,
                reaper_probe: noop_reaper_probe(),
            },
            EvictionGate::default(),
        ),
    )
    .await;
    assert!(outcome.is_ok(), "disabled run_loop must return immediately");
}

/// `gc_tick` honours the active set passed in by the caller — a
/// build in the active set is filtered out at plan time and
/// never reaches the disk-delete closure. Regression test for the
/// "GC ran during a long-lived sandbox-create" race scenario.
#[tokio::test]
async fn gc_tick_skips_active_builds_passed_in_by_caller() {
    use std::sync::Mutex;
    let raw = crate::db::test_conn();
    seed_build(&raw, "seed", "h1", 1_000);
    crate::db::set_last_used_at_for_test(&raw, "seed", "h1", 1).unwrap();
    let db = Arc::new(Mutex::new(raw));

    let cfg = GcConfig {
        enabled: true,
        budget_bytes: 100, // way over budget
        headroom_fraction: 0.9,
        interval: Duration::from_secs(60),
        grace: Duration::from_secs(0),
    };

    let disk_called = Arc::new(Mutex::new(false));
    let disk_called_capture = Arc::clone(&disk_called);
    let delete_disk: DeleteDiskFn = Arc::new(move |_, _| {
        let captured = Arc::clone(&disk_called_capture);
        Box::pin(async move {
            *captured.lock().unwrap() = true;
            Ok(())
        })
    });
    let disk_size: DiskSizeFn = Arc::new(|_, _, fallback, _| fallback);

    let active = keys(&[("seed", "h1")]);
    let report = gc_tick(
        Arc::clone(&db),
        &cfg,
        active,
        HashSet::new(),
        disk_size,
        delete_disk,
        noop_reaper_probe(),
    )
    .await
    .unwrap();

    // Active filter happens at plan time, so the candidate list
    // is empty — `deleted` is zero and disk closure was never
    // called.
    assert_eq!(report.candidates_planned, 0);
    assert_eq!(report.deleted, 0);
    assert!(!*disk_called.lock().unwrap());
    let remaining: i64 = db
        .lock()
        .unwrap()
        .query_row("SELECT COUNT(*) FROM template_builds", [], |r| r.get(0))
        .unwrap();
    assert_eq!(remaining, 1);
}

/// The review-pointed gap behind the warm-pool protected set: a
/// release warm pools target, but which is neither locally pinned
/// nor backed by a resident warm slot (so it is absent from the
/// pin tables AND the active set), must survive a GC tick once the
/// caller supplies it in the warm-pool set. Same shape as
/// `gc_tick_skips_active_builds_passed_in_by_caller`, with the
/// protection coming from the new third input.
#[tokio::test]
async fn gc_tick_skips_warm_pool_protected_builds() {
    use std::sync::Mutex;
    let raw = crate::db::test_conn();
    seed_build(&raw, "web-runtime", "sha256:pool", 1_000);
    crate::db::set_last_used_at_for_test(&raw, "web-runtime", "sha256:pool", 1).unwrap();
    let db = Arc::new(Mutex::new(raw));

    let cfg = GcConfig {
        enabled: true,
        budget_bytes: 100, // way over budget
        headroom_fraction: 0.9,
        interval: Duration::from_secs(60),
        grace: Duration::from_secs(0),
    };

    let disk_called = Arc::new(Mutex::new(false));
    let disk_called_capture = Arc::clone(&disk_called);
    let delete_disk: DeleteDiskFn = Arc::new(move |_, _| {
        let captured = Arc::clone(&disk_called_capture);
        Box::pin(async move {
            *captured.lock().unwrap() = true;
            Ok(())
        })
    });
    let disk_size: DiskSizeFn = Arc::new(|_, _, fallback, _| fallback);

    let warm_pool = keys(&[("web-runtime", "sha256:pool")]);
    let report = gc_tick(
        Arc::clone(&db),
        &cfg,
        HashSet::new(), // no resident warm slot => empty active set
        warm_pool,
        disk_size,
        delete_disk,
        noop_reaper_probe(),
    )
    .await
    .unwrap();

    assert_eq!(report.candidates_planned, 0);
    assert_eq!(report.deleted, 0);
    assert!(!*disk_called.lock().unwrap());
    let remaining: i64 = db
        .lock()
        .unwrap()
        .query_row("SELECT COUNT(*) FROM template_builds", [], |r| r.get(0))
        .unwrap();
    assert_eq!(remaining, 1);
}

/// Execute-phase re-check: a build that enters the warm-pool set
/// between plan and execute (same tick snapshot handed to both) is
/// dropped with `SkippedWarmPoolAtExecute` and no disk or DB
/// mutation. Mirrors
/// `execute_one_skips_active_added_between_plan_and_execute`.
#[test]
fn execute_one_skips_warm_pool_added_between_plan_and_execute() {
    let conn = crate::db::test_conn();
    seed_build(&conn, "web-runtime", "sha256:pool", 500);
    let cand = EvictionCandidate {
        name: "web-runtime".into(),
        content_hash: "sha256:pool".into(),
        size_bytes: 500,
    };
    let warm_pool = keys(&[("web-runtime", "sha256:pool")]);
    let disk_called = std::cell::Cell::new(false);
    let outcome = execute_one(&conn, &HashSet::new(), &warm_pool, &cand, |_, _| {
        disk_called.set(true);
        Ok(())
    })
    .unwrap();

    assert_eq!(outcome, EvictionOutcome::SkippedWarmPoolAtExecute);
    assert!(
        !disk_called.get(),
        "disk delete must not run for a warm-pool protected build"
    );
    let row_exists: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM template_builds WHERE name = 'web-runtime'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(row_exists, 1);
    let state: String = conn
        .query_row(
            "SELECT state FROM template_builds WHERE name = 'web-runtime'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(state, "ready", "CAS to 'deleting' must not have run");
}

/// An unset budget env var enables the loop with the default budget; an
/// explicit zero is the opt-out.
#[test]
fn gc_config_default_budget_and_explicit_zero_opt_out() {
    let env = pplx_space_test_env::TestEnv::default();
    let cfg = GcConfig::from_environment(&env);
    assert!(cfg.enabled, "GC is enabled by default");
    assert_eq!(cfg.budget_bytes, GcConfig::DEFAULT_BUDGET_BYTES);

    let mut env = pplx_space_test_env::TestEnv::default();
    env.set(GcConfig::ENV_BUDGET, "0");
    assert!(
        !GcConfig::from_environment(&env).enabled,
        "an explicit zero budget disables GC"
    );
}

/// The loop deletes cached builds, so a budget that is not a byte count must
/// disable it rather than silently apply the default budget.
#[test]
fn gc_config_malformed_budget_disables_gc() {
    for value in ["not-a-number", "-5", "10GB", "1.5", ""] {
        let mut env = pplx_space_test_env::TestEnv::default();
        env.set(GcConfig::ENV_BUDGET, value);
        assert!(
            !GcConfig::from_environment(&env).enabled,
            "{value:?} must disable GC"
        );
    }
    let mut env = pplx_space_test_env::TestEnv::default();
    env.set(GcConfig::ENV_BUDGET, " 4096 ");
    let cfg = GcConfig::from_environment(&env);
    assert!(cfg.enabled);
    assert_eq!(cfg.budget_bytes, 4096);
}

/// `execute_one` on a row that's already gone returns
/// `SkippedRowMissing` without invoking disk delete. Covers the
/// case where a concurrent `DeleteTemplateBuild` RPC raced us
/// (e.g. an operator manually deleted between plan and execute).
#[test]
fn execute_one_skips_when_row_missing() {
    let conn = crate::db::test_conn();
    // Note: NO seed_build call — the row never existed.

    let cand = EvictionCandidate {
        name: "seed".to_string(),
        content_hash: "h1".to_string(),
        size_bytes: 1_000,
    };
    let disk_called = std::cell::Cell::new(false);
    let outcome = execute_one(&conn, &HashSet::new(), &HashSet::new(), &cand, |_, _| {
        disk_called.set(true);
        Ok(())
    })
    .unwrap();

    assert_eq!(outcome, EvictionOutcome::SkippedRowMissing);
    assert!(
        !disk_called.get(),
        "disk delete must not run when the DB row is already gone"
    );
}

/// Different templates pinning the same digest are independent
/// keys. The pin/active sets are `(name, hash)` tuples — a pin on
/// `("seed", h)` must NOT protect a build at `("code-runtime", h)`.
/// Mirrors the same invariant proven in
/// `prefetch::tests::release_dispatches_when_tag_pass_held_different_template`.
#[test]
fn pin_key_is_template_plus_hash_not_just_hash() {
    let now = 10_000u64;
    // Both builds share hash "h"; only the "seed" one is pinned.
    // The "code-runtime" build must still be eligible.
    let builds = vec![
        build("seed", "h", 500, 0, now - 5000),
        build("code-runtime", "h", 500, 0, now - 4000),
    ];
    let pinned = keys(&[("seed", "h")]);
    // total=1000, budget=400, target=360, need=640.
    // Only code-runtime/h is eligible; freed=500 < need=640 → plan still includes it.
    let plan = plan_evictions(
        builds,
        &pinned,
        &keys(&[]),
        &keys(&[]),
        &cfg(400, 0.9, 60, now),
    );
    assert_eq!(plan.len(), 1);
    assert_eq!(plan[0].name, "code-runtime");
    assert_eq!(plan[0].content_hash, "h");
}

// ── Phase 2 failure ↔ stuck-deleting recovery ────────────────────
//
// The comment on `execute_one_keeps_db_row_when_disk_delete_fails`
// claims "the build row stays so the next tick can retry". In
// practice the CAS in `execute_precheck` only matches
// `state='ready'`, so the next tick logs SkippedRowMissing and the
// row sits in `state='deleting'` until daemon restart. That is a
// liveness gap with two production-realistic triggers, both
// surfaced by the btrfs install path:
//
// 1. Transient nodemgr unreachable during Phase 2: the closure's
//    fallback runs `remove_build_artifacts` (plain `remove_dir_all`,
//    not subvol-aware), which returns Err because `rmdir` rejects
//    the subvolume root. Closure surfaces Err; Phase 3 is skipped.
// 2. Slow EBS delete that exceeds the gRPC deadline: nodemgr
//    returns DeadlineExceeded; same closure-Err shape.
//
// Both cases leave the build invisible to `resolve_template`
// (state filter) and unupsertable by a concurrent `do_pull` (ON
// CONFLICT guard). Startup recovery only resolves this on daemon restart,
// so it cannot bound recovery time by itself.
//
// The fix lives at the `gc_tick` layer, not at `execute_one_async`:
// a single "stuck-deleting reaper" pass runs at the start of every
// tick, BEFORE any Phase 1 CAS, and resolves leftover `'deleting'`
// rows by consulting on-disk state — drop the row if the digest
// dir is gone (Phase 2 partially succeeded), reset it to `'ready'`
// if the bytes are intact (Phase 2 failed cleanly). Race-free
// because no concurrent eviction is in flight at start-of-tick:
// `execute_precheck` is the sole writer of `'deleting'`, and it
// hasn't run yet within this tick.

/// Capture of the current behavior so the fix below has something
/// to push against: when Phase 2 errors, the row stays in
/// `state='deleting'`. Documents the state we walk INTO so the
/// reaper tests below can document the way OUT.
#[tokio::test]
async fn execute_one_async_leaves_row_in_deleting_after_phase2_error() {
    use std::sync::Mutex;
    let raw = crate::db::test_conn();
    seed_build(&raw, "seed", "h1", 1_000);
    let db = Arc::new(Mutex::new(raw));

    let cand = EvictionCandidate {
        name: "seed".to_string(),
        content_hash: "h1".to_string(),
        size_bytes: 1_000,
    };
    let delete_disk: DeleteDiskFn = Arc::new(|_, _| {
        Box::pin(async move {
            Err(rusqlite::Error::SqliteFailure(
                rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_IOERR),
                Some("nodemgr unreachable; fallback rmdir refused subvol root".into()),
            ))
        })
    });

    let outcome = execute_one_async(
        Arc::clone(&db),
        HashSet::new(),
        HashSet::new(),
        cand,
        delete_disk,
    )
    .await;
    assert!(outcome.is_err(), "Phase 2 error must bubble out");

    let state: String = db
        .lock()
        .unwrap()
        .query_row(
            "SELECT state FROM template_builds WHERE name = ?1 AND content_hash = ?2",
            params!["seed", "h1"],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        state, "deleting",
        "row must remain marked 'deleting' after a failed disk delete",
    );
}

/// `gc_tick` end-to-end: a prior tick left the row in
/// `state='deleting'` (Phase 2 failed). The current tick's reaper
/// must observe that the on-disk artifact is still present (the
/// nodemgr RPC error path didn't wipe contents), reset the row
/// to `'ready'`, and the planner inside the same tick must then
/// be free to plan it again and finalize the delete.
///
/// **Will fail on current main**: with no reaper, the planner's
/// `list_eviction_candidates` filters on `state='ready'`, so the
/// stuck row is invisible and never re-evicted.
#[tokio::test]
async fn gc_tick_recovers_stuck_deleting_row_when_disk_intact() {
    use std::sync::Mutex;
    let raw = crate::db::test_conn();
    seed_build(&raw, "seed", "h1", 1_000);
    // Plant the stuck-deleting state directly (models a prior
    // tick whose Phase 2 errored after Phase 1 committed).
    raw.execute(
        "UPDATE template_builds SET state = 'deleting' WHERE name = ?1 AND content_hash = ?2",
        params!["seed", "h1"],
    )
    .unwrap();
    crate::db::set_last_used_at_for_test(&raw, "seed", "h1", 1).unwrap();
    let db = Arc::new(Mutex::new(raw));

    let cfg = GcConfig {
        enabled: true,
        budget_bytes: 100,
        headroom_fraction: 0.9,
        interval: Duration::from_secs(60),
        grace: Duration::from_secs(0),
    };

    let deleted = Arc::new(Mutex::new(Vec::<(String, String)>::new()));
    let deleted_capture = Arc::clone(&deleted);
    let delete_disk: DeleteDiskFn = Arc::new(move |n, h| {
        let captured = Arc::clone(&deleted_capture);
        let name = n.to_string();
        let hash = h.to_string();
        Box::pin(async move {
            captured.lock().unwrap().push((name, hash));
            Ok(())
        })
    });
    let disk_size: DiskSizeFn = Arc::new(|_, _, fallback, _| fallback);

    // The reaper reports "Intact" for the stuck row so the tick
    // resets it to 'ready' and the planner re-evicts it in this
    // same call.
    let reaper_probe: ReaperDiskProbeFn =
        Arc::new(|_, _| Box::pin(async { ReaperDiskState::Intact }));

    let report = gc_tick(
        Arc::clone(&db),
        &cfg,
        HashSet::new(),
        HashSet::new(),
        disk_size,
        delete_disk,
        reaper_probe,
    )
    .await
    .unwrap();

    assert_eq!(
        report.deleted, 1,
        "the reaper must restore the stuck row to 'ready' so the same tick re-plans and deletes it",
    );
    assert_eq!(
        *deleted.lock().unwrap(),
        vec![("seed".to_string(), "h1".to_string())],
        "the disk closure must run for the recovered row",
    );
    let remaining: i64 = db
        .lock()
        .unwrap()
        .query_row("SELECT COUNT(*) FROM template_builds", [], |r| r.get(0))
        .unwrap();
    assert_eq!(
        remaining, 0,
        "row must be deleted after the recovery tick succeeds"
    );
}

/// Same shape as above, but the disk has already been (partially)
/// wiped — `rootfs.ext4` is gone, the on-disk recovery probe
/// reports "missing". The reaper should drop the row outright
/// rather than reset it to `'ready'`, otherwise a sandbox-create
/// against the half-wiped build would fail at `launch_prepare`'s
/// `cp --reflink` step with a confusing "no such file" error.
/// `do_pull` will re-materialize the build on the next access via
/// its existing `digest_dir_existed` path.
///
/// **Will fail on current main**: no reaper exists, so the row
/// stays `'deleting'` forever.
#[tokio::test]
async fn gc_tick_recovers_stuck_deleting_row_when_disk_missing_by_dropping_row() {
    use std::sync::Mutex;
    let raw = crate::db::test_conn();
    seed_build(&raw, "seed", "h1", 1_000);
    raw.execute(
        "UPDATE template_builds SET state = 'deleting' WHERE name = ?1 AND content_hash = ?2",
        params!["seed", "h1"],
    )
    .unwrap();
    let db = Arc::new(Mutex::new(raw));

    let cfg = GcConfig {
        enabled: true,
        budget_bytes: 100,
        headroom_fraction: 0.9,
        interval: Duration::from_secs(60),
        grace: Duration::from_secs(0),
    };

    // The disk closure should not be invoked: the reaper drops
    // the row before the planner can pick it up.
    let disk_called = Arc::new(Mutex::new(false));
    let disk_called_capture = Arc::clone(&disk_called);
    let delete_disk: DeleteDiskFn = Arc::new(move |_, _| {
        let captured = Arc::clone(&disk_called_capture);
        Box::pin(async move {
            *captured.lock().unwrap() = true;
            Ok(())
        })
    });
    let disk_size: DiskSizeFn = Arc::new(|_, _, fallback, _| fallback);

    // Reaper closure: report "missing" for our test build so the
    // reaper drops the row. Production wires this to a real disk
    // probe (rootfs.ext4 presence check).
    let reaper_probe: ReaperDiskProbeFn =
        Arc::new(|_, _| Box::pin(async { ReaperDiskState::Missing }));

    let _ = gc_tick(
        Arc::clone(&db),
        &cfg,
        HashSet::new(),
        HashSet::new(),
        disk_size,
        delete_disk,
        reaper_probe,
    )
    .await
    .unwrap();

    assert!(
        !*disk_called.lock().unwrap(),
        "the reaper must drop the row before the planner can replan; disk closure must NOT run",
    );
    let remaining: i64 = db
        .lock()
        .unwrap()
        .query_row("SELECT COUNT(*) FROM template_builds", [], |r| r.get(0))
        .unwrap();
    assert_eq!(
        remaining, 0,
        "row must be dropped when on-disk state is missing"
    );
}

/// A pin appearing while the row is stuck in `'deleting'` must
/// restore the build to `'ready'` so resolve_template can return
/// it again. Without this, an operator pinning a tag to a
/// stuck-deleting build sees the pin land but the build remains
/// invisible until daemon restart — the pin is silently
/// ineffective.
///
/// **Will fail on current main**: the reaper doesn't exist and
/// `execute_precheck`'s pin check returns SkipPinned without
/// updating state.
#[tokio::test]
async fn gc_tick_releases_stuck_deleting_row_when_pin_lands() {
    use std::sync::Mutex;
    let raw = crate::db::test_conn();
    seed_build(&raw, "seed", "h1", 1_000);
    raw.execute(
        "UPDATE template_builds SET state = 'deleting' WHERE name = ?1 AND content_hash = ?2",
        params!["seed", "h1"],
    )
    .unwrap();
    plant_pin(&raw, "seed", "h1");
    let db = Arc::new(Mutex::new(raw));

    let cfg = GcConfig {
        enabled: true,
        budget_bytes: 100,
        headroom_fraction: 0.9,
        interval: Duration::from_secs(60),
        grace: Duration::from_secs(0),
    };

    let delete_disk: DeleteDiskFn = Arc::new(|_, _| Box::pin(async { Ok(()) }));
    let disk_size: DiskSizeFn = Arc::new(|_, _, fallback, _| fallback);

    // Reaper reports "Intact" — bytes are still on disk in this
    // scenario (the prior tick's Phase 2 errored cleanly).
    let reaper_probe: ReaperDiskProbeFn =
        Arc::new(|_, _| Box::pin(async { ReaperDiskState::Intact }));

    let _ = gc_tick(
        Arc::clone(&db),
        &cfg,
        HashSet::new(),
        HashSet::new(),
        disk_size,
        delete_disk,
        reaper_probe,
    )
    .await
    .unwrap();

    let state: String = db
        .lock()
        .unwrap()
        .query_row(
            "SELECT state FROM template_builds WHERE name = ?1 AND content_hash = ?2",
            params!["seed", "h1"],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        state, "ready",
        "the reaper must restore a pinned stuck-deleting row to 'ready' so the pin is honored",
    );
}

/// Seed `name@hash` created and last used at `secs`, so a test controls which
/// side of an idle bound it falls on.
fn seed_build_touched_at(conn: &Connection, name: &str, hash: &str, secs: u64) {
    seed_build(conn, name, hash, 100);
    conn.execute(
        "UPDATE template_builds SET created_at = ?3, last_used_at = ?3
         WHERE name = ?1 AND content_hash = ?2",
        params![name, hash, secs.to_string()],
    )
    .unwrap();
}

/// One row of `idle_template_eviction_takes_every_build_only_when_none_is_protected`.
struct IdleCase {
    case: &'static str,
    /// Each build of `doomed` and its last touch.
    builds: &'static [(&'static str, u64)],
    pinned: &'static [&'static str],
    active: &'static [&'static str],
    warm_pool: &'static [&'static str],
    expected: IdleTemplateEviction,
}

impl IdleCase {
    const fn new(
        case: &'static str,
        builds: &'static [(&'static str, u64)],
        pinned: &'static [&'static str],
        active: &'static [&'static str],
        warm_pool: &'static [&'static str],
        expected: IdleTemplateEviction,
    ) -> Self {
        Self {
            case,
            builds,
            pinned,
            active,
            warm_pool,
            expected,
        }
    }
}

#[tokio::test]
async fn idle_template_eviction_takes_every_build_only_when_none_is_protected() {
    use IdleTemplateKept::{Active, Pinned, Recent, WarmPool};
    let bound = 1_000;
    let kept = |reason| IdleTemplateEviction::Kept { evicted: 0, reason };
    let row = IdleCase::new;
    let cases = [
        row(
            "idle",
            &[("h1", 500), ("h2", 1_000)],
            &[],
            &[],
            &[],
            IdleTemplateEviction::Evicted,
        ),
        row(
            "one recent",
            &[("h1", 500), ("h2", 1_001)],
            &[],
            &[],
            &[],
            kept(Recent),
        ),
        row("pinned", &[("h1", 500)], &["h1"], &[], &[], kept(Pinned)),
        row("in use", &[("h1", 500)], &[], &["h1"], &[], kept(Active)),
        row(
            "warm pool",
            &[("h1", 500)],
            &[],
            &[],
            &["h1"],
            kept(WarmPool),
        ),
        row(
            "not cached",
            &[],
            &[],
            &[],
            &[],
            IdleTemplateEviction::NotFound,
        ),
    ];
    for IdleCase {
        case,
        builds,
        pinned,
        active,
        warm_pool,
        expected,
    } in cases
    {
        let conn = crate::db::test_conn();
        seed_build_touched_at(&conn, "other", "h1", 1);
        for (hash, touched) in builds {
            seed_build_touched_at(&conn, "doomed", hash, *touched);
        }
        for hash in pinned {
            plant_pin(&conn, "doomed", hash);
        }
        crate::db::upsert_template_settings(
            &conn,
            "doomed",
            &pplx_space_common_core::types::AutoLifecyclePolicy::disabled(),
        )
        .unwrap();
        let db = Arc::new(Mutex::new(conn));
        let deleted = Arc::new(Mutex::new(Vec::new()));
        let delete_disk: DeleteDiskFn = {
            let deleted = Arc::clone(&deleted);
            Arc::new(move |name, hash| {
                deleted.lock().unwrap().push(format!("{name}@{hash}"));
                Box::pin(async { Ok(()) })
            })
        };
        let pairs = |hashes: &[&str]| {
            hashes
                .iter()
                .map(|hash| ("doomed".to_string(), (*hash).to_string()))
                .collect::<HashSet<_>>()
        };

        let outcome = evict_idle_template(
            Arc::clone(&db),
            &EvictionGate::default(),
            "doomed",
            bound,
            pairs(active),
            pairs(warm_pool),
            EvictionDisk {
                delete: delete_disk,
                probe: noop_reaper_probe(),
            },
        )
        .await
        .unwrap();

        assert_eq!(outcome, expected, "{case}");
        let conn = db.lock().unwrap();
        let left = crate::db::list_builds_by_name(&conn, "doomed").unwrap();
        let settings = crate::db::get_template_settings(&conn, "doomed").unwrap();
        if expected == IdleTemplateEviction::Evicted {
            assert!(left.is_empty(), "{case}: every build is gone");
            assert!(settings.is_none(), "{case}: so are the template's settings");
            let mut deleted = deleted.lock().unwrap().clone();
            deleted.sort();
            assert_eq!(deleted, ["doomed@h1", "doomed@h2"], "{case}");
        } else {
            assert_eq!(left.len(), builds.len(), "{case}: no build is evicted");
            assert!(settings.is_some(), "{case}: settings stay");
            assert!(deleted.lock().unwrap().is_empty(), "{case}");
        }
        assert_eq!(
            crate::db::list_builds_by_name(&conn, "other")
                .unwrap()
                .len(),
            1,
            "{case}: other templates are untouched"
        );
    }
}

/// What happens while a build's files are being deleted, with the DB lock
/// released: another build is handed to a sandbox create (`GetTemplate`
/// touches it), a pull records a new build, or the disk delete fails.
#[tokio::test]
async fn idle_template_eviction_rechecks_what_changes_during_a_delete() {
    #[derive(Clone, Copy, Debug)]
    enum During {
        OtherBuildUsed,
        NewBuildPulled,
        DiskDeleteFails,
    }
    for during in [
        During::OtherBuildUsed,
        During::NewBuildPulled,
        During::DiskDeleteFails,
    ] {
        let conn = crate::db::test_conn();
        seed_build_touched_at(&conn, "doomed", "h1", 500);
        seed_build_touched_at(&conn, "doomed", "h2", 500);
        crate::db::upsert_template_settings(
            &conn,
            "doomed",
            &pplx_space_common_core::types::AutoLifecyclePolicy::disabled(),
        )
        .unwrap();
        let db = Arc::new(Mutex::new(conn));
        let first = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let delete_disk: DeleteDiskFn = {
            let db = Arc::clone(&db);
            Arc::new(move |name, hash| {
                let first = first.swap(false, std::sync::atomic::Ordering::SeqCst);
                let fail = first && matches!(during, During::DiskDeleteFails);
                if first {
                    let conn = db.lock().unwrap();
                    match during {
                        During::OtherBuildUsed => {
                            let other = if hash == "h1" { "h2" } else { "h1" };
                            crate::db::touch_build(&conn, name, other).unwrap();
                        }
                        During::NewBuildPulled => seed_build(&conn, name, "h3", 100),
                        During::DiskDeleteFails => {}
                    }
                }
                Box::pin(async move {
                    if fail {
                        Err(rusqlite::Error::InvalidQuery)
                    } else {
                        Ok(())
                    }
                })
            })
        };
        let intact: ReaperDiskProbeFn =
            Arc::new(|_, _| Box::pin(async { ReaperDiskState::Intact }));

        let outcome = evict_idle_template(
            Arc::clone(&db),
            &EvictionGate::default(),
            "doomed",
            1_000,
            keys(&[]),
            keys(&[]),
            EvictionDisk {
                delete: delete_disk,
                probe: intact,
            },
        )
        .await;

        let conn = db.lock().unwrap();
        let ready = crate::db::list_eviction_candidates(&conn)
            .unwrap()
            .into_iter()
            .map(|build| build.content_hash)
            .collect::<Vec<_>>();
        let settings_kept = crate::db::get_template_settings(&conn, "doomed")
            .unwrap()
            .is_some();
        match during {
            During::OtherBuildUsed => {
                assert_eq!(
                    outcome.unwrap(),
                    IdleTemplateEviction::Kept {
                        evicted: 1,
                        reason: IdleTemplateKept::Changed,
                    }
                );
                assert_eq!(ready.len(), 1, "the build used meanwhile stays ready");
            }
            During::NewBuildPulled => {
                assert_eq!(
                    outcome.unwrap(),
                    IdleTemplateEviction::Kept {
                        evicted: 2,
                        reason: IdleTemplateKept::Changed,
                    },
                    "the template is still cached, so it is reported kept"
                );
                assert_eq!(ready, ["h3"], "the pulled build keeps its row");
            }
            During::DiskDeleteFails => {
                outcome.expect_err("the failed disk delete is reported");
                assert_eq!(ready.len(), 2, "the failed build is back to ready");
                assert!(
                    !crate::db::is_build_marked_deleting(&conn, "doomed", "h1").unwrap()
                        && !crate::db::is_build_marked_deleting(&conn, "doomed", "h2").unwrap(),
                    "nothing is left in deleting"
                );
            }
        }
        assert!(
            settings_kept,
            "{during:?}: a build is left, so settings stay"
        );
    }
}

/// A GC tick's repair pass puts a `deleting` row with intact artifacts back to
/// `ready`. It must not do that to a build an idle eviction is still deleting,
/// or `GetTemplate` could hand that build to a sandbox create just before its
/// files go.
#[tokio::test]
async fn gc_repair_waits_for_a_live_idle_eviction() {
    let conn = crate::db::test_conn();
    seed_build_touched_at(&conn, "doomed", "h1", 500);
    let db = Arc::new(Mutex::new(conn));
    let gate = EvictionGate::default();
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let delete_disk: DeleteDiskFn = {
        let (entered, release) = (Arc::clone(&entered), Arc::clone(&release));
        Arc::new(move |_, _| {
            let (entered, release) = (Arc::clone(&entered), Arc::clone(&release));
            Box::pin(async move {
                entered.notify_one();
                release.notified().await;
                Ok(())
            })
        })
    };
    let eviction = tokio::spawn({
        let (db, gate) = (Arc::clone(&db), gate.clone());
        async move {
            evict_idle_template(
                db,
                &gate,
                "doomed",
                1_000,
                keys(&[]),
                keys(&[]),
                EvictionDisk {
                    delete: delete_disk,
                    probe: noop_reaper_probe(),
                },
            )
            .await
        }
    });
    entered.notified().await;
    assert_eq!(
        evict_idle_template(
            Arc::clone(&db),
            &gate,
            "doomed",
            1_000,
            keys(&[]),
            keys(&[]),
            EvictionDisk {
                delete: Arc::new(|_, _| panic!("a second eviction must not start")),
                probe: noop_reaper_probe(),
            },
        )
        .await
        .unwrap(),
        IdleTemplateEviction::Busy,
        "a second eviction does not wait for the gate"
    );

    let reaped = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let reaper_probe: ReaperDiskProbeFn = {
        let reaped = Arc::clone(&reaped);
        Arc::new(move |_, _| {
            reaped.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Box::pin(async { ReaperDiskState::Intact })
        })
    };
    let shutdown = CancellationToken::new();
    let gc = tokio::spawn(run_loop(
        Arc::clone(&db),
        GcConfig {
            enabled: true,
            budget_bytes: u64::MAX,
            headroom_fraction: 0.9,
            interval: Duration::from_millis(10),
            grace: Duration::from_secs(0),
        },
        shutdown.clone(),
        GcHooks {
            fetch_active: Arc::new(|| Box::pin(async { Ok(HashSet::new()) })),
            fetch_warm_pool: empty_warm_pool_fetch(),
            disk_size_for: Arc::new(|_, _, fallback, _| fallback),
            delete_disk_for: Arc::new(|_, _| Box::pin(async { Ok(()) })),
            reaper_probe,
        },
        gate,
    ));
    // Long enough for several ticks had the loop not waited for the gate.
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(
        reaped.load(std::sync::atomic::Ordering::SeqCst),
        0,
        "no repair pass ran during the eviction"
    );
    assert!(
        crate::db::is_build_marked_deleting(&db.lock().unwrap(), "doomed", "h1").unwrap(),
        "the build stays out of reach of GetTemplate"
    );

    release.notify_one();
    assert_eq!(
        eviction.await.unwrap().unwrap(),
        IdleTemplateEviction::Evicted
    );
    shutdown.cancel();
    gc.await.unwrap();
    assert!(
        crate::db::list_builds_by_name(&db.lock().unwrap(), "doomed")
            .unwrap()
            .is_empty()
    );
}
