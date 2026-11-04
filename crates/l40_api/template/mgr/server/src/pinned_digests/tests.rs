use super::*;

fn store_with_in_memory_db() -> PinnedDigestsStore {
    let conn = crate::db::test_conn();
    PinnedDigestsStore::new(Arc::new(Mutex::new(conn)))
}

#[test]
fn upsert_release_replaces_metadata_on_conflict() {
    let store = store_with_in_memory_db();

    store
        .upsert_release("seed", "sha256:aa", "rel-1", "2026-01-01T00:00:00Z")
        .unwrap();
    store
        .upsert_release("seed", "sha256:aa", "rel-2", "2026-02-01T00:00:00Z")
        .unwrap();

    let (release_id, release_created_at): (String, String) = {
        let conn = store.conn.lock().unwrap();
        conn.query_row(
            "SELECT release_id, release_created_at FROM template_pinned_releases
             WHERE name = ?1 AND content_hash = ?2",
            params!["seed", "sha256:aa"],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap()
    };
    assert_eq!(release_id, "rel-2");
    assert_eq!(release_created_at, "2026-02-01T00:00:00Z");
}

#[test]
fn list_pinned_hashes_unions_tag_and_release_pins() {
    let store = store_with_in_memory_db();

    store.upsert_tag("seed", "latest", "sha256:aa").unwrap();
    store
        .upsert_release("seed", "sha256:aa", "rel-1", "2026-01-01T00:00:00Z")
        .unwrap();
    store
        .upsert_release("seed", "sha256:bb", "rel-2", "2026-02-01T00:00:00Z")
        .unwrap();

    let mut hashes = store.list_pinned_hashes_for_template("seed").unwrap();
    hashes.sort();
    assert_eq!(
        hashes,
        vec!["sha256:aa".to_string(), "sha256:bb".to_string()]
    );
}

#[test]
fn prune_without_local_builds_drops_phantom_pins_only() {
    let store = store_with_in_memory_db();
    {
        let conn = store.conn.lock().unwrap();
        crate::db::upsert_build(
            &conn,
            &crate::db::BuildUpsert {
                name: "seed",
                content_hash: "sha256:aa",
                build_date: "2026-01-01T00:00:00Z",
                manifest_json: "{}",
                runtime: "fc",
                arch: "x86_64",
                artifact_size_bytes: 1,
                state: "ready",
            },
        )
        .unwrap();
    }
    store.upsert_tag("seed", "latest", "sha256:aa").unwrap();
    store
        .upsert_release("seed", "sha256:aa", "rel-1", "2026-01-01T00:00:00Z")
        .unwrap();
    store
        .upsert_tag("app-runtime", "latest", "sha256:missing")
        .unwrap();
    store
        .upsert_release(
            "app-runtime",
            "sha256:missing",
            "rel-2",
            "2026-01-01T00:00:00Z",
        )
        .unwrap();

    let pruned = store.prune_without_local_builds().unwrap();

    assert_eq!(pruned, 2);
    assert_eq!(
        store.list_pinned_hashes_for_template("seed").unwrap(),
        vec!["sha256:aa".to_string()]
    );
    assert!(
        store
            .list_pinned_hashes_for_template("app-runtime")
            .unwrap()
            .is_empty()
    );
}

#[test]
fn prune_releases_outside_only_touches_listed_templates() {
    let store = store_with_in_memory_db();

    store
        .upsert_release("seed", "sha256:aa", "rel-1", "2026-01-01T00:00:00Z")
        .unwrap();
    store
        .upsert_release("seed", "sha256:bb", "rel-2", "2026-02-01T00:00:00Z")
        .unwrap();
    store
        .upsert_release("seed", "sha256:cc", "rel-3", "2026-03-01T00:00:00Z")
        .unwrap();
    store
        .upsert_release("app-runtime", "sha256:dd", "rel-4", "2026-04-01T00:00:00Z")
        .unwrap();

    // Prune seed down to {aa, cc}; app-runtime must be untouched
    // because it isn't in the templates list — a transient
    // cluster-api failure must not flush an unrelated template's
    // pins.
    store
        .prune_releases_outside(
            &["seed".to_string()],
            &[
                ("seed".to_string(), "sha256:aa".to_string()),
                ("seed".to_string(), "sha256:cc".to_string()),
            ],
        )
        .unwrap();

    let mut seed = store.list_release_hashes("seed").unwrap();
    seed.sort();
    assert_eq!(seed, vec!["sha256:aa".to_string(), "sha256:cc".to_string()]);
    let app = store.list_release_hashes("app-runtime").unwrap();
    assert_eq!(app, vec!["sha256:dd".to_string()]);
}

#[test]
fn prune_releases_outside_empty_keep_clears_listed_templates() {
    let store = store_with_in_memory_db();

    store
        .upsert_release("seed", "sha256:aa", "rel-1", "2026-01-01T00:00:00Z")
        .unwrap();
    store
        .upsert_release("app-runtime", "sha256:bb", "rel-2", "2026-02-01T00:00:00Z")
        .unwrap();

    store
        .prune_releases_outside(&["seed".to_string()], &[])
        .unwrap();

    assert!(store.list_release_hashes("seed").unwrap().is_empty());
    assert_eq!(
        store.list_release_hashes("app-runtime").unwrap(),
        vec!["sha256:bb".to_string()]
    );
}

/// Regression test: a transient per-row `upsert_release` failure in
/// `prefetch::tick()` used to skip the `release_pins_keep.push`
/// step, leaving a previously-pinned digest absent from the
/// keep-set passed to this prune call. Result: the prior pin
/// row was deleted, dropping GC protection on a digest that the
/// cluster still reports as part of the latest-N window.
///
/// The bug lives in `tick()`'s control flow (no clean injection
/// point for SQLite faults). This test verifies the
/// *consequence* at the closest layer where the behavior is
/// observable: given a previously-pinned digest, the prune
/// preserves it iff the keep-set includes it. That makes the
/// keep-set the entire game — which is why the
/// `tick()` fix moves the keep-set push to before the upsert
/// attempt.
#[test]
fn prune_releases_outside_drops_prior_pin_when_keep_set_omits_it() {
    let store = store_with_in_memory_db();

    store
        .upsert_release("seed", "sha256:aa", "rel-1", "2026-01-01T00:00:00Z")
        .unwrap();
    store
        .upsert_release("seed", "sha256:bb", "rel-2", "2026-02-01T00:00:00Z")
        .unwrap();

    // Bug-shaped keep-set: simulate the pre-fix tick() where an
    // upsert failure on `sha256:bb` caused that entry to be
    // omitted from `release_pins_keep`. The cluster still says
    // sha256:bb is in the latest-N window, but the prune doesn't
    // know that.
    let bug_keep_set: Vec<(String, String)> = vec![
        ("seed".to_string(), "sha256:aa".to_string()),
        // sha256:bb intentionally missing here
    ];
    store
        .prune_releases_outside(&["seed".to_string()], &bug_keep_set)
        .unwrap();

    // The previously-pinned sha256:bb row has been flushed —
    // that's the bug. The post-fix keep-set always includes
    // every cluster-returned (template, hash) pair, so this
    // scenario can no longer arise from a transient upsert
    // failure.
    let surviving = store.list_release_hashes("seed").unwrap();
    assert_eq!(surviving, vec!["sha256:aa".to_string()]);
}

/// Companion: with the post-fix keep-set (every cluster-returned
/// digest included regardless of upsert outcome), both pins
/// survive even when one upsert "failed" this tick.
#[test]
fn prune_releases_outside_preserves_prior_pin_when_keep_set_includes_it() {
    let store = store_with_in_memory_db();

    store
        .upsert_release("seed", "sha256:aa", "rel-1", "2026-01-01T00:00:00Z")
        .unwrap();
    store
        .upsert_release("seed", "sha256:bb", "rel-2", "2026-02-01T00:00:00Z")
        .unwrap();

    // Post-fix keep-set: both cluster-returned digests are
    // included even though the bb upsert would have failed.
    let fixed_keep_set: Vec<(String, String)> = vec![
        ("seed".to_string(), "sha256:aa".to_string()),
        ("seed".to_string(), "sha256:bb".to_string()),
    ];
    store
        .prune_releases_outside(&["seed".to_string()], &fixed_keep_set)
        .unwrap();

    let mut surviving = store.list_release_hashes("seed").unwrap();
    surviving.sort();
    assert_eq!(
        surviving,
        vec!["sha256:aa".to_string(), "sha256:bb".to_string()]
    );
}

#[test]
fn prune_tags_outside_drops_only_removed_tuples() {
    let store = store_with_in_memory_db();

    store.upsert_tag("seed", "latest", "sha256:aa").unwrap();
    store
        .upsert_tag("code-runtime", "latest", "sha256:bb")
        .unwrap();
    store.upsert_tag("seed", "stable", "sha256:cc").unwrap();

    store
        .prune_tags_outside(&[
            ("seed".to_string(), "latest".to_string()),
            ("code-runtime".to_string(), "latest".to_string()),
        ])
        .unwrap();

    let mut hashes = store.list_pinned_hashes_for_template("seed").unwrap();
    hashes.sort();
    assert_eq!(hashes, vec!["sha256:aa".to_string()]);
}
