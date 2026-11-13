use super::*;

type StrKey = (String, String);

fn item(name: &str, hash: &str, size: u64, created: u64, used: u64) -> EvictionInput<StrKey> {
    EvictionInput {
        key: (name.to_string(), hash.to_string()),
        size_bytes: size,
        created_at_secs: created,
        last_used_at_secs: used,
    }
}

fn keys(pairs: &[(&str, &str)]) -> HashSet<StrKey> {
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

/// The caller-computed-need entry point: 0 bytes to free is a
/// no-op regardless of item ages, and a non-zero need plans in
/// the same LRU order as the budget path.
#[test]
fn plan_to_free_honors_caller_need() {
    let now = 10_000u64;
    let items = vec![
        item("seed", "newer", 500, 0, now - 1000),
        item("seed", "oldest", 500, 0, now - 5000),
    ];
    let none = plan_evictions_to_free(items.clone(), &keys(&[]), &keys(&[]), 0, 60, now);
    assert!(none.is_empty());
    let plan = plan_evictions_to_free(items, &keys(&[]), &keys(&[]), 400, 60, now);
    assert_eq!(plan.len(), 1);
    assert_eq!(plan[0].key.1, "oldest");
}

#[test]
fn under_budget_returns_nothing() {
    let items = vec![item("seed", "h1", 100, 0, 0), item("seed", "h2", 200, 0, 0)];
    let plan = plan_evictions(items, &keys(&[]), &keys(&[]), &cfg(1_000, 0.9, 60, 10_000));
    assert!(plan.is_empty());
}

#[test]
fn pinned_item_is_skipped() {
    let items = vec![item("seed", "h1", 1_000, 0, 0)];
    let pinned = keys(&[("seed", "h1")]);
    let plan = plan_evictions(items, &pinned, &keys(&[]), &cfg(100, 0.9, 60, 10_000));
    assert!(plan.is_empty());
}

#[test]
fn active_item_is_skipped() {
    let items = vec![item("seed", "h1", 1_000, 0, 0)];
    let active = keys(&[("seed", "h1")]);
    let plan = plan_evictions(items, &keys(&[]), &active, &cfg(100, 0.9, 60, 10_000));
    assert!(plan.is_empty());
}

#[test]
fn recently_created_within_grace_is_skipped() {
    let items = vec![item("seed", "h1", 1_000, 9_995, 9_995)];
    let plan = plan_evictions(items, &keys(&[]), &keys(&[]), &cfg(100, 0.9, 60, 10_000));
    assert!(
        plan.is_empty(),
        "freshly created item must not be evicted inside grace"
    );
}

#[test]
fn recently_used_within_grace_is_skipped() {
    let items = vec![item("seed", "h1", 1_000, 10_000 - 3600, 9_995)];
    let plan = plan_evictions(items, &keys(&[]), &keys(&[]), &cfg(100, 0.9, 60, 10_000));
    assert!(
        plan.is_empty(),
        "item accessed inside grace must not be evicted"
    );
}

#[test]
fn evicts_oldest_lru_first() {
    let now = 10_000u64;
    let items = vec![
        item("seed", "newer", 500, 0, now - 1000),
        item("seed", "oldest", 500, 0, now - 5000),
        item("seed", "middle", 500, 0, now - 3000),
    ];
    let plan = plan_evictions(items, &keys(&[]), &keys(&[]), &cfg(1_000, 0.9, 60, now));
    assert_eq!(plan.len(), 2);
    assert_eq!(plan[0].key.1, "oldest");
    assert_eq!(plan[1].key.1, "middle");
}

#[test]
fn stops_at_headroom_target() {
    let now = 10_000u64;
    let items = vec![
        item("seed", "a", 250, 0, now - 4000),
        item("seed", "b", 250, 0, now - 3000),
        item("seed", "c", 250, 0, now - 2000),
        item("seed", "d", 250, 0, now - 1000),
    ];
    let plan = plan_evictions(items, &keys(&[]), &keys(&[]), &cfg(800, 0.9, 60, now));
    assert_eq!(plan.len(), 2);
    assert_eq!(plan[0].key.1, "a");
    assert_eq!(plan[1].key.1, "b");
}

#[test]
fn clamps_headroom_target_to_budget_range() {
    assert_eq!(headroom_target_bytes(1_000, f64::NAN), 0);
    assert_eq!(headroom_target_bytes(1_000, -0.5), 0);
    assert_eq!(headroom_target_bytes(1_000, 1.5), 1_000);
}

#[test]
fn evicts_multiple_until_headroom_reached() {
    let now = 10_000u64;
    let items = vec![
        item("seed", "a", 100, 0, now - 5000),
        item("seed", "b", 100, 0, now - 4000),
        item("seed", "c", 100, 0, now - 3000),
        item("seed", "d", 100, 0, now - 2000),
        item("seed", "e", 100, 0, now - 1000),
    ];
    let plan = plan_evictions(items, &keys(&[]), &keys(&[]), &cfg(200, 0.9, 60, now));
    assert_eq!(plan.len(), 4);
    assert_eq!(
        plan.iter().map(|c| c.key.1.as_str()).collect::<Vec<_>>(),
        vec!["a", "b", "c", "d"],
    );
}

#[test]
fn over_budget_but_everything_pinned_returns_empty() {
    let items = vec![item("seed", "h1", 500, 0, 0), item("seed", "h2", 500, 0, 0)];
    let pinned = keys(&[("seed", "h1"), ("seed", "h2")]);
    let plan = plan_evictions(items, &pinned, &keys(&[]), &cfg(100, 0.9, 60, 10_000));
    assert!(plan.is_empty());
}

#[test]
fn lru_tiebreak_is_stable_on_key_order() {
    let now = 10_000u64;
    let items = vec![
        item("zebra", "h1", 500, 0, 100),
        item("alpha", "h2", 500, 0, 100),
        item("alpha", "h1", 500, 0, 100),
    ];
    let plan = plan_evictions(items, &keys(&[]), &keys(&[]), &cfg(500, 0.9, 60, now));
    assert_eq!(plan.len(), 3);
    assert_eq!(plan[0].key, ("alpha".to_string(), "h1".to_string()));
    assert_eq!(plan[1].key, ("alpha".to_string(), "h2".to_string()));
    assert_eq!(plan[2].key, ("zebra".to_string(), "h1".to_string()));
}

#[test]
fn pinned_and_active_skips_do_not_stop_iteration() {
    let now = 10_000u64;
    let items = vec![
        item("seed", "pinned", 500, 0, now - 5000),
        item("seed", "active", 500, 0, now - 4000),
        item("seed", "evictable", 500, 0, now - 3000),
        item("seed", "fresh", 500, 0, now - 2000),
    ];
    let pinned = keys(&[("seed", "pinned")]);
    let active = keys(&[("seed", "active")]);
    let plan = plan_evictions(items, &pinned, &active, &cfg(1_000, 0.9, 60, now));
    assert_eq!(plan.len(), 2);
    assert_eq!(plan[0].key.1, "evictable");
    assert_eq!(plan[1].key.1, "fresh");
}

/// Mixed-key-type smoke: the planner is generic, so any key with
/// the right trait bounds works. Templates use `(String, String)`,
/// snapshots will use a single id string. Exercise the single-id
/// shape so regressions there are caught here.
#[test]
fn works_with_single_string_key() {
    let now = 10_000u64;
    let items = vec![
        EvictionInput {
            key: "art-a".to_string(),
            size_bytes: 500,
            created_at_secs: 0,
            last_used_at_secs: now - 3000,
        },
        EvictionInput {
            key: "art-b".to_string(),
            size_bytes: 500,
            created_at_secs: 0,
            last_used_at_secs: now - 1000,
        },
    ];
    // total=1000, budget=600, target=540, need_to_free=460.
    // After 1 eviction (500): freed ≥ need → break next iter.
    let plan = plan_evictions(
        items,
        &HashSet::new(),
        &HashSet::new(),
        &cfg(600, 0.9, 60, now),
    );
    assert_eq!(plan.len(), 1);
    assert_eq!(plan[0].key, "art-a");
}
