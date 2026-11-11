use super::*;
use std::num::NonZeroUsize;
use time::OffsetDateTime;

/// A base instant plus an offset, so every test states ages explicitly instead
/// of sleeping. `Instant` has no public constructor, hence the `now()` anchor.
fn at(base: Instant, secs: u64) -> Instant {
    base + Duration::from_secs(secs)
}

/// A stand-in for Postgres's `clock_timestamp()`. Ordering is what the cache
/// uses this for, so the tests only need it to be monotone in `secs` — the
/// epoch it is anchored to is irrelevant.
fn observed(secs: u64) -> OffsetDateTime {
    OffsetDateTime::UNIX_EPOCH + Duration::from_secs(secs)
}

fn cache(ttl_secs: u64, ceiling_secs: u64) -> TemplateVisibilityCache {
    TemplateVisibilityCache::new(CacheConfig {
        ttl: Duration::from_secs(ttl_secs),
        stale_ceiling: Duration::from_secs(ceiling_secs),
        max_entries: NonZeroUsize::new(1024).unwrap(),
    })
}

#[test]
fn a_recent_read_answers_without_a_query() {
    let base = Instant::now();
    let cache = cache(30, 600);
    cache.store("seed", TemplateVisibility::Generic, base, observed(0));

    assert_eq!(
        cache.get_fresh("seed", at(base, 29)),
        Some(TemplateVisibility::Generic)
    );
}

#[test]
fn an_unknown_template_is_never_answered_for() {
    let base = Instant::now();
    let cache = cache(30, 600);
    cache.store("seed", TemplateVisibility::Generic, base, observed(0));

    assert_eq!(cache.get_fresh("code-runtime", base), None);
    assert_eq!(cache.get_stale("code-runtime", base), None);
}

/// The two windows are independent, and this is the whole point of the design:
/// past the TTL the value is too old to answer *instead of* querying, but still
/// good enough to answer *after a query that could not reach the row*.
#[test]
fn past_the_ttl_a_read_is_no_longer_fresh_but_is_still_usable_as_a_fallback() {
    let base = Instant::now();
    let cache = cache(30, 600);
    cache.store("seed", TemplateVisibility::OrgSpecific, base, observed(0));

    assert_eq!(cache.get_fresh("seed", at(base, 31)), None);
    assert_eq!(
        cache.get_stale("seed", at(base, 31)),
        Some(TemplateVisibility::OrgSpecific)
    );
}

/// Invariant 3. A sustained outage must go back to failing closed, which is the
/// property that distinguishes this from a permanent allowlist carve-out.
#[test]
fn past_the_stale_ceiling_nothing_is_served() {
    let base = Instant::now();
    let cache = cache(30, 600);
    cache.store("seed", TemplateVisibility::Generic, base, observed(0));

    assert_eq!(
        cache.get_stale("seed", at(base, 599)),
        Some(TemplateVisibility::Generic)
    );
    assert_eq!(cache.get_stale("seed", at(base, 600)), None);
    assert_eq!(cache.get_stale("seed", at(base, 601)), None);
}

/// A read exactly *at* the limit is expired. Asserted on the pure helper rather
/// than through the cache because the same comparison bounds both windows, and
/// an off-by-one here silently widens the isolation window in invariant 3.
#[test]
fn an_entry_exactly_at_the_limit_is_expired() {
    let base = Instant::now();
    let limit = Duration::from_secs(30);

    assert!(entry_is_usable(base, limit, base));
    assert!(entry_is_usable(
        base,
        limit,
        base + Duration::from_millis(29_999)
    ));
    assert!(!entry_is_usable(base, limit, base + limit));
    assert!(!entry_is_usable(
        base,
        limit,
        base + limit + Duration::from_millis(1)
    ));

    // A zero limit needs no guard of its own: no age is less than zero, so a
    // disabled window answers false even for an entry stored this instant.
    // `get_within` short-circuits before calling this, so nothing depends on it
    // — but the comment there claims this holds, so it gets an assertion.
    assert!(!entry_is_usable(base, Duration::ZERO, base));
    assert!(!entry_is_usable(
        base,
        Duration::ZERO,
        base + Duration::from_secs(1)
    ));
}

/// `Duration::ZERO` on either knob reproduces the pre-cache behavior for that
/// half, which is the kill switch. Tested per window because they disable
/// independently: `ttl = 0` with a live ceiling is a documented configuration
/// (no staleness on the happy path, resilience only on the sad one).
#[test]
fn a_zero_window_disables_that_half_of_the_cache() {
    let base = Instant::now();

    let no_fresh = cache(0, 600);
    no_fresh.store("seed", TemplateVisibility::Generic, base, observed(0));
    assert_eq!(
        no_fresh.get_fresh("seed", base),
        None,
        "ttl 0 must always query"
    );
    assert_eq!(
        no_fresh.get_stale("seed", base),
        Some(TemplateVisibility::Generic),
        "a disabled fresh window must not disable the fallback"
    );

    let no_fallback = cache(30, 0);
    no_fallback.store("seed", TemplateVisibility::Generic, base, observed(0));
    assert_eq!(
        no_fallback.get_stale("seed", base),
        None,
        "ceiling 0 must degrade immediately"
    );
    assert_eq!(
        no_fallback.get_fresh("seed", base),
        Some(TemplateVisibility::Generic),
        "a disabled fallback must not disable the fresh window"
    );

    let off = cache(0, 0);
    off.store("seed", TemplateVisibility::Generic, base, observed(0));
    assert_eq!(off.get_fresh("seed", base), None);
    assert_eq!(off.get_stale("seed", base), None);
}

/// Invariant 2's mechanism. The caller invalidates when it *read* the row and
/// the answer was unusable (no row, an unparseable value, or a durable failure);
/// the cache must then answer in neither window, not merely treat the value as
/// stale.
///
/// This asserts only through the getters, which an `invalidate` rewritten to
/// *age* the entry would also satisfy. The removal itself is established by
/// `invalidate_removes_the_key_entirely`, which reads the map — and the
/// distinction matters, because a retained-but-aged entry changes which `store`
/// arm a later straggler hits.
#[test]
fn invalidate_removes_the_entry_from_both_windows() {
    let base = Instant::now();
    let cache = cache(30, 600);
    cache.store("seed", TemplateVisibility::Generic, base, observed(0));

    cache.invalidate("seed");

    assert_eq!(cache.get_fresh("seed", base), None);
    assert_eq!(cache.get_stale("seed", base), None);
}

/// A later read replaces the earlier one *and* restarts both windows — that is
/// what makes a recovered database heal the cache rather than leave a value
/// aging out under its original timestamp.
#[test]
fn a_later_read_replaces_the_value_and_restarts_the_windows() {
    let base = Instant::now();
    let cache = cache(30, 600);
    cache.store("seed", TemplateVisibility::Generic, base, observed(0));

    cache.store(
        "seed",
        TemplateVisibility::OrgSpecific,
        at(base, 100),
        observed(100),
    );

    assert_eq!(
        cache.get_fresh("seed", at(base, 110)),
        Some(TemplateVisibility::OrgSpecific),
        "stale by the first read's clock, fresh by the second's"
    );
}

/// Serving a value must not extend the window bounding how stale it may get: a
/// hit that bumped `read_at` would let a hot template's entry live forever on
/// reads alone.
///
/// This is the *freshness* half of "hits don't touch the timestamps", and it does
/// not exercise eviction — `max_entries` is the default here, so the map never
/// reaches capacity and `evict_oldest` cannot fire; mutating the eviction key
/// leaves this green. The eviction half is
/// `eviction_keys_on_the_server_observation_not_the_local_ask`. Both follow from
/// the same decision, and each needs its own test, so neither doc should claim
/// the other's mechanism.
#[test]
fn serving_a_value_does_not_extend_its_life() {
    let base = Instant::now();
    let cache = cache(30, 600);
    cache.store("seed", TemplateVisibility::Generic, base, observed(0));

    for offset in [1, 10, 20, 29] {
        assert_eq!(
            cache.get_fresh("seed", at(base, offset)),
            Some(TemplateVisibility::Generic)
        );
    }

    assert_eq!(
        cache.get_fresh("seed", at(base, 31)),
        None,
        "four hits must not have pushed the expiry out"
    );
}

/// The cap evicts rather than growing past `max_entries`.
///
/// Both clocks advance in lockstep here, so this deliberately does **not** pin
/// *which* timestamp eviction keys on — `eviction_keys_on_the_server_observation_not_the_local_ask`
/// does that by skewing them. Named "refreshed" because it is about the cap, not
/// about the clock choice.
#[test]
fn the_entry_cap_drops_the_least_recently_refreshed() {
    let base = Instant::now();
    let cache = TemplateVisibilityCache::new(CacheConfig {
        ttl: Duration::from_secs(30),
        stale_ceiling: Duration::from_secs(600),
        max_entries: NonZeroUsize::new(2).unwrap(),
    });

    cache.store("oldest", TemplateVisibility::Generic, base, observed(0));
    cache.store(
        "middle",
        TemplateVisibility::Generic,
        at(base, 1),
        observed(1),
    );
    cache.store(
        "newest",
        TemplateVisibility::Generic,
        at(base, 2),
        observed(2),
    );

    assert_eq!(
        cache.get_stale("oldest", at(base, 3)),
        None,
        "the least-recently-refreshed entry must be the one dropped"
    );
    assert_eq!(
        cache.get_stale("middle", at(base, 3)),
        Some(TemplateVisibility::Generic)
    );
    assert_eq!(
        cache.get_stale("newest", at(base, 3)),
        Some(TemplateVisibility::Generic)
    );
}

/// Refreshing a template already in the map must not evict anything: the cap
/// binds on *new* keys only. Without that branch split, a full cache serving a
/// steady set of templates would evict a live entry on every refresh.
#[test]
fn refreshing_an_existing_entry_at_capacity_evicts_nothing() {
    let base = Instant::now();
    let cache = TemplateVisibilityCache::new(CacheConfig {
        ttl: Duration::from_secs(30),
        stale_ceiling: Duration::from_secs(600),
        max_entries: NonZeroUsize::new(2).unwrap(),
    });
    cache.store("a", TemplateVisibility::Generic, base, observed(0));
    cache.store("b", TemplateVisibility::Generic, at(base, 1), observed(1));

    cache.store(
        "b",
        TemplateVisibility::OrgSpecific,
        at(base, 2),
        observed(2),
    );

    assert_eq!(
        cache.get_stale("a", at(base, 3)),
        Some(TemplateVisibility::Generic),
        "refreshing b must not have evicted a"
    );
    assert_eq!(
        cache.get_stale("b", at(base, 3)),
        Some(TemplateVisibility::OrgSpecific)
    );
}

/// Defensive, not a live path: `Instant` is monotonic and the only production
/// caller passes `started` to `get_fresh` and a strictly later `Instant::now()`
/// to `get_stale`, so `now < asked_at` is unreachable from `template_visibility`
/// as written. It is pinned anyway because a second call site could thread the
/// instants differently, and saturating to a zero age — reading as "just
/// refreshed" — is the answer that keeps a sub-tick inversion from widening a
/// window rather than narrowing it.
///
/// Not a panic test: `Instant`'s `Sub` already saturates on this toolchain, so
/// `-` and `saturating_duration_since` behave identically here. Verified rather
/// than assumed — an earlier version of this comment claimed the subtraction
/// panics, and it does not.
#[test]
fn a_clock_inversion_reads_as_just_refreshed() {
    let base = Instant::now();
    let cache = cache(30, 600);
    cache.store(
        "seed",
        TemplateVisibility::Generic,
        at(base, 10),
        observed(10),
    );

    assert_eq!(
        cache.get_fresh("seed", base),
        Some(TemplateVisibility::Generic)
    );
}

/// Clones share one map. `ClusterApiService` derives `Clone` and tonic clones
/// it per connection, so a per-clone map would mean a cache that never hits.
#[test]
fn clones_share_one_map() {
    let base = Instant::now();
    let cache = cache(30, 600);
    let clone = cache.clone();

    clone.store("seed", TemplateVisibility::Generic, base, observed(0));

    assert_eq!(
        cache.get_fresh("seed", at(base, 1)),
        Some(TemplateVisibility::Generic),
        "a read through one handle must be visible through the other"
    );
}

#[test]
fn config_defaults_are_the_documented_values() {
    let config = CacheConfig::default();
    assert_eq!(
        config.ttl,
        Duration::ZERO,
        "unset must mean off — the charts are not the only startup path"
    );
    assert_eq!(config.stale_ceiling, Duration::ZERO);
    assert_eq!(config.max_entries, NonZeroUsize::new(1024).unwrap());
}

#[test]
fn duration_parsing_falls_back_on_anything_but_a_plain_integer() {
    assert_eq!(
        parse_duration_secs("VAR", Some("45")),
        Some(Duration::from_secs(45))
    );
    assert_eq!(
        parse_duration_secs("VAR", Some("  45\n")),
        Some(Duration::from_secs(45)),
        "surrounding whitespace is trimmed, as elsewhere in this crate"
    );
    assert_eq!(
        parse_duration_secs("VAR", Some("0")),
        Some(Duration::ZERO),
        "zero is a real setting — the kill switch — not a parse failure"
    );

    for raw in ["", "  ", "30s", "-1", "1.5", "thirty"] {
        assert_eq!(parse_duration_secs("VAR", Some(raw)), None, "raw: {raw:?}");
    }
    assert_eq!(parse_duration_secs("VAR", None), None);
}

/// The converse, so the guard cannot be satisfied by simply never storing: a
/// genuinely newer read does replace the older value.
#[test]
fn a_newer_read_replaces_an_older_one() {
    let base = Instant::now();
    let cache = cache(30, 600);

    cache.store("seed", TemplateVisibility::Generic, base, observed(0));
    cache.store(
        "seed",
        TemplateVisibility::OrgSpecific,
        at(base, 5),
        observed(5),
    );

    assert_eq!(
        cache.get_fresh("seed", at(base, 6)),
        Some(TemplateVisibility::OrgSpecific)
    );
}

/// Two reads carrying the same `clock_timestamp()` cannot be ordered by it, so
/// the later writer wins.
///
/// Not "the same snapshot": timestamps have finite resolution while snapshots are
/// fixed independently at statement start, so equal stamps can still straddle a
/// flip and the winner may be the pre-flip value. That residual is real and
/// bounded by the window that served it — the same bound as the rest of the
/// ordering residual. Pinned because the guard is a
/// strict `>`, and flipping it to `>=` would make a same-instant refresh a
/// silent no-op.
#[test]
fn an_equally_recent_read_may_overwrite() {
    let base = Instant::now();
    let cache = cache(30, 600);

    cache.store("seed", TemplateVisibility::Generic, base, observed(0));
    cache.store("seed", TemplateVisibility::OrgSpecific, base, observed(0));

    assert_eq!(
        cache.get_fresh("seed", at(base, 1)),
        Some(TemplateVisibility::OrgSpecific)
    );
}

/// The regression test for the ordering fix, and the only one that can catch it.
///
/// The two clocks must be allowed to disagree. A caller captures its local
/// instant *before* acquiring a connection, so on a 3-connection pool under
/// contention a caller that started earlier can snapshot the row later than one
/// that started after it. Ordering on the local instant therefore rejects the
/// fresher observation — which is the exact lost update the guard exists to
/// prevent, arrived at from the other side.
///
/// Here B starts later locally (`at(base, 5)`) but observed the row earlier
/// (`observed(1)`, pre-flip `generic`); A starts earlier (`base`) but observed it
/// later (`observed(9)`, post-flip `org_specific`). A must win. Ordering on the
/// local instant would give the opposite answer, so this fails if the guard ever
/// goes back to comparing request starts.
#[test]
fn ordering_follows_the_server_observation_not_the_local_request_start() {
    let base = Instant::now();
    let cache = cache(30, 600);

    cache.store(
        "seed",
        TemplateVisibility::Generic,
        at(base, 5),
        observed(1),
    );
    cache.store("seed", TemplateVisibility::OrgSpecific, base, observed(9));

    assert_eq!(
        cache.get_fresh("seed", at(base, 6)),
        Some(TemplateVisibility::OrgSpecific),
        "the later server observation must win despite carrying the older local instant"
    );
}

/// With both windows disabled the cache must not write either — the kill switch
/// is documented as reproducing the pre-cache behavior exactly, which includes
/// not taking a lock on a path that had none before.
///
/// Asserted on the map's length, not on the getters. Both getters short-circuit
/// on a zero window before touching the map, so a getter-based assertion holds
/// identically whether the store landed or not: it could not fail, which makes
/// the name a claim its body cannot falsify.
///
/// The length is checked **immediately after the store**. An earlier version
/// invalidated first and then asserted, which re-introduced the same vacuity by
/// another route: a regressed `store` would insert, `invalidate` would remove,
/// and `len() == 0` would still hold. Mutation-checked — deleting `store`'s
/// early return fails this test.
///
/// Scoped to `store` deliberately. `invalidate` has the same short-circuit, for
/// the same documented lock-free reason, and it is **not** covered here or
/// anywhere: with both windows disabled nothing can enter the map, so there is
/// never an entry for `invalidate` to remove and `len()` cannot distinguish the
/// short-circuit from a no-op removal. Observing it would take a lock-acquisition
/// counter, which is more test machinery than the property earns. Verified by
/// reading, not by assertion — said out loud so the gap is not mistaken for
/// coverage, which is what a name like "neither stores nor invalidates" would
/// have done.
#[test]
fn a_fully_disabled_cache_stores_nothing() {
    let base = Instant::now();
    let off = cache(0, 0);

    off.store("seed", TemplateVisibility::Generic, base, observed(0));

    assert_eq!(off.len(), 0, "a disabled cache must not store");
    assert_eq!(off.get_fresh("seed", base), None);
    assert_eq!(off.get_stale("seed", base), None);
}

/// The ceiling has to be at least the TTL or it is not a ceiling: both windows
/// funnel through one comparison, so an independently-raised TTL would extend
/// the real bound to `max(ttl, ceiling)` — silently, on a healthy database.
#[test]
fn the_staleness_ceiling_is_clamped_up_to_the_ttl() {
    let raised = CacheConfig {
        ttl: Duration::from_secs(3600),
        stale_ceiling: Duration::from_secs(600),
        max_entries: NonZeroUsize::new(1024).unwrap(),
    }
    .with_ceiling_at_least_ttl();
    assert_eq!(raised.stale_ceiling, Duration::from_secs(3600));
    assert_eq!(raised.ttl, Duration::from_secs(3600), "ttl is not lowered");

    let untouched = CacheConfig::default().with_ceiling_at_least_ttl();
    assert_eq!(
        untouched,
        CacheConfig::default(),
        "the default needs no clamp"
    );

    let off = CacheConfig {
        ttl: Duration::ZERO,
        stale_ceiling: Duration::ZERO,
        max_entries: NonZeroUsize::new(8).unwrap(),
    };
    assert_eq!(
        off.with_ceiling_at_least_ttl(),
        off,
        "the kill switch must not be clamped into life"
    );
}

/// M2's regression test: a zero ceiling means *disabled* and the clamp must not
/// resurrect it.
///
/// The clamp exists so an independently-raised TTL cannot extend the real bound
/// past the ceiling. But zero is a different kind of value from a duration — it
/// is the documented per-window kill switch — so clamping it would silently give
/// an operator who set the ceiling to `0` a 30-second fallback instead of none.
#[test]
fn a_zero_ceiling_survives_the_clamp() {
    let disabled_fallback = CacheConfig {
        ttl: Duration::from_secs(30),
        stale_ceiling: Duration::ZERO,
        max_entries: NonZeroUsize::new(1024).unwrap(),
    };
    assert_eq!(
        disabled_fallback.with_ceiling_at_least_ttl(),
        disabled_fallback,
        "a deliberately disabled fallback must not be clamped into life"
    );

    // ... while a non-zero ceiling below the TTL still gets raised.
    let raised = CacheConfig {
        ttl: Duration::from_secs(30),
        stale_ceiling: Duration::from_secs(5),
        max_entries: NonZeroUsize::new(1024).unwrap(),
    }
    .with_ceiling_at_least_ttl();
    assert_eq!(raised.stale_ceiling, Duration::from_secs(30));
}

/// M3's regression test: an implausible window is capped, so a typo in the TTL
/// cannot ride the clamp onto the ceiling.
///
/// `600000` is the shape that matters — a fat-fingered `600`, or seconds
/// mistaken for milliseconds. Uncapped it is 6.9 days, and the clamp would carry
/// it onto the ceiling, serving a pre-flip `generic` for a week per replica.
#[test]
fn an_implausible_window_is_capped() {
    // Asserted against the literal, not against `MAX_WINDOW`. `Some(MAX_WINDOW)`
    // is self-referential: raising the constant to 600000 would make the cap a
    // no-op and this assertion would still hold, shipping exactly the week-long
    // staleness the cap exists to prevent.
    assert_eq!(
        parse_duration_secs("VAR", Some("600000")),
        Some(Duration::from_secs(3600)),
        "a typo-shaped value is capped at one hour, not honored"
    );
    assert_eq!(
        parse_duration_secs("VAR", Some("3600")),
        Some(Duration::from_secs(3600)),
        "the ceiling itself is still accepted"
    );
    assert_eq!(
        parse_duration_secs("VAR", Some("30")),
        Some(Duration::from_secs(30)),
        "ordinary values are untouched"
    );
    assert_eq!(
        parse_duration_secs("VAR", Some("0")),
        Some(Duration::ZERO),
        "the kill switch is not affected by the cap"
    );
}

/// Invalidation removes rather than retains, which is the deliberate trade
/// documented on `invalidate`. Asserted on the map so "removed" is observable;
/// a getter assertion cannot tell removal from a retained-but-silent slot.
#[test]
fn invalidate_removes_the_key_entirely() {
    let base = Instant::now();
    let cache = cache(30, 600);
    cache.store("seed", TemplateVisibility::Generic, base, observed(0));
    assert_eq!(cache.len(), 1);

    cache.invalidate("seed");

    assert_eq!(cache.len(), 0, "the key is gone, not retained as a marker");
}

/// Eviction keys on the *server* observation, not the local ask instant.
///
/// The earlier version of this test advanced both clocks in lockstep, so it
/// could not tell which one eviction used — and `evict_oldest`'s doc makes a
/// specific claim about it. Here the two disagree: `stale-observation` was asked
/// about most recently but observed first, so it is the one that must go.
#[test]
fn eviction_keys_on_the_server_observation_not_the_local_ask() {
    let base = Instant::now();
    let cache = TemplateVisibilityCache::new(CacheConfig {
        ttl: Duration::from_secs(30),
        stale_ceiling: Duration::from_secs(600),
        max_entries: NonZeroUsize::new(2).unwrap(),
    });

    // Asked last, observed first — the oldest observation in the map.
    cache.store(
        "stale-observation",
        TemplateVisibility::Generic,
        at(base, 9),
        observed(1),
    );
    // Asked first, observed later.
    cache.store(
        "fresh-observation",
        TemplateVisibility::Generic,
        base,
        observed(5),
    );

    cache.store("newcomer", TemplateVisibility::Generic, base, observed(6));

    assert_eq!(
        cache.get_stale("stale-observation", at(base, 10)),
        None,
        "the oldest observation must be the evicted one"
    );
    assert_eq!(
        cache.get_stale("fresh-observation", at(base, 10)),
        Some(TemplateVisibility::Generic)
    );
    // The incoming key has to be read back, or an `evict_oldest` that dropped the
    // *newcomer* instead of the resident oldest would satisfy both assertions
    // above — `stale-observation` would read `None` for the wrong reason.
    assert_eq!(
        cache.get_stale("newcomer", at(base, 10)),
        Some(TemplateVisibility::Generic),
        "eviction must make room for the incoming key, not discard it"
    );
    assert_eq!(cache.len(), 2, "one evicted, two resident");
}

/// A name too long to key is simply not cached, which is what makes
/// `max_entries` a memory bound rather than a count. It must still be safe to
/// call — the lookup itself still works, it just re-queries every time.
#[test]
fn an_overlong_template_name_is_not_cached() {
    let base = Instant::now();
    let cache = cache(30, 600);
    let long = "x".repeat(MAX_KEY_LEN + 1);
    let at_limit = "y".repeat(MAX_KEY_LEN);

    cache.store(&long, TemplateVisibility::Generic, base, observed(0));
    cache.store(&at_limit, TemplateVisibility::Generic, base, observed(0));

    assert_eq!(
        cache.get_fresh(&long, base),
        None,
        "over the cap: not cached"
    );
    assert_eq!(
        cache.get_fresh(&at_limit, base),
        Some(TemplateVisibility::Generic),
        "exactly at the cap: cached, so the boundary is inclusive"
    );
    assert_eq!(cache.len(), 1);
}

/// An older observation does not overwrite a newer one — and does not delete it
/// either.
///
/// The "does not delete" half is the part worth pinning. An earlier revision
/// removed the entry when a rejected observation *disagreed*, on the theory that
/// two differing reads cannot be ordered by a clock we may not trust. But the
/// predicate fires on the ordinary forward-clock straggler race this ordering
/// exists to handle, where the resident value is the correct one: a pre-flip
/// reader landing after a post-flip reader deleted the post-flip value, and a
/// third older reader then found no entry and re-inserted the pre-flip one as
/// fresh. Disagreement is exactly when the resident value must be kept.
#[test]
fn an_older_read_neither_overwrites_nor_deletes_a_newer_one() {
    let base = Instant::now();
    let cache = cache(30, 600);
    cache.store(
        "seed",
        TemplateVisibility::OrgSpecific,
        at(base, 5),
        observed(5),
    );

    cache.store("seed", TemplateVisibility::Generic, base, observed(0));

    assert_eq!(
        cache.get_fresh("seed", at(base, 6)),
        Some(TemplateVisibility::OrgSpecific),
        "the newer value must survive a disagreeing straggler"
    );
    assert_eq!(cache.len(), 1, "and must not be deleted by it");
}

/// The key cap must stay above the template-name validator's own limit.
///
/// Asserted against the literal `255`, not against the validator's constant and
/// not against `MAX_KEY_LEN` itself. The two tests either side of this one say
/// the same thing about their constants for the same reason: a test that derives
/// its expectation from the value it is pinning passes for every value, so it
/// pins nothing. `MAX_KEY_LEN`'s doc calls this coupling load-bearing, and the
/// regression it names is silent — every template with a longer name stops being
/// cached, `store` returns early on every request, and the fleet re-queries on
/// every release resolution while `outcome:refreshed` sits at 100%, which is the
/// shape a healthy cache has.
#[test]
fn the_key_cap_clears_the_template_name_validator() {
    // A `const` block, so shrinking the cap fails to compile (E0080) rather than
    // failing at run time — clippy's `assertions_on_constants` asks for this
    // shape and it is the stronger one. Verified, including the limit: it fires
    // when the *test target is built* (`cargo test`/`cargo nextest`), not under
    // `cargo check`, which does not const-evaluate an inline const block. The
    // repository validation runs both, so the guard is reached either way.
    const {
        assert!(
            MAX_KEY_LEN > 255,
            "MAX_KEY_LEN must exceed the 255-byte template-name limit enforced at \
             common/core/src/types/template/identity.rs, or valid names silently \
             stop being cached"
        );
    }
}
