use super::*;

fn dispatched_set(pairs: &[(&str, &str)]) -> HashSet<(String, String)> {
    pairs
        .iter()
        .map(|(n, h)| ((*n).to_string(), (*h).to_string()))
        .collect()
}

/// Headline dedup invariant: when the latest-N pass resolves the
/// same `(template, content_hash)` that the tag pass already
/// scheduled, `classify_release_dispatch` returns
/// `CoalescedWithTag` — i.e. only one pull is scheduled even
/// though two passes both wanted the digest.
///
/// `locally_cached: false` keeps the AlreadyCached branch out of
/// the picture so this asserts the dedup ordering specifically:
/// "already-dispatched takes precedence over the local-cache
/// check".
#[test]
fn release_resolved_by_tag_and_latest_n_dispatches_once() {
    let already = dispatched_set(&[("seed", "sha256:aa")]);
    let decision = classify_release_dispatch(&already, "seed", "sha256:aa", false);
    assert_eq!(decision, ReleaseDispatchDecision::CoalescedWithTag);
}

/// Negative case for the dedup: the `HashSet` key is the full
/// `(template, content_hash)` tuple, so two different templates
/// holding the same digest are still independent dispatches.
/// Without this, a tag pass on `seed` could incorrectly suppress
/// a release-pass dispatch on `code-runtime` whenever the two
/// templates happened to point at the same digest.
#[test]
fn release_dispatches_when_tag_pass_held_different_template() {
    let already = dispatched_set(&[("seed", "sha256:aa")]);
    let decision = classify_release_dispatch(&already, "code-runtime", "sha256:aa", false);
    assert_eq!(decision, ReleaseDispatchDecision::Dispatch);
}

/// The dedup check fires before the local-cache check. A digest
/// already dispatched by the tag pass MUST be reported as
/// `CoalescedWithTag` even if `local_build_exists` would also
/// say "you don't need to pull" — because metric attribution
/// should reflect *why* the pull was skipped, and the dedup
/// reason is the more specific one. Reversing the order would
/// silently mis-attribute the outcome.
#[test]
fn coalesced_outcome_takes_precedence_over_already_cached() {
    let already = dispatched_set(&[("seed", "sha256:aa")]);
    let decision = classify_release_dispatch(&already, "seed", "sha256:aa", true);
    assert_eq!(decision, ReleaseDispatchDecision::CoalescedWithTag);
}

/// Fresh digest, nothing on disk → schedule a pull.
#[test]
fn fresh_release_dispatches() {
    let already = dispatched_set(&[]);
    let decision = classify_release_dispatch(&already, "seed", "sha256:aa", false);
    assert_eq!(decision, ReleaseDispatchDecision::Dispatch);
}

/// Fresh digest, already on disk → record the pin but don't
/// pull. `tick` still upserts the release pin separately; this
/// classifier only decides whether a pull is needed.
#[test]
fn locally_cached_release_short_circuits_without_dispatch() {
    let already = dispatched_set(&[]);
    let decision = classify_release_dispatch(&already, "seed", "sha256:aa", true);
    assert_eq!(decision, ReleaseDispatchDecision::AlreadyCached);
}

#[test]
fn configured_prefetch_tags_drive_unique_templates() {
    let tags = parse_prefetch_tags(
        r#"["custom-template:latest","custom-template:stable","other-template"]"#,
    )
    .unwrap();

    let names = unique_prefetch_templates(&tags);

    assert_eq!(names, ["custom-template", "other-template"]);
    assert_eq!(tags[1].1.as_str(), "stable");
    assert_eq!(tags[2].1.as_str(), "latest");
    assert!(parse_prefetch_tags("[]").unwrap().is_empty());
    for invalid in [
        r#"["../invalid:latest"]"#,
        r#"["custom-template:bad tag"]"#,
        r#"["seed@latest"]"#,
        r#"[["seed","latest"]]"#,
        r#"["seed@sha256:111111111111"]"#,
        r#"["seed@sha256:1111111111111111111111111111111111111111111111111111111111111111"]"#,
    ] {
        assert!(parse_prefetch_tags(invalid).is_err(), "accepted {invalid}");
    }
}

fn test_tag_tuples() -> Vec<(TemplateName, TemplateTag)> {
    parse_prefetch_tags(r#"["seed:latest","seed:stable","code-runtime:latest"]"#).unwrap()
}

/// A tuple whose template resolved as org-specific leaves the tag
/// prune keep-set — its pin must be actively dropped so GC can
/// evict the rootfs — while every other configured tuple stays kept.
/// This is the data-isolation half of the visibility gate.
#[test]
fn org_specific_tuple_leaves_tag_prune_keep_set() {
    let excluded: HashSet<(String, String)> =
        std::iter::once(("seed".to_string(), "latest".to_string())).collect();
    let keep = tag_prune_keep_set(&test_tag_tuples(), &excluded);
    assert!(!keep.contains(&("seed".to_string(), "latest".to_string())));
    assert_eq!(keep.len(), test_tag_tuples().len() - 1);
}

/// An org-specific verdict from the latest-N pass drops ALL of the
/// template's tag tuples from the keep-set, not just a tuple that
/// resolved. The tag pass may have seen no visibility for them at
/// all — a deleted tag resolves `Unresolved` there — so without
/// this a stale tag pin from the template's generic days would keep
/// its rootfs GC-protected on shared nodes.
#[test]
fn latest_pass_org_specific_drops_template_tag_tuples() {
    let mut tuples: HashSet<(String, String)> = HashSet::new();
    let mut templates: BTreeSet<String> = BTreeSet::new();
    mark_template_org_specific(&test_tag_tuples(), &mut tuples, &mut templates, "seed");
    let keep = tag_prune_keep_set(&test_tag_tuples(), &tuples);
    assert!(!keep.contains(&("seed".to_string(), "latest".to_string())));
    // Other templates' tuples are untouched.
    assert!(keep.contains(&("code-runtime".to_string(), "latest".to_string())));
}

/// The marker still queues the template for the release-pin flush
/// even when it has no configured tag tuples at all.
#[test]
fn mark_template_without_tag_tuples_still_queues_release_flush() {
    let mut tuples: HashSet<(String, String)> = HashSet::new();
    let mut templates: BTreeSet<String> = BTreeSet::new();
    mark_template_org_specific(
        &test_tag_tuples(),
        &mut tuples,
        &mut templates,
        "acme-private",
    );
    assert!(tuples.is_empty());
    assert!(templates.contains("acme-private"));
}

/// A tag-pass org-specific verdict flushes the template's release
/// pins even when the latest-N pass produced no verdict for it
/// (`latest_n = 0`, or an RPC failure). Data isolation can't depend
/// on which RPC happened to succeed: without this, existing
/// `template_pinned_releases` rows would keep the org-specific
/// rootfs GC-protected.
#[test]
fn tag_only_org_specific_verdict_flushes_release_pins() {
    let mut tuples: HashSet<(String, String)> = HashSet::new();
    let mut templates: BTreeSet<String> = BTreeSet::new();
    mark_template_org_specific(&test_tag_tuples(), &mut tuples, &mut templates, "seed");

    // Latest pass never ran for "seed": it isn't prune-eligible and
    // a stale keep entry exists from a prior generic tick.
    let mut prunable = vec!["code-runtime".to_string()];
    let mut keep = vec![
        ("seed".to_string(), "sha256:aa".to_string()),
        ("code-runtime".to_string(), "sha256:bb".to_string()),
    ];
    flush_org_specific_release_pins(&templates, &mut prunable, &mut keep);

    // "seed" becomes prune-eligible with zero keep entries → its
    // release pins flush; "code-runtime" is untouched.
    assert!(prunable.contains(&"seed".to_string()));
    assert!(!keep.iter().any(|(name, _)| name == "seed"));
    assert_eq!(
        keep,
        vec![("code-runtime".to_string(), "sha256:bb".to_string())]
    );
}

/// A template already present in `prunable` is not duplicated by the
/// flush. `tick` cannot currently produce that input — the two
/// collections are disjoint by construction, see
/// `flush_org_specific_release_pins` — so this pins the helper's
/// contract for a future third writer of `prunable_templates` rather
/// than a reachable tick state.
#[test]
fn flush_does_not_duplicate_prunable_templates() {
    let mut templates: BTreeSet<String> = BTreeSet::new();
    templates.insert("seed".to_string());
    let mut prunable = vec!["seed".to_string()];
    let mut keep = Vec::new();
    flush_org_specific_release_pins(&templates, &mut prunable, &mut keep);
    assert_eq!(prunable, vec!["seed".to_string()]);
}

/// A verdict already accumulated this tick overrides a skewed or
/// stale `Generic` read: verdicts only harden, never soften. Without
/// it, a pass could re-pin and dispatch pulls for a template the
/// other pass had already confirmed private. Both passes route their
/// reported visibility through this.
#[test]
fn accumulated_verdict_overrides_skewed_generic_read() {
    use TemplateVisibility::{Generic, OrgSpecific, Unspecified};
    assert_eq!(hardened_visibility(true, Generic), OrgSpecific);
    assert_eq!(hardened_visibility(true, Unspecified), OrgSpecific);
    assert_eq!(hardened_visibility(true, OrgSpecific), OrgSpecific);
    // Without an accumulated verdict, the reported visibility stands.
    assert_eq!(hardened_visibility(false, Generic), Generic);
    assert_eq!(hardened_visibility(false, Unspecified), Unspecified);
    assert_eq!(hardened_visibility(false, OrgSpecific), OrgSpecific);
}

/// The template-level verdict survives an empty latest-releases
/// list: the response-level visibility field stands alone when there
/// are no entries to fold. An org-specific template whose releases
/// are all gone (tag pass: `Unresolved`; latest pass: zero entries)
/// still reports `OrgSpecific`, so its stale tag pins drop. With
/// entries, response and entry folds combine restrictively.
#[test]
fn response_level_visibility_survives_empty_entry_list() {
    use crate::volume::combine_response_visibility;
    use TemplateVisibility::{Generic, OrgSpecific, Unspecified};
    assert_eq!(combine_response_visibility(OrgSpecific, None), OrgSpecific);
    assert_eq!(combine_response_visibility(Generic, None), Generic);
    // Old server / old schema: nothing set anywhere.
    assert_eq!(combine_response_visibility(Unspecified, None), Unspecified);
    // Disagreement between response level and entries resolves to
    // the most restrictive answer.
    assert_eq!(
        combine_response_visibility(Generic, Some(OrgSpecific)),
        OrgSpecific
    );
    assert_eq!(
        combine_response_visibility(OrgSpecific, Some(Generic)),
        OrgSpecific
    );
    assert_eq!(combine_response_visibility(Generic, Some(Generic)), Generic);
}

/// With nothing excluded (the steady state: every prefetch
/// template is generic), the keep-set is exactly the const list —
/// i.e. the visibility gate changes nothing for generic templates.
#[test]
fn empty_exclusions_keep_full_const_list() {
    let keep = tag_prune_keep_set(&test_tag_tuples(), &HashSet::new());
    assert_eq!(keep.len(), test_tag_tuples().len());
    for (name, tag) in test_tag_tuples() {
        assert!(keep.contains(&(name.to_string(), tag.to_string())));
    }
}

/// The fold across a response's entries keeps the most restrictive
/// visibility: a confirmed org-specific dominates (pins must drop),
/// unknown beats generic (not known to be generic → don't warm),
/// and only all-generic stays generic.
#[test]
fn visibility_fold_keeps_most_restrictive() {
    use crate::volume::fold_visibility;
    use TemplateVisibility::{Generic, OrgSpecific, Unspecified};
    assert_eq!(fold_visibility(Generic, Generic), Generic);
    assert_eq!(fold_visibility(Generic, Unspecified), Unspecified);
    assert_eq!(fold_visibility(Unspecified, Generic), Unspecified);
    assert_eq!(fold_visibility(Generic, OrgSpecific), OrgSpecific);
    assert_eq!(fold_visibility(OrgSpecific, Generic), OrgSpecific);
    assert_eq!(fold_visibility(Unspecified, OrgSpecific), OrgSpecific);
    assert_eq!(fold_visibility(OrgSpecific, Unspecified), OrgSpecific);
}

/// Every combination of the two prune outcomes maps to the right
/// result, and a double failure reports both messages rather than
/// hiding one. The prunes clean different tables and eviction reads
/// their union, so neither a lost message nor a skipped prune is
/// tolerable.
#[test]
fn combine_prune_errors_reports_every_outcome() {
    assert_eq!(combine_prune_errors(Ok(()), Ok(())), Ok(()));
    assert_eq!(
        combine_prune_errors(Err("tags".to_string()), Ok(())),
        Err("tags".to_string())
    );
    assert_eq!(
        combine_prune_errors(Ok(()), Err("releases".to_string())),
        Err("releases".to_string())
    );
    assert_eq!(
        combine_prune_errors(Err("tags".to_string()), Err("releases".to_string())),
        Err("tags; releases".to_string())
    );
}

/// A tag-pass pull queued on a `Generic` read is suppressed when a
/// later verdict confirms the same template org-specific, and other
/// templates' queued pulls are untouched. This is what keeps a private
/// rootfs off a shared node during a flip: once the pull is spawned
/// nothing recalls it, so the verdict has to apply while it is still a
/// queued decision. Distinct tags and hashes per fixture so a swapped
/// field fails rather than passing on a shared value.
#[test]
fn org_specific_verdict_suppresses_a_queued_tag_pull() {
    let pending = vec![
        PendingTagPull {
            name: "seed",
            tag: "latest",
            hash: "sha256:aa".to_string(),
        },
        PendingTagPull {
            name: "code-runtime",
            tag: "stable",
            hash: "sha256:bb".to_string(),
        },
    ];
    let mut org_specific: BTreeSet<String> = BTreeSet::new();
    org_specific.insert("seed".to_string());

    let (dispatchable, suppressed) = partition_pending_tag_pulls(pending, &org_specific);

    assert_eq!(dispatchable.len(), 1);
    assert_eq!(dispatchable[0].name, "code-runtime");
    assert_eq!(dispatchable[0].tag, "stable");
    assert_eq!(dispatchable[0].hash, "sha256:bb");

    // The suppressed entry keeps tag and hash so the drain can record
    // `skipped_org_specific` against the key pass 1 would have used.
    assert_eq!(suppressed.len(), 1);
    assert_eq!(suppressed[0].name, "seed");
    assert_eq!(suppressed[0].tag, "latest");
    assert_eq!(suppressed[0].hash, "sha256:aa");

    // The steady state: no verdict accumulated, so nothing is held back
    // and deferring dispatch costs nothing.
    let pending = vec![PendingTagPull {
        name: "seed",
        tag: "latest",
        hash: "sha256:aa".to_string(),
    }];
    let (dispatchable, suppressed) = partition_pending_tag_pulls(pending, &BTreeSet::new());
    assert_eq!(dispatchable.len(), 1);
    assert!(suppressed.is_empty());
}

/// Every tuple of a confirmed template is suppressed, not just one.
/// The reachable shape this models: both tuples resolved `Generic` in
/// the tag pass and queued, then the latest-N pass reported
/// `OrgSpecific` for the template. (The other multi-tuple sequence — a
/// second tuple reading `OrgSpecific` directly — never queues that
/// tuple, since the tag pass gates before queueing, so it leaves one
/// entry and is covered by the suppressed side of the test above.)
#[test]
fn every_tuple_of_a_confirmed_template_is_suppressed() {
    let pending = vec![
        PendingTagPull {
            name: "seed",
            tag: "latest",
            hash: "sha256:aa".to_string(),
        },
        PendingTagPull {
            name: "seed",
            tag: "stable",
            hash: "sha256:bb".to_string(),
        },
    ];
    let mut org_specific: BTreeSet<String> = BTreeSet::new();
    org_specific.insert("seed".to_string());

    let (dispatchable, suppressed) = partition_pending_tag_pulls(pending, &org_specific);

    assert!(dispatchable.is_empty());
    assert_eq!(
        suppressed.iter().map(|p| p.tag).collect::<Vec<_>>(),
        vec!["latest", "stable"]
    );
}

/// Only a tick that read the node's environments itself knows them: one using
/// the last answer doesn't, so the release-pin grace counts from the last
/// real read. An ignored selector ages its own lookup instead.
#[test]
fn only_a_fresh_read_knows_the_node_s_environments() {
    let read_at = std::time::Instant::now();
    let last = environments::LastEnvironments {
        environments: crate::node_environments::NodeEnvironments::default(),
        read_at,
    };
    let earlier = read_at.checked_sub(Duration::from_secs(60));
    assert!(knew_environments_this_tick(earlier, Some(&last)));
    assert!(knew_environments_this_tick(None, Some(&last)));
    assert!(
        !knew_environments_this_tick(Some(read_at), Some(&last)),
        "the last answer"
    );
    assert!(!knew_environments_this_tick(None, None), "never read");
}

/// A tick that read the node's environments dates the release-pin grace
/// from the read, not from when the tick recorded it after its pulls
/// drained; one using the last answer leaves the date alone.
#[test]
fn the_pin_grace_counts_from_the_environments_read() {
    let started_at = std::time::Instant::now();
    // A read a second before the tick records it, as a drain in between.
    let read_at = started_at.checked_sub(Duration::from_secs(1)).unwrap();
    let mut known = KnownEnvironments {
        last: Some(environments::LastEnvironments {
            environments: crate::node_environments::NodeEnvironments::default(),
            read_at,
        }),
        answered_at: started_at,
        started_at,
        tag_answered_at: HashMap::new(),
        release_answered_at: HashMap::new(),
    };

    assert!(record_environments_read(&mut known, None));
    assert_eq!(known.answered_at, read_at);

    known.answered_at = started_at;
    assert!(
        !record_environments_read(&mut known, Some(read_at)),
        "the last answer"
    );
    assert_eq!(known.answered_at, started_at, "left alone");
}

/// A failed read reuses the last answer for the tag-partition prune only
/// while it's within the pin grace at prune time, so a drain that crosses the
/// grace doesn't keep environment tag pins past it; a read the tick made
/// itself stands however long the drain takes.
#[test]
fn a_reused_environments_answer_stops_keeping_tag_pins_at_the_grace() {
    let node = crate::node_environments::NodeEnvironments::default();
    let read_at = std::time::Instant::now();
    let last = environments::LastEnvironments {
        environments: node.clone(),
        read_at,
    };
    // The read time didn't move: the tick reused the last answer.
    let reused = |since_read: Duration| {
        environments_for_partition_prune(
            Some(&node),
            Some(read_at),
            Some(&last),
            read_at + since_read,
        )
    };
    assert!(reused(ENVIRONMENT_PIN_GRACE - Duration::from_secs(10)).is_some());
    assert!(
        reused(ENVIRONMENT_PIN_GRACE + Duration::from_secs(50)).is_none(),
        "a drain that crossed the grace"
    );
    let earlier_read = read_at.checked_sub(Duration::from_secs(60)).unwrap();
    assert!(
        environments_for_partition_prune(
            Some(&node),
            Some(earlier_read),
            Some(&last),
            read_at + ENVIRONMENT_PIN_GRACE + Duration::from_secs(50),
        )
        .is_some(),
        "a read this tick made"
    );
    assert!(environments_for_partition_prune(None, None, None, read_at).is_none());
}

/// The environment pass's release keep entries stand only as long as the
/// environments it ran for: a reused answer that a drain carried past the
/// grace keeps none, and a read the tick made itself keeps them all.
#[test]
fn a_reused_environments_answer_stops_keeping_release_pins_at_the_grace() {
    let node = crate::node_environments::NodeEnvironments::default();
    let read_at = std::time::Instant::now();
    let last = environments::LastEnvironments {
        environments: node.clone(),
        read_at,
    };
    let keep = || vec![("base".to_string(), "sha256:twin".to_string())];
    let kept = |read_before: std::time::Instant, since_read: Duration| {
        environment_release_keep(
            environments_for_partition_prune(
                Some(&node),
                Some(read_before),
                Some(&last),
                read_at + since_read,
            ),
            keep(),
        )
    };
    assert_eq!(
        kept(read_at, ENVIRONMENT_PIN_GRACE - Duration::from_secs(10)),
        keep()
    );
    assert!(
        kept(read_at, ENVIRONMENT_PIN_GRACE + Duration::from_secs(50)).is_empty(),
        "a drain that crossed the grace"
    );
    let earlier_read = read_at.checked_sub(Duration::from_secs(60)).unwrap();
    assert_eq!(
        kept(
            earlier_read,
            ENVIRONMENT_PIN_GRACE + Duration::from_secs(50)
        ),
        keep(),
        "a read this tick made"
    );
}

/// Shortly after startup, a tick that can't read the node's environments
/// leaves release pins unpruned, since some may be only the environment
/// pass's; known environments, or a node-manager silent past the grace,
/// prune as usual.
#[test]
fn release_pins_hold_only_while_environments_are_unknown_after_startup() {
    assert!(holds_release_pins(false, Duration::ZERO));
    assert!(holds_release_pins(
        false,
        ENVIRONMENT_PIN_GRACE - Duration::from_secs(1)
    ));
    assert!(!holds_release_pins(false, ENVIRONMENT_PIN_GRACE));
    assert!(!holds_release_pins(true, Duration::ZERO));
}

/// A tag lookup whose environment selector went unapplied leaves only that
/// tag's binding unknown. Within its pin grace its pin stays; past it, only
/// its pin ages out. Another tag's pin, as one whose lookup failed for
/// another reason, stays while the node runs its environment, and the
/// legacy partition's pins are the environment-less pass's.
#[tokio::test]
async fn an_ignored_selector_expires_only_its_own_tag_s_pin() {
    use crate::db::TemplateCachePartition;
    let environment = pplx_space_execution_spec::EnvironmentReference::new(
        pplx_space_content_digest::Digest::sha256([1; 32]),
    );
    let partition =
        pplx_space_node_runtime_core::catalog::template_cache_partition(Some(environment));
    let node = crate::node_environments::NodeEnvironments {
        runnable: BTreeSet::from([environment]),
    };
    let left_after = async |expired: bool| {
        let pins =
            PinnedDigestsStore::new(crate::db::SharedConnection::new(crate::db::test_conn()));
        for tag in ["errored", "ignored"] {
            pins.upsert_tag("seed", tag, partition, "sha256:aa")
                .unwrap();
        }
        pins.upsert_tag(
            "seed",
            "ignored",
            TemplateCachePartition::Legacy,
            "sha256:aa",
        )
        .unwrap();
        prune_tag_partitions(
            &pins,
            crate::node_environments::EnvironmentAwareness::On,
            Some(&node),
            if expired {
                vec![("seed".to_string(), "ignored".to_string(), partition)]
            } else {
                Vec::new()
            },
            false,
        )
        .await
        .unwrap();
        let conn = pins.conn();
        let conn = conn.lock().unwrap();
        let mut stmt = conn
            .raw()
            .prepare(
                "SELECT tag, cache_partition FROM template_pinned_tags \
                 ORDER BY tag, cache_partition",
            )
            .unwrap();
        stmt.query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
            .unwrap()
            .collect::<rusqlite::Result<Vec<(String, String)>>>()
            .unwrap()
    };
    let pin = |tag: &str, partition: &str| (tag.to_string(), partition.to_string());
    assert_eq!(
        left_after(false).await,
        vec![
            pin("errored", &partition.key()),
            pin("ignored", ""),
            pin("ignored", &partition.key()),
        ],
        "within the grace"
    );
    assert_eq!(
        left_after(true).await,
        vec![pin("errored", &partition.key()), pin("ignored", ""),],
        "past the grace"
    );
}

/// Each ignored tag lookup ages out from its own last answer. A tag whose
/// selector stays ignored past the grace expires, and doesn't expire
/// another tag that a recent tick answered when that one's selector is
/// ignored once. A lookup never answered counts from the loop's start.
#[test]
fn each_ignored_tag_lookup_ages_out_from_its_own_last_answer() {
    use crate::db::TemplateCachePartition;
    let partition =
        TemplateCachePartition::Environment(pplx_space_execution_spec::EnvironmentReference::new(
            pplx_space_content_digest::Digest::sha256([1; 32]),
        ));
    let lookup = |tag: &str| ("seed".to_string(), tag.to_string(), partition);
    let start = std::time::Instant::now();
    let mut answered_at = std::collections::HashMap::new();

    // Within the grace of the loop's start, B's ignored lookup holds.
    assert!(
        age_lookups(
            &mut answered_at,
            vec![(lookup("a"), start)],
            vec![lookup("b")],
            start,
            start
        )
        .0
        .is_empty()
    );
    // A keeps answering while B stays ignored past the grace: B expires.
    let late = start + ENVIRONMENT_PIN_GRACE;
    let tick = late - Duration::from_secs(30);
    age_lookups(
        &mut answered_at,
        vec![(lookup("a"), tick)],
        vec![lookup("b")],
        start,
        tick,
    );
    assert_eq!(
        age_lookups(
            &mut answered_at,
            vec![(lookup("a"), late)],
            vec![lookup("b")],
            start,
            late
        )
        .0,
        vec![lookup("b")]
    );
    // A ignored once, a tick after its last answer, holds.
    let next = late + Duration::from_secs(30);
    assert_eq!(
        age_lookups(
            &mut answered_at,
            Vec::new(),
            vec![lookup("a"), lookup("b")],
            start,
            next
        )
        .0,
        vec![lookup("b")]
    );
    // Past its own grace, A expires too.
    assert_eq!(
        age_lookups(
            &mut answered_at,
            Vec::new(),
            vec![lookup("a")],
            start,
            late + ENVIRONMENT_PIN_GRACE
        )
        .0,
        vec![lookup("a")]
    );
}

/// Each ignored latest-releases lookup holds its template's release pins
/// only within its own grace: a template whose selector stays ignored past
/// it holds nothing, while one ignored a tick after its own last answer
/// still holds, however long the other has gone unanswered.
#[test]
fn each_ignored_release_lookup_holds_its_template_only_within_its_own_grace() {
    let environment = pplx_space_execution_spec::EnvironmentReference::new(
        pplx_space_content_digest::Digest::sha256([1; 32]),
    );
    let lookup = |template: &str| (template.to_string(), environment);
    let start = std::time::Instant::now();
    let mut answered_at = std::collections::HashMap::new();
    // Each answer here arrives at the prune's own `now`.
    let mut age = |answered: Vec<_>, ignored: Vec<_>, now| {
        let answered = answered.into_iter().map(|lookup| (lookup, now)).collect();
        age_lookups(&mut answered_at, answered, ignored, start, now)
    };

    // Within the grace of the loop's start, B holds.
    assert_eq!(
        age(vec![lookup("a")], vec![lookup("b")], start),
        (Vec::new(), vec![lookup("b")])
    );
    // A keeps answering while B stays ignored past the grace: B stops
    // holding, so its template prunes again.
    let late = start + ENVIRONMENT_PIN_GRACE;
    age(
        vec![lookup("a")],
        vec![lookup("b")],
        late - Duration::from_secs(30),
    );
    assert_eq!(
        age(vec![lookup("a")], vec![lookup("b")], late),
        (vec![lookup("b")], Vec::new())
    );
    // A ignored once, a tick after its last answer, still holds.
    assert_eq!(
        age(
            Vec::new(),
            vec![lookup("a"), lookup("b")],
            late + Duration::from_secs(30)
        ),
        (vec![lookup("b")], vec![lookup("a")])
    );
}

/// An ignored release lookup within its grace keeps only its own template
/// out of the release prune, and an org-specific verdict on that template
/// still flushes it.
#[test]
fn a_holding_release_lookup_keeps_only_its_template_s_pins_short_of_a_verdict() {
    let environment = pplx_space_execution_spec::EnvironmentReference::new(
        pplx_space_content_digest::Digest::sha256([1; 32]),
    );
    let names = |names: &[&str]| {
        names
            .iter()
            .map(|name| name.to_string())
            .collect::<Vec<_>>()
    };
    let holding = vec![("held".to_string(), environment)];
    let mut prunable = names(&["held", "other"]);
    hold_release_pins_of(&holding, &mut prunable);
    assert_eq!(prunable, names(&["other"]));

    let mut keep = vec![("held".to_string(), "sha256:aa".to_string())];
    flush_org_specific_release_pins(
        &BTreeSet::from(["held".to_string()]),
        &mut prunable,
        &mut keep,
    );
    assert_eq!(
        prunable,
        names(&["other", "held"]),
        "the verdict flushes it"
    );
    assert!(keep.is_empty());
}

/// Tick after tick, the loop's own record of release lookups' answers decides
/// which ignored ones hold their template's pins: one answered by an earlier
/// tick holds until a full grace after that answer, one never answered holds
/// until a grace after the loop started, and a template whose lookup isn't
/// ignored is pruned as usual.
#[test]
fn ignored_release_lookups_hold_their_pins_tick_after_tick_from_the_last_answer() {
    let environment = pplx_space_execution_spec::EnvironmentReference::new(
        pplx_space_content_digest::Digest::sha256([1; 32]),
    );
    let answered = ("answered".to_string(), environment);
    let never = ("never".to_string(), environment);
    let started_at = std::time::Instant::now();
    let mut known = KnownEnvironments {
        last: None,
        answered_at: started_at,
        started_at,
        tag_answered_at: HashMap::new(),
        release_answered_at: HashMap::new(),
    };
    // One tick's prune: which of the three templates it would prune.
    let mut tick = |answers: Vec<(environments::ReleaseLookupKey, std::time::Instant)>,
                    ignored: Vec<environments::ReleaseLookupKey>,
                    now| {
        let mut prunable = vec![
            "answered".to_string(),
            "never".to_string(),
            "other".to_string(),
        ];
        hold_ignored_release_lookups(&mut known, answers, ignored, &mut prunable, now);
        prunable
    };
    let both_ignored = || vec![answered.clone(), never.clone()];
    let answer = started_at + Duration::from_secs(300);

    // The first tick's selector applies for `answered`, so nothing holds.
    assert_eq!(
        tick(vec![(answered.clone(), answer)], Vec::new(), answer),
        ["answered", "never", "other"]
    );
    // From then on the cluster ignores both lookups' selectors.
    assert_eq!(
        tick(
            Vec::new(),
            both_ignored(),
            started_at + ENVIRONMENT_PIN_GRACE / 2
        ),
        ["other"],
        "both within their grace"
    );
    assert_eq!(
        tick(
            Vec::new(),
            both_ignored(),
            started_at + ENVIRONMENT_PIN_GRACE
        ),
        ["never", "other"],
        "the one never answered ran out a grace after the loop started"
    );
    assert_eq!(
        tick(Vec::new(), both_ignored(), answer + ENVIRONMENT_PIN_GRACE),
        ["answered", "never", "other"],
        "the answered one ran out a grace after its answer"
    );
}

/// An ignored lookup's grace counts from when its last answer arrived, not
/// from the prune of the tick that got it: a tick that answers early and
/// prunes late doesn't stretch the grace.
#[test]
fn a_lookup_s_grace_counts_from_its_answer_not_its_tick_s_prune() {
    let environment = pplx_space_execution_spec::EnvironmentReference::new(
        pplx_space_content_digest::Digest::sha256([1; 32]),
    );
    let lookup = ("seed".to_string(), environment);
    let start = std::time::Instant::now();
    let mut answered_at = std::collections::HashMap::new();
    let answer = start + Duration::from_secs(10);
    // The tick that got the answer prunes two minutes later.
    age_lookups(
        &mut answered_at,
        vec![(lookup.clone(), answer)],
        Vec::new(),
        start,
        answer + Duration::from_secs(120),
    );
    assert_eq!(
        age_lookups(
            &mut answered_at,
            Vec::new(),
            vec![lookup.clone()],
            start,
            answer + ENVIRONMENT_PIN_GRACE
        ),
        (vec![lookup], Vec::new()),
        "the grace ran out a full grace after the answer"
    );
}

/// A lookup answered twice in one tick, as one configuration lists twice,
/// ages from its newer answer, whichever order the two arrive in.
#[test]
fn a_lookup_answered_twice_ages_from_its_newer_answer() {
    let environment = pplx_space_execution_spec::EnvironmentReference::new(
        pplx_space_content_digest::Digest::sha256([1; 32]),
    );
    let lookup = ("seed".to_string(), environment);
    let start = std::time::Instant::now();
    let (older, newer) = (
        start + Duration::from_secs(1),
        start + Duration::from_secs(8),
    );
    let mut answered_at = std::collections::HashMap::new();
    age_lookups(
        &mut answered_at,
        vec![(lookup.clone(), newer), (lookup.clone(), older)],
        Vec::new(),
        start,
        newer,
    );
    let ignored_at = |now| {
        age_lookups(
            &mut answered_at.clone(),
            Vec::new(),
            vec![lookup.clone()],
            start,
            now,
        )
    };
    assert_eq!(
        ignored_at(older + ENVIRONMENT_PIN_GRACE),
        (Vec::new(), vec![lookup.clone()]),
        "still holding a grace after the older answer"
    );
    assert_eq!(
        ignored_at(newer + ENVIRONMENT_PIN_GRACE),
        (vec![lookup.clone()], Vec::new())
    );
}
