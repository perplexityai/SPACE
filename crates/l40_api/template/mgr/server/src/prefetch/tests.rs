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
