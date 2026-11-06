use super::*;
use pplx_space_common_core::types::SandboxLifecycle;

/// The kill switch is production wiring, so pin every path through it: the
/// default with the var unset, an explicit disable, each truthy spelling the
/// cluster-controller's `env_config::truthy_default` accepts, and — the one
/// that matters most — an unrecognized value falling back to the safe mode
/// rather than being read as "on".
#[test]
fn residency_evidence_parses_the_kill_switch_conservatively() {
    assert_eq!(
        ResidencyEvidence::from_env_value(None),
        ResidencyEvidence::LifecycleOnly,
        "unset must leave the census discount off"
    );
    assert_eq!(
        ResidencyEvidence::default(),
        ResidencyEvidence::LifecycleOnly,
        "the census discount stays opt-in per environment"
    );

    for on in ["1", "true", "True", "TRUE"] {
        assert_eq!(
            ResidencyEvidence::from_env_value(Some(on)),
            ResidencyEvidence::NodeCensusAware,
            "{on:?} must enable the discount"
        );
    }
    for off in ["0", "false", "False", "FALSE"] {
        assert_eq!(
            ResidencyEvidence::from_env_value(Some(off)),
            ResidencyEvidence::LifecycleOnly,
            "{off:?} must disable the discount"
        );
    }

    // A typo must not enable a mode that can terminate a live node.
    for invalid in ["", " ", "yes", "on", "TrUe", "enabled", "2", "null"] {
        assert_eq!(
            ResidencyEvidence::from_env_value(Some(invalid)),
            ResidencyEvidence::LifecycleOnly,
            "{invalid:?} is not a recognized value and must fall back to the default"
        );
    }
}

/// Both gates must build their policy branch from this one helper, so the
/// disabled mode is a literal `TRUE` (nothing to evaluate) and the enabled mode
/// is exactly the negated absence proof.
#[test]
fn occupies_predicate_is_the_single_policy_branch() {
    assert_eq!(
        occupies_node_predicate("s", "$1", ResidencyEvidence::LifecycleOnly),
        "TRUE",
        "the kill switch must cost nothing to evaluate"
    );
    assert_eq!(
        occupies_node_predicate("s", "$1", ResidencyEvidence::NodeCensusAware),
        format!("NOT {}", node_proves_sandbox_absent("s", "$1")),
    );
}

/// The heartbeat term must key on the expression the caller passed, not on the
/// row's own column: a bound parameter is what lets Postgres hoist the subquery
/// into a once-per-execution InitPlan instead of hashing the whole
/// `node_status_snapshots` relation per row.
#[test]
fn heartbeat_fragment_keys_on_the_caller_supplied_node_expression() {
    let fragment = node_heartbeat_fresh("$1");
    assert!(fragment.contains("rnss.node_id = $1"));
    assert!(!fragment.contains("active_node_id"));
}

#[test]
fn observation_fragment_keys_on_sandbox_and_node_only() {
    let fragment = node_observes_sandbox("sandboxes", "$1");
    assert!(fragment.contains("rsos.sandbox_id = sandboxes.id"));
    assert!(fragment.contains("rsos.node_id = $1"));
    // Presence, not agreement: a census row whose status disagrees with the
    // cluster lifecycle still means the node holds state for the sandbox.
    assert!(
        !fragment.contains("status"),
        "matching on observed status would discount rows whose only copy is \
         node-local: {fragment}"
    );
}

/// The absence proof must be a conjunction in exactly this direction: fresh
/// heartbeat AND fresh census AND settled placement AND no census row. Any
/// other composition (an `OR`, or negating one of the evidence terms)
/// discounts rows on nodes the control plane cannot see, which is the data-loss
/// direction.
#[test]
fn absence_proof_requires_a_fresh_heartbeat_and_negates_only_the_observation() {
    let fragment = node_proves_sandbox_absent("s", "$1");
    let heartbeat = node_heartbeat_fresh("$1");
    let census = node_census_fresh("$1");
    let settled = placement_settled("s", CENSUS_PLACEMENT_SETTLE);
    let observed = node_observes_sandbox("s", "$1");

    assert!(fragment.contains(&heartbeat));
    assert!(
        fragment.contains(&census),
        "a node whose inventory publisher has wedged keeps a fresh heartbeat, so \
         census age has to be its own conjunct: {fragment}"
    );
    assert!(
        fragment.contains(&settled),
        "a row placed since the node's last census must not be proven absent: {fragment}"
    );
    assert!(fragment.contains(&format!("AND NOT {observed}")));
    assert!(
        !fragment.contains(&format!("NOT {settled}")),
        "the settle term must gate the discount, not invert it: {fragment}"
    );
    assert!(
        !fragment.contains(&format!("NOT {heartbeat}")),
        "an unreachable or stale-heartbeat node must never prove absence: {fragment}"
    );
    assert!(
        !fragment.contains(&format!("NOT {census}")),
        "a stale census must withhold the discount, not grant it: {fragment}"
    );
    assert!(
        !fragment.contains(" OR "),
        "must be a conjunction: {fragment}"
    );
}

/// The census term is the whole point of gating on last-complete-inventory
/// rather than on the heartbeat, so pin its three fail-closed properties: it
/// reads `sandbox_inventory_pruned_at` (the server's prune verdict) and not
/// `observed_at` / `stale_after` (refreshed by every publication), it keys on
/// the caller's bound parameter so Postgres can hoist it into an InitPlan, and
/// it treats a NULL timestamp explicitly rather than leaving the answer to
/// three-valued logic.
#[test]
fn census_freshness_reads_the_prune_timestamp_and_fails_closed_on_null() {
    let fragment = node_census_fresh("$1");

    assert!(fragment.contains("cnss.node_id = $1"));
    assert!(fragment.contains("cnss.sandbox_inventory_pruned_at IS NOT NULL"));
    assert!(fragment.contains(&format!(
        "make_interval(secs => {})",
        CENSUS_INVENTORY_MAX_AGE.as_secs()
    )));
    for heartbeat_column in ["observed_at", "stale_after", "reachable"] {
        assert!(
            !fragment.contains(heartbeat_column),
            "{heartbeat_column} is refreshed by every publication and proves \
             nothing about census age: {fragment}"
        );
    }
}

/// A NULL `placement_changed_at` must read as not-settled. Unknown placement
/// age is not evidence, and a bare `< NOW() - interval` comparison yields NULL
/// there, which SQL would then treat as not-true anyway — but only by accident
/// of three-valued logic. The explicit `IS NOT NULL` makes it intentional.
#[test]
fn placement_settle_treats_unknown_placement_age_as_unsettled() {
    let fragment = placement_settled("s", CENSUS_PLACEMENT_SETTLE);
    assert!(fragment.contains("s.placement_changed_at IS NOT NULL"));
    assert!(fragment.contains(&format!(
        "make_interval(secs => {})",
        CENSUS_PLACEMENT_SETTLE.as_secs()
    )));
}

/// The set is an include-list, and these two absences are load-bearing rather
/// than oversights: `created` has no pod placed and `stopped` has no running VM,
/// so sandbox-manager cannot service an evacuation for either and a drain that
/// counted them would wait forever. A "not terminal" exclusion would admit both.
#[test]
fn resident_lifecycles_exclude_the_non_evacuable_states() {
    for absent in [
        SandboxLifecycle::Created,
        SandboxLifecycle::Stopped,
        SandboxLifecycle::Suspended,
        SandboxLifecycle::Deleting,
        SandboxLifecycle::Failed,
    ] {
        assert!(
            !RESIDENT_LIFECYCLES.contains(&absent),
            "{absent} must not count as residency"
        );
    }
    for present in [
        SandboxLifecycle::Started,
        SandboxLifecycle::Paused,
        SandboxLifecycle::Restoring,
        SandboxLifecycle::Suspending,
    ] {
        assert!(
            RESIDENT_LIFECYCLES.contains(&present),
            "{present} occupies the node and must count"
        );
    }
}

/// `RESIDENT_LIFECYCLES` is `&[SandboxLifecycle]`, so a misspelling is now
/// unrepresentable and needs no test. What the type cannot catch is a *valid*
/// lifecycle on the wrong side of the line, in either direction: one wrongly
/// added over-counts and stalls a drain, one wrongly omitted under-counts and
/// ends a drain on a node that still holds the sandbox.
///
/// The `match` is the load-bearing part, and it is exhaustive on purpose: a new
/// `SandboxLifecycle` variant stops this file compiling until someone decides
/// whether a drain has to wait for it. That decision is then cross-checked
/// against the constant, so classifying a variant one way and listing it the
/// other fails too.
#[test]
fn every_sandbox_lifecycle_is_classified_resident_or_excluded() {
    fn expected_resident(lifecycle: SandboxLifecycle) -> bool {
        match lifecycle {
            SandboxLifecycle::Creating
            | SandboxLifecycle::Starting
            | SandboxLifecycle::Started
            | SandboxLifecycle::Pausing
            | SandboxLifecycle::Paused
            | SandboxLifecycle::Resuming
            | SandboxLifecycle::Stopping
            | SandboxLifecycle::Suspending
            | SandboxLifecycle::Restoring
            | SandboxLifecycle::Checkpointing => true,
            // No pod placed yet, so nothing on the node to evacuate.
            SandboxLifecycle::Created
            // Suspend completed; the row is no longer tied to a node.
            | SandboxLifecycle::Suspended
            // No running VM, and sandbox-manager refuses `suspend_sandbox`.
            | SandboxLifecycle::Stopped
            // Already being torn down; no state a drain could preserve.
            | SandboxLifecycle::Deleting
            // Held for investigation rather than moved.
            | SandboxLifecycle::Quarantined
            // Terminal.
            | SandboxLifecycle::Failed => false,
        }
    }

    // Every entry in the constant must be classified resident.
    for lifecycle in RESIDENT_LIFECYCLES {
        assert!(
            expected_resident(*lifecycle),
            "{lifecycle} is in RESIDENT_LIFECYCLES but classified non-resident"
        );
    }

    // And every lifecycle classified resident must be in the constant. Iterating
    // the parsed entries above cannot catch an omission, so the excluded side is
    // named explicitly and the two are required to partition the enum.
    let excluded = [
        SandboxLifecycle::Created,
        SandboxLifecycle::Suspended,
        SandboxLifecycle::Stopped,
        SandboxLifecycle::Deleting,
        SandboxLifecycle::Quarantined,
        SandboxLifecycle::Failed,
    ];
    for lifecycle in excluded {
        assert!(
            !expected_resident(lifecycle),
            "{lifecycle} is listed excluded but classified resident"
        );
        assert!(
            !RESIDENT_LIFECYCLES.contains(&lifecycle),
            "{lifecycle} is listed excluded but present in RESIDENT_LIFECYCLES"
        );
    }
    assert_eq!(
        RESIDENT_LIFECYCLES.len() + excluded.len(),
        16,
        "RESIDENT_LIFECYCLES and the excluded list must partition SandboxLifecycle's \
         16 variants; update both this count and the match above when a variant is added"
    );
}

#[test]
fn resident_lifecycle_fragment_lists_every_value_for_the_given_alias() {
    let fragment = resident_lifecycle("s");

    assert!(fragment.starts_with("s.lifecycle IN ("));
    for lifecycle in RESIDENT_LIFECYCLES {
        assert!(
            fragment.contains(&format!("'{lifecycle}'")),
            "{lifecycle} missing from {fragment}"
        );
    }
    // Inlined rather than bound, so composing it never shifts a caller's
    // parameter numbering.
    assert!(!fragment.contains('$'), "{fragment}");
}

/// The resident guard both node-release gates evaluate. Two properties, and
/// both are load-bearing:
///
/// * It is an **exclusion** list. Read as an include-list of four it would count
///   nothing; read as an exclusion it counts every other lifecycle — including
///   the `created` / `stopped` / `deleting` / `quarantined` rows node-drain's
///   resident include-list omits, which is the whole reason node-drain has to
///   evaluate this rather than its own set.
/// * It carries the census term, so the two gates discount the same phantom
///   rows. A node released by one gate's discount and refused by the other's
///   absence of it parks forever.
#[test]
fn active_resident_is_an_exclusion_list_carrying_the_census_term() {
    let fragment = active_resident_predicate("sandboxes", "$1", ResidencyEvidence::NodeCensusAware);

    assert!(fragment.contains("sandboxes.active_node_id = $1"));
    // The exact clause, not four `contains` checks: a fifth exclusion silently
    // released a whole lifecycle's worth of nodes, and `contains` would pass.
    assert!(
        fragment.contains(
            "sandboxes.lifecycle NOT IN ('failed', 'cleanup_required', 'deleted', 'suspended')"
        ),
        "an include-list, a reordering, or a fifth exclusion all change which \
         nodes get released: {fragment}"
    );
    assert!(
        fragment.contains(&occupies_node_predicate(
            "sandboxes",
            "$1",
            ResidencyEvidence::NodeCensusAware
        )),
        "the census discount must apply here too, or the two gates disagree \
         about phantom rows: {fragment}"
    );
}

/// Composable: the fragment is parenthesized and the kill switch collapses its
/// census term to a literal `TRUE`, so a caller can `AND` it into a larger
/// predicate without the disabled mode costing an evaluation.
#[test]
fn active_resident_is_parenthesized_and_honors_the_kill_switch() {
    let fragment = active_resident_predicate("s", "$1", ResidencyEvidence::LifecycleOnly);
    assert!(fragment.starts_with('('), "{fragment}");
    assert!(fragment.trim_end().ends_with(')'), "{fragment}");
    assert!(fragment.contains("AND TRUE"), "{fragment}");
    assert!(
        !fragment.contains("sandbox_observed_states"),
        "the disabled mode must not join the census: {fragment}"
    );
}

/// Keys on the caller's alias and node expression rather than hard-coding the
/// bare table name it was extracted from, so a caller scanning `sandboxes s`
/// gets valid SQL instead of an unqualified column reference.
#[test]
fn active_resident_keys_on_the_caller_supplied_alias() {
    // Both modes. Under `LifecycleOnly` the census term collapses to a literal
    // `TRUE`, so that variant cannot exhibit the bug this guards against — only
    // the census-aware rendering exercises `node_proves_sandbox_absent`,
    // `placement_settled`, and `node_observes_sandbox`, each of which re-derives
    // the alias.
    for evidence in [
        ResidencyEvidence::LifecycleOnly,
        ResidencyEvidence::NodeCensusAware,
    ] {
        let fragment = active_resident_predicate("s", "scope.node_id", evidence);
        assert!(
            fragment.contains("s.active_node_id = scope.node_id"),
            "{evidence}: {fragment}"
        );
        assert!(
            fragment.contains("s.lifecycle NOT IN ("),
            "{evidence}: {fragment}"
        );
        assert!(
            !fragment.contains("sandboxes."),
            "{evidence}: a hard-coded table name breaks every caller that scans \
             `sandboxes s`: {fragment}"
        );
    }
}
