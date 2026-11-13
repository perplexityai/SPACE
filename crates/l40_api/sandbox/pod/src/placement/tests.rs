use std::collections::BTreeSet;

use uuid::Uuid;

use super::*;
use pplx_space_common_core::types::{
    NodeGroupId, NodeGroupPolicy, NodeGroupTemplatePolicy, TemplateCompatSpec, TemplateName,
    TemplateVisibility,
};

const TEST_ORG_ID: OrgId = OrgId::from_uuid(Uuid::from_u128(0x2));
const OTHER_ORG_ID: OrgId = OrgId::from_uuid(Uuid::from_u128(0x3));

fn generic_template() -> TemplateCompatSpec {
    TemplateCompatSpec {
        name: TemplateName::parse("seed").unwrap(),
        org_id: None,
        visibility: TemplateVisibility::Generic,
    }
}

fn org_specific_template(org_id: OrgId) -> TemplateCompatSpec {
    TemplateCompatSpec {
        name: TemplateName::parse("acme-secret").unwrap(),
        org_id: Some(org_id),
        visibility: TemplateVisibility::OrgSpecific,
    }
}

fn group(name: &str) -> NodeGroupName {
    NodeGroupName::parse(name).unwrap()
}

fn shared(name: &str) -> NodeGroup {
    NodeGroup {
        id: NodeGroupId::new(),
        name: group(name),
        policy: NodeGroupPolicy::Generic { is_default: false },
        template_allowlist: BTreeSet::new(),
    }
}

fn dedicated(name: &str, org_id: OrgId) -> NodeGroup {
    NodeGroup {
        id: NodeGroupId::new(),
        name: group(name),
        policy: NodeGroupPolicy::Dedicated {
            org_id,
            template_policy: NodeGroupTemplatePolicy::Default,
        },
        template_allowlist: BTreeSet::new(),
    }
}

#[test]
fn empty_candidate_set_fails_closed() {
    assert_eq!(
        NodeGroupPlacement::for_caller(Some(TEST_ORG_ID), &generic_template(), &[]),
        Err(NodeGroupPlacementError::NoCompatibleNodeGroup)
    );
}

#[test]
fn another_orgs_dedicated_candidate_is_rejected() {
    // The escape this constructor exists to close: a caller that flattens a
    // foreign dedicated group into the candidate set would otherwise get a
    // rendered boundary admitting that org's hardware, leaving the node taint
    // as the only control — and none at all for a pre-bound pod, which
    // `NoSchedule` does not fence.
    assert_eq!(
        NodeGroupPlacement::for_caller(
            Some(TEST_ORG_ID),
            &generic_template(),
            &[shared("workers"), dedicated("other-ded", OTHER_ORG_ID)],
        ),
        Err(NodeGroupPlacementError::IncompatibleCandidate {
            group: group("other-ded"),
            template: TemplateName::parse("seed").unwrap(),
            caller: Some(TEST_ORG_ID),
        })
    );
}

#[test]
fn shared_only_placement_emits_no_toleration_or_preference() {
    let placement = NodeGroupPlacement::for_caller(
        Some(TEST_ORG_ID),
        &generic_template(),
        &[shared("workers")],
    )
    .unwrap();
    assert_eq!(placement.caller_org(), Some(TEST_ORG_ID));
    assert!(!placement.targets_dedicated_capacity());
    assert_eq!(placement.dedicated_toleration(), None);
    assert!(placement.preferred_dedicated_term().is_none());
}

#[test]
fn candidates_are_deduplicated_and_sorted() {
    let placement = NodeGroupPlacement::for_caller(
        Some(TEST_ORG_ID),
        &generic_template(),
        &[
            shared("workers"),
            dedicated("acme-dedicated", TEST_ORG_ID),
            shared("workers"),
        ],
    )
    .unwrap();
    let expression = placement.required_match_expression();
    assert_eq!(expression.key, SANDBOX_NODE_GROUP_LABEL);
    assert_eq!(expression.operator, "In");
    assert_eq!(
        expression.values,
        Some(vec!["acme-dedicated".to_string(), "workers".to_string()])
    );
}

#[test]
fn dedicated_subset_is_derived_from_the_callers_own_groups() {
    let placement = NodeGroupPlacement::for_caller(
        Some(TEST_ORG_ID),
        &generic_template(),
        &[
            shared("workers"),
            dedicated("acme-b", TEST_ORG_ID),
            dedicated("acme-a", TEST_ORG_ID),
        ],
    )
    .unwrap();

    assert_eq!(placement.caller_org(), Some(TEST_ORG_ID));
    assert!(placement.targets_dedicated_capacity());

    let toleration = placement.dedicated_toleration().unwrap();
    assert_eq!(toleration.key.as_deref(), Some(DEDICATED_NODE_TAINT_KEY));
    assert_eq!(toleration.operator.as_deref(), Some("Equal"));
    assert_eq!(toleration.value, Some(TEST_ORG_ID.as_uuid().to_string()));
    assert_eq!(toleration.effect.as_deref(), Some("NoSchedule"));
    assert_eq!(toleration.toleration_seconds, None);

    let term = placement.preferred_dedicated_term().unwrap();
    // The literal, not the constant against itself: Kubernetes rejects a
    // preferred weight above 100 at pod-create, failing the whole restore.
    assert_eq!(term.weight, 100);
    let expressions = term.preference.match_expressions.unwrap();
    assert_eq!(expressions.len(), 1);
    assert_eq!(expressions[0].key, SANDBOX_NODE_GROUP_LABEL);
    assert_eq!(expressions[0].operator, "In");
    // Sorted and deduplicated, so the rendered spec is deterministic.
    assert_eq!(
        expressions[0].values,
        Some(vec!["acme-a".to_string(), "acme-b".to_string()])
    );
}

#[test]
fn a_name_appearing_with_two_owners_is_rejected() {
    // `node_groups.name` is UNIQUE, so this is unreachable from a real read —
    // but the constructor's job is that no caller bug yields a boundary
    // admitting the wrong nodes, and a name claimed by both shared and
    // dedicated capacity would do exactly that.
    assert_eq!(
        NodeGroupPlacement::for_caller(
            Some(TEST_ORG_ID),
            &generic_template(),
            &[
                shared("acme-dedicated"),
                dedicated("acme-dedicated", TEST_ORG_ID)
            ],
        ),
        Err(NodeGroupPlacementError::ConflictingGroupPolicy {
            group: group("acme-dedicated"),
        })
    );
}

#[test]
fn an_all_shared_placement_still_records_its_caller_org() {
    // The org is what lets the pod builder reject a candidate set computed for
    // somebody else. Recording it only alongside dedicated capacity would leave
    // an all-shared placement for the wrong org undetectable.
    let placement = NodeGroupPlacement::for_caller(
        Some(OTHER_ORG_ID),
        &generic_template(),
        &[shared("workers")],
    )
    .unwrap();
    assert_eq!(placement.caller_org(), Some(OTHER_ORG_ID));
    assert!(!placement.targets_dedicated_capacity());
}

#[test]
fn shared_capacity_is_rejected_for_an_org_specific_template() {
    // The data-isolation guarantee: an org-specific template's rootfs must
    // never reach shared hardware, so a candidate set that offers it is a
    // caller bug the constructor refuses rather than one the boundary admits.
    assert_eq!(
        NodeGroupPlacement::for_caller(
            Some(TEST_ORG_ID),
            &org_specific_template(TEST_ORG_ID),
            &[shared("workers")],
        ),
        Err(NodeGroupPlacementError::IncompatibleCandidate {
            group: group("workers"),
            template: TemplateName::parse("acme-secret").unwrap(),
            caller: Some(TEST_ORG_ID),
        })
    );
}

#[test]
fn a_restricted_group_that_excludes_the_template_is_rejected() {
    // `template_policy: Restricted` admits only the allowlisted templates, so
    // an owned group is not automatically legal either.
    let restricted = NodeGroup {
        id: NodeGroupId::new(),
        name: group("acme-restricted"),
        policy: NodeGroupPolicy::Dedicated {
            org_id: TEST_ORG_ID,
            template_policy: NodeGroupTemplatePolicy::Restricted,
        },
        template_allowlist: BTreeSet::from([TemplateName::parse("other").unwrap()]),
    };
    assert_eq!(
        NodeGroupPlacement::for_caller(Some(TEST_ORG_ID), &generic_template(), &[restricted]),
        Err(NodeGroupPlacementError::IncompatibleCandidate {
            group: group("acme-restricted"),
            template: TemplateName::parse("seed").unwrap(),
            caller: Some(TEST_ORG_ID),
        })
    );
}

#[test]
fn an_org_specific_template_may_use_its_own_dedicated_group() {
    let placement = NodeGroupPlacement::for_caller(
        Some(TEST_ORG_ID),
        &org_specific_template(TEST_ORG_ID),
        &[dedicated("acme-dedicated", TEST_ORG_ID)],
    )
    .unwrap();
    assert!(placement.targets_dedicated_capacity());
    assert_eq!(
        placement.required_match_expression().values,
        Some(vec!["acme-dedicated".to_string()])
    );
}

#[test]
fn an_org_less_caller_gets_shared_capacity_and_nothing_else() {
    // The warm pool is the caller with no org. What it must never receive is a
    // boundary that lets its pods land on somebody's reserved hardware — no
    // toleration, no dedicated bias, and no dedicated group among its
    // candidates.
    //
    // Note what this does *not* pin. The constructor also matches the dedicated
    // subset through `Some(caller_org)` rather than comparing two `Option`s, so
    // a generic group's `None` owner is not read as "the org-less caller's own".
    // Collapsing that to `group.org_id() == caller_org` leaves all twelve tests
    // here green, because `dedicated` is built by `caller_org.filter(..)` and is
    // therefore `None` for an org-less caller whatever the subset holds. The
    // guard is redundant by construction and documented as such at its site;
    // this test asserts the observable, not the belt-and-braces.
    let placement =
        NodeGroupPlacement::for_caller(None, &generic_template(), &[shared("workers")]).unwrap();
    assert_eq!(placement.caller_org(), None);
    assert!(
        !placement.targets_dedicated_capacity(),
        "an org-less caller has no dedicated capacity to bias toward or tolerate"
    );
    assert!(placement.admits_shared_capacity());
    assert_eq!(
        placement.required_match_expression().values,
        Some(vec!["workers".to_string()])
    );

    // Handed a dedicated group anyway, the constructor refuses rather than
    // filtering: an org-less caller matches no owner, so the set it was given
    // is not the set the relation defines.
    assert_eq!(
        NodeGroupPlacement::for_caller(
            None,
            &generic_template(),
            &[dedicated("acme-dedicated", TEST_ORG_ID)],
        ),
        Err(NodeGroupPlacementError::IncompatibleCandidate {
            group: group("acme-dedicated"),
            template: TemplateName::parse("seed").unwrap(),
            caller: None,
        })
    );

    // An org-specific template has no org-less reading at all, whatever
    // capacity is on offer.
    assert_eq!(
        NodeGroupPlacement::for_caller(
            None,
            &org_specific_template(TEST_ORG_ID),
            &[dedicated("acme-dedicated", TEST_ORG_ID)],
        ),
        Err(NodeGroupPlacementError::IncompatibleCandidate {
            group: group("acme-dedicated"),
            template: TemplateName::parse("acme-secret").unwrap(),
            caller: None,
        })
    );
}

#[test]
fn admits_shared_capacity_is_false_exactly_when_every_candidate_is_the_callers_own() {
    // The question a warm-pool claim asks: could a pod that is already running
    // on shared capacity satisfy this boundary at all? A sandbox that can also
    // use shared groups can adopt one; a sandbox whose whole set is its own
    // dedicated hardware never can, because no pooled pod is built for an org.
    let mixed = NodeGroupPlacement::for_caller(
        Some(TEST_ORG_ID),
        &generic_template(),
        &[shared("workers"), dedicated("acme-dedicated", TEST_ORG_ID)],
    )
    .unwrap();
    assert!(
        mixed.admits_shared_capacity(),
        "a set with one non-dedicated group admits a pooled pod"
    );
    assert!(mixed.targets_dedicated_capacity());

    let dedicated_only = NodeGroupPlacement::for_caller(
        Some(TEST_ORG_ID),
        &org_specific_template(TEST_ORG_ID),
        &[
            dedicated("acme-dedicated", TEST_ORG_ID),
            dedicated("acme-gpu", TEST_ORG_ID),
        ],
    )
    .unwrap();
    assert!(
        !dedicated_only.admits_shared_capacity(),
        "every candidate is the caller's own hardware, so no pooled pod can satisfy it"
    );

    // Duplicates are the trap: both lists are deduplicated, and comparing a
    // deduplicated subset against a *raw* candidate count would call this set
    // mixed. Three names in, two distinct — and still dedicated-only.
    let duplicated = NodeGroupPlacement::for_caller(
        Some(TEST_ORG_ID),
        &org_specific_template(TEST_ORG_ID),
        &[
            dedicated("acme-dedicated", TEST_ORG_ID),
            dedicated("acme-gpu", TEST_ORG_ID),
            dedicated("acme-dedicated", TEST_ORG_ID),
        ],
    )
    .unwrap();
    assert!(!duplicated.admits_shared_capacity());
}
