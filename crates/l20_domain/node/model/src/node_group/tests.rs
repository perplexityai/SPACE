use super::*;

fn org(tag: u8) -> OrgId {
    OrgId::from_uuid(Uuid::from_u128(tag as u128))
}

fn generic_template(name: &str) -> TemplateCompatSpec {
    TemplateCompatSpec {
        name: TemplateName::parse(name).unwrap(),
        org_id: None,
        visibility: TemplateVisibility::Generic,
    }
}

fn org_template(name: &str, owner: OrgId) -> TemplateCompatSpec {
    TemplateCompatSpec {
        name: TemplateName::parse(name).unwrap(),
        org_id: Some(owner),
        visibility: TemplateVisibility::OrgSpecific,
    }
}

fn group(
    name: &str,
    org_id: Option<OrgId>,
    template_policy: NodeGroupTemplatePolicy,
    allowlist: &[&str],
) -> NodeGroup {
    let policy = match org_id {
        None => NodeGroupPolicy::Generic { is_default: false },
        Some(org_id) => NodeGroupPolicy::Dedicated {
            org_id,
            template_policy,
        },
    };
    NodeGroup {
        id: NodeGroupId::new(),
        name: NodeGroupName::parse(name).unwrap(),
        policy,
        template_allowlist: allowlist
            .iter()
            .map(|name| TemplateName::parse(*name).unwrap())
            .collect(),
    }
}

/// The RFC's worked NxN example, extended to both callers per cell.
///
/// Orgs A and B. Generic templates `base`, `python`. Org-specific
/// templates `a-secret` (A) and `b-secret` (B). Groups: G (generic),
/// NA (A, default policy), NB (B, restricted to {base, b-secret}).
#[test]
fn rfc_worked_example_matrix() {
    let org_a = org(0xA);
    let org_b = org(0xB);

    let base = generic_template("base");
    let python = generic_template("python");
    let a_secret = org_template("a-secret", org_a);
    let b_secret = org_template("b-secret", org_b);

    let g = group("g", None, NodeGroupTemplatePolicy::Default, &[]);
    let na = group("na", Some(org_a), NodeGroupTemplatePolicy::Default, &[]);
    let nb = group(
        "nb",
        Some(org_b),
        NodeGroupTemplatePolicy::Restricted,
        &["base", "b-secret"],
    );

    // (template, caller, expected for [G, NA, NB])
    let cases = [
        (&base, org_a, [true, true, false]),
        (&base, org_b, [true, false, true]),
        (&python, org_a, [true, true, false]),
        (&python, org_b, [true, false, false]),
        (&a_secret, org_a, [false, true, false]),
        (&a_secret, org_b, [false, false, false]),
        (&b_secret, org_a, [false, false, false]),
        (&b_secret, org_b, [false, false, true]),
    ];
    for (template, caller, expected) in cases {
        for (group, want) in [&g, &na, &nb].into_iter().zip(expected) {
            assert_eq!(
                group.compatible(Some(caller), template),
                want,
                "template {} caller {} group {}",
                template.name,
                caller,
                group.name,
            );
        }
    }
}

/// Capacity isolation: a dedicated group rejects every non-owner caller
/// for every template, regardless of policy or allowlist content.
#[test]
fn dedicated_group_rejects_non_owner_callers() {
    let owner = org(1);
    let stranger = org(2);
    let templates = [
        generic_template("base"),
        org_template("owner-secret", owner),
        org_template("stranger-secret", stranger),
    ];
    let groups = [
        group("d", Some(owner), NodeGroupTemplatePolicy::Default, &[]),
        group(
            "r",
            Some(owner),
            NodeGroupTemplatePolicy::Restricted,
            &["base", "owner-secret", "stranger-secret"],
        ),
    ];
    for g in &groups {
        for t in &templates {
            assert!(
                !g.compatible(Some(stranger), t),
                "group {} template {}",
                g.name,
                t.name,
            );
        }
    }
}

/// Data isolation: an org-specific template is incompatible with generic
/// groups and with other orgs' groups — even when an (invalid) allowlist
/// row names it.
#[test]
fn allowlist_cannot_override_cross_org_rule() {
    let org_a = org(1);
    let org_b = org(2);
    let a_secret = org_template("a-secret", org_a);

    let generic = group(
        "g",
        None,
        NodeGroupTemplatePolicy::Default,
        // Allowlist rows on a generic group are meaningless; make sure the
        // predicate doesn't consult them.
        &["a-secret"],
    );
    let foreign = group(
        "nb",
        Some(org_b),
        NodeGroupTemplatePolicy::Restricted,
        &["a-secret"],
    );

    for caller in [org_a, org_b] {
        assert!(!generic.compatible(Some(caller), &a_secret));
        assert!(!foreign.compatible(Some(caller), &a_secret));
    }
}

/// An unowned org-specific template (forbidden at the database layer)
/// fails closed everywhere.
#[test]
fn unowned_org_specific_template_fails_closed() {
    let caller = org(1);
    let orphan = TemplateCompatSpec {
        name: TemplateName::parse("orphan").unwrap(),
        org_id: None,
        visibility: TemplateVisibility::OrgSpecific,
    };
    assert!(!orphan.launchable_by(Some(caller)));

    let groups = [
        group("g", None, NodeGroupTemplatePolicy::Default, &[]),
        group("d", Some(caller), NodeGroupTemplatePolicy::Default, &[]),
        group(
            "r",
            Some(caller),
            NodeGroupTemplatePolicy::Restricted,
            &["orphan"],
        ),
    ];
    for g in &groups {
        assert!(!g.compatible(Some(caller), &orphan), "group {}", g.name);
    }
}

#[test]
fn restricted_group_with_empty_allowlist_admits_nothing() {
    let owner = org(1);
    let g = group("r", Some(owner), NodeGroupTemplatePolicy::Restricted, &[]);
    assert!(!g.compatible(Some(owner), &generic_template("base")));
    assert!(!g.compatible(Some(owner), &org_template("mine", owner)));
}

#[test]
fn validate_allowlist_entry_enforces_cross_org_rule() {
    let org_a = org(1);
    let org_b = org(2);
    let restricted = group("r", Some(org_a), NodeGroupTemplatePolicy::Restricted, &[]);
    let generic_group = group("g", None, NodeGroupTemplatePolicy::Default, &[]);

    assert_eq!(
        restricted.validate_allowlist_entry(&generic_template("base")),
        Ok(()),
    );
    assert_eq!(
        restricted.validate_allowlist_entry(&org_template("mine", org_a)),
        Ok(()),
    );
    assert_eq!(
        restricted.validate_allowlist_entry(&org_template("theirs", org_b)),
        Err(NodeGroupAllowlistError::CrossOrgTemplate {
            template: TemplateName::parse("theirs").unwrap(),
            template_org: org_b,
            group_org: org_a,
        }),
    );
    assert_eq!(
        generic_group.validate_allowlist_entry(&org_template("mine", org_a)),
        Err(NodeGroupAllowlistError::OrgSpecificOnGenericGroup {
            template: TemplateName::parse("mine").unwrap(),
        }),
    );
    assert_eq!(
        generic_group.validate_allowlist_entry(&generic_template("base")),
        Ok(()),
    );
    let orphan = TemplateCompatSpec {
        name: TemplateName::parse("orphan").unwrap(),
        org_id: None,
        visibility: TemplateVisibility::OrgSpecific,
    };
    assert_eq!(
        restricted.validate_allowlist_entry(&orphan),
        Err(NodeGroupAllowlistError::UnownedOrgSpecificTemplate {
            template: TemplateName::parse("orphan").unwrap(),
        }),
    );
}

#[test]
fn node_group_name_accepts_label_safe_slugs() {
    for name in ["workers", "a", "0", "org-acme-1", "x1-y2-z3"] {
        assert!(NodeGroupName::parse(name).is_ok(), "{name:?}");
    }
    let max = "a".repeat(NODE_GROUP_NAME_MAX_LEN);
    NodeGroupName::parse(max).unwrap();
}

#[test]
fn node_group_name_rejects_invalid_slugs() {
    assert_eq!(
        NodeGroupName::parse(""),
        Err(NodeGroupNameParseError::Empty),
    );
    let too_long = "a".repeat(NODE_GROUP_NAME_MAX_LEN + 1);
    assert_eq!(
        NodeGroupName::parse(too_long),
        Err(NodeGroupNameParseError::TooLong {
            max: NODE_GROUP_NAME_MAX_LEN,
            actual: NODE_GROUP_NAME_MAX_LEN + 1,
        }),
    );
    for name in ["Workers", "under_score", "dot.name", "sp ace", "héllo"] {
        assert_eq!(
            NodeGroupName::parse(name),
            Err(NodeGroupNameParseError::InvalidCharacter),
            "{name:?}",
        );
    }
    for name in ["-leading", "trailing-", "-"] {
        assert_eq!(
            NodeGroupName::parse(name),
            Err(NodeGroupNameParseError::InvalidBoundary),
            "{name:?}",
        );
    }
}

#[test]
fn template_policy_round_trips_canonical_strings() {
    for policy in [
        NodeGroupTemplatePolicy::Default,
        NodeGroupTemplatePolicy::Restricted,
    ] {
        let parsed: NodeGroupTemplatePolicy = policy.as_str().parse().unwrap();
        assert_eq!(parsed, policy);
        assert_eq!(policy.to_string(), policy.as_str());
    }
    for value in ["", "Default", "RESTRICTED", "open"] {
        assert!(
            value.parse::<NodeGroupTemplatePolicy>().is_err(),
            "{value:?}"
        );
    }
}
