//! Resolver tests.
//!
//! The Postgres-backed cases run the real statement against a migrated
//! database, because the two properties worth pinning are both properties of
//! the *query*: that it selects every compatible group rather than the default
//! generic one, and that an empty `node_groups` table is distinguishable from
//! an absent `templates` row. A fake store would assert neither.

use pplx_space_common_core::types::{OrgId, SandboxId};

use std::collections::BTreeSet;

use k8s_openapi::api::core::v1::Node;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use pplx_space_common_core::kubernetes::SANDBOX_NODE_GROUP_LABEL;
use pplx_space_common_core::types::{
    NodeGroupId, NodeGroupName, NodeGroupPolicy, NodeGroupTemplatePolicy, TemplateCompatSpec,
    TemplateName,
};
use sqlx::PgPool;
use uuid::Uuid;

use super::*;

type TestResult<T = ()> = pplx_space_db_test_fixtures::TestDbResult<T>;

/// Orgs seeded by the baseline migration's fixture data.
const ORG_A: Uuid = Uuid::from_u128(0x1);
const ORG_B: Uuid = Uuid::from_u128(0x3);

/// A pool pointed at nothing. Any statement issued against it fails, which is
/// what makes "the gate issued no query" an assertion rather than a claim.
fn undialed_pool() -> PgPool {
    PgPool::connect_lazy("postgresql://127.0.0.1:1/does-not-exist")
        .expect("a lazy pool builds without connecting")
}

async fn insert_template(
    pool: &PgPool,
    name: &str,
    visibility: &str,
    org_id: Option<OrgId>,
) -> TestResult {
    sqlx::query("INSERT INTO templates (id, name, org_id, visibility) VALUES ($1, $2, $3, $4)")
        .bind(Uuid::now_v7())
        .bind(name)
        .bind(org_id)
        .bind(visibility)
        .execute(pool)
        .await?;
    Ok(())
}

async fn insert_group(
    pool: &PgPool,
    name: &str,
    org_id: Option<OrgId>,
    template_policy: NodeGroupTemplatePolicy,
    is_default: bool,
) -> TestResult<Uuid> {
    let id = Uuid::now_v7();
    sqlx::query(
        "INSERT INTO node_groups (id, name, org_id, template_policy, is_default)
         VALUES ($1, $2, $3, $4, $5)",
    )
    .bind(id)
    .bind(name)
    .bind(org_id)
    .bind(template_policy.as_str())
    .bind(is_default)
    .execute(pool)
    .await?;
    Ok(id)
}

async fn allow_template(pool: &PgPool, group_id: Uuid, template_name: &str) -> TestResult {
    sqlx::query(
        "INSERT INTO node_group_template_compat (node_group_id, template_name) VALUES ($1, $2)",
    )
    .bind(group_id)
    .bind(template_name)
    .execute(pool)
    .await?;
    Ok(())
}

/// The candidate set as the pod would render it: the required term's values.
/// Asserting on the rendered expression rather than on a private field keeps
/// these tests pinned to what reaches Kubernetes.
fn candidate_names(placement: &NodeGroupPlacement) -> Vec<String> {
    placement
        .required_match_expression()
        .values
        .unwrap_or_default()
}

fn placed(resolved: &ResolvedPlacement) -> &NodeGroupPlacement {
    match resolved {
        ResolvedPlacement::Placed(placement) => placement,
        other => panic!("expected a resolved placement, got {other:?}"),
    }
}

fn enabled_resolver(pool: &PgPool) -> NodeGroupPlacementResolver {
    NodeGroupPlacementResolver::new(pool.clone(), true)
}

async fn fixture() -> TestResult<Option<pplx_space_db_test_fixtures::TestDb>> {
    pplx_space_db_test_fixtures::try_pg_test_db().await
}

/// The gate is a gate, not a filter applied after the read: a disabled
/// resolver resolves against a pool that cannot connect, so a query would
/// surface as an error rather than as `Disabled`.
#[tokio::test(flavor = "current_thread")]
async fn a_closed_gate_issues_no_query() {
    let resolver = NodeGroupPlacementResolver::new(undialed_pool(), false);
    let resolved = resolver
        .resolve(
            ResolvePath::Create,
            PlacementSubject::Sandbox(OrgId::from_uuid(ORG_A)),
            "seed",
        )
        .await
        .expect("a closed gate cannot fail");
    assert_eq!(resolved, ResolvedPlacement::Disabled);
    assert!(resolved.placement().is_none());
}

/// **The regression this issue exists to prevent.** A generic template is
/// compatible with *every* generic group, not just the `is_default` one, and
/// nothing downstream would catch the narrower query: `NodeGroup::compatible`
/// never reads `is_default`, so a set containing only the default group
/// satisfies the predicate and every existing test.
///
/// The fixture makes the wrong answer reachable — there is a default generic
/// group *and* a non-default one — so the assertion fails if the query ever
/// narrows to the default.
#[tokio::test(flavor = "current_thread")]
async fn generic_template_admits_every_generic_group_not_just_the_default() -> TestResult {
    let Some(fixture) = fixture().await? else {
        return Ok(());
    };
    let pool = &fixture.pool;
    insert_template(pool, "seed", "generic", None).await?;
    insert_group(
        pool,
        "workers",
        None,
        NodeGroupTemplatePolicy::Default,
        true,
    )
    .await?;
    insert_group(
        pool,
        "workers-2",
        None,
        NodeGroupTemplatePolicy::Default,
        false,
    )
    .await?;
    // Another org's dedicated capacity, which must not appear.
    insert_group(
        pool,
        "org-b-1",
        Some(OrgId::from_uuid(ORG_B)),
        NodeGroupTemplatePolicy::Default,
        false,
    )
    .await?;

    let resolved = enabled_resolver(pool)
        .resolve(
            ResolvePath::Create,
            PlacementSubject::Sandbox(OrgId::from_uuid(ORG_A)),
            "seed",
        )
        .await?;
    let placement = placed(&resolved);
    assert_eq!(candidate_names(placement), vec!["workers", "workers-2"]);
    assert!(
        !placement.targets_dedicated_capacity(),
        "org A owns no dedicated group here"
    );
    Ok(())
}

/// An org that owns dedicated capacity gets `{all generic} ∪ {its own
/// compatible dedicated}` for a generic template, with only its own groups in
/// the dedicated subset the preferred term biases toward.
#[tokio::test(flavor = "current_thread")]
async fn caller_dedicated_groups_join_the_generic_set() -> TestResult {
    let Some(fixture) = fixture().await? else {
        return Ok(());
    };
    let pool = &fixture.pool;
    insert_template(pool, "seed", "generic", None).await?;
    insert_group(
        pool,
        "workers",
        None,
        NodeGroupTemplatePolicy::Default,
        true,
    )
    .await?;
    insert_group(
        pool,
        "org-a-1",
        Some(OrgId::from_uuid(ORG_A)),
        NodeGroupTemplatePolicy::Default,
        false,
    )
    .await?;
    insert_group(
        pool,
        "org-b-1",
        Some(OrgId::from_uuid(ORG_B)),
        NodeGroupTemplatePolicy::Default,
        false,
    )
    .await?;

    let resolved = enabled_resolver(pool)
        .resolve(
            ResolvePath::Create,
            PlacementSubject::Sandbox(OrgId::from_uuid(ORG_A)),
            "seed",
        )
        .await?;
    let placement = placed(&resolved);
    assert_eq!(candidate_names(placement), vec!["org-a-1", "workers"]);
    assert!(placement.targets_dedicated_capacity());
    assert!(placement.admits_group("org-a-1"));
    assert!(
        !placement.admits_group("org-b-1"),
        "another org's dedicated group is never a candidate"
    );
    Ok(())
}

/// An org-specific template's set is exactly its own org's compatible
/// dedicated groups — the shared pool is excluded even though it exists and
/// admits every generic template.
#[tokio::test(flavor = "current_thread")]
async fn org_specific_template_never_admits_shared_capacity() -> TestResult {
    let Some(fixture) = fixture().await? else {
        return Ok(());
    };
    let pool = &fixture.pool;
    insert_template(
        pool,
        "acme-tools",
        "org_specific",
        Some(OrgId::from_uuid(ORG_A)),
    )
    .await?;
    insert_group(
        pool,
        "workers",
        None,
        NodeGroupTemplatePolicy::Default,
        true,
    )
    .await?;
    insert_group(
        pool,
        "org-a-1",
        Some(OrgId::from_uuid(ORG_A)),
        NodeGroupTemplatePolicy::Default,
        false,
    )
    .await?;

    let resolved = enabled_resolver(pool)
        .resolve(
            ResolvePath::Create,
            PlacementSubject::Sandbox(OrgId::from_uuid(ORG_A)),
            "acme-tools",
        )
        .await?;
    let placement = placed(&resolved);
    assert_eq!(candidate_names(placement), vec!["org-a-1"]);
    assert!(placement.targets_dedicated_capacity());
    Ok(())
}

/// A `restricted` dedicated group admits only the templates in its allowlist,
/// which is the one arm of the predicate that reads the compat rows — so this
/// also covers the aggregated allowlist column decoding.
#[tokio::test(flavor = "current_thread")]
async fn restricted_group_admits_only_its_allowlisted_templates() -> TestResult {
    let Some(fixture) = fixture().await? else {
        return Ok(());
    };
    let pool = &fixture.pool;
    insert_template(
        pool,
        "acme-tools",
        "org_specific",
        Some(OrgId::from_uuid(ORG_A)),
    )
    .await?;
    insert_template(
        pool,
        "acme-other",
        "org_specific",
        Some(OrgId::from_uuid(ORG_A)),
    )
    .await?;
    let restricted = insert_group(
        pool,
        "org-a-restricted",
        Some(OrgId::from_uuid(ORG_A)),
        NodeGroupTemplatePolicy::Restricted,
        false,
    )
    .await?;
    allow_template(pool, restricted, "acme-tools").await?;
    let resolver = enabled_resolver(pool);
    let org_a = PlacementSubject::Sandbox(OrgId::from_uuid(ORG_A));

    let allowed = resolver
        .resolve(ResolvePath::Create, org_a, "acme-tools")
        .await?;
    assert_eq!(
        candidate_names(placed(&allowed)),
        vec!["org-a-restricted"],
        "the allowlisted template lands on the restricted group"
    );

    let refused = resolver
        .resolve(ResolvePath::Create, org_a, "acme-other")
        .await
        .expect_err("a template outside the allowlist has no compatible group");
    assert!(
        matches!(
            refused,
            NodeGroupPlacementResolveError::NoCompatibleNodeGroup { .. }
        ),
        "expected a fail-closed empty set, got {refused:?}"
    );
    Ok(())
}

/// The decision that makes the gate safe to flip. On a cluster with **no
/// generic group** the two visibilities diverge: a generic template reports the
/// feature unprovisioned and renders nothing, while an org-specific one fails
/// closed. Asserted over an empty table *and* over a table holding only
/// dedicated capacity, because the rule is "no generic group admits it", not
/// "the table is empty".
#[tokio::test(flavor = "current_thread")]
async fn no_generic_group_is_unprovisioned_for_generic_and_closed_for_org_specific() -> TestResult {
    let Some(fixture) = fixture().await? else {
        return Ok(());
    };
    let pool = &fixture.pool;
    insert_template(pool, "seed", "generic", None).await?;
    insert_template(
        pool,
        "acme-tools",
        "org_specific",
        Some(OrgId::from_uuid(ORG_A)),
    )
    .await?;
    let resolver = enabled_resolver(pool);
    let org_a = PlacementSubject::Sandbox(OrgId::from_uuid(ORG_A));

    // An unprovisioned cluster has an empty node-group table.
    assert_eq!(
        resolver.resolve(ResolvePath::Create, org_a, "seed").await?,
        ResolvedPlacement::Unprovisioned,
        "a generic create on an unprovisioned cluster must render no constraints"
    );
    let closed = resolver
        .resolve(ResolvePath::Create, org_a, "acme-tools")
        .await
        .expect_err("an org-specific template has no shared fallback");
    assert!(matches!(
        closed,
        NodeGroupPlacementResolveError::NoCompatibleNodeGroup { .. }
    ));

    // Dedicated capacity alone does not provision the shared pool.
    insert_group(
        pool,
        "org-b-1",
        Some(OrgId::from_uuid(ORG_B)),
        NodeGroupTemplatePolicy::Default,
        false,
    )
    .await?;
    assert_eq!(
        resolver.resolve(ResolvePath::Create, org_a, "seed").await?,
        ResolvedPlacement::Unprovisioned,
        "a non-empty table with no generic group is still unprovisioned shared capacity"
    );

    // And one generic group is all it takes to start enforcing.
    insert_group(
        pool,
        "workers",
        None,
        NodeGroupTemplatePolicy::Default,
        true,
    )
    .await?;
    assert_eq!(
        candidate_names(placed(
            &resolver.resolve(ResolvePath::Create, org_a, "seed").await?
        )),
        vec!["workers"],
    );
    Ok(())
}

/// A template name with no row at all is a different condition from a cluster
/// with no groups, and the resolver must not confuse the two: it cannot know
/// the visibility, so it fails closed instead of assuming `generic`.
#[tokio::test(flavor = "current_thread")]
async fn absent_template_row_fails_closed() -> TestResult {
    let Some(fixture) = fixture().await? else {
        return Ok(());
    };
    let pool = &fixture.pool;
    insert_group(
        pool,
        "workers",
        None,
        NodeGroupTemplatePolicy::Default,
        true,
    )
    .await?;

    let error = enabled_resolver(pool)
        .resolve(
            ResolvePath::Restore,
            PlacementSubject::Sandbox(OrgId::from_uuid(ORG_A)),
            "reaped-template",
        )
        .await
        .expect_err("no templates row means no derivable candidate set");
    assert!(
        matches!(error, NodeGroupPlacementResolveError::TemplateAbsent { .. }),
        "expected TemplateAbsent, got {error:?}"
    );
    Ok(())
}

/// `desired_state` is a publication concern, not a compatibility one: a
/// suspended sandbox whose template is being deleted still has to resolve a set
/// to restore into. The create path's own authorization gate is what rejects an
/// unpublished template, and it runs earlier.
#[tokio::test(flavor = "current_thread")]
async fn a_template_pending_deletion_still_resolves() -> TestResult {
    let Some(fixture) = fixture().await? else {
        return Ok(());
    };
    let pool = &fixture.pool;
    insert_template(pool, "seed", "generic", None).await?;
    sqlx::query("UPDATE templates SET desired_state = 'deleted' WHERE name = 'seed'")
        .execute(pool)
        .await?;
    insert_group(
        pool,
        "workers",
        None,
        NodeGroupTemplatePolicy::Default,
        true,
    )
    .await?;

    let resolved = enabled_resolver(pool)
        .resolve(
            ResolvePath::Restore,
            PlacementSubject::Sandbox(OrgId::from_uuid(ORG_A)),
            "seed",
        )
        .await?;
    assert_eq!(candidate_names(placed(&resolved)), vec!["workers"]);
    Ok(())
}

/// A warm-pool slot carries no org, so its set is the generic groups and never
/// a dedicated target — the pod builder renders an unsatisfiable boundary for
/// an org-less pod that claims dedicated capacity, so this is a correctness
/// requirement, not a preference. An org-specific pool template (which the
/// design forbids) fails closed instead of parking private rootfs on shared
/// capacity.
#[tokio::test(flavor = "current_thread")]
async fn warm_pool_resolves_generic_capacity_only() -> TestResult {
    let Some(fixture) = fixture().await? else {
        return Ok(());
    };
    let pool = &fixture.pool;
    insert_template(pool, "seed", "generic", None).await?;
    insert_template(
        pool,
        "acme-tools",
        "org_specific",
        Some(OrgId::from_uuid(ORG_A)),
    )
    .await?;
    insert_group(
        pool,
        "workers",
        None,
        NodeGroupTemplatePolicy::Default,
        true,
    )
    .await?;
    insert_group(
        pool,
        "org-a-1",
        Some(OrgId::from_uuid(ORG_A)),
        NodeGroupTemplatePolicy::Default,
        false,
    )
    .await?;
    let resolver = enabled_resolver(pool);

    let resolved = resolver
        .resolve(ResolvePath::WarmPool, PlacementSubject::WarmPool, "seed")
        .await?;
    let placement = placed(&resolved);
    assert_eq!(candidate_names(placement), vec!["workers"]);
    assert!(
        !placement.targets_dedicated_capacity(),
        "a pooled pod must never target dedicated capacity"
    );

    let refused = resolver
        .resolve(
            ResolvePath::WarmPool,
            PlacementSubject::WarmPool,
            "acme-tools",
        )
        .await
        .expect_err("an org-specific pool template has no org to be compatible with");
    assert!(matches!(
        refused,
        NodeGroupPlacementResolveError::NoCompatibleNodeGroup { .. }
    ));
    Ok(())
}

// ── local-only restore pre-flight ───────────────────────────────────────────

fn generic_group(name: &str) -> NodeGroup {
    NodeGroup {
        id: NodeGroupId::new(),
        name: NodeGroupName::parse(name).expect("test group name is a valid slug"),
        policy: NodeGroupPolicy::Generic { is_default: false },
        template_allowlist: BTreeSet::new(),
    }
}

fn shared_placement() -> NodeGroupPlacement {
    let template = TemplateCompatSpec {
        name: TemplateName::parse("seed").expect("valid template name"),
        org_id: None,
        visibility: TemplateVisibility::Generic,
    };
    NodeGroupPlacement::for_caller(
        Some(OrgId::from_uuid(ORG_A)),
        &template,
        &[generic_group("workers"), generic_group("workers-2")],
    )
    .expect("two generic groups admit a generic template")
}

fn node_labeled(group: Option<&str>) -> Node {
    Node {
        metadata: ObjectMeta {
            name: Some("ip-10-0-0-1.ec2.internal".to_string()),
            labels: group.map(|group| {
                std::collections::BTreeMap::from([(
                    SANDBOX_NODE_GROUP_LABEL.to_string(),
                    group.to_string(),
                )])
            }),
            ..Default::default()
        },
        ..Default::default()
    }
}

/// A local-only restore's `spec.nodeName` pin skips kube-scheduler, so the
/// candidate set is only evaluated by kubelet admission — terminally. The
/// pre-flight turns a node outside the set into an actionable error before a
/// pod exists.
///
/// The property, stated without reference to the code: a sandbox whose only
/// copy sits on a node the boundary now excludes must be told it needs
/// migrating, and must not be told to try again on a node that has none of its
/// bytes.
#[test]
fn local_restore_pin_is_refused_when_the_source_node_left_the_set() {
    let placement = shared_placement();
    let sandbox_id = SandboxId::new();

    let in_set = check_local_restore_source(
        &placement,
        sandbox_id,
        "ip-10-0-0-1.ec2.internal",
        RestoreSourceNode::Resolved(&node_labeled(Some("workers-2"))),
    );
    assert!(in_set.is_ok(), "a labeled, in-set source keeps its pin");

    for label in [None, Some("org-b-1")] {
        let node = node_labeled(label);
        let error = check_local_restore_source(
            &placement,
            sandbox_id,
            "ip-10-0-0-1.ec2.internal",
            RestoreSourceNode::Resolved(&node),
        )
        .expect_err("a source outside the candidate set must not be pinned to");
        assert_eq!(error.node_name, "ip-10-0-0-1.ec2.internal");
        // The clause has to read correctly in both shapes: a foreign label has
        // a group to name, an absent one does not and must not be given an
        // invented name.
        let expected = match label {
            Some(group) => format!("its node group is {group}"),
            None => "it carries no node-group label".to_string(),
        };
        assert_eq!(
            error.source_placement, expected,
            "the error says why the node is outside the set, without inventing a group name"
        );
        assert!(
            !error.to_string().contains("<unlabeled>"),
            "no sentinel may reach the caller: {error}"
        );
    }
}

/// Not knowing is not the same as knowing the node is outside the set. A cold
/// reflector is a control-plane state, and a vanished node fails the restore at
/// binding with its pre-existing shape; neither may convert into a placement
/// failure. Kubelet admission stays the backstop.
#[test]
fn an_unreadable_source_node_keeps_its_pin() {
    let placement = shared_placement();
    let sandbox_id = SandboxId::new();
    for source in [RestoreSourceNode::CacheCold, RestoreSourceNode::Absent] {
        assert!(
            check_local_restore_source(&placement, sandbox_id, "ip-10-0-0-1.ec2.internal", source,)
                .is_ok(),
            "{source:?} must not fail the restore",
        );
    }
}
