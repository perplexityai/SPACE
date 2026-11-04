use pplx_space_common_core::types::NodeGroupId;
use pplx_space_common_core::types::{NodeId, OrgId};

use pplx_space_common_core::sql;
use pplx_space_common_core::types::{NodeGroupName, NodeGroupTemplatePolicy};
use sqlx::PgPool;
use uuid::Uuid;

use super::*;

use crate::routes::admin::test_fixtures::{TestResult, postgres_fixture};

/// Orgs seeded by the baseline migration's fixture data.
const ORG_A: Uuid = Uuid::from_u128(0x1);
const ORG_B: Uuid = Uuid::from_u128(0x3);
const SEEDED_USER: Uuid = Uuid::from_u128(0x10);

fn group_name(value: &str) -> NodeGroupName {
    NodeGroupName::parse(value).expect("test group name is a valid slug")
}

async fn insert_node_in_group(
    pool: &PgPool,
    group_id: impl Into<Option<NodeGroupId>>,
) -> TestResult<NodeId> {
    insert_node_with_role(pool, group_id, "worker").await
}

async fn insert_node_with_role(
    pool: &PgPool,
    group_id: impl Into<Option<NodeGroupId>>,
    role: &str,
) -> TestResult<NodeId> {
    let node_id = NodeId::new();
    sqlx::query(
        "INSERT INTO nodes (id, address, role, managed_by, state, node_group_id)
         VALUES ($1, '10.0.0.1'::INET, $2, 'k8s', 'ready', $3)",
    )
    .bind(node_id)
    .bind(role)
    .bind(group_id.into())
    .execute(pool)
    .await?;
    Ok(node_id)
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

async fn insert_sandbox(
    pool: &PgPool,
    lifecycle: &str,
    active_node_id: Option<NodeId>,
    target_node_id: Option<NodeId>,
) -> TestResult {
    sqlx::query(
        "INSERT INTO sandboxes (
             id, user_id, org_id, lifecycle, placement, managed_by,
             active_node_id, target_node_id, placement_changed_at,
             network_policy_generation
         )
         VALUES ($1, $2, $3, $4, 'on_node', 'k8s', $5, $6, NOW() - INTERVAL '1 hour', 1)",
    )
    .bind(Uuid::now_v7())
    .bind(SEEDED_USER)
    .bind(ORG_A)
    .bind(lifecycle)
    .bind(active_node_id)
    .bind(target_node_id)
    .execute(pool)
    .await?;
    Ok(())
}

/// Unique name + singleton default are DB-enforced, surfaced through the
/// constraint names the create handler maps to 409s; the default move
/// helper relocates the flag transactionally.
#[tokio::test(flavor = "current_thread")]
async fn create_conflicts_and_default_move() -> TestResult {
    let Some(fixture) = postgres_fixture().await? else {
        return Ok(());
    };
    let pool = &fixture.pool;

    let shared = insert_node_group(
        pool,
        &group_name("workers-shared"),
        None,
        NodeGroupTemplatePolicy::Default,
        true,
    )
    .await?;

    let dup_name = insert_node_group(
        pool,
        &group_name("workers-shared"),
        None,
        NodeGroupTemplatePolicy::Default,
        false,
    )
    .await
    .unwrap_err();
    assert_eq!(
        violated_constraint(&dup_name).as_deref(),
        Some("node_groups_name_key")
    );

    let second_default = insert_node_group(
        pool,
        &group_name("workers-second"),
        None,
        NodeGroupTemplatePolicy::Default,
        true,
    )
    .await
    .unwrap_err();
    assert_eq!(
        violated_constraint(&second_default).as_deref(),
        Some("idx_node_groups_single_default")
    );

    // Move the default onto a fresh generic group.
    let other = insert_node_group(
        pool,
        &group_name("workers-other"),
        None,
        NodeGroupTemplatePolicy::Default,
        false,
    )
    .await?;
    let mut tx = pool.begin().await?;
    assert_eq!(
        lock_current_default_for_update(&mut tx).await?,
        Some(shared)
    );
    move_default_in_tx(&mut tx, other).await?;
    tx.commit().await?;

    let store = AdminNodeGroupStore::new(pool.clone());
    assert!(store.fetch(other).await?.expect("row").is_default);
    assert!(!store.fetch(shared).await?.expect("row").is_default);
    Ok(())
}

/// The schema backstops the handler's pre-validation: a restricted
/// generic group and an org-owned default are both unrepresentable.
#[tokio::test(flavor = "current_thread")]
async fn check_constraints_backstop_handler_validation() -> TestResult {
    let Some(fixture) = postgres_fixture().await? else {
        return Ok(());
    };
    let pool = &fixture.pool;

    let restricted_generic = insert_node_group(
        pool,
        &group_name("bad-generic"),
        None,
        NodeGroupTemplatePolicy::Restricted,
        false,
    )
    .await
    .unwrap_err();
    assert_eq!(
        violated_constraint(&restricted_generic).as_deref(),
        Some("node_groups_restricted_requires_org")
    );

    let org_default = insert_node_group(
        pool,
        &group_name("bad-default"),
        Some(OrgId::from_uuid(ORG_A)),
        NodeGroupTemplatePolicy::Default,
        true,
    )
    .await
    .unwrap_err();
    assert_eq!(
        violated_constraint(&org_default).as_deref(),
        Some("node_groups_default_is_generic")
    );
    Ok(())
}

/// The `:assign-org` residue guard counts live placements and local-only
/// suspended pins on the group's nodes; a durable suspended sandbox does
/// not block.
#[tokio::test(flavor = "current_thread")]
async fn group_occupancy_counts_active_and_pinned() -> TestResult {
    let Some(fixture) = postgres_fixture().await? else {
        return Ok(());
    };
    let pool = &fixture.pool;
    let group = insert_node_group(
        pool,
        &group_name("org-a-dedicated"),
        Some(OrgId::from_uuid(ORG_A)),
        NodeGroupTemplatePolicy::Default,
        false,
    )
    .await?;
    let node = insert_node_in_group(pool, group).await?;

    let mut tx = pool.begin().await?;
    let empty = group_occupancy_in_tx(&mut tx, group).await?;
    tx.commit().await?;
    assert_eq!((empty.active, empty.local_snapshots), (0, 0));

    insert_sandbox(pool, "started", Some(node), None).await?;
    insert_sandbox(pool, "suspended", None, Some(node)).await?;

    let mut tx = pool.begin().await?;
    let occupied = group_occupancy_in_tx(&mut tx, group).await?;
    tx.commit().await?;
    assert_eq!(occupied.active, 1, "live placement counts");
    assert_eq!(
        occupied.local_snapshots, 1,
        "local-only suspended pin counts"
    );
    Ok(())
}

/// A non-evacuation restore in flight pins its sandbox to a group node via
/// `target_node_id` while `active_node_id` is NULL. That still occupies the
/// group, so `:assign-org` must not reassign out from under it — the
/// residue guard counts it (mirroring the node-drain blocker). An
/// evacuation restore (reads from durable S3) does not block.
#[tokio::test(flavor = "current_thread")]
async fn group_occupancy_counts_restoring_target_pins() -> TestResult {
    let Some(fixture) = postgres_fixture().await? else {
        return Ok(());
    };
    let pool = &fixture.pool;
    let group = insert_node_group(
        pool,
        &group_name("org-a-dedicated"),
        Some(OrgId::from_uuid(ORG_A)),
        NodeGroupTemplatePolicy::Default,
        false,
    )
    .await?;
    let node = insert_node_in_group(pool, group).await?;

    // Non-evacuation restore: active_node_id NULL, target_node_id pins the
    // group node. Must count as active occupancy.
    insert_sandbox(pool, "restoring", None, Some(node)).await?;
    let mut tx = pool.begin().await?;
    let occ = group_occupancy_in_tx(&mut tx, group).await?;
    tx.commit().await?;
    assert_eq!(
        occ.active, 1,
        "non-evacuation restoring target-pin occupies the group"
    );

    // An evacuation restore (marker set) reads from S3 and does not pin the
    // group for reassignment purposes — matches the drain blocker.
    sqlx::query(
        "UPDATE sandboxes SET evacuation_restore_target = 'started'
          WHERE lifecycle = 'restoring'",
    )
    .execute(pool)
    .await?;
    let mut tx = pool.begin().await?;
    let occ = group_occupancy_in_tx(&mut tx, group).await?;
    tx.commit().await?;
    assert_eq!(
        occ.active, 0,
        "evacuation restore does not block reassignment"
    );
    Ok(())
}

/// With no incumbent default (the pre-seed window), two concurrent
/// `:set-default` moves have no row to serialize on; the single-default unique
/// index is the remaining backstop and the loser's violation must be
/// identifiable by constraint name so the handler can map it to a 409
/// rather than a 500.
#[tokio::test(flavor = "current_thread")]
async fn concurrent_set_default_loser_hits_identifiable_conflict() -> TestResult {
    let Some(fixture) = postgres_fixture().await? else {
        return Ok(());
    };
    let pool = &fixture.pool;
    let group_a = insert_node_group(
        pool,
        &group_name("workers-a"),
        None,
        NodeGroupTemplatePolicy::Default,
        false,
    )
    .await?;
    let group_b = insert_node_group(
        pool,
        &group_name("workers-b"),
        None,
        NodeGroupTemplatePolicy::Default,
        false,
    )
    .await?;

    // tx1 sets A default but hasn't committed; tx2's move scans no
    // committed default (nothing to clear), then blocks on the single-default
    // unique index until tx1 commits — and loses with a violation.
    let mut tx1 = pool.begin().await?;
    assert_eq!(lock_current_default_for_update(&mut tx1).await?, None);
    move_default_in_tx(&mut tx1, group_a).await?;

    let loser_pool = pool.clone();
    let loser = tokio::spawn(async move {
        let mut tx2 = loser_pool.begin().await?;
        assert_eq!(lock_current_default_for_update(&mut tx2).await?, None);
        move_default_in_tx(&mut tx2, group_b).await?;
        tx2.commit().await
    });
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    tx1.commit().await?;

    let err = loser.await?.expect_err("the concurrent move must lose");
    assert_eq!(
        violated_constraint(&err).as_deref(),
        Some("idx_node_groups_single_default"),
        "the loser's error must be mappable to a retryable conflict"
    );
    Ok(())
}

/// An in-flight suspend-snapshot upload sourced *from* a group node must
/// count as group residue even when the sandbox's `target_node_id` has
/// already drifted off-group — otherwise `:assign-org` could reassign
/// the group while one of its nodes still holds the only copy, the same
/// case the per-node `:terminate` guard blocks.
#[tokio::test(flavor = "current_thread")]
async fn group_occupancy_counts_inflight_uploads_from_group_nodes() -> TestResult {
    let Some(fixture) = postgres_fixture().await? else {
        return Ok(());
    };
    let pool = &fixture.pool;
    let group = insert_node_group(
        pool,
        &group_name("org-a-dedicated"),
        Some(OrgId::from_uuid(ORG_A)),
        NodeGroupTemplatePolicy::Default,
        false,
    )
    .await?;
    let group_node = insert_node_in_group(pool, group).await?;
    let off_group_node = insert_node_in_group(pool, None).await?;

    // Suspended sandbox pinned to an off-group node, but its (fresh)
    // full-snapshot upload is still in flight from the group node.
    let sandbox = Uuid::now_v7();
    sqlx::query(
        "INSERT INTO sandboxes (
             id, user_id, org_id, lifecycle, placement, managed_by,
             target_node_id, placement_changed_at, network_policy_generation
         )
         VALUES ($1, $2, $3, 'suspended', 'unplaced', 'k8s', $4,
                 NOW() - INTERVAL '1 hour', 1)",
    )
    .bind(sandbox)
    .bind(SEEDED_USER)
    .bind(ORG_A)
    .bind(off_group_node)
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO snapshots (id, sandbox_id, type, state, source_node_id, timestamp)
         VALUES ($1, $2, 'full', 'creating', $3, NOW())",
    )
    .bind(Uuid::now_v7())
    .bind(sandbox)
    .bind(group_node)
    .execute(pool)
    .await?;

    let mut tx = pool.begin().await?;
    let occupancy = group_occupancy_in_tx(&mut tx, group).await?;
    tx.commit().await?;
    assert_eq!(
        occupancy.local_snapshots, 1,
        "in-flight upload from a group node is group residue"
    );
    Ok(())
}

/// The cached-rootfs guard strands only org-specific templates whose
/// owner differs from the new assignment; generic rootfs never blocks.
#[tokio::test(flavor = "current_thread")]
async fn stranded_cached_templates_respects_owner_and_visibility() -> TestResult {
    let Some(fixture) = postgres_fixture().await? else {
        return Ok(());
    };
    let pool = &fixture.pool;
    let group = insert_node_group(
        pool,
        &group_name("org-a-dedicated"),
        Some(OrgId::from_uuid(ORG_A)),
        NodeGroupTemplatePolicy::Default,
        false,
    )
    .await?;
    let node = insert_node_in_group(pool, group).await?;
    insert_template(
        pool,
        "a-secret",
        "org_specific",
        Some(OrgId::from_uuid(ORG_A)),
    )
    .await?;
    insert_template(pool, "base-generic", "generic", None).await?;
    for template in ["a-secret", "base-generic"] {
        sqlx::query("INSERT INTO node_template_inventory (node_id, template_name) VALUES ($1, $2)")
            .bind(node)
            .bind(template)
            .execute(pool)
            .await?;
    }

    let mut tx = pool.begin().await?;
    // Releasing to generic strands A's org-specific rootfs.
    assert_eq!(
        stranded_cached_templates_in_tx(&mut tx, group, None).await?,
        1
    );
    // Re-assigning to the same owner strands nothing.
    assert_eq!(
        stranded_cached_templates_in_tx(&mut tx, group, Some(OrgId::from_uuid(ORG_A))).await?,
        0
    );
    // Handing the group to another org strands A's rootfs.
    assert_eq!(
        stranded_cached_templates_in_tx(&mut tx, group, Some(OrgId::from_uuid(ORG_B))).await?,
        1
    );
    tx.commit().await?;
    Ok(())
}

/// Releasing (or re-assigning) a restricted group resets the policy and
/// clears the allowlist in the same transaction.
#[tokio::test(flavor = "current_thread")]
async fn releasing_restricted_group_resets_policy_and_clears_compat() -> TestResult {
    let Some(fixture) = postgres_fixture().await? else {
        return Ok(());
    };
    let pool = &fixture.pool;
    let group = insert_node_group(
        pool,
        &group_name("org-a-restricted"),
        Some(OrgId::from_uuid(ORG_A)),
        NodeGroupTemplatePolicy::Restricted,
        false,
    )
    .await?;
    insert_template(pool, "base-generic", "generic", None).await?;
    let mut tx = pool.begin().await?;
    replace_compat_in_tx(&mut tx, group, &["base-generic".to_string()]).await?;
    tx.commit().await?;

    let mut tx = pool.begin().await?;
    update_group_org_in_tx(&mut tx, group, None, true).await?;
    tx.commit().await?;

    let store = AdminNodeGroupStore::new(pool.clone());
    let row = store.fetch(group).await?.expect("row");
    assert_eq!(row.org_id, None);
    assert_eq!(row.template_policy, NodeGroupTemplatePolicy::Default);
    assert!(
        !store
            .compat_templates_for(&[group])
            .await?
            .contains_key(&group),
        "allowlist must be cleared with the release"
    );
    Ok(())
}

/// Full-replace compat semantics, and the policy switch back to
/// `default` clears the allowlist so it cannot silently resurrect.
#[tokio::test(flavor = "current_thread")]
async fn compat_replace_and_policy_switch_clear() -> TestResult {
    let Some(fixture) = postgres_fixture().await? else {
        return Ok(());
    };
    let pool = &fixture.pool;
    let group = insert_node_group(
        pool,
        &group_name("org-a-restricted"),
        Some(OrgId::from_uuid(ORG_A)),
        NodeGroupTemplatePolicy::Restricted,
        false,
    )
    .await?;
    insert_template(pool, "base-generic", "generic", None).await?;
    insert_template(
        pool,
        "a-secret",
        "org_specific",
        Some(OrgId::from_uuid(ORG_A)),
    )
    .await?;

    let mut tx = pool.begin().await?;
    let known = templates_by_names_in_tx(
        &mut tx,
        &["a-secret".to_string(), "base-generic".to_string()],
    )
    .await?;
    assert_eq!(known.len(), 2);
    replace_compat_in_tx(
        &mut tx,
        group,
        &["a-secret".to_string(), "base-generic".to_string()],
    )
    .await?;
    tx.commit().await?;

    // Replace shrinks to one row.
    let mut tx = pool.begin().await?;
    replace_compat_in_tx(&mut tx, group, &["a-secret".to_string()]).await?;
    tx.commit().await?;
    let store = AdminNodeGroupStore::new(pool.clone());
    assert_eq!(
        store
            .compat_templates_for(&[group])
            .await?
            .remove(&group)
            .unwrap_or_default(),
        vec!["a-secret".to_string()]
    );

    // Switching to `default` clears the rows.
    let mut tx = pool.begin().await?;
    set_template_policy_in_tx(&mut tx, group, NodeGroupTemplatePolicy::Default).await?;
    tx.commit().await?;
    assert!(
        !store
            .compat_templates_for(&[group])
            .await?
            .contains_key(&group)
    );
    Ok(())
}

/// A `PUT .../compat` validation must serialize against a concurrent
/// narrowing visibility flip on the same template: without row locks on
/// the validation read, the flip can commit between validate and insert,
/// leaving another org's org-specific template in the allowlist — the
/// hard cross-org rule violated with no database constraint to catch it.
#[tokio::test(flavor = "current_thread")]
async fn compat_validation_serializes_against_concurrent_narrowing() -> TestResult {
    use pplx_space_common_core::types::TemplateVisibility;

    use crate::routes::admin::template_store;

    let Some(fixture) = postgres_fixture().await? else {
        return Ok(());
    };
    let pool = &fixture.pool;
    let group = insert_node_group(
        pool,
        &group_name("org-a-restricted"),
        Some(OrgId::from_uuid(ORG_A)),
        NodeGroupTemplatePolicy::Restricted,
        false,
    )
    .await?;
    insert_template(pool, "victim", "generic", None).await?;

    // Compat transaction: lock the group, validate the allowlist. At
    // validation time the template is still generic, so it passes.
    let mut compat_tx = pool.begin().await?;
    lock_group_for_update(&mut compat_tx, group)
        .await?
        .expect("group row");
    let known = templates_by_names_in_tx(&mut compat_tx, &["victim".to_string()]).await?;
    assert!(
        known
            .iter()
            .all(|row| row.visibility == TemplateVisibility::Generic),
        "validation observes the pre-flip generic state"
    );

    // Concurrent narrowing flip to org B — the production sequence:
    // lock the template row, write visibility+owner, prune cross-org
    // compat rows, commit. With the validation read holding FOR SHARE
    // this blocks until the compat transaction commits, and the prune
    // then removes the freshly-inserted row; without it, the flip
    // commits immediately and the compat insert lands after the prune.
    let flip_pool = pool.clone();
    let flip = tokio::spawn(async move {
        let mut tx = flip_pool.begin().await?;
        template_store::lock_template_for_update(&mut tx, "victim").await?;
        template_store::update_template_visibility_in_tx(
            &mut tx,
            "victim",
            TemplateVisibility::OrgSpecific,
            Some(OrgId::from_uuid(ORG_B)),
        )
        .await?;
        template_store::prune_cross_org_compat_in_tx(&mut tx, "victim", OrgId::from_uuid(ORG_B))
            .await?;
        tx.commit().await?;
        Ok::<_, sqlx::Error>(())
    });
    // Let the flip task reach (and pre-fix: pass) its lock acquisition.
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    replace_compat_in_tx(&mut compat_tx, group, &["victim".to_string()]).await?;
    compat_tx.commit().await?;
    flip.await??;

    // Invariant: no group's allowlist may name another org's
    // org-specific template, regardless of interleaving.
    let cross_org: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)::BIGINT
           FROM node_group_template_compat c
           JOIN node_groups g ON g.id = c.node_group_id
           JOIN templates t ON t.name = c.template_name
          WHERE t.visibility = 'org_specific'
            AND t.org_id IS DISTINCT FROM g.org_id",
    )
    .fetch_one(pool)
    .await?;
    assert_eq!(
        cross_org, 0,
        "allowlist must never retain another org's org-specific template"
    );
    Ok(())
}

/// `nodes_count` must include NULL-membership nodes for the singleton
/// default group: unlabeled (or unknown-label) nodes mirror as NULL,
/// which everywhere else resolves to the default generic group. Other
/// groups count only explicit membership.
#[tokio::test(flavor = "current_thread")]
async fn default_group_nodes_count_includes_null_membership() -> TestResult {
    let Some(fixture) = postgres_fixture().await? else {
        return Ok(());
    };
    let pool = &fixture.pool;
    let default_group = insert_node_group(
        pool,
        &group_name("workers-shared"),
        None,
        NodeGroupTemplatePolicy::Default,
        true,
    )
    .await?;
    let dedicated = insert_node_group(
        pool,
        &group_name("org-a-dedicated"),
        Some(OrgId::from_uuid(ORG_A)),
        NodeGroupTemplatePolicy::Default,
        false,
    )
    .await?;
    insert_node_in_group(pool, default_group).await?;
    insert_node_in_group(pool, None).await?; // unlabeled worker — resolves to default
    insert_node_in_group(pool, dedicated).await?;
    // Non-worker rows always sit at NULL (the mirror stamps workers
    // only); they are not members of the default *worker* pool.
    insert_node_with_role(pool, None, "builder").await?;
    insert_node_with_role(pool, None, "cluster").await?;

    let store = AdminNodeGroupStore::new(pool.clone());
    assert_eq!(
        store.fetch(default_group).await?.expect("row").nodes_count,
        2,
        "default group counts explicit + NULL worker membership, not builder/cluster rows"
    );
    assert_eq!(
        store.fetch(dedicated).await?.expect("row").nodes_count,
        1,
        "non-default groups count only explicit membership"
    );
    Ok(())
}

/// `nodes_count` in the row select reflects mirrored membership, and the
/// `nodes.node_group_id` FK blocks deleting a group that still has
/// nodes.
#[tokio::test(flavor = "current_thread")]
async fn delete_blocked_by_node_fk() -> TestResult {
    let Some(fixture) = postgres_fixture().await? else {
        return Ok(());
    };
    let pool = &fixture.pool;
    let group = insert_node_group(
        pool,
        &group_name("org-a-dedicated"),
        Some(OrgId::from_uuid(ORG_A)),
        NodeGroupTemplatePolicy::Default,
        false,
    )
    .await?;
    insert_node_in_group(pool, group).await?;

    let store = AdminNodeGroupStore::new(pool.clone());
    assert_eq!(store.fetch(group).await?.expect("row").nodes_count, 1);

    let mut tx = pool.begin().await?;
    let err = delete_group_in_tx(&mut tx, group).await.unwrap_err();
    assert_eq!(
        violated_constraint(&err).as_deref(),
        Some("nodes_node_group_id_fkey")
    );
    Ok(())
}

/// The predicate behind the create handler's provisioning-order guard.
///
/// Worth its own test because the state it prevents is a **latch**, not a
/// transient: the labeler's `ensure_default_group` seeds the default generic
/// group only while no non-default group exists, so a dedicated group created
/// on an empty table blocks that seeding permanently — and the default can be
/// neither deleted nor un-defaulted, so no admin route clears it. One unguarded
/// call would leave the cluster with dedicated capacity and no generic group,
/// where every org without dedicated capacity resolves an empty candidate set,
/// renders no boundary, and is held off the dedicated nodes only by the taint.
#[tokio::test(flavor = "current_thread")]
async fn generic_group_existence_gates_dedicated_creation() -> TestResult {
    let Some(fixture) = postgres_fixture().await? else {
        return Ok(());
    };
    let pool = &fixture.pool;

    // Empty table: nothing to host a generic template, so a dedicated group
    // must be refused by the caller.
    assert!(
        !generic_node_group_exists(pool).await?,
        "an empty table has no generic group"
    );

    // A dedicated group alone does not satisfy it — this is the case the guard
    // exists for, and asserting it is what keeps the predicate from being
    // written as a bare row count.
    let org_id = OrgId::new();
    sqlx::query(sql!(
        "INSERT INTO organizations (id, name, slug) VALUES ($1, 'Guard Org', $2)"
    ))
    .bind(org_id)
    .bind(format!("guard-org-{org_id}"))
    .execute(pool)
    .await?;
    insert_node_group(
        pool,
        &group_name("org-dedicated"),
        Some(org_id),
        NodeGroupTemplatePolicy::Default,
        false,
    )
    .await?;
    assert!(
        !generic_node_group_exists(pool).await?,
        "a dedicated group is not a generic one, however many exist"
    );

    // A generic group satisfies it whether or not it is the cluster default,
    // because compatibility does not read `is_default`.
    insert_node_group(
        pool,
        &group_name("workers-extra"),
        None,
        NodeGroupTemplatePolicy::Default,
        false,
    )
    .await?;
    assert!(
        generic_node_group_exists(pool).await?,
        "a non-default generic group still hosts generic templates"
    );
    Ok(())
}
