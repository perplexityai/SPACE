//! Inventory verbs against Postgres.

use pplx_space_db_test_fixtures::{TestDbResult, pg_test_db};
use sqlx::PgPool;
use uuid::Uuid;

use pplx_space_template_model::TemplateName;

use super::*;

/// Orgs seeded by the baseline migration's fixture data.
const ORG_A: Uuid = Uuid::from_u128(0x1);
const ORG_B: Uuid = Uuid::from_u128(0x3);

async fn insert_group(pool: &PgPool, org_id: Option<OrgId>) -> TestDbResult<NodeGroupId> {
    let id = NodeGroupId::new();
    sqlx::query(
        "INSERT INTO node_groups (id, name, org_id, template_policy, is_default)
         VALUES ($1, 'org-a-dedicated', $2, 'default', FALSE)",
    )
    .bind(id)
    .bind(org_id)
    .execute(pool)
    .await?;
    Ok(id)
}

async fn insert_worker(pool: &PgPool, group_id: NodeGroupId) -> TestDbResult<NodeId> {
    let node_id = NodeId::new();
    sqlx::query(
        "INSERT INTO nodes (id, address, role, managed_by, state, node_group_id)
         VALUES ($1, '10.0.0.1'::INET, 'worker', 'k8s', 'ready', $2)",
    )
    .bind(node_id)
    .bind(group_id)
    .execute(pool)
    .await?;
    Ok(node_id)
}

async fn insert_template(
    pool: &PgPool,
    name: &str,
    visibility: &str,
    org_id: Option<OrgId>,
) -> TestDbResult<()> {
    sqlx::query("INSERT INTO templates (id, name, org_id, visibility) VALUES ($1, $2, $3, $4)")
        .bind(Uuid::now_v7())
        .bind(name)
        .bind(org_id)
        .bind(visibility)
        .execute(pool)
        .await?;
    Ok(())
}

async fn cache(pool: &PgPool, node_id: NodeId, template: &str) -> TestDbResult<()> {
    sqlx::query("INSERT INTO node_template_inventory (node_id, template_name) VALUES ($1, $2)")
        .bind(node_id)
        .bind(template)
        .execute(pool)
        .await?;
    Ok(())
}

/// The cached-rootfs guard strands only org-specific templates whose
/// owner differs from the new assignment; generic rootfs never blocks.
#[tokio::test(flavor = "current_thread")]
async fn stranded_templates_respect_owner_and_visibility() -> TestDbResult<()> {
    let db = pg_test_db().await?;
    let pool = &db.pool;
    let group = insert_group(pool, Some(OrgId::from_uuid(ORG_A))).await?;
    let node = insert_worker(pool, group).await?;
    insert_template(
        pool,
        "a-secret",
        "org_specific",
        Some(OrgId::from_uuid(ORG_A)),
    )
    .await?;
    insert_template(pool, "base-generic", "generic", None).await?;
    for template in ["a-secret", "base-generic"] {
        cache(pool, node, template).await?;
    }

    // Releasing to generic strands A's org-specific rootfs.
    assert_eq!(stranded_org_specific_templates(pool, group, None).await?, 1);
    // Re-assigning to the same owner strands nothing.
    assert_eq!(
        stranded_org_specific_templates(pool, group, Some(OrgId::from_uuid(ORG_A))).await?,
        0
    );
    // Handing the group to another org strands A's rootfs.
    assert_eq!(
        stranded_org_specific_templates(pool, group, Some(OrgId::from_uuid(ORG_B))).await?,
        1
    );
    Ok(())
}

/// The node-scoped deletes remove one build, one template, or a list of
/// templates, and leave other nodes' and templates' rows alone.
#[tokio::test(flavor = "current_thread")]
async fn node_scoped_deletes_touch_only_their_rows() -> TestDbResult<()> {
    let db = pg_test_db().await?;
    let pool = &db.pool;
    let group = insert_group(pool, None).await?;
    let node = insert_worker(pool, group).await?;
    let other = insert_worker(pool, group).await?;
    for template in ["alpha", "beta", "gamma"] {
        insert_template(pool, template, "generic", None).await?;
    }
    for (node_id, template, hash) in [
        (node, "alpha", "sha256:a1"),
        (node, "alpha", "sha256:a2"),
        (node, "beta", "sha256:b1"),
        (node, "gamma", "sha256:c1"),
        (other, "alpha", "sha256:a1"),
    ] {
        sqlx::query(
            "INSERT INTO node_template_inventory (node_id, template_name, content_hash)
             VALUES ($1, $2, $3)",
        )
        .bind(node_id)
        .bind(template)
        .bind(hash)
        .execute(pool)
        .await?;
    }

    delete_node_template_build(pool, node, "alpha", "sha256:a1").await?;
    assert_eq!(node_template_build_count(pool, node, "alpha").await?, 1);
    delete_node_template(pool, node, "alpha").await?;
    assert_eq!(node_template_build_count(pool, node, "alpha").await?, 0);
    assert_eq!(node_template_build_count(pool, other, "alpha").await?, 1);

    let removed =
        delete_node_templates(pool, node, &["beta".to_string(), "missing".to_string()]).await?;
    assert_eq!(removed, 1);
    assert_eq!(node_template_build_count(pool, node, "gamma").await?, 1);
    assert_eq!(template_inventory_rows(pool, "alpha").await?, 1);
    Ok(())
}

struct PoolInventory<'a> {
    node_id: NodeId,
    template: &'a str,
    content_hash: &'a str,
    pool_target: Option<i32>,
    pool_current: i32,
    sandbox_ids: &'a [String],
}

async fn insert_pool_inventory(pool: &PgPool, input: PoolInventory<'_>) -> TestDbResult<()> {
    sqlx::query(
        "INSERT INTO node_template_inventory (
             node_id, template_name, content_hash, runtime,
             pool_target, pool_current, pool_pending, pool_sandbox_ids
         )
         VALUES ($1, $2, $3, 'fc', $4, $5, 0, $6)",
    )
    .bind(input.node_id)
    .bind(input.template)
    .bind(input.content_hash)
    .bind(input.pool_target)
    .bind(input.pool_current)
    .bind(Json(input.sandbox_ids))
    .execute(pool)
    .await?;
    Ok(())
}

async fn insert_node_with_role(pool: &PgPool, role: &str) -> TestDbResult<NodeId> {
    let node_id = NodeId::new();
    sqlx::query(
        "INSERT INTO nodes (id, address, role, managed_by, state)
         VALUES ($1, '10.0.0.10'::INET, $2, 'space', 'ready')",
    )
    .bind(node_id)
    .bind(role)
    .execute(pool)
    .await?;
    Ok(node_id)
}

/// The observed pool sums the counts of the builder's current build once per
/// row, not once per sandbox id, and leaves out a stale build's rows.
#[tokio::test(flavor = "current_thread")]
async fn template_pool_observed_does_not_multiply_counts_by_sandbox_ids() -> TestDbResult<()> {
    let db = pg_test_db().await?;
    let pool = &db.pool;
    let template = TemplateName::parse("seed")?;
    insert_template(pool, template.as_str(), "generic", None).await?;

    let builder_id = insert_node_with_role(pool, "builder").await?;
    insert_pool_inventory(
        pool,
        PoolInventory {
            node_id: builder_id,
            template: template.as_str(),
            content_hash: "sha256:current",
            pool_target: None,
            pool_current: 0,
            sandbox_ids: &[],
        },
    )
    .await?;

    let worker_id = insert_node_with_role(pool, "worker").await?;
    let sandbox_ids = (0..15)
        .map(|index| format!("sandbox-{index}"))
        .collect::<Vec<_>>();
    insert_pool_inventory(
        pool,
        PoolInventory {
            node_id: worker_id,
            template: template.as_str(),
            content_hash: "sha256:current",
            pool_target: Some(15),
            pool_current: 15,
            sandbox_ids: &sandbox_ids,
        },
    )
    .await?;

    let stale_worker_id = insert_node_with_role(pool, "worker").await?;
    insert_pool_inventory(
        pool,
        PoolInventory {
            node_id: stale_worker_id,
            template: template.as_str(),
            content_hash: "sha256:old",
            pool_target: Some(100),
            pool_current: 100,
            sandbox_ids: &["stale-sandbox".to_string()],
        },
    )
    .await?;

    let observed = template_pool_observed(pool, &template)
        .await?
        .expect("pool should be observed");

    assert_eq!(observed.pool_target, Some(15));
    assert_eq!(observed.pool_current, 15);
    assert_eq!(observed.pool_pending, 0);
    assert_eq!(observed.pool_sandbox_ids.0.len(), 15);
    assert!(
        !observed
            .pool_sandbox_ids
            .0
            .iter()
            .any(|id| id == "stale-sandbox")
    );
    Ok(())
}

/// A caller's snapshot payload, as the `Snapshots`-generic rows decode it.
#[derive(Debug, PartialEq, Eq, serde::Deserialize)]
struct TestSnapshot {
    vcpu: u32,
    snapshot_hash: String,
}

/// The rows generic over the `snapshots` payload decode it into the caller's
/// type, through both the node listing and the template variants.
#[tokio::test(flavor = "current_thread")]
async fn snapshot_generic_rows_decode_the_caller_payload() -> TestDbResult<()> {
    let db = pg_test_db().await?;
    let pool = &db.pool;
    let group = insert_group(pool, None).await?;
    let node = insert_worker(pool, group).await?;
    insert_template(pool, "alpha", "generic", None).await?;
    sqlx::query(
        r#"INSERT INTO node_template_inventory (node_id, template_name, content_hash, snapshots)
           VALUES ($1, 'alpha', 'sha256:a1', '[{"vcpu": 2, "snapshot_hash": "sha256:s1"}]')"#,
    )
    .bind(node)
    .execute(pool)
    .await?;
    let expected = vec![TestSnapshot {
        vcpu: 2,
        snapshot_hash: "sha256:s1".to_string(),
    }];

    let builds = node_template_builds::<Vec<TestSnapshot>>(pool, node).await?;
    assert_eq!(builds.len(), 1);
    assert_eq!(builds[0].name, "alpha");
    assert_eq!(builds[0].content_hash.as_deref(), Some("sha256:a1"));
    assert_eq!(builds[0].snapshots.0, expected);

    let variants = template_variants::<Vec<TestSnapshot>>(pool, &["alpha".to_string()]).await?;
    assert_eq!(variants.len(), 1);
    assert_eq!(variants[0].template_name, "alpha");
    assert_eq!(variants[0].content_hash, "sha256:a1");
    assert_eq!(variants[0].snapshots.0, expected);
    Ok(())
}
