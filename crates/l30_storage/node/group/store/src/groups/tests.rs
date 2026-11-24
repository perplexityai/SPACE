//! Group rows against Postgres: the worker count, the default group's count
//! of unlabeled workers, the listing's order, and the locked read.

use pplx_space_db_test_fixtures::{TestDbResult, pg_test_db, try_pg_test_db};
use sqlx::PgPool;

use super::*;

/// Seeded by the baseline migration's fixture data.
const ORG_A: OrgId = OrgId::from_uuid(uuid::Uuid::from_u128(0x1));

async fn group(
    pool: &PgPool,
    name: &str,
    org_id: Option<OrgId>,
    is_default: bool,
) -> TestDbResult<NodeGroupId> {
    let id = NodeGroupId::new();
    crate::insert_node_group(
        pool,
        crate::NewNodeGroup {
            id,
            name: &NodeGroupName::parse(name).expect("a valid group name"),
            org_id,
            template_policy: NodeGroupTemplatePolicy::Default,
            is_default,
        },
    )
    .await?;
    Ok(id)
}

async fn node(pool: &PgPool, role: &str, group: Option<NodeGroupId>) -> TestDbResult<()> {
    sqlx::query(sql!(
        "INSERT INTO nodes (id, address, role, managed_by, state, node_group_id)
         VALUES ($1, '10.0.0.80'::INET, $2, 'k8s', 'ready', $3)"
    ))
    .bind(pplx_space_accounts::NodeId::new())
    .bind(role)
    .bind(group)
    .execute(pool)
    .await?;
    Ok(())
}

/// What a test compares of a row: everything but the creation time.
fn summary(row: &NodeGroupRow) -> (NodeGroupId, String, bool, i64) {
    (
        row.id,
        row.name.to_string(),
        row.is_default,
        row.nodes_count,
    )
}

/// Two labeled workers, one in each group, plus two unlabeled workers and an
/// unlabeled builder. The default group counts its own worker and the two
/// unlabeled ones; the other group counts only its own. Builders never count.
async fn seed(pool: &PgPool) -> TestDbResult<(NodeGroupId, NodeGroupId)> {
    let shared = group(pool, "workers-shared", None, true).await?;
    let other = group(pool, "workers-other", None, false).await?;
    node(pool, "worker", Some(shared)).await?;
    node(pool, "worker", Some(other)).await?;
    node(pool, "worker", None).await?;
    node(pool, "worker", None).await?;
    node(pool, "builder", None).await?;
    node(pool, "builder", Some(other)).await?;
    Ok((shared, other))
}

/// `nodes_count` must include NULL-membership nodes for the singleton
/// default group: unlabeled (or unknown-label) nodes mirror as NULL,
/// which everywhere else resolves to the default generic group. Other
/// groups count only explicit membership.
#[tokio::test(flavor = "current_thread")]
async fn default_group_nodes_count_includes_null_membership() -> TestDbResult<()> {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let default_group = group(pool, "workers-shared", None, true).await?;
    let dedicated = group(pool, "org-a-dedicated", Some(ORG_A), false).await?;
    node(pool, "worker", Some(default_group)).await?;
    node(pool, "worker", None).await?; // unlabeled worker — resolves to default
    node(pool, "worker", Some(dedicated)).await?;
    // Non-worker rows always sit at NULL (the mirror stamps workers
    // only); they are not members of the default *worker* pool.
    node(pool, "builder", None).await?;
    node(pool, "cluster", None).await?;

    assert!(node_group(pool, NodeGroupId::new()).await?.is_none());
    let default_row = node_group(pool, default_group).await?.expect("row");
    assert_eq!(
        default_row.nodes_count, 2,
        "default group counts explicit + NULL worker membership, not builder/cluster rows"
    );
    assert_eq!(
        (
            default_row.name.as_str(),
            default_row.org_id,
            default_row.is_default
        ),
        ("workers-shared", None, true)
    );
    let dedicated_row = node_group(pool, dedicated).await?.expect("row");
    assert_eq!(
        dedicated_row.nodes_count, 1,
        "non-default groups count only explicit membership"
    );
    assert_eq!(
        (dedicated_row.org_id, dedicated_row.template_policy),
        (Some(ORG_A), NodeGroupTemplatePolicy::Default)
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn the_listing_orders_groups_by_name() -> TestDbResult<()> {
    let db = pg_test_db().await?;
    let pool = &db.pool;
    assert!(node_groups(pool).await?.is_empty());
    let (shared, other) = seed(pool).await?;

    let rows = node_groups(pool).await?;
    assert_eq!(
        rows.iter().map(summary).collect::<Vec<_>>(),
        vec![
            (other, "workers-other".to_string(), false, 1),
            (shared, "workers-shared".to_string(), true, 3),
        ]
    );
    Ok(())
}

/// The locked read returns the row [`node_group`] does, and holds the group
/// row until the transaction ends; the counted nodes stay unlocked.
#[tokio::test(flavor = "current_thread")]
async fn the_locked_read_returns_the_same_row_and_holds_it() -> TestDbResult<()> {
    let db = pg_test_db().await?;
    let pool = &db.pool;
    let (shared, _) = seed(pool).await?;
    let unlocked = node_group(pool, shared).await?.expect("the default group");

    let mut tx = pool.begin().await?;
    assert!(
        lock_node_group(&mut tx, NodeGroupId::new())
            .await?
            .is_none()
    );
    let locked = lock_node_group(&mut tx, shared)
        .await?
        .expect("the default group");
    assert_eq!(summary(&locked), summary(&unlocked));
    assert_eq!(locked.created_at, unlocked.created_at);
    assert!(!crate::locks::tests::row_acquirable(pool, shared).await?);
    assert!(crate::locks::tests::nodes_acquirable(pool).await?);
    tx.commit().await?;

    assert!(crate::locks::tests::row_acquirable(pool, shared).await?);
    Ok(())
}
