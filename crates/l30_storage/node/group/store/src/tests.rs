//! Node-group verbs against Postgres.

use pplx_space_db_test_fixtures::{TestDbResult, pg_test_db};

use super::*;

fn group_name(value: &str) -> NodeGroupName {
    NodeGroupName::parse(value).expect("test group name is a valid slug")
}

async fn insert_group(
    pool: &sqlx::PgPool,
    name: &str,
    org_id: Option<OrgId>,
) -> TestDbResult<NodeGroupId> {
    let id = NodeGroupId::new();
    insert_node_group(
        pool,
        NewNodeGroup {
            id,
            name: &group_name(name),
            org_id,
            template_policy: NodeGroupTemplatePolicy::Default,
            is_default: false,
        },
    )
    .await?;
    Ok(id)
}

/// Generic-group existence: the check the admin create route asks before an
/// org-owned group, so a dedicated group never lands on an empty table, where
/// the labeler's default-group seeding could no longer run. Only an org-less
/// group counts, default or not.
#[tokio::test(flavor = "current_thread")]
async fn generic_group_existence_counts_only_org_less_groups() -> TestDbResult<()> {
    let db = pg_test_db().await?;
    let pool = &db.pool;

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
    insert_group(pool, "org-dedicated", Some(org_id)).await?;
    assert!(
        !generic_node_group_exists(pool).await?,
        "a dedicated group is not a generic one, however many exist"
    );

    // A generic group satisfies it whether or not it is the cluster default,
    // because compatibility does not read `is_default`.
    insert_group(pool, "workers-extra", None).await?;
    assert!(
        generic_node_group_exists(pool).await?,
        "a non-default generic group still hosts generic templates"
    );
    Ok(())
}

/// The mirror's name map lists every group.
#[tokio::test(flavor = "current_thread")]
async fn node_group_names_lists_every_group() -> TestDbResult<()> {
    let db = pg_test_db().await?;
    let pool = &db.pool;
    let generic = insert_group(pool, "workers-a", None).await?;
    let other = insert_group(pool, "workers-b", None).await?;

    let mut names = node_group_names(pool)
        .await?
        .into_iter()
        .map(|row| (row.name, row.id))
        .collect::<Vec<_>>();
    names.sort_by(|left, right| left.0.cmp(&right.0));
    assert_eq!(
        names,
        vec![
            ("workers-a".to_string(), generic),
            ("workers-b".to_string(), other)
        ]
    );
    Ok(())
}

/// The default-group seed: the insert lands once, a second default is kept
/// out as an outcome rather than an error, and the census counts only the
/// non-default groups.
#[tokio::test(flavor = "current_thread")]
async fn default_group_seed_and_census() -> TestDbResult<()> {
    let db = pg_test_db().await?;
    let pool = &db.pool;
    assert!(default_node_group(pool).await?.is_none());
    assert_eq!(non_default_node_group_count(pool).await?, 0);

    let seeded = NodeGroupId::new();
    assert!(insert_default_node_group(pool, seeded, &group_name("workers")).await?);
    assert!(
        !insert_default_node_group(pool, NodeGroupId::new(), &group_name("workers-other")).await?,
        "the singleton default index keeps a second default out"
    );
    let default = default_node_group(pool)
        .await?
        .expect("the seeded group is the default");
    assert_eq!((default.id, default.name.as_str()), (seeded, "workers"));

    insert_group(pool, "workers-extra", None).await?;
    assert_eq!(non_default_node_group_count(pool).await?, 1);
    Ok(())
}
