//! The node-group locks against Postgres: what each returns, what it holds
//! off while the caller's transaction is open, and that it ends with that
//! transaction.

use pplx_space_db_test_fixtures::{TestDbResult, pg_test_db};
use sqlx::PgPool;

use super::*;

/// Whether another transaction can lock `node_group_id`'s row now.
pub(crate) async fn row_acquirable(
    pool: &PgPool,
    node_group_id: NodeGroupId,
) -> TestDbResult<bool> {
    let mut other = pool.begin().await?;
    let result = sqlx::query(sql!(
        "SELECT 1 FROM node_groups WHERE id = $1 FOR UPDATE NOWAIT"
    ))
    .bind(node_group_id)
    .execute(&mut *other)
    .await;
    other.rollback().await?;
    not_lock_unavailable(result)
}

/// Whether another transaction can lock every `nodes` row now.
pub(crate) async fn nodes_acquirable(pool: &PgPool) -> TestDbResult<bool> {
    let mut other = pool.begin().await?;
    let result = sqlx::query(sql!("SELECT 1 FROM nodes FOR UPDATE NOWAIT"))
        .execute(&mut *other)
        .await;
    other.rollback().await?;
    not_lock_unavailable(result)
}

/// Whether another transaction can insert a group now, without waiting.
async fn insert_acquirable(pool: &PgPool) -> TestDbResult<bool> {
    let mut other = pool.begin().await?;
    sqlx::query(sql!("SELECT set_config('lock_timeout', '50ms', TRUE)"))
        .execute(&mut *other)
        .await?;
    let result = crate::insert_node_group(
        &mut *other,
        crate::NewNodeGroup {
            id: NodeGroupId::new(),
            name: &pplx_space_node_model::NodeGroupName::parse("workers-probe")
                .expect("a valid group name"),
            org_id: None,
            template_policy: pplx_space_node_model::NodeGroupTemplatePolicy::Default,
            is_default: false,
        },
    )
    .await
    .map_err(sqlx::Error::from);
    other.rollback().await?;
    not_lock_unavailable(result)
}

/// Whether another transaction can take the seed's table lock now, without
/// waiting.
async fn table_lock_acquirable(pool: &PgPool) -> TestDbResult<bool> {
    let mut other = pool.begin().await?;
    sqlx::query(sql!("SELECT set_config('lock_timeout', '50ms', TRUE)"))
        .execute(&mut *other)
        .await?;
    let result = lock_node_groups_share_row_exclusive(&mut *other)
        .await
        .map_err(sqlx::Error::from);
    other.rollback().await?;
    not_lock_unavailable(result)
}

fn not_lock_unavailable<T>(result: Result<T, sqlx::Error>) -> TestDbResult<bool> {
    match result {
        Ok(_) => Ok(true),
        Err(sqlx::Error::Database(error)) if error.code().as_deref() == Some("55P03") => Ok(false),
        Err(error) => Err(error.into()),
    }
}

/// No default, nothing to lock; with one, its id, its row held until the
/// transaction ends.
#[tokio::test(flavor = "current_thread")]
async fn the_default_row_lock_returns_the_default_and_holds_it() -> TestDbResult<()> {
    let db = pg_test_db().await?;
    let pool = &db.pool;

    let mut tx = pool.begin().await?;
    assert_eq!(lock_default_node_group(&mut *tx).await?, None);
    tx.commit().await?;

    let default = NodeGroupId::new();
    assert!(
        crate::insert_default_node_group(
            pool,
            default,
            &pplx_space_node_model::NodeGroupName::parse("workers").expect("a valid group name"),
        )
        .await?
    );
    let mut tx = pool.begin().await?;
    assert_eq!(lock_default_node_group(&mut *tx).await?, Some(default));
    assert!(!row_acquirable(pool, default).await?);
    tx.rollback().await?;

    assert!(row_acquirable(pool, default).await?);
    Ok(())
}

/// The table lock holds off a group insert and a second seeder's table lock
/// until the transaction ends, and leaves plain reads alone.
#[tokio::test(flavor = "current_thread")]
async fn the_table_lock_holds_off_writers_until_the_transaction_ends() -> TestDbResult<()> {
    let db = pg_test_db().await?;
    let pool = &db.pool;

    let mut tx = pool.begin().await?;
    lock_node_groups_share_row_exclusive(&mut *tx).await?;
    assert!(!insert_acquirable(pool).await?);
    assert!(!table_lock_acquirable(pool).await?);
    assert_eq!(crate::non_default_node_group_count(pool).await?, 0);
    tx.commit().await?;

    assert!(insert_acquirable(pool).await?);
    assert!(table_lock_acquirable(pool).await?);
    Ok(())
}
