//! Characterization of the statements that end a create:
//! [`create::delete_failed_create`] removes the row and its cascading
//! dependents whatever the lifecycle, [`create::record_created_placement`]
//! writes the node and `placement_changed_at` whatever the lifecycle, and
//! [`create::finalize_created`] records `command_ready_pending` as the inverse
//! of the reported command readiness on a `creating` row.

use super::*;
use crate::create;

/// `active_node_id`, `placement_changed_at IS NOT NULL`, `lifecycle`,
/// `placement` and `command_ready_pending`, as stored.
type Placement = (Option<NodeId>, bool, String, String, bool);

async fn read_placement(pool: &PgPool, id: Uuid) -> TestResult<Placement> {
    Ok(sqlx::query_as::<_, Placement>(sql!(
        "SELECT active_node_id, placement_changed_at IS NOT NULL, lifecycle, placement,
                command_ready_pending
           FROM sandboxes WHERE id = $1"
    ))
    .bind(id)
    .fetch_one(pool)
    .await?)
}

async fn row_exists(pool: &PgPool, id: Uuid) -> TestResult<bool> {
    let (exists,) = sqlx::query_as::<_, (bool,)>(sql!(
        "SELECT EXISTS (SELECT 1 FROM sandboxes WHERE id = $1)"
    ))
    .bind(id)
    .fetch_one(pool)
    .await?;
    Ok(exists)
}

async fn dependent_exists(pool: &PgPool, id: Uuid) -> TestResult<bool> {
    let (exists,) = sqlx::query_as::<_, (bool,)>(sql!(
        "SELECT EXISTS (SELECT 1 FROM sandbox_ecw_credential_epochs WHERE sandbox_id = $1)"
    ))
    .bind(id)
    .fetch_one(pool)
    .await?;
    Ok(exists)
}

#[tokio::test(flavor = "current_thread")]
async fn delete_failed_create_removes_the_row_whatever_its_lifecycle() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let unrelated = insert_sandbox(&db.pool, None, "creating").await?;

    for lifecycle in ["creating", "restoring", "started", "failed", "deleting"] {
        let id = insert_sandbox(&db.pool, None, lifecycle).await?;
        // A dependent whose foreign key to `sandboxes` cascades on delete.
        sqlx::query(sql!(
            "INSERT INTO sandbox_ecw_credential_epochs (sandbox_id) VALUES ($1)"
        ))
        .bind(id)
        .execute(&db.pool)
        .await?;

        create::delete_failed_create(&db.pool, SandboxId::from_uuid(id)).await?;

        assert!(!row_exists(&db.pool, id).await?, "{lifecycle} row deleted");
        assert!(
            !dependent_exists(&db.pool, id).await?,
            "{lifecycle} row's cascading dependent deleted"
        );
    }

    assert!(
        row_exists(&db.pool, unrelated).await?,
        "only the named row is deleted"
    );
    // An id with no row is not an error.
    create::delete_failed_create(&db.pool, SandboxId::new()).await?;
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn record_created_placement_records_the_node_on_any_lifecycle() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };

    // No lifecycle predicate, so a `failed` row is written too.
    for lifecycle in ["creating", "failed"] {
        let id = insert_sandbox(&db.pool, None, lifecycle).await?;
        let (_, changed_at_set, ..) = read_placement(&db.pool, id).await?;
        assert!(
            !changed_at_set,
            "{lifecycle} row starts with no placement change"
        );
        let node = NodeId::new();

        create::record_created_placement(&db.pool, SandboxId::from_uuid(id), node).await?;

        let (active_node_id, changed_at_set, stored_lifecycle, placement, _) =
            read_placement(&db.pool, id).await?;
        assert_eq!(active_node_id, Some(node), "{lifecycle} row's node");
        assert!(
            changed_at_set,
            "{lifecycle} row's placement_changed_at stamped"
        );
        assert_eq!(stored_lifecycle, lifecycle, "lifecycle unchanged");
        assert_eq!(
            placement, "unplaced",
            "{lifecycle} row's placement unchanged"
        );
    }
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn finalize_created_records_command_readiness_on_a_creating_row() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };

    for command_ready in [false, true] {
        let id = insert_sandbox(&db.pool, None, "creating").await?;
        let node = NodeId::new();

        let matched = create::finalize_created(
            &db.pool,
            SandboxId::from_uuid(id),
            "started",
            "on_node",
            Some(node),
            command_ready,
            None,
        )
        .await?;

        assert!(
            matched,
            "command_ready = {command_ready}: a creating row matches"
        );
        let (active_node_id, _, lifecycle, placement, command_ready_pending) =
            read_placement(&db.pool, id).await?;
        assert_eq!(
            (active_node_id, lifecycle.as_str(), placement.as_str()),
            (Some(node), "started", "on_node"),
            "command_ready = {command_ready}: the reported state"
        );
        assert_eq!(
            command_ready_pending, !command_ready,
            "command_ready = {command_ready}: pending is its inverse"
        );
    }
    Ok(())
}
