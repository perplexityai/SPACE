//! The UUID identifiers that `pplx_space_uuid_id::postgres_uuid!` gives a
//! Postgres codec, in accounts, node-model, sandbox-model and snapshot-model,
//! whose `sqlx` features this store enables, reach Postgres as `UUID`s, and
//! lists of them as `UUID[]`s, and decode back to the same values.

use std::any::type_name;
use std::fmt::Debug;

use pplx_space_accounts::UserId;
use pplx_space_db_test_fixtures::try_pg_test_db;
use pplx_space_node_model::NodeGroupId;
use pplx_space_sandbox_model::{SandboxActivationAttemptId, SandboxCleanupEpisodeId, SandboxId};
use pplx_space_snapshot_model::{LocalArtifactId, SnapshotId};
use pplx_space_sql_locator::sql;
use pplx_space_test_env::TestResult;
use sqlx::postgres::PgHasArrayType;
use sqlx::{Decode, Encode, PgPool, Postgres, Type};
use uuid::Uuid;

/// Binds an identifier and a list of two, and reads back what Postgres made of
/// them: the parameter types it was sent, the UUID it read from the scalar's
/// bytes, and both values decoded again.
async fn assert_binds_as_uuid<T>(pool: &PgPool, from_uuid: fn(Uuid) -> T) -> TestResult
where
    T: Copy
        + Debug
        + PartialEq
        + Send
        + Unpin
        + Type<Postgres>
        + PgHasArrayType
        + for<'q> Encode<'q, Postgres>
        + for<'r> Decode<'r, Postgres>
        + 'static,
{
    let uuid = Uuid::now_v7();
    let id = from_uuid(uuid);
    let ids = vec![id, from_uuid(Uuid::nil())];
    let (id_type, ids_type, id_text, id_back, ids_back) =
        sqlx::query_as::<_, (String, String, String, T, Vec<T>)>(sql!(
            "SELECT pg_typeof($1)::TEXT, pg_typeof($2)::TEXT, $1::TEXT, $1, $2"
        ))
        .bind(id)
        .bind(&ids)
        .fetch_one(pool)
        .await?;
    let name = type_name::<T>();
    assert_eq!(id_type, "uuid", "{name}");
    assert_eq!(ids_type, "uuid[]", "{name}");
    assert_eq!(id_text, uuid.to_string(), "{name}");
    assert_eq!(id_back, id, "{name}");
    assert_eq!(ids_back, ids, "{name}");
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn uuid_identifiers_bind_and_decode_as_postgres_uuids() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    assert_binds_as_uuid(&db.pool, LocalArtifactId::from_uuid).await?;
    assert_binds_as_uuid(&db.pool, NodeGroupId::from_uuid).await?;
    assert_binds_as_uuid(&db.pool, SandboxActivationAttemptId::from_uuid).await?;
    assert_binds_as_uuid(&db.pool, SandboxCleanupEpisodeId::from_uuid).await?;
    assert_binds_as_uuid(&db.pool, SandboxId::from_uuid).await?;
    assert_binds_as_uuid(&db.pool, SnapshotId::from_uuid).await?;
    // Made by `uuid_id!`, like every account identifier.
    assert_binds_as_uuid(&db.pool, UserId::from_uuid).await?;
    Ok(())
}
