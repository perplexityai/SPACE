//! PostgreSQL UUID codecs preserve domain identities through storage boundaries.

use super::{LocalArtifactId, NodeGroupId, PodMaterializationId, SandboxId, SnapshotId};
use uuid::Uuid;

macro_rules! postgres_uuid {
    ($($name:ty),+ $(,)?) => {$(
        impl sqlx::Type<sqlx::Postgres> for $name {
            fn type_info() -> sqlx::postgres::PgTypeInfo {
                <Uuid as sqlx::Type<sqlx::Postgres>>::type_info()
            }
            fn compatible(ty: &sqlx::postgres::PgTypeInfo) -> bool {
                <Uuid as sqlx::Type<sqlx::Postgres>>::compatible(ty)
            }
        }
        impl<'q> sqlx::Encode<'q, sqlx::Postgres> for $name {
            fn encode_by_ref(&self, buf: &mut sqlx::postgres::PgArgumentBuffer) -> Result<sqlx::encode::IsNull, sqlx::error::BoxDynError> {
                <Uuid as sqlx::Encode<'q, sqlx::Postgres>>::encode_by_ref(&self.as_uuid(), buf)
            }
        }
        impl<'r> sqlx::Decode<'r, sqlx::Postgres> for $name {
            fn decode(value: sqlx::postgres::PgValueRef<'r>) -> Result<Self, sqlx::error::BoxDynError> {
                Ok(Self::from_uuid(<Uuid as sqlx::Decode<'r, sqlx::Postgres>>::decode(value)?))
            }
        }
    )+};
}

postgres_uuid!(
    LocalArtifactId,
    NodeGroupId,
    PodMaterializationId,
    SnapshotId
);

macro_rules! postgres_uuid_array {
    ($($name:ty),+ $(,)?) => {$(
        impl sqlx::postgres::PgHasArrayType for $name {
            fn array_type_info() -> sqlx::postgres::PgTypeInfo {
                <Uuid as sqlx::postgres::PgHasArrayType>::array_type_info()
            }
        }
    )+};
}

postgres_uuid_array!(
    LocalArtifactId,
    NodeGroupId,
    PodMaterializationId,
    SandboxId,
    SnapshotId
);
