//! PostgreSQL UUID codecs preserve domain identities through storage boundaries.

use super::NodeGroupId;
use uuid::Uuid;

macro_rules! postgres_uuid {
    ($($name:ty),+ $(,)?) => {$(
        impl pplx_space_sql_codec::Type<pplx_space_sql_codec::Postgres> for $name {
            fn type_info() -> pplx_space_sql_codec::postgres::PgTypeInfo {
                <Uuid as pplx_space_sql_codec::Type<pplx_space_sql_codec::Postgres>>::type_info()
            }
            fn compatible(ty: &pplx_space_sql_codec::postgres::PgTypeInfo) -> bool {
                <Uuid as pplx_space_sql_codec::Type<pplx_space_sql_codec::Postgres>>::compatible(ty)
            }
        }
        impl<'q> pplx_space_sql_codec::Encode<'q, pplx_space_sql_codec::Postgres> for $name {
            fn encode_by_ref(&self, buf: &mut pplx_space_sql_codec::postgres::PgArgumentBuffer) -> Result<pplx_space_sql_codec::encode::IsNull, pplx_space_sql_codec::error::BoxDynError> {
                <Uuid as pplx_space_sql_codec::Encode<'q, pplx_space_sql_codec::Postgres>>::encode_by_ref(&self.as_uuid(), buf)
            }
        }
        impl<'r> pplx_space_sql_codec::Decode<'r, pplx_space_sql_codec::Postgres> for $name {
            fn decode(value: pplx_space_sql_codec::postgres::PgValueRef<'r>) -> Result<Self, pplx_space_sql_codec::error::BoxDynError> {
                Ok(Self::from_uuid(<Uuid as pplx_space_sql_codec::Decode<'r, pplx_space_sql_codec::Postgres>>::decode(value)?))
            }
        }
    )+};
}

postgres_uuid!(NodeGroupId);

macro_rules! postgres_uuid_array {
    ($($name:ty),+ $(,)?) => {$(
        impl pplx_space_sql_codec::postgres::PgHasArrayType for $name {
            fn array_type_info() -> pplx_space_sql_codec::postgres::PgTypeInfo {
                <Uuid as pplx_space_sql_codec::postgres::PgHasArrayType>::array_type_info()
            }
        }
    )+};
}

postgres_uuid_array!(NodeGroupId);
