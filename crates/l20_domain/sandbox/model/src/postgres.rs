//! PostgreSQL UUID array codecs preserve sandbox identities through storage
//! boundaries.

use super::SandboxId;
use uuid::Uuid;

macro_rules! postgres_uuid_array {
    ($($name:ty),+ $(,)?) => {$(
        impl pplx_space_sql_codec::postgres::PgHasArrayType for $name {
            fn array_type_info() -> pplx_space_sql_codec::postgres::PgTypeInfo {
                <Uuid as pplx_space_sql_codec::postgres::PgHasArrayType>::array_type_info()
            }
        }
    )+};
}

postgres_uuid_array!(SandboxId);
