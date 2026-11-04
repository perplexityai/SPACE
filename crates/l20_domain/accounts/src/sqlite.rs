//! SQLite encodes domain UUIDs as text to preserve existing node-local schemas.

use super::{JobId, NodeId};

macro_rules! sqlite_uuid {
    ($($name:ty),+ $(,)?) => {$(
        impl rusqlite::types::ToSql for $name {
            fn to_sql(&self) -> rusqlite::Result<rusqlite::types::ToSqlOutput<'_>> {
                Ok(self.to_string().into())
            }
        }

        impl rusqlite::types::FromSql for $name {
            fn column_result(value: rusqlite::types::ValueRef<'_>) -> rusqlite::types::FromSqlResult<Self> {
                value.as_str()?.parse().map_err(|error| rusqlite::types::FromSqlError::Other(Box::new(error)))
            }
        }
    )+};
}

sqlite_uuid!(JobId, NodeId);
