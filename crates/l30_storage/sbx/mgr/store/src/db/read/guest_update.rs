use crate::db::{Connection, Result, guest_updates::Record};

pub fn guest_update(conn: &Connection, operation: uuid::Uuid) -> Result<Option<Record>> {
    crate::db::guest_updates::record(conn, operation)
}

pub fn active_guest_update(conn: &Connection, sandbox: &str) -> Result<Option<Record>> {
    crate::db::guest_updates::active(conn, sandbox)
}

pub fn guest_update_retry_ready(conn: &Connection, record: &Record) -> Result<bool> {
    crate::db::guest_updates::retry_ready(conn, record)
}
