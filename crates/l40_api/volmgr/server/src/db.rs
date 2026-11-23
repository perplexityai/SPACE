use rusqlite::Connection;

pub(crate) use pplx_space_volmgr_store::active_btrfs_upload_payloads;
pub use pplx_space_volmgr_store::{
    AttachmentSandboxKey, delete_attachment, get_attachment, list_attachments, upsert_attachment,
};

/// Open the volume manager SQLite database. The service must run its embedded
/// migrations after taking ownership and before calling this function.
pub fn open(path: &str) -> rusqlite::Result<Connection> {
    Connection::open(path)
}

#[cfg(test)]
mod tests;
