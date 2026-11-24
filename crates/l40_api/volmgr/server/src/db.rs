use std::path::Path;

pub(crate) use pplx_space_volmgr_store::active_btrfs_upload_payloads;
pub use pplx_space_volmgr_store::{
    AttachmentSandboxKey, Connection, delete_attachment, get_attachment, list_attachments,
    upsert_attachment,
};

/// Open the volume manager SQLite database. The service must run its embedded
/// migrations after taking ownership and before calling this function.
pub fn open(path: &str) -> pplx_space_volmgr_store::Result<Connection> {
    pplx_space_volmgr_store::database::open(Path::new(path))
}

#[cfg(test)]
mod tests;
