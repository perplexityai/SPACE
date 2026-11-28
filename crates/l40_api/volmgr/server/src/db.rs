use std::path::Path;

use pplx_space_volmgr_store::Connection;
pub(crate) use pplx_space_volmgr_store::active_btrfs_upload_payloads;

/// Open the volume manager SQLite database. The service must run its embedded
/// migrations after taking ownership and before calling this function.
pub fn open(path: &str) -> pplx_space_volmgr_store::Result<Connection> {
    pplx_space_volmgr_store::database::open(Path::new(path))
}
