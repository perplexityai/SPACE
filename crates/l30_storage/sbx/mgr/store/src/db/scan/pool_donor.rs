//! A paused pool member that may donate itself to a create.

use crate::db::{Connection, Result, SandboxOwner, SandboxRow};

/// The oldest paused member of `template`'s pool, with `content_hash` and
/// `required_user` when given. A hint only: claiming it is a separate guarded
/// write that rechecks every condition.
pub fn pool_donor(
    conn: &Connection,
    template: &str,
    content_hash: Option<&str>,
    required_user: Option<&SandboxOwner>,
) -> Result<Option<SandboxRow>> {
    crate::db::find_pool_sandbox(conn, template, content_hash, required_user)
}

/// Up to `limit` paused members of `template`'s pool, oldest first, with
/// `content_hash` and `required_user` when given. Hints only, like
/// [`pool_donor`]: a caller that skips some still claims one through the
/// guarded write.
pub fn pool_donors(
    conn: &Connection,
    template: &str,
    content_hash: Option<&str>,
    required_user: Option<&SandboxOwner>,
    limit: u32,
) -> Result<Vec<SandboxRow>> {
    crate::db::find_pool_sandboxes(conn, template, content_hash, required_user, limit)
}
