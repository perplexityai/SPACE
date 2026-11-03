//! The Native `files:*` operations, and what each does with its
//! `Idempotency-Key`.

use std::fmt;

use pplx_space_public_api::routes::{
    CREATE_DIRECTORY, DISABLE_FILE_CHANGES, ENABLE_FILE_CHANGES, LIST_FILES, LIST_MODIFIED_FILES,
    NativeOperation, READ_FILE, REMOVE_FILE, RENAME_FILE, STAT_FILE, WRITE_FILE, WRITE_FILE_BATCH,
};

/// One Native `files:*` route.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum FileOperation {
    Stat,
    List,
    Read,
    Write,
    WriteBatch,
    Mkdir,
    Remove,
    Rename,
    ChangesEnable,
    ChangesDisable,
    Modified,
}

impl FileOperation {
    pub const ALL: [Self; 11] = [
        Self::Stat,
        Self::List,
        Self::Read,
        Self::Write,
        Self::WriteBatch,
        Self::Mkdir,
        Self::Remove,
        Self::Rename,
        Self::ChangesEnable,
        Self::ChangesDisable,
        Self::Modified,
    ];

    pub fn route(self) -> NativeOperation {
        match self {
            Self::Stat => STAT_FILE,
            Self::List => LIST_FILES,
            Self::Read => READ_FILE,
            Self::Write => WRITE_FILE,
            Self::WriteBatch => WRITE_FILE_BATCH,
            Self::Mkdir => CREATE_DIRECTORY,
            Self::Remove => REMOVE_FILE,
            Self::Rename => RENAME_FILE,
            Self::ChangesEnable => ENABLE_FILE_CHANGES,
            Self::ChangesDisable => DISABLE_FILE_CHANGES,
            Self::Modified => LIST_MODIFIED_FILES,
        }
    }

    /// The route's `files:` action.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Stat => "stat",
            Self::List => "list",
            Self::Read => "read",
            Self::Write => "write",
            Self::WriteBatch => "write_batch",
            Self::Mkdir => "mkdir",
            Self::Remove => "remove",
            Self::Rename => "rename",
            Self::ChangesEnable => "changes_enable",
            Self::ChangesDisable => "changes_disable",
            Self::Modified => "modified",
        }
    }

    /// Whether the request must carry a valid `Idempotency-Key`.
    pub fn takes_idempotency_key(self) -> bool {
        self.binds_idempotency_key() || matches!(self, Self::ChangesEnable | Self::ChangesDisable)
    }

    /// Whether the key is bound to the request's fingerprint before the
    /// sandbox sees it, so a changed request under it is refused.
    pub fn binds_idempotency_key(self) -> bool {
        self.sends_guest_key() || matches!(self, Self::Mkdir | Self::Remove)
    }

    /// Whether the sandbox replays the request by its [`crate::guest_key`].
    pub fn sends_guest_key(self) -> bool {
        matches!(self, Self::Write | Self::WriteBatch | Self::Rename)
    }

    /// A mutation the sandbox runs without its key, whose failures
    /// [`crate::unprotected_mutation_error`] maps.
    pub fn is_unprotected_mutation(self) -> bool {
        matches!(
            self,
            Self::Mkdir | Self::Remove | Self::ChangesEnable | Self::ChangesDisable
        )
    }
}

impl fmt::Display for FileOperation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}
