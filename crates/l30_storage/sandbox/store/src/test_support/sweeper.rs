//! The transient-lifecycle sweeper's production statements, exposed for
//! other crates' tests that run them directly.
//!
//! Each item passes through to the [`crate::sweeper`] item of the same name,
//! which that module's verbs bind; nothing here copies a statement.

use pplx_space_sandbox_model::SandboxLifecycle;

use crate::sweeper::FailGuard;

/// [`crate::sweeper::list_command_ready_pending`]'s candidate select.
pub const LIST_COMMAND_READY_PENDING_SQL: &str = crate::sweeper::LIST_COMMAND_READY_PENDING_SQL;

/// [`crate::sweeper::heal_command_ready_pending`]'s marker clear.
pub const HEAL_COMMAND_READY_PENDING_SQL: &str = crate::sweeper::HEAL_COMMAND_READY_PENDING_SQL;

/// [`crate::sweeper::reap_command_ready_pending`]'s fail CAS.
pub const REAP_COMMAND_READY_PENDING_SQL: &str = crate::sweeper::REAP_COMMAND_READY_PENDING_SQL;

/// [`crate::sweeper::repair_failed_active_execution`]'s repair CAS.
pub const REPAIR_FAILED_ACTIVE_EXECUTION_SQL: &str =
    crate::sweeper::REPAIR_FAILED_ACTIVE_EXECUTION_SQL;

/// [`crate::sweeper::fail_aged_candidate`]'s per-row fail CAS.
pub fn fail_candidate_query(lifecycle: SandboxLifecycle, guard: FailGuard) -> String {
    crate::sweeper::fail_candidate_query(lifecycle, guard)
}

/// [`crate::sweeper::converge_restoring`]'s converge CAS.
pub fn converge_restoring_query() -> String {
    crate::sweeper::converge_restoring_query()
}

/// [`crate::sweeper::revert_to_suspended`]'s revert CAS.
pub fn revert_to_suspended_query() -> String {
    crate::sweeper::revert_to_suspended_query()
}

/// [`crate::sweeper::undo_revert_to_suspended`]'s compensation.
pub fn undo_revert_to_suspended_query() -> String {
    crate::sweeper::undo_revert_to_suspended_query()
}
