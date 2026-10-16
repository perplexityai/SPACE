//! Test-only fault: fail one restore prepare for one sandbox.
//!
//! Kind smoke scenarios need a restore that fails after its activation claim,
//! so the node rolls the attempt back exactly as it does for a real prepare
//! failure (for example a disk-admission refusal). Nothing in a running
//! cluster produces that failure on demand, and every other lever (filling the
//! disk, restarting a service) also moves placement or races the restore.
//!
//! The hook is compiled in only with the `test-hooks` feature, which release
//! builds never enable. Without it, [`take_restore_prepare_fault`] is a
//! constant `None` and nothing here reads the environment or the filesystem.
//!
//! With the feature, the test-hook variable [`RESTORE_PREPARE_FAULT_DIR_ENV`]
//! names a directory. A file in it named exactly after a sandbox id arms one
//! fault for that sandbox. The next restore prepare of that sandbox removes
//! the file and fails instead of calling node-manager. Removing the file is
//! what fires the fault, so it fires at most once per file, and two restores
//! racing for the same marker cannot both fail.

/// Test hook: directory of one-shot restore-prepare fault markers.
#[cfg(any(test, feature = "test-hooks"))]
pub(super) const RESTORE_PREPARE_FAULT_DIR_ENV: &str =
    "SPACE_SBXMGR_TEST_RESTORE_PREPARE_FAULT_DIR";

/// Message of the injected failure. Its code, `RESOURCE_EXHAUSTED`, is the
/// one a real disk-admission refusal from node-manager carries.
#[cfg(any(test, feature = "test-hooks"))]
pub(super) const INJECTED_FAULT_MESSAGE: &str = "test hook: injected restore prepare failure";

/// Consume this sandbox's armed fault, if there is one.
///
/// Returns the status restore prepare must fail with, or `None` to call
/// node-manager as usual.
#[cfg(feature = "test-hooks")]
pub(super) fn take_restore_prepare_fault(sandbox_id: &str) -> Option<tonic::Status> {
    take_restore_prepare_fault_in(&pplx_space_common_core::env::ProcessEnvironment, sandbox_id)
}

/// Without `test-hooks` there is no fault to take.
#[cfg(not(feature = "test-hooks"))]
#[inline(always)]
pub(super) fn take_restore_prepare_fault(_sandbox_id: &str) -> Option<tonic::Status> {
    None
}

/// [`take_restore_prepare_fault`] over a given environment.
#[cfg(any(test, feature = "test-hooks"))]
fn take_restore_prepare_fault_in(
    env: &dyn pplx_space_common_core::env::Environment,
    sandbox_id: &str,
) -> Option<tonic::Status> {
    let dir = pplx_space_common_core::env::test_hook(env, RESTORE_PREPARE_FAULT_DIR_ENV)?;
    let fired = consume_fault_marker(std::path::Path::new(&dir), sandbox_id);
    fired.then(|| {
        tracing::warn!(
            sandbox_id,
            hook = RESTORE_PREPARE_FAULT_DIR_ENV,
            "test hook fired: failing this restore prepare once"
        );
        tonic::Status::resource_exhausted(INJECTED_FAULT_MESSAGE)
    })
}

/// Remove `<dir>/<sandbox_id>`; true only if this call removed it.
///
/// The id must be a single plain path component, so a marker can never name
/// a file outside `dir`.
#[cfg(any(test, feature = "test-hooks"))]
fn consume_fault_marker(dir: &std::path::Path, sandbox_id: &str) -> bool {
    let plain_component = !sandbox_id.is_empty()
        && !sandbox_id.starts_with('.')
        && !sandbox_id.contains(['/', '\\', '\0']);
    plain_component && std::fs::remove_file(dir.join(sandbox_id)).is_ok()
}

#[cfg(test)]
mod tests;
