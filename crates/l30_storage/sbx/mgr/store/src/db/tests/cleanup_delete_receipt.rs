//! The delete receipt a cleanup's row delete writes, which a teardown poll
//! reads to answer DELETED once the row is gone.

use pplx_space_common_core::types::{ExecutionGeneration, LocalArtifactId};

use super::state_write_fixtures::insert_row;
use crate::db::{begin_teardown_suspend, test_conn};
use crate::state::SandboxState;

/// A cleanup that deletes a row reads the row's latest teardown operation,
/// finished ones included, to cover its generation in the delete receipt. A
/// finished one whose payload doesn't decode names no generation; the delete
/// still goes through, covering the row's own.
#[test]
fn a_cleanup_delete_survives_a_teardown_operation_that_does_not_decode() {
    let conn = test_conn();
    let id = insert_row(&conn, SandboxState::Paused);
    begin_teardown_suspend(
        &conn,
        &id,
        ExecutionGeneration::new(3).unwrap(),
        LocalArtifactId::new(),
    )
    .unwrap();
    conn.execute(
        "UPDATE jobs SET payload = '{', status = 'failed' WHERE subject_id = ?1",
        crate::db::fixture::params![id],
    )
    .unwrap();
    super::state_write_fixtures::force_state(&conn, &id, SandboxState::Deleting);
    assert!(
        crate::db::set_cleanup_intent(&conn, &id, false, crate::db::CleanupCompletion::DeleteRow)
            .unwrap()
    );

    crate::db::complete_row_intent_cleanup(
        &conn,
        &id,
        crate::db::CleanupCompletion::DeleteRow,
        None,
    )
    .expect("the delete doesn't depend on the teardown operation decoding");
    assert!(crate::db::get_sandbox(&conn, &id).unwrap().is_none());
}
