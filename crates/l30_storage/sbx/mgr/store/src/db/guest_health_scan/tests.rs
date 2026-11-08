use pplx_space_common_core::types::SandboxId;

use crate::db::fixture::insert_test_sandbox;
use crate::state::SandboxState;

use super::*;

#[test]
fn statuses_follow_the_ids_and_report_missing_rows_as_none() {
    let conn = crate::db::test_conn();
    let [started, missing, stopped] = [(); 3].map(|()| SandboxId::new().to_string());
    insert_test_sandbox(&conn, &started, SandboxState::Started);
    insert_test_sandbox(&conn, &stopped, SandboxState::Stopped);
    let ids = [stopped.clone(), missing.clone(), started.clone()];

    let statuses: Vec<_> = statuses_for_ids(&conn, &ids)
        .unwrap()
        .into_iter()
        .map(|(id, status)| (id.clone(), status.unwrap()))
        .collect();

    assert_eq!(
        statuses,
        [
            (stopped, Some(SandboxState::Stopped.into())),
            (missing, None),
            (started, Some(SandboxState::Started.into())),
        ]
    );
}
