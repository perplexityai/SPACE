use pplx_space_common_core::types::SandboxId;

use crate::db::fixture::insert_test_sandbox;
use crate::state::SandboxState;

use super::*;

#[test]
fn state_labels_follow_the_ids_and_report_missing_rows_as_none() {
    let conn = crate::db::test_conn();
    let [started, missing, stopped] = [(); 3].map(|()| SandboxId::new().to_string());
    insert_test_sandbox(&conn, &started, SandboxState::Started);
    insert_test_sandbox(&conn, &stopped, SandboxState::Stopped);
    let ids = [stopped.clone(), missing.clone(), started.clone()];

    let labels: Vec<_> = state_labels_for_ids(&conn, &ids)
        .unwrap()
        .into_iter()
        .map(|(id, label)| (id.clone(), label.unwrap()))
        .collect();

    assert_eq!(
        labels,
        [
            (stopped, Some("stopped".to_owned())),
            (missing, None),
            (started, Some("started".to_owned())),
        ]
    );
}
