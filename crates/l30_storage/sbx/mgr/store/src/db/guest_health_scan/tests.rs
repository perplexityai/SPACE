use pplx_space_common_core::types::SandboxId;

use crate::db::fixture::insert_test_sandbox;
use crate::state::SandboxState;

use super::*;

#[test]
fn probe_contexts_follow_the_ids_and_report_missing_rows_as_none() {
    let conn = crate::db::test_conn();
    let [started, missing, stopped] = [(); 3].map(|()| SandboxId::new().to_string());
    insert_test_sandbox(&conn, &started, SandboxState::Started);
    insert_test_sandbox(&conn, &stopped, SandboxState::Stopped);
    conn.execute(
        "UPDATE sandboxes SET template = 'other-template' WHERE id = ?1",
        [&stopped],
    )
    .unwrap();
    let ids = [stopped.clone(), missing.clone(), started.clone()];

    let contexts: Vec<_> = health_probe_contexts_for_ids(&conn, &ids)
        .unwrap()
        .into_iter()
        .map(|(id, context)| (id.clone(), context.unwrap()))
        .collect();

    assert_eq!(
        contexts,
        [
            (
                stopped,
                Some(SandboxHealthProbeContext {
                    state: SandboxState::Stopped,
                    template: Some("other-template".to_owned()),
                })
            ),
            (missing, None),
            (
                started,
                Some(SandboxHealthProbeContext {
                    state: SandboxState::Started,
                    template: Some("seed".to_owned()),
                })
            ),
        ]
    );
}

#[test]
fn unreadable_template_does_not_suppress_health_probe_context() {
    let conn = crate::db::test_conn();
    let ids = [(); 2].map(|()| SandboxId::new().to_string());
    for id in &ids {
        insert_test_sandbox(&conn, id, SandboxState::Started);
    }
    conn.execute(
        "UPDATE sandboxes SET template = x'00' WHERE id = ?1",
        [&ids[0]],
    )
    .unwrap();

    let contexts = health_probe_contexts_for_ids(&conn, &ids).unwrap();
    assert_eq!(
        contexts[0].1.as_ref().unwrap(),
        &Some(SandboxHealthProbeContext {
            state: SandboxState::Started,
            template: None,
        }),
        "unreadable telemetry metadata must not suppress health probing",
    );
    assert_eq!(
        contexts[1].1.as_ref().unwrap(),
        &Some(SandboxHealthProbeContext {
            state: SandboxState::Started,
            template: Some("seed".to_owned()),
        }),
    );
}
