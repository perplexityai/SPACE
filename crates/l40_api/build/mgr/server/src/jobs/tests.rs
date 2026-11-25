use super::*;
use pplx_space_build_mgr_store::get_build_result;
use rusqlite::params;

fn test_db() -> SharedConnection {
    let conn = rusqlite::Connection::open_in_memory().unwrap();
    crate::tests::initialize_schema(&conn);

    conn.execute(
        "INSERT INTO jobs (job_id, kind, payload) VALUES ('019d84bd-280d-7331-99c6-000000000001', ?1, '{}')",
        params![BUILD_JOB_KIND],
    )
    .unwrap();
    SharedConnection::new(database::Connection::from_raw(conn))
}

#[test]
fn stores_and_reads_build_result() {
    let db = test_db();
    let result = BuildJobResult {
        template_id: "seed".to_string(),
        state: BuildTerminalState::Succeeded,
        content_hash: Some("sha256:abc".to_string()),
        error: None,
    };

    store_build_result(
        &db,
        &"019d84bd-280d-7331-99c6-000000000001".parse().unwrap(),
        &result,
    )
    .unwrap();
    let stored = {
        let conn = db.lock().unwrap();
        get_build_result(
            &conn,
            &"019d84bd-280d-7331-99c6-000000000001".parse().unwrap(),
        )
        .unwrap()
        .unwrap()
    };

    assert_eq!(stored.template_id, "seed");
    assert_eq!(stored.state, BuildTerminalState::Succeeded);
    assert_eq!(stored.content_hash.as_deref(), Some("sha256:abc"));
    assert_eq!(stored.error, None);
}
