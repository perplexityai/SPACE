use super::*;
use crate::db::test_conn;

/// An evacuation keeps its prior and artifact when a later epoch records it
/// again, moves to the later epoch only, and is forgotten with the epoch
/// that holds it; a later epoch's row survives the cancel of an earlier one.
#[test]
fn an_evacuation_moves_forward_and_goes_with_its_cancelled_epoch() {
    let conn = test_conn();
    begin_drain_evacuation(&conn, "sbx-a", 5, DrainPrior::Paused, 1).unwrap();
    assert!(complete_drain_evacuation(&conn, "sbx-a", "art-a").unwrap());
    begin_drain_evacuation(&conn, "sbx-a", 9, DrainPrior::Started, 2).unwrap();
    begin_drain_evacuation(&conn, "sbx-a", 3, DrainPrior::Started, 3).unwrap();
    begin_drain_evacuation(&conn, "sbx-b", 5, DrainPrior::Stopped, 4).unwrap();
    assert!(!complete_drain_evacuation(&conn, "sbx-none", "art").unwrap());
    assert_eq!(
        drain_evacuations(&conn).unwrap(),
        vec![
            DrainEvacuation {
                sandbox_id: "sbx-a".into(),
                epoch: 9,
                prior: DrainPrior::Paused,
                artifact_id: Some("art-a".into()),
            },
            DrainEvacuation {
                sandbox_id: "sbx-b".into(),
                epoch: 5,
                prior: DrainPrior::Stopped,
                artifact_id: None,
            },
        ]
    );
    assert_eq!(forget_drain_evacuations_through(&conn, 5).unwrap(), 1);
    assert_eq!(
        drain_evacuations(&conn)
            .unwrap()
            .into_iter()
            .map(|row| row.sandbox_id)
            .collect::<Vec<_>>(),
        ["sbx-a"]
    );
}
