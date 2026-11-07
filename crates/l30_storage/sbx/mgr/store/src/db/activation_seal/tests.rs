use super::*;
use crate::db::test_conn;

/// A seal and the unseal that follows it both read back as written, and a
/// zero epoch or a value SQLite cannot hold fails without touching the
/// stored row.
#[test]
fn activation_seal_round_trips_and_refuses_unstorable_values() {
    let conn = test_conn();
    assert_eq!(activation_seal(&conn).unwrap(), None);

    let sealed = ActivationSealRecord {
        sealed_epoch: Some(7),
        sealed_since_ms: 1_700_000_000_123,
        last_cancelled_epoch: None,
    };
    write_activation_seal(&conn, sealed).unwrap();
    assert_eq!(activation_seal(&conn).unwrap(), Some(sealed));

    let unsealed = ActivationSealRecord {
        sealed_epoch: None,
        last_cancelled_epoch: Some(7),
        ..sealed
    };
    write_activation_seal(&conn, unsealed).unwrap();
    assert_eq!(activation_seal(&conn).unwrap(), Some(unsealed));

    for unstorable in [
        ActivationSealRecord {
            sealed_epoch: Some(0),
            sealed_since_ms: 1,
            last_cancelled_epoch: None,
        },
        ActivationSealRecord {
            sealed_epoch: Some(u64::MAX),
            sealed_since_ms: 1,
            last_cancelled_epoch: None,
        },
        ActivationSealRecord {
            sealed_epoch: Some(1),
            sealed_since_ms: u64::MAX,
            last_cancelled_epoch: None,
        },
    ] {
        write_activation_seal(&conn, unstorable).unwrap_err();
        assert_eq!(activation_seal(&conn).unwrap(), Some(unsealed));
    }
}

/// A cancelled epoch of zero or one SQLite cannot hold fails without
/// touching the row.
#[test]
fn a_cancelled_epoch_must_be_storable() {
    let conn = test_conn();
    for unstorable in [Some(0), Some(u64::MAX)] {
        write_activation_seal(
            &conn,
            ActivationSealRecord {
                sealed_epoch: None,
                sealed_since_ms: 0,
                last_cancelled_epoch: unstorable,
            },
        )
        .unwrap_err();
        assert_eq!(activation_seal(&conn).unwrap(), None);
    }
}

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

/// Every prior the CHECK admits reads back, and a value outside it is
/// refused by the table.
#[test]
fn every_prior_round_trips_and_others_are_refused() {
    let conn = test_conn();
    for (index, prior) in DrainPrior::ALL.into_iter().enumerate() {
        let id = format!("sbx-{index}");
        begin_drain_evacuation(&conn, &id, 1, prior, 0).unwrap();
    }
    let read: Vec<_> = drain_evacuations(&conn)
        .unwrap()
        .into_iter()
        .map(|row| row.prior)
        .collect();
    assert_eq!(read, DrainPrior::ALL);
    conn.execute(
        "INSERT INTO drain_evacuations (sandbox_id, epoch, prior, recorded_at_ms)
         VALUES ('sbx-x', 1, 'suspended', 0)",
        [],
    )
    .unwrap_err();
}
