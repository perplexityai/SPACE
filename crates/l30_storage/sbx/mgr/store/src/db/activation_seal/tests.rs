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
    };
    write_activation_seal(&conn, sealed).unwrap();
    assert_eq!(activation_seal(&conn).unwrap(), Some(sealed));

    let unsealed = ActivationSealRecord {
        sealed_epoch: None,
        ..sealed
    };
    write_activation_seal(&conn, unsealed).unwrap();
    assert_eq!(activation_seal(&conn).unwrap(), Some(unsealed));

    for unstorable in [
        ActivationSealRecord {
            sealed_epoch: Some(0),
            sealed_since_ms: 1,
        },
        ActivationSealRecord {
            sealed_epoch: Some(u64::MAX),
            sealed_since_ms: 1,
        },
        ActivationSealRecord {
            sealed_epoch: Some(1),
            sealed_since_ms: u64::MAX,
        },
    ] {
        write_activation_seal(&conn, unstorable).unwrap_err();
        assert_eq!(activation_seal(&conn).unwrap(), Some(unsealed));
    }
}
