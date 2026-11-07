use super::*;
use crate::db::test_conn;

fn recorded(conn: &Connection) -> Vec<(String, DrainPrior)> {
    drain_evacuations(conn)
        .unwrap()
        .into_iter()
        .map(|row| (row.sandbox_id, row.prior))
        .collect()
}

/// A later drain suspend of the same sandbox keeps its first record, and
/// answers the prior it holds; a forget removes only its own, and a cancel
/// writes the seal row and forgets every record together.
#[test]
fn a_record_is_kept_and_a_cancel_forgets_every_one() {
    let conn = test_conn();
    assert_eq!(
        record_drain_suspend(&conn, "sbx-a", DrainPrior::Started).unwrap(),
        DrainPrior::Started
    );
    assert_eq!(
        record_drain_suspend(&conn, "sbx-a", DrainPrior::Paused).unwrap(),
        DrainPrior::Started,
        "a suspend that failed after its pause leaves the row paused"
    );
    record_drain_suspend(&conn, "sbx-b", DrainPrior::Paused).unwrap();
    record_drain_suspend(&conn, "sbx-c", DrainPrior::Started).unwrap();
    forget_drain_suspend(&conn, "sbx-c").unwrap();
    assert_eq!(
        recorded(&conn),
        [
            ("sbx-a".to_owned(), DrainPrior::Started),
            ("sbx-b".to_owned(), DrainPrior::Paused),
        ]
    );

    let cancelled = ActivationSealRecord {
        epoch: 9,
        sealed: false,
    };
    cancel_drain(&conn, cancelled).unwrap();
    assert_eq!(recorded(&conn), []);
    assert_eq!(activation_seal(&conn).unwrap(), Some(cancelled));
}
