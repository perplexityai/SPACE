use super::*;
use crate::db::test_conn;

fn recorded(conn: &Connection) -> Vec<(String, DrainPrior)> {
    drain_evacuations(conn)
        .unwrap()
        .into_iter()
        .map(|row| (row.sandbox_id, row.prior))
        .collect()
}

/// A later drain suspend of the same sandbox replaces its record, a forget
/// removes only its own, and a cancel writes the seal row and forgets every
/// record together.
#[test]
fn a_record_is_replaced_and_a_cancel_forgets_every_one() {
    let conn = test_conn();
    record_drain_suspend(&conn, "sbx-a", DrainPrior::Paused).unwrap();
    record_drain_suspend(&conn, "sbx-a", DrainPrior::Started).unwrap();
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
