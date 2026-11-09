use super::*;

#[test]
fn maintenance_is_reversible_but_never_reopens_shutdown_or_bypasses_another_owner() {
    let lifecycle = Lifecycle::new(CancellationToken::new());
    let existing = lifecycle.admit().unwrap();
    let hold = lifecycle.hold(uuid::Uuid::now_v7()).unwrap();
    assert_eq!(lifecycle.admitted_count(), 1);
    let Err(_) = lifecycle.admit() else {
        panic!("operation must be rejected");
    };
    let Err(_) = lifecycle.hold(uuid::Uuid::now_v7()) else {
        panic!("operation must be rejected");
    };
    let other = Lifecycle::new(CancellationToken::new());
    let Err(_) = other.admit_maintenance(&hold) else {
        panic!("operation must be rejected");
    };
    drop(existing);
    assert_eq!(lifecycle.admitted_count(), 0);
    drop(lifecycle.admit_maintenance(&hold).unwrap());
    drop(hold);
    drop(lifecycle.admit().unwrap());
    let hold = lifecycle.hold(uuid::Uuid::now_v7()).unwrap();
    lifecycle.close();
    let Err(_) = lifecycle.admit_maintenance(&hold) else {
        panic!("operation must be rejected");
    };
    drop(hold);
    let Err(_) = lifecycle.admit() else {
        panic!("operation must be rejected");
    };
}
