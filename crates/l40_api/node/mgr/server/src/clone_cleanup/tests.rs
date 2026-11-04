use super::*;

#[test]
fn disposal_budget_bounds_work_and_a_retry_finishes_partial_cleanup() {
    let directory = tempfile::tempdir().unwrap();
    for name in ["first", "second", "third"] {
        std::fs::write(directory.path().join(name), b"partial clone").unwrap();
    }
    let root = File::open(directory.path()).unwrap();
    let mut budget = Budget {
        remaining: 1,
        deadline: Instant::now() + Duration::from_secs(60),
    };
    assert!(matches!(
        remove_contents(&root, &mut budget, 0),
        Err(CloneDisposalError::BudgetExhausted)
    ));
    assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 2);
    let mut retry_budget = Budget {
        remaining: 3,
        deadline: Instant::now() + Duration::from_secs(60),
    };
    remove_contents(&root, &mut retry_budget, 0).unwrap();
    assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 0);
}
