use super::*;

#[test]
fn directory_census_never_returns_a_successful_truncation() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::create_dir(dir.path().join("one")).unwrap();
    std::fs::create_dir(dir.path().join("two")).unwrap();
    std::fs::write(dir.path().join("ignored-file"), []).unwrap();
    for (remaining, age) in [(1, Duration::ZERO), (100, Duration::from_secs(3))] {
        DirectoryBudget {
            remaining,
            started: Instant::now() - age,
        }
        .directories(dir.path())
        .unwrap_err();
    }
    assert_eq!(
        DirectoryBudget::default()
            .directories(dir.path())
            .unwrap()
            .len(),
        2
    );
    DirectoryBudget::default()
        .directories(&dir.path().join("ignored-file"))
        .unwrap_err();
    assert!(
        DirectoryBudget::default()
            .directories(&dir.path().join("absent"))
            .unwrap()
            .is_empty()
    );
}
