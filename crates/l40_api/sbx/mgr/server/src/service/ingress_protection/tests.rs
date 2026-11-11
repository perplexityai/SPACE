use super::*;

#[test]
fn completion_is_published_with_zero_count_and_old_execution_is_isolated() {
    let old = Protection::default();
    let replacement = Protection::default();
    let first = old.acquire();
    let second = old.acquire();
    let current = replacement.acquire();
    drop(first);
    assert_eq!(old.observation().0, 1);
    assert_eq!(old.observation().1, None);
    drop(second);
    let (count, release, age) = old.observation();
    assert_eq!(count, 0);
    assert!(release.is_some());
    assert!(age.is_none());
    assert_eq!(replacement.observation().0, 1);
    // Failed persistence leaves the observation available for another scan.
    assert_eq!(old.observation().1, release);
    old.consumed(release.unwrap());
    assert_eq!(old.observation().1, None);
    drop(current);
}
