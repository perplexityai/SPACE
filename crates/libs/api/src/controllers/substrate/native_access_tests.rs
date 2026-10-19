use super::{active, scope};

#[tokio::test]
async fn only_work_inside_the_scope_is_native() {
    assert!(!active(), "no request marks the task by default");
    assert!(scope(async { active() }).await);
    assert!(!active(), "the mark ends with the scope");
    let spawned = scope(async { tokio::spawn(async { active() }).await.unwrap() }).await;
    assert!(!spawned, "work spawned onto another task keeps the default");
}
