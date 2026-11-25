use pplx_space_db_test_fixtures::try_pg_test_db;
use pplx_space_test_env::TestResult;

use super::super::ownership_test_fixture::{
    execution_state, execution_version, insert_execution, insert_sandbox,
};
use super::retire_stale_execution;

#[tokio::test(flavor = "current_thread")]
async fn retire_stale_execution_retires_only_the_observed_execution() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let sandbox_id = insert_sandbox(pool, "suspended").await?;
    let execution = insert_execution(pool, sandbox_id, 3, "running", true, Some("uid-3")).await?;
    let before = execution_version(pool, execution.id).await?;

    // The losing cases: a different generation, pod name or pod UID (a NULL
    // UID included) is another execution, and nothing changes.
    for (generation, pod_name, pod_uid) in [
        (4, execution.pod_name.as_str(), Some("uid-3")),
        (3, "sbx-other", Some("uid-3")),
        (3, execution.pod_name.as_str(), Some("uid-other")),
        (3, execution.pod_name.as_str(), None),
    ] {
        assert!(!retire_stale_execution(pool, sandbox_id, generation, pod_name, pod_uid).await?);
    }
    assert_eq!(execution_version(pool, execution.id).await?, before);

    assert!(retire_stale_execution(pool, sandbox_id, 3, &execution.pod_name, Some("uid-3")).await?);
    assert_eq!(
        execution_state(pool, execution.id).await?,
        (false, "suspended".to_string())
    );
    // Retired, it no longer matches `active`.
    let retired = execution_version(pool, execution.id).await?;
    assert!(
        !retire_stale_execution(pool, sandbox_id, 3, &execution.pod_name, Some("uid-3")).await?
    );
    assert_eq!(execution_version(pool, execution.id).await?, retired);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn retire_stale_execution_matches_a_null_pod_uid_null_safe() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let sandbox_id = insert_sandbox(pool, "suspended").await?;
    let execution = insert_execution(pool, sandbox_id, 1, "creating", true, None).await?;
    assert!(!retire_stale_execution(pool, sandbox_id, 1, &execution.pod_name, Some("")).await?);
    assert!(retire_stale_execution(pool, sandbox_id, 1, &execution.pod_name, None).await?);
    Ok(())
}
