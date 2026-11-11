use super::{is_lock_contention, is_lock_not_available};
use pplx_space_db_test_fixtures::{raise_sqlstate, try_pg_test_db};
use pplx_space_sql_locator::sql;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[tokio::test]
async fn classifies_contention_sqlstates() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };

    // 55P03 is what a role-level `lock_timeout` expiry and a `NOWAIT` refusal
    // both raise, so both predicates must accept it.
    let lock_not_available = raise_sqlstate(&db.pool, "55P03").await;
    assert!(is_lock_not_available(&lock_not_available));
    assert!(is_lock_contention(&lock_not_available));

    for sqlstate in ["40001", "40P01"] {
        let error = raise_sqlstate(&db.pool, sqlstate).await;
        assert!(
            is_lock_contention(&error),
            "{sqlstate} must classify as contention"
        );
        assert!(
            !is_lock_not_available(&error),
            "{sqlstate} is not a lock-wait expiry"
        );
    }

    // 57014 is a `statement_timeout` cancellation: deliberately excluded, so
    // a caller that wants it retryable has to say so at its own call site.
    let cancelled = raise_sqlstate(&db.pool, "57014").await;
    assert!(!is_lock_contention(&cancelled));
    assert!(!is_lock_not_available(&cancelled));

    // A unique-violation is a real conflict, not contention: retrying it
    // reproduces it.
    let unique_violation = raise_sqlstate(&db.pool, "23505").await;
    assert!(!is_lock_contention(&unique_violation));
    assert!(!is_lock_not_available(&unique_violation));

    Ok(())
}

#[tokio::test]
async fn non_database_errors_are_not_contention() -> TestResult {
    // `RowNotFound` carries no SQLSTATE. Neither predicate may treat a
    // missing `code()` as a match, or every non-database failure would be
    // retried as contention.
    let error = sqlx::Error::RowNotFound;
    assert!(!is_lock_contention(&error));
    assert!(!is_lock_not_available(&error));
    Ok(())
}

/// The end-to-end shape the audit rests on: with a transaction-local
/// `lock_timeout` standing in for the role-level one, a blocked statement
/// aborts with a SQLSTATE these predicates classify — rather than holding its
/// pooled connection until `statement_timeout`.
#[tokio::test]
async fn a_bounded_lock_wait_classifies_as_contention() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };

    let mut blocker = db.pool.begin().await?;
    sqlx::query(sql!("SELECT pg_advisory_xact_lock(918273)"))
        .execute(&mut *blocker)
        .await?;

    let mut waiter = db.pool.begin().await?;
    sqlx::query(sql!("SELECT set_config('lock_timeout', '150ms', TRUE)"))
        .execute(&mut *waiter)
        .await?;
    let blocked = sqlx::query(sql!("SELECT pg_advisory_xact_lock(918273)"))
        .execute(&mut *waiter)
        .await
        .expect_err("the bounded advisory-lock wait must abort");
    assert!(is_lock_not_available(&blocked));
    assert!(is_lock_contention(&blocked));

    drop(waiter);
    blocker.rollback().await?;
    Ok(())
}
