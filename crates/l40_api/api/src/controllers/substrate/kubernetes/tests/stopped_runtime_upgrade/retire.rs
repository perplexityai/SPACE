//! Retiring the upgrade's execution: a fenced retire that misses isn't done,
//! and an execution that wasn't retired answers retryably until a retry retires
//! it.

use super::*;

/// An upgrade whose execution couldn't be retired, because its pod's delete
/// failed or found a pod with another UID at the name (a UID precondition
/// failure), leaves the execution and its pod, which nothing but a retry takes
/// over, so the answer is retryable: for an upgrade that reached the target,
/// whose backup is already enqueued, and for a rollback. The retry retires
/// and only then answers: the upgrade, or a retryable refusal for the
/// rollback, which the node reports again for the retry's pod.
#[rstest::rstest]
#[case::upgraded(Report::Upgraded { publish: true }, "delete", false)]
#[case::rolled_back(Report::RolledBack, "rolled back", false)]
#[case::uid_changed(Report::Upgraded { publish: true }, "no longer the one", true)]
#[tokio::test(flavor = "current_thread")]
async fn an_unretired_execution_answers_retryably_until_a_retry_retires_it(
    #[case] report: Report,
    #[case] reason: &str,
    #[case] uid_precondition_fails: bool,
) -> Result<(), BoxError> {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let s = subject().await?;
    s.node().report(report);
    if uid_precondition_fails {
        s.pods()
            .precondition_failing_deletes
            .store(1, Ordering::SeqCst);
    } else {
        // A rollback's pod is deleted by the wait that settles on it, then
        // by the retire: both fail.
        let failing = if matches!(report, Report::RolledBack) {
            2
        } else {
            1
        };
        s.pods().failing_deletes.store(failing, Ordering::SeqCst);
    }

    let error = s.upgrade().await.unwrap_err();

    let SubstrateError::Unavailable { message } = &error else {
        panic!("{error:?}");
    };
    assert!(message.contains(reason), "{message}");
    s.assert_kept_for_a_retry("after the unretired execution")
        .await?;

    let outcome = s.upgrade().await;

    if matches!(report, Report::RolledBack) {
        let error = outcome.unwrap_err();
        assert!(
            matches!(error, SubstrateError::Unavailable { .. }),
            "{error:?}"
        );
        assert!(s.enqueued_backups().is_empty());
    } else {
        upgraded(&outcome?);
    }
    s.assert_stopped_without_execution("after the retry").await
}

/// Retiring an upgrade's execution that changed after it was read, so the
/// fenced update matches no row, isn't done: the execution stays active with
/// what it recorded, and the retire answers a retryable error rather than
/// letting the operation report success.
#[tokio::test(flavor = "current_thread")]
async fn a_retire_whose_fenced_update_misses_is_not_done() -> Result<(), BoxError> {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let s = subject().await?;
    held_execution(&s, "stopped", -3600).await?;
    let attempt = left_by(&s).await?;
    // Stands in for a change between the retire's read and its fenced
    // update: the update that would finish the execution matches nothing.
    sqlx::raw_sql(sql!(
        "CREATE FUNCTION keep_execution_active() RETURNS trigger AS $$
         BEGIN
             IF OLD.active AND NOT NEW.active THEN RETURN NULL; END IF;
             RETURN NEW;
         END $$ LANGUAGE plpgsql;
         CREATE TRIGGER keep_execution_active
             BEFORE UPDATE ON sandbox_k8s_executions
             FOR EACH ROW EXECUTE FUNCTION keep_execution_active();"
    ))
    .execute(&s.db.pool)
    .await?;

    let error = s
        .substrate
        .retire_upgrade_boot(s.sandbox_id, 7, attempt, None, None)
        .await
        .unwrap_err();

    assert!(
        matches!(error, SubstrateError::Unavailable { .. }),
        "{error:?}"
    );
    assert!(
        s.active_execution()
            .await?
            .is_some_and(|(generation, _, _)| generation == 7)
    );
    Ok(())
}

/// A retire whose attempt no longer owns the execution deletes the pod at
/// its name only when nothing adopted the execution, since an adopter owns
/// the pod with it, and an upgrade whose backup it can't confirm isn't done
/// either way. A boot that never saw its pod's UID (a rollback's receipt
/// retired the execution before it was recorded) deletes the pod there that
/// carries its operation, and leaves one that doesn't.
#[rstest::rstest]
#[case::adopted(true, false, true, true)]
#[case::adopted_before_its_backup(true, true, true, true)]
#[case::finished_elsewhere(false, false, true, true)]
#[case::finished_before_its_backup(false, true, true, true)]
#[case::finished_before_its_uid_was_recorded(false, false, false, true)]
#[case::finished_with_another_operations_pod(false, false, false, false)]
#[tokio::test(flavor = "current_thread")]
async fn a_retire_that_lost_its_execution_keeps_an_adopted_pod_and_an_unconfirmed_backup(
    #[case] adopted: bool,
    #[case] backup: bool,
    #[case] uid_recorded: bool,
    #[case] pod_carries_the_operation: bool,
) -> Result<(), BoxError> {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let s = subject().await?;
    held_execution(&s, "stopped", 3600).await?;
    if !adopted {
        sqlx::query(sql!(
            "UPDATE sandbox_k8s_executions SET active = FALSE WHERE sandbox_id = $1"
        ))
        .bind(s.sandbox_id)
        .execute(&s.db.pool)
        .await?;
    }
    let operation_id = uuid::Uuid::now_v7();
    let carried = if pod_carries_the_operation {
        operation_id
    } else {
        uuid::Uuid::now_v7()
    };
    s.pods().annotate(&[(
        SANDBOX_RUNTIME_UPGRADE_OPERATION_ID_ANNOTATION,
        &carried.to_string(),
    )]);
    let pod_name = format!("space-sbx-{}-g7", s.sandbox_id.as_uuid().simple());
    let booted = crate::controllers::substrate::kubernetes::adapter::BootedPod {
        uid: uid_recorded
            .then(|| pplx_space_common_core::types::PodUid::parse(format!("{pod_name}-uid")))
            .transpose()?,
        name: pod_name,
        operation_id,
    };

    let retired = s
        .substrate
        .retire_upgrade_boot(
            s.sandbox_id,
            7,
            pplx_space_common_core::types::SandboxActivationAttemptId::new(),
            Some(booted),
            backup.then(tokio::time::Instant::now),
        )
        .await;

    assert_eq!(retired.is_err(), backup, "{retired:?}");
    assert_eq!(s.pods().exists(), adopted || !pod_carries_the_operation);
    Ok(())
}

/// A boot that settled without reaching the target, whose execution then
/// couldn't be retired, answers retryably (503): only a retry takes the
/// execution over. Except a retire no retry can get past, an execution whose
/// pod name doesn't parse (`Failed`), which stays `Failed`, as it does for an
/// upgrade that reached the target.
#[test]
fn an_unretired_settled_failure_is_retryable_unless_no_retry_can_retire_it() {
    use crate::controllers::substrate::kubernetes::adapter::unretired_settled_failure;
    let rolled_back = || SubstrateError::Unavailable {
        message: "rolled back".to_string(),
    };
    let unretirable = unretired_settled_failure(
        rolled_back(),
        SubstrateError::Failed {
            message: "invalid pod".to_string(),
        },
    );
    assert!(
        matches!(&unretirable, SubstrateError::Failed { message }
            if message.contains("rolled back") && message.contains("invalid pod")),
        "{unretirable:?}"
    );
    let unretired = unretired_settled_failure(
        rolled_back(),
        SubstrateError::Unavailable {
            message: "delete failed".to_string(),
        },
    );
    assert!(
        matches!(&unretired, SubstrateError::Unavailable { message }
            if message.contains("rolled back") && message.contains("delete failed")),
        "{unretired:?}"
    );
}
