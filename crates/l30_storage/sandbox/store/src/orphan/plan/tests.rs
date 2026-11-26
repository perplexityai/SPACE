use pplx_space_db_test_fixtures::try_pg_test_db;
use pplx_space_k8s_ids::PodUid;
use pplx_space_sandbox_model::SandboxLifecycle;
use pplx_space_test_env::TestResult;

use super::super::{classify_guard_from_sql, classify_guard_lock_sql, classify_guard_sql};
use super::*;
use crate::k8s_executions::ownership_test_fixture::{
    execution_key_share_blocked, insert_execution, insert_sandbox, sandbox_key_share_blocked,
};

const STOP: &[&str] = &["failed", "quarantined", "deleted"];
const LIVE: &[&str] = &["started"];
const PENDING: &[&str] = &["creating", "starting"];
const ACTIVE_REQUIRED: &[&str] = &["started", "creating", "starting"];

fn pod_name_guard<'a>(name: &'a str, uid: Option<&'a PodUid>) -> OrphanGuard<'a> {
    OrphanGuard::PodName {
        name,
        uid,
        live_pod_required_lifecycles: LIVE,
        pending_pod_lifecycles: PENDING,
        create_age_threshold_secs: 300,
    }
}

fn normalized(sql: &str) -> String {
    sql.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// Each statement reads the guard the resolution writes in `crate::orphan`
/// re-check: the same `FROM` items, predicate and lock targets.
#[test]
fn each_plan_spells_the_classification_guard_the_resolutions_use() {
    let uid = PodUid::parse("uid-1").expect("a pod UID");
    let guards = [
        (
            pod_name_guard("pod", Some(&uid)),
            &*LOCK_POD_NAME_PLAN,
            &*POD_NAME_PLAN,
        ),
        (
            OrphanGuard::Forgotten {
                active_execution_required_lifecycles: ACTIVE_REQUIRED,
            },
            &*LOCK_FORGOTTEN_PLAN,
            &*FORGOTTEN_PLAN,
        ),
        (
            OrphanGuard::LegacyDeleting,
            &*LOCK_LEGACY_DELETING_PLAN,
            &*LEGACY_DELETING_PLAN,
        ),
    ];
    // The six statements share one select list, after their `sql!` locators:
    // they differ from their guard's `FROM` items on.
    let select_list = |statement: &str| {
        let (_, statement) = statement
            .split_once("*/ ")
            .expect("each statement carries its locator");
        let statement = normalized(statement);
        let (select_list, _) = statement
            .split_once(" AS node_reports_copy FROM ")
            .expect("each statement ends its select list with the node report");
        select_list.to_string()
    };
    let shared = select_list(&LOCK_POD_NAME_PLAN);
    for (_, locked, unlocked) in &guards {
        assert_eq!(select_list(locked), shared);
        assert_eq!(select_list(unlocked), shared);
    }

    for (guard, locked, unlocked) in guards {
        let tail = format!(
            "FROM {} WHERE s.id = $1 AND s.lifecycle <> ALL($2) AND {}",
            classify_guard_from_sql(&guard),
            classify_guard_sql(&guard),
        );
        let lock = format!(" FOR UPDATE OF {}", classify_guard_lock_sql(&guard));
        assert!(
            normalized(locked).ends_with(&normalized(&format!("{tail}{lock}"))),
            "{guard:?}: {locked}"
        );
        assert!(
            normalized(unlocked).ends_with(&normalized(&tail)),
            "{guard:?}: {unlocked}"
        );
    }
}

#[tokio::test(flavor = "current_thread")]
async fn lock_plan_locks_the_row_and_the_matched_execution_for_a_missing_pod() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let sandbox_id = insert_sandbox(pool, "started").await?;
    let execution = insert_execution(pool, sandbox_id, 1, "running", true, Some("uid-1")).await?;
    let uid = PodUid::parse("uid-1").expect("a pod UID");
    let other_uid = PodUid::parse("uid-other").expect("a pod UID");

    // The losing cases: another pod name or UID, or a stop lifecycle, match
    // nothing and lock nothing.
    for guard in [
        pod_name_guard("sbx-other", Some(&uid)),
        pod_name_guard(&execution.pod_name, Some(&other_uid)),
        pod_name_guard(&execution.pod_name, None),
    ] {
        let mut tx = pool.begin().await?;
        assert!(
            lock_plan(&mut *tx, sandbox_id, STOP, &guard)
                .await?
                .is_none()
        );
        assert!(!sandbox_key_share_blocked(pool, sandbox_id).await?);
        assert!(!execution_key_share_blocked(pool, execution.id).await?);
        tx.rollback().await?;
    }
    let guard = pod_name_guard(&execution.pod_name, Some(&uid));
    let mut tx = pool.begin().await?;
    assert!(
        lock_plan(&mut *tx, sandbox_id, &["started"], &guard)
            .await?
            .is_none()
    );
    tx.rollback().await?;

    // `FOR UPDATE OF s, e`: both rows, so that even a key-share lock waits on
    // each, until the transaction ends.
    let mut tx = pool.begin().await?;
    let plan = lock_plan(&mut *tx, sandbox_id, STOP, &guard)
        .await?
        .expect("the row matches the guard");
    assert_eq!(plan.from_lifecycle, SandboxLifecycle::Started);
    assert!(!plan.creating_snapshot);
    assert!(!plan.node_reports_copy);
    assert!(sandbox_key_share_blocked(pool, sandbox_id).await?);
    assert!(execution_key_share_blocked(pool, execution.id).await?);
    tx.commit().await?;
    assert!(!sandbox_key_share_blocked(pool, sandbox_id).await?);
    assert!(!execution_key_share_blocked(pool, execution.id).await?);

    // The unlocked read answers the same and locks neither.
    let mut tx = pool.begin().await?;
    let plan = read_plan(&mut *tx, sandbox_id, STOP, &guard)
        .await?
        .expect("the row matches the guard");
    assert_eq!(plan.from_lifecycle, SandboxLifecycle::Started);
    assert!(!sandbox_key_share_blocked(pool, sandbox_id).await?);
    assert!(!execution_key_share_blocked(pool, execution.id).await?);
    tx.rollback().await?;
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn lock_plan_locks_only_the_row_for_a_forgotten_or_deleting_guard() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let forgotten = insert_sandbox(pool, "started").await?;
    let forgotten_execution =
        insert_execution(pool, forgotten, 1, "forgotten", false, None).await?;
    let deleting = insert_sandbox(pool, "deleting").await?;
    let deleting_execution = insert_execution(pool, deleting, 1, "running", true, None).await?;
    let forgotten_guard = OrphanGuard::Forgotten {
        active_execution_required_lifecycles: ACTIVE_REQUIRED,
    };

    // The losing cases: a lifecycle the guard doesn't cover matches nothing
    // and locks nothing.
    let mut tx = pool.begin().await?;
    assert!(
        lock_plan(&mut *tx, deleting, STOP, &forgotten_guard)
            .await?
            .is_none()
    );
    assert!(
        lock_plan(&mut *tx, forgotten, STOP, &OrphanGuard::LegacyDeleting)
            .await?
            .is_none()
    );
    assert!(!sandbox_key_share_blocked(pool, forgotten).await?);
    assert!(!sandbox_key_share_blocked(pool, deleting).await?);
    tx.rollback().await?;

    // `FOR UPDATE OF s`: the sandbox row, not its executions.
    for (sandbox_id, execution, guard, lifecycle) in [
        (
            forgotten,
            &forgotten_execution,
            forgotten_guard,
            SandboxLifecycle::Started,
        ),
        (
            deleting,
            &deleting_execution,
            OrphanGuard::LegacyDeleting,
            SandboxLifecycle::Deleting,
        ),
    ] {
        let mut tx = pool.begin().await?;
        let plan = lock_plan(&mut *tx, sandbox_id, STOP, &guard)
            .await?
            .expect("the row matches the guard");
        assert_eq!(plan.from_lifecycle, lifecycle);
        assert!(sandbox_key_share_blocked(pool, sandbox_id).await?);
        assert!(!execution_key_share_blocked(pool, execution.id).await?);
        tx.commit().await?;
        assert!(!sandbox_key_share_blocked(pool, sandbox_id).await?);

        // The unlocked read answers the same and locks nothing.
        let mut tx = pool.begin().await?;
        let plan = read_plan(&mut *tx, sandbox_id, STOP, &guard)
            .await?
            .expect("the row matches the guard");
        assert_eq!(plan.from_lifecycle, lifecycle);
        assert!(!sandbox_key_share_blocked(pool, sandbox_id).await?);
        tx.rollback().await?;
    }
    Ok(())
}
