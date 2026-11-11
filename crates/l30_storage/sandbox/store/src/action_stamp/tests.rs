use pplx_space_common_core::sql;
use pplx_space_common_core::types::{SandboxId, SubstrateKind};
use pplx_space_db_test_fixtures::{SEED_ORG_ID, SEED_USER_ID, try_pg_test_db};
use pplx_space_test_env::TestResult;
use sqlx::PgPool;

use super::{ActionStamp, StartSubstrate, stamp_action_lifecycle, stamp_started_after_start};

/// The lifecycles another intent can own while an action is in flight: a
/// delete, a stop or a suspend that has begun or finished, and a restore
/// whose owner the action does not join.
const CROSS_INTENT: [&str; 6] = [
    "deleting",
    "stopping",
    "stopped",
    "suspending",
    "suspended",
    "restoring",
];

/// Each stamp with lifecycles it must write over: the ones its action begins
/// from or a concurrent completion of it leaves, and `restoring` for a start
/// or a stop, the two stamps that write over it (see the module docs'
/// `restoring` bullet).
const OWN_STATES: [(ActionStamp, &[&str]); 6] = [
    (
        ActionStamp::Start(StartSubstrate::ClaimsRestoring),
        &["created", "stopped", "restoring", "started"],
    ),
    (
        ActionStamp::Start(StartSubstrate::LeavesSuspended),
        &["created", "stopped", "suspended", "restoring", "started"],
    ),
    (ActionStamp::Resume, &["paused", "started"]),
    (ActionStamp::Pause, &["started", "paused"]),
    (
        ActionStamp::Stop,
        &["started", "paused", "restoring", "stopped"],
    ),
    (ActionStamp::Suspend, &["started", "paused", "suspended"]),
];

async fn insert_sandbox(pool: &PgPool, lifecycle: &str) -> TestResult<SandboxId> {
    let id = SandboxId::new();
    sqlx::query(sql!(
        "INSERT INTO sandboxes
            (id, user_id, org_id, managed_by, lifecycle, placement, template_id,
             network_policy_generation)
         VALUES ($1, $2, $3, $4, $5, 'unplaced', 'tmpl-action-stamp-test', 1)"
    ))
    .bind(id)
    .bind(SEED_USER_ID)
    .bind(SEED_ORG_ID)
    .bind(SubstrateKind::Space)
    .bind(lifecycle)
    .execute(pool)
    .await?;
    Ok(id)
}

async fn read_row(pool: &PgPool, id: SandboxId) -> TestResult<(String, String)> {
    Ok(sqlx::query_as(sql!(
        "SELECT lifecycle, placement FROM sandboxes WHERE id = $1"
    ))
    .bind(id)
    .fetch_one(pool)
    .await?)
}

/// Each of `own` with `true` (stamped), then each cross-intent lifecycle not
/// in `own` with `false` (refused).
fn expected_cases(own: &[&'static str]) -> Vec<(&'static str, bool)> {
    let refused = CROSS_INTENT
        .into_iter()
        .filter(|lifecycle| !own.contains(lifecycle));
    own.iter()
        .map(|lifecycle| (*lifecycle, true))
        .chain(refused.map(|lifecycle| (lifecycle, false)))
        .collect()
}

#[tokio::test(flavor = "current_thread")]
async fn each_stamp_writes_its_own_states_and_refuses_another_intents() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let mut mismatches = Vec::new();
    for (stamp, own) in OWN_STATES {
        for (lifecycle, stamps) in expected_cases(own) {
            let id = insert_sandbox(pool, lifecycle).await?;
            let written = stamp_action_lifecycle(pool, id, stamp).await?;
            let (after, _) = read_row(pool, id).await?;
            let expected = if stamps {
                stamp.lifecycle().as_str()
            } else {
                lifecycle
            };
            if written != stamps || after != expected {
                mismatches.push(format!(
                    "{stamp:?} from {lifecycle}: written {written}, row now {after}"
                ));
            }
        }
    }
    assert!(mismatches.is_empty(), "{mismatches:#?}");
    Ok(())
}

/// The start's own stamp, which also places the row, follows the same sets
/// as [`stamp_action_lifecycle`] with [`ActionStamp::Start`].
#[tokio::test(flavor = "current_thread")]
async fn the_started_stamp_writes_a_starts_own_states_and_refuses_another_intents() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let mut mismatches = Vec::new();
    for (stamp, own) in OWN_STATES {
        let ActionStamp::Start(substrate) = stamp else {
            continue;
        };
        for (lifecycle, stamps) in expected_cases(own) {
            let id = insert_sandbox(pool, lifecycle).await?;
            let written = stamp_started_after_start(pool, id, substrate).await?;
            let after = read_row(pool, id).await?;
            let expected = if stamps {
                ("started".to_owned(), "on_node".to_owned())
            } else {
                (lifecycle.to_owned(), "unplaced".to_owned())
            };
            if written != stamps || after != expected {
                mismatches.push(format!(
                    "{substrate:?} from {lifecycle}: written {written}, row now {after:?}"
                ));
            }
        }
    }
    assert!(mismatches.is_empty(), "{mismatches:#?}");
    Ok(())
}
