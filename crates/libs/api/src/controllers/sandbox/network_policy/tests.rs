use super::*;
use pplx_space_db_test_fixtures::{SEED_ORG_ID, SEED_USER_ID, pg_test_db};
use pplx_space_test_env::TestResult;

#[tokio::test]
async fn policy_replacement_is_atomic_and_retries_preserve_the_original_target() -> TestResult {
    let db = pg_test_db().await?;
    let id = SandboxId::new();
    sqlx::query(sql!(
        "INSERT INTO sandboxes (id, user_id, org_id, managed_by, lifecycle, placement, network_policy_generation)
         VALUES ($1, $2, $3, 'k8s', 'started', 'unplaced', 1)"
    ))
    .bind(id)
    .bind(SEED_USER_ID)
    .bind(SEED_ORG_ID)
    .execute(&db.pool)
    .await?;
    let controller = SandboxController::new(crate::test_support::direct_substrate(&db.pool));
    let initial = fetch_sandbox(&db.pool, id).await?.expect("seeded sandbox");
    let revision = policy_revision(&initial)?;
    let (first, second) = tokio::join!(
        controller.replace_network_policy(
            &db.pool,
            id,
            NetworkPolicyV1::deny_all(),
            &revision,
            "one",
            "deny"
        ),
        controller.replace_network_policy(
            &db.pool,
            id,
            NetworkPolicyV1::allow_all(),
            &revision,
            "two",
            "allow"
        ),
    );
    assert_ne!(first.is_ok(), second.is_ok(), "one CAS must win");
    let (target, key, hash, policy, conflict) = match (first, second) {
        (Ok(target), Err(conflict)) => {
            (target, "one", "deny", NetworkPolicyV1::deny_all(), conflict)
        }
        (Err(conflict), Ok(target)) => (
            target,
            "two",
            "allow",
            NetworkPolicyV1::allow_all(),
            conflict,
        ),
        _ => unreachable!(),
    };
    assert_eq!(
        conflict.status(),
        axum::http::StatusCode::CONFLICT,
        "{conflict:?}"
    );
    let current = fetch_sandbox(&db.pool, id).await?.unwrap();
    assert_eq!(desired_policy_target(&current)?, Some(target.clone()));
    assert_eq!(
        current.network_policy_generation,
        initial.network_policy_generation + 1
    );
    let later_policy = if policy == NetworkPolicyV1::allow_all() {
        NetworkPolicyV1::deny_all()
    } else {
        NetworkPolicyV1::allow_all()
    };
    let later = controller
        .replace_network_policy(
            &db.pool,
            id,
            later_policy,
            &policy_revision(&current)?,
            "later",
            "later-request",
        )
        .await?;
    assert_eq!(
        controller
            .replace_network_policy(&db.pool, id, policy.clone(), &revision, key, hash)
            .await?,
        target
    );
    assert_eq!(
        controller
            .replace_network_policy(&db.pool, id, policy, &revision, key, "different")
            .await
            .unwrap_err()
            .status(),
        axum::http::StatusCode::CONFLICT
    );
    let after_replay = fetch_sandbox(&db.pool, id).await?.unwrap();
    assert_eq!(desired_policy_target(&after_replay)?, Some(later));
    assert_eq!(
        controller
            .observe_network_policy(&db.pool, &after_replay)
            .await?
            .state,
        PolicyEnforcementState::Pending,
        "stored desired policy is not enforcement proof"
    );
    Ok(())
}
