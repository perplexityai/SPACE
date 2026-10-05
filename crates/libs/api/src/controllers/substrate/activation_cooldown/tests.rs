use super::*;

#[tokio::test]
async fn activation_wait_is_shared_bounded_and_bypassed_by_an_owner_or_success()
-> pplx_space_test_env::TestResult {
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db().await? else {
        return Ok(());
    };
    let id = SandboxId::new();
    sqlx::query(sql!("INSERT INTO sandboxes (id, user_id, org_id, managed_by, lifecycle, template_id, network_policy_generation, activation_rollback_retry_after)
        VALUES ($1, '00000000-0000-0000-0000-000000000010', '00000000-0000-0000-0000-000000000001', 'k8s', 'suspended', 'synthetic', 1, NOW() + INTERVAL '30 seconds')"))
        .bind(id).execute(&db.pool).await?;
    assert!(
        matches!(
            return_wait_to_request(wait(&db.pool, id, Duration::from_secs(2))).await,
            Err(SubstrateError::ActivationCoolingDown { .. })
        ),
        "an ingress owner must return cooldown to its request, not sleep in its slot"
    );
    assert!(matches!(
        wait(&db.pool, id, Duration::from_millis(30)).await,
        Err(SubstrateError::Timeout { .. })
    ));
    for owner in [true, false] {
        let waiting = wait(&db.pool, id, Duration::from_secs(2));
        tokio::pin!(waiting);
        tokio::select! {
            biased;
            result = &mut waiting => panic!("cooldown returned early: {result:?}"),
            _ = tokio::time::sleep(Duration::from_millis(20)) => {}
        }
        if owner {
            sqlx::query(sql!("UPDATE sandboxes SET lifecycle = 'restoring', restore_lease_token = space_uuid_v7(), restore_lease_expires_at = NOW() + INTERVAL '1 minute' WHERE id = $1"))
                .bind(id).execute(&db.pool).await?;
        } else {
            sqlx::query(sql!(
                "UPDATE sandboxes SET lifecycle = 'started' WHERE id = $1"
            ))
            .bind(id)
            .execute(&db.pool)
            .await?;
        }
        assert!(waiting.await? > Duration::from_secs(1));
        if owner {
            sqlx::query(sql!(
                "UPDATE sandboxes SET lifecycle = 'suspended' WHERE id = $1"
            ))
            .bind(id)
            .execute(&db.pool)
            .await?;
        }
    }
    Ok(())
}
