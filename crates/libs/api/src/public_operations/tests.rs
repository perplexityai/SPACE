use pplx_space_common_core::types::{OrgId, SandboxId, UserId};

use pplx_space_db_test_fixtures::{try_pg_test_db, try_pg_test_db_with_max_connections};
use pplx_space_public_api::{
    PublicSandboxId, PublicSandboxOperationResult, SandboxLifecycleAction, SandboxLifecycleState,
};

use super::*;

use pplx_space_test_env::TestResult;

#[tokio::test]
async fn fork_operation_kind_persists_in_native_scope() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let org_id = seed_org(&db.pool).await?;
    let actor_id = seed_user(&db.pool, OrgId::from_uuid(org_id)).await?;
    let source_id = Uuid::now_v7();
    let source_id_string = source_id.to_string();
    let key = IdempotencyKey::parse("fork-sandbox-1")?;
    let fingerprint = RequestFingerprint::parse("f".repeat(64))?;
    let store = PublicOperationStore::new(db.pool.clone());

    let result = store
        .create_operation(CreatePublicOperation {
            org_id: OrgId::from_uuid(org_id),
            actor_user_id: Some(UserId::from_uuid(actor_id)),
            api_product: ApiProduct::Native,
            kind: OperationKind::ForkSandbox,
            target_type: Some(OperationTargetType::Sandbox),
            target_id: Some(&source_id_string),
            payload_json: serde_json::json!({
                "source_sandbox_id": source_id,
                "requested_timeout_seconds": 300,
                "requested_count": 2,
            }),
            idempotency: Some(CreatePublicOperationIdempotency {
                method: HttpOperationMethod::Post,
                path_template: OperationPathTemplate::new("/sandboxes/{sandboxID}/fork"),
                key: &key,
                request_fingerprint: &fingerprint,
            }),
            traceparent: None,
        })
        .await?;
    let CreatePublicOperationResult::Created(operation_id) = result else {
        return Err("fork operation was not created".into());
    };

    let operation = store.fetch_operation_admin(operation_id).await?.unwrap();
    assert_eq!(operation.api_product, ApiProduct::Native);
    assert_eq!(operation.kind, OperationKind::ForkSandbox);
    assert_eq!(
        operation.target_id.as_deref(),
        Some(source_id_string.as_str())
    );
    Ok(())
}

#[tokio::test]
async fn create_operation_idempotency_reuses_matching_fingerprint() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let org_id = seed_org(&db.pool).await?;
    let actor_id = seed_user(&db.pool, OrgId::from_uuid(org_id)).await?;
    let key = IdempotencyKey::parse("create-sandbox-1")?;
    let fingerprint = RequestFingerprint::parse("a".repeat(64))?;
    let store = PublicOperationStore::new(db.pool.clone());

    let input = CreatePublicOperation {
        org_id: OrgId::from_uuid(org_id),
        actor_user_id: Some(UserId::from_uuid(actor_id)),
        api_product: ApiProduct::Native,
        kind: OperationKind::CreateSandbox,
        target_type: Some(OperationTargetType::Sandbox),
        target_id: Some("sbx_pending"),
        payload_json: serde_json::json!({"template": "seed"}),
        idempotency: Some(CreatePublicOperationIdempotency {
            method: HttpOperationMethod::Post,
            path_template: OperationPathTemplate::new("/v1/sandboxes"),
            key: &key,
            request_fingerprint: &fingerprint,
        }),
        traceparent: None,
    };

    let created = store.create_operation(input.clone()).await?;
    let CreatePublicOperationResult::Created(operation_id) = created else {
        panic!("first insert should create operation: {created:?}");
    };

    let reused = store.create_operation(input).await?;
    assert_eq!(reused, CreatePublicOperationResult::Existing(operation_id));

    let operation = store
        .fetch_operation(OrgId::from_uuid(org_id), operation_id)
        .await?
        .expect("operation should be tenant-visible");
    assert_eq!(operation.id, operation_id);
    assert_eq!(operation.kind, OperationKind::CreateSandbox);
    assert_eq!(operation.state, OperationState::Queued);
    Ok(())
}

#[tokio::test]
async fn create_operation_reuses_key_after_idempotency_retention_expires() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let org_id = seed_org(&db.pool).await?;
    let actor_id = seed_user(&db.pool, OrgId::from_uuid(org_id)).await?;
    let key = IdempotencyKey::parse("expired-create-sandbox-key")?;
    let fingerprint = RequestFingerprint::parse("a".repeat(64))?;
    let store = PublicOperationStore::new(db.pool.clone());
    let input = CreatePublicOperation {
        org_id: OrgId::from_uuid(org_id),
        actor_user_id: Some(UserId::from_uuid(actor_id)),
        api_product: ApiProduct::Native,
        kind: OperationKind::CreateSandbox,
        target_type: Some(OperationTargetType::Sandbox),
        target_id: Some("sbx_expired_key"),
        payload_json: serde_json::json!({"template": "seed"}),
        idempotency: Some(CreatePublicOperationIdempotency {
            method: HttpOperationMethod::Post,
            path_template: OperationPathTemplate::new("/v1/sandboxes"),
            key: &key,
            request_fingerprint: &fingerprint,
        }),
        traceparent: None,
    };

    let first_operation_id = store
        .create_operation(input.clone())
        .await?
        .expect_created();
    sqlx::query(sql!(
        "UPDATE public_idempotency_records
         SET expires_at = NOW() - INTERVAL '1 second'
         WHERE operation_id = $1"
    ))
    .bind(first_operation_id.as_uuid())
    .execute(&db.pool)
    .await?;

    let second_operation_id = store.create_operation(input).await?.expect_created();
    assert_ne!(second_operation_id, first_operation_id);
    let retained_operation_id = sqlx::query_scalar::<_, Uuid>(sql!(
        "SELECT operation_id
         FROM public_idempotency_records
         WHERE org_id = $1
           AND api_product = 'native'
           AND method = 'POST'
           AND path_template = '/v1/sandboxes'
           AND idempotency_key = $2"
    ))
    .bind(org_id)
    .bind(key.as_str())
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(retained_operation_id, second_operation_id.as_uuid());
    Ok(())
}

#[tokio::test]
async fn create_operation_idempotency_rejects_fingerprint_mismatch() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let org_id = seed_org(&db.pool).await?;
    let actor_id = seed_user(&db.pool, OrgId::from_uuid(org_id)).await?;
    let key = IdempotencyKey::parse("create-sandbox-1")?;
    let first = RequestFingerprint::parse("a".repeat(64))?;
    let second = RequestFingerprint::parse("b".repeat(64))?;
    let store = PublicOperationStore::new(db.pool.clone());

    let input = CreatePublicOperation {
        org_id: OrgId::from_uuid(org_id),
        actor_user_id: Some(UserId::from_uuid(actor_id)),
        api_product: ApiProduct::Native,
        kind: OperationKind::CreateSandbox,
        target_type: Some(OperationTargetType::Sandbox),
        target_id: None,
        payload_json: serde_json::json!({}),
        idempotency: Some(CreatePublicOperationIdempotency {
            method: HttpOperationMethod::Post,
            path_template: OperationPathTemplate::new("/v1/sandboxes"),
            key: &key,
            request_fingerprint: &first,
        }),
        traceparent: None,
    };
    let created = store.create_operation(input).await?;
    let CreatePublicOperationResult::Created(operation_id) = created else {
        panic!("first insert should create operation: {created:?}");
    };

    let conflict = store
        .create_operation(CreatePublicOperation {
            idempotency: Some(CreatePublicOperationIdempotency {
                method: HttpOperationMethod::Post,
                path_template: OperationPathTemplate::new("/v1/sandboxes"),
                key: &key,
                request_fingerprint: &second,
            }),
            traceparent: None,
            ..CreatePublicOperation {
                org_id: OrgId::from_uuid(org_id),
                actor_user_id: Some(UserId::from_uuid(actor_id)),
                api_product: ApiProduct::Native,
                kind: OperationKind::CreateSandbox,
                target_type: Some(OperationTargetType::Sandbox),
                target_id: None,
                payload_json: serde_json::json!({}),
                idempotency: None,
                traceparent: None,
            }
        })
        .await?;

    assert_eq!(
        conflict,
        CreatePublicOperationResult::IdempotencyConflict {
            operation_id: Some(operation_id),
            expected_fingerprint: first,
        }
    );
    Ok(())
}

#[tokio::test]
async fn concurrent_idempotent_create_reuses_existing_operation() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let org_id = seed_org(&db.pool).await?;
    let store = PublicOperationStore::new(db.pool.clone());

    sqlx::query(sql!(
        "CREATE OR REPLACE FUNCTION slow_idempotency_insert_for_test()
         RETURNS trigger
         LANGUAGE plpgsql
         AS $$
         BEGIN
             PERFORM pg_sleep(1);
             RETURN NEW;
         END;
         $$"
    ))
    .execute(&db.pool)
    .await?;
    sqlx::query(sql!(
        "CREATE TRIGGER slow_idempotency_insert_for_test
         BEFORE INSERT ON public_idempotency_records
         FOR EACH ROW
         EXECUTE FUNCTION slow_idempotency_insert_for_test()"
    ))
    .execute(&db.pool)
    .await?;

    let first = tokio::spawn(create_idempotent_test_operation(
        store.clone(),
        OrgId::from_uuid(org_id),
    ));
    tokio::time::sleep(Duration::from_millis(150)).await;
    let second = tokio::spawn(create_idempotent_test_operation(
        store,
        OrgId::from_uuid(org_id),
    ));

    let first = first.await??;
    let second = second.await??;
    let (created, reused) = match (first, second) {
        (
            CreatePublicOperationResult::Created(created),
            CreatePublicOperationResult::Existing(reused),
        )
        | (
            CreatePublicOperationResult::Existing(reused),
            CreatePublicOperationResult::Created(created),
        ) => (created, reused),
        other => panic!("expected one create and one idempotent reuse, got {other:?}"),
    };
    assert_eq!(created, reused);
    Ok(())
}

fn delete_operation_input(org_id: OrgId, target_id: &str) -> CreatePublicOperation<'_> {
    CreatePublicOperation {
        org_id,
        actor_user_id: None,
        api_product: ApiProduct::Native,
        kind: OperationKind::DeleteSandbox,
        target_type: Some(OperationTargetType::Sandbox),
        target_id: Some(target_id),
        payload_json: serde_json::json!({"action": "delete"}),
        idempotency: None,
        traceparent: None,
    }
}

#[tokio::test]
async fn active_sandbox_deletes_coalesce_per_tenant_and_target() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let org_id = seed_org(&db.pool).await?;
    let store = PublicOperationStore::new(db.pool.clone());
    let target_id = PublicSandboxId::new().to_string();

    let (first, second) = tokio::join!(
        store.create_or_join_active_sandbox_delete(
            OrgId::from_uuid(org_id),
            &target_id,
            serde_json::json!({"action": "delete"}),
            None,
        ),
        store.create_or_join_active_sandbox_delete(
            OrgId::from_uuid(org_id),
            &target_id,
            serde_json::json!({"action": "delete"}),
            None,
        ),
    );
    let ids = [first?, second?].map(|result| match result {
        CreatePublicOperationResult::Created(id) | CreatePublicOperationResult::Existing(id) => id,
        other => panic!("unexpected delete admission result: {other:?}"),
    });
    assert_eq!(ids[0], ids[1]);

    let other_target = PublicSandboxId::new().to_string();
    let other_target_id = match store
        .create_or_join_active_sandbox_delete(
            OrgId::from_uuid(org_id),
            &other_target,
            serde_json::json!({"action": "delete"}),
            None,
        )
        .await?
    {
        CreatePublicOperationResult::Created(id) => id,
        other => panic!("different target should create independently: {other:?}"),
    };
    let other_org_id = seed_org(&db.pool).await?;
    let other_org_operation_id = match store
        .create_or_join_active_sandbox_delete(
            OrgId::from_uuid(other_org_id),
            &target_id,
            serde_json::json!({"action": "delete"}),
            None,
        )
        .await?
    {
        CreatePublicOperationResult::Created(id) => id,
        other => panic!("different tenant should create independently: {other:?}"),
    };
    assert_ne!(ids[0], other_target_id);
    assert_ne!(ids[0], other_org_operation_id);

    let active: i64 = sqlx::query_scalar(sql!(
        "SELECT COUNT(*)
           FROM public_operations
          WHERE org_id = $1 AND kind = 'delete_sandbox' AND target_id = $2
            AND state IN ('queued', 'running')"
    ))
    .bind(org_id)
    .bind(&target_id)
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(active, 1);
    Ok(())
}

#[tokio::test]
async fn implicit_wake_sorts_after_a_suspend_committed_during_its_lock_wait() -> TestResult {
    let Some(db) = try_pg_test_db_with_max_connections(4).await? else {
        return Ok(());
    };
    let org_id = OrgId::from_uuid(seed_org(&db.pool).await?);
    let store = PublicOperationStore::new(db.pool.clone());
    let target_id = PublicSandboxId::new().to_string();

    let mut lock_tx = db.pool.begin().await?;
    sqlx::query(sql!(
        "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))"
    ))
    .bind(format!(
        "{org_id}:{target_id}:{}",
        OperationKind::WakeSandbox.as_str()
    ))
    .execute(&mut *lock_tx)
    .await?;

    let wake = tokio::spawn({
        let store = store.clone();
        let target_id = target_id.clone();
        async move {
            store
                .create_or_join_active_sandbox_wake(
                    org_id,
                    &target_id,
                    serde_json::json!({"action": "wake"}),
                    None,
                )
                .await
        }
    });
    // The wake's transaction has begun, and so fixed its start time, once it
    // waits on the advisory lock held above.
    wait_for_advisory_lock_waiter(&db.pool).await?;

    let suspend_id =
        create_test_operation_for_target(&store, org_id, OperationKind::SuspendSandbox, &target_id)
            .await?
            .expect_created();
    lock_tx.commit().await?;
    let wake_id = match wake.await?? {
        CreatePublicOperationResult::Created(id) => id,
        other => panic!("a wake queued before the suspend must not be joined: {other:?}"),
    };

    assert_eq!(
        dispatch_order(&db.pool, org_id, &target_id).await?,
        vec![suspend_id.as_uuid(), wake_id.as_uuid()]
    );
    Ok(())
}

#[tokio::test]
async fn operation_sorts_after_one_committed_during_its_idempotency_lock_wait() -> TestResult {
    let Some(db) = try_pg_test_db_with_max_connections(4).await? else {
        return Ok(());
    };
    let org_id = OrgId::from_uuid(seed_org(&db.pool).await?);
    let store = PublicOperationStore::new(db.pool.clone());
    let target_id = PublicSandboxId::new().to_string();
    let key = IdempotencyKey::parse("suspend-after-lock-wait")?;
    let fingerprint = RequestFingerprint::parse("e".repeat(64))?;
    let path_template = OperationPathTemplate::new("/v1/sandboxes/{sandbox_id}:suspend");

    let mut lock_tx = db.pool.begin().await?;
    lock_idempotency_key(
        &mut lock_tx,
        org_id,
        ApiProduct::Native,
        &CreatePublicOperationIdempotency {
            method: HttpOperationMethod::Post,
            path_template,
            key: &key,
            request_fingerprint: &fingerprint,
        },
    )
    .await?;

    let suspend = tokio::spawn({
        let store = store.clone();
        let target_id = target_id.clone();
        async move {
            store
                .create_operation(CreatePublicOperation {
                    org_id,
                    actor_user_id: None,
                    api_product: ApiProduct::Native,
                    kind: OperationKind::SuspendSandbox,
                    target_type: Some(OperationTargetType::Sandbox),
                    target_id: Some(&target_id),
                    payload_json: serde_json::json!({}),
                    idempotency: Some(CreatePublicOperationIdempotency {
                        method: HttpOperationMethod::Post,
                        path_template,
                        key: &key,
                        request_fingerprint: &fingerprint,
                    }),
                    traceparent: None,
                })
                .await
        }
    });
    wait_for_advisory_lock_waiter(&db.pool).await?;

    let stop_id =
        create_test_operation_for_target(&store, org_id, OperationKind::StopSandbox, &target_id)
            .await?
            .expect_created();
    lock_tx.commit().await?;
    let suspend_id = suspend.await??.expect_created();

    assert_eq!(
        dispatch_order(&db.pool, org_id, &target_id).await?,
        vec![stop_id.as_uuid(), suspend_id.as_uuid()]
    );
    Ok(())
}

/// Returns once some transaction waits on an advisory lock in this database.
async fn wait_for_advisory_lock_waiter(pool: &PgPool) -> TestResult {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let waiting: i64 = sqlx::query_scalar(sql!(
                "SELECT COUNT(*)
                   FROM pg_locks
                  WHERE locktype = 'advisory'
                    AND NOT granted
                    AND database = (SELECT oid FROM pg_database WHERE datname = current_database())"
            ))
            .fetch_one(pool)
            .await?;
            if waiting > 0 {
                return Ok::<_, sqlx::Error>(());
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await??;
    Ok(())
}

/// A target's operations in the dispatcher's `(requested_at, operation_id)`
/// order.
async fn dispatch_order(
    pool: &PgPool,
    org_id: OrgId,
    target_id: &str,
) -> Result<Vec<Uuid>, sqlx::Error> {
    sqlx::query_scalar(sql!(
        "SELECT operation_id
           FROM public_operations
          WHERE org_id = $1 AND target_id = $2
          ORDER BY requested_at, operation_id"
    ))
    .bind(org_id)
    .bind(target_id)
    .fetch_all(pool)
    .await
}

#[tokio::test]
async fn queued_delete_survives_observer_loss_and_reaches_authoritative_finality() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let org_id = seed_org(&db.pool).await?;
    let sandbox_id = PublicSandboxId::new();
    let target_id = sandbox_id.to_string();
    let operation_id = match PublicOperationStore::new(db.pool.clone())
        .create_or_join_active_sandbox_delete(
            OrgId::from_uuid(org_id),
            &target_id,
            serde_json::json!({"action": "delete"}),
            None,
        )
        .await?
    {
        CreatePublicOperationResult::Created(id) => id,
        other => panic!("first delete should create durable work: {other:?}"),
    };

    // The route owns no execution claim. Dropping its only store handle models
    // a disconnected HTTP observer before the dispatcher sees the queued row.
    let dispatcher_store = PublicOperationStore::new(db.pool.clone());
    let claimed = dispatcher_store
        .claim_operations(ClaimPublicOperations {
            limits: ClaimLimits::any(1),
            claim_stale_after: Duration::from_secs(60),
            claimable_kinds: &[OperationKind::DeleteSandbox],
        })
        .await?;
    let operation = claimed
        .operations
        .into_iter()
        .next()
        .expect("dispatcher claims delete after observer disappears");
    assert_eq!(operation.id, operation_id);
    let result = PublicOperationResult::Sandbox(PublicSandboxOperationResult {
        sandbox_id,
        state: SandboxLifecycleState::Deleted,
        action: Some(SandboxLifecycleAction::Delete),
        controller_response: None,
    });
    assert!(
        dispatcher_store
            .complete_claimed_operation(CompleteClaimedPublicOperation {
                claim_token: claimed.claim_token,
                operation_id,
                progress_message: "sandbox deleted".to_string(),
                result_json: serde_json::to_value(&result)?,
            })
            .await?
    );

    let finalized = dispatcher_store
        .fetch_operation(OrgId::from_uuid(org_id), operation_id)
        .await?
        .expect("completed delete remains visible");
    assert_eq!(finalized.state, OperationState::Succeeded);
    assert_eq!(finalized.result, Some(result));
    Ok(())
}

#[tokio::test]
async fn create_claimed_operation_is_invisible_to_fresh_dispatcher_claims() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let org_id = seed_org(&db.pool).await?;
    let store = PublicOperationStore::new(db.pool.clone());
    let sandbox_id = PublicSandboxId::new();
    let target_id = sandbox_id.to_string();

    let created = store
        .create_claimed_operation(delete_operation_input(OrgId::from_uuid(org_id), &target_id))
        .await?;
    let CreateClaimedPublicOperationResult::Claimed {
        claim_token,
        operation,
    } = created
    else {
        panic!("create without idempotency must claim: {created:?}");
    };
    assert_eq!(operation.attempts, 1);
    assert_eq!(operation.api_product, ApiProduct::Native);
    assert_eq!(operation.target_id.as_deref(), Some(target_id.as_str()));

    // A dispatcher tick with a normal staleness window must not steal
    // the inline claim.
    let claim = store
        .claim_operations(ClaimPublicOperations {
            limits: ClaimLimits::any(10),
            claim_stale_after: Duration::from_secs(300),
            claimable_kinds: &[OperationKind::DeleteSandbox],
        })
        .await?;
    assert!(claim.operations.is_empty());

    // Inline completion persists under the atomic claim's token.
    let updated = store
        .complete_claimed_operation(CompleteClaimedPublicOperation {
            claim_token,
            operation_id: operation.id,
            progress_message: "sandbox delete dispatched".to_string(),
            result_json: serde_json::to_value(PublicOperationResult::Sandbox(
                PublicSandboxOperationResult {
                    sandbox_id,
                    state: SandboxLifecycleState::Deleted,
                    action: None,
                    controller_response: None,
                },
            ))?,
        })
        .await?;
    assert!(updated);
    let record = store
        .fetch_operation(OrgId::from_uuid(org_id), operation.id)
        .await?
        .expect("operation should be tenant-visible");
    assert_eq!(record.state, OperationState::Succeeded);
    Ok(())
}

#[tokio::test]
async fn stale_inline_claim_is_reclaimed_for_background_redrive() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let org_id = seed_org(&db.pool).await?;
    let store = PublicOperationStore::new(db.pool.clone());
    let target_id = PublicSandboxId::new().to_string();

    let created = store
        .create_claimed_operation(delete_operation_input(OrgId::from_uuid(org_id), &target_id))
        .await?;
    let CreateClaimedPublicOperationResult::Claimed { operation, .. } = created else {
        panic!("create without idempotency must claim: {created:?}");
    };

    // Simulate the inline owner dying between the external teardown and
    // the DB finalization: the claim is never completed, so once it goes
    // stale a dispatcher claim reclaims the operation and re-drives the
    // same idempotent delete.
    let claim = store
        .claim_operations(ClaimPublicOperations {
            limits: ClaimLimits::any(10),
            claim_stale_after: Duration::ZERO,
            claimable_kinds: &[OperationKind::DeleteSandbox],
        })
        .await?;
    assert_eq!(claim.operations.len(), 1);
    assert_eq!(claim.operations[0].id, operation.id);
    assert_eq!(claim.operations[0].attempts, 2);
    Ok(())
}

#[tokio::test]
async fn operation_events_are_tenant_scoped() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let org_id = seed_org(&db.pool).await?;
    let other_org_id = seed_org(&db.pool).await?;
    let store = PublicOperationStore::new(db.pool.clone());

    let CreatePublicOperationResult::Created(operation_id) = store
        .create_operation(CreatePublicOperation {
            org_id: OrgId::from_uuid(org_id),
            actor_user_id: None,
            api_product: ApiProduct::Native,
            kind: OperationKind::WakeSandbox,
            target_type: Some(OperationTargetType::Sandbox),
            target_id: Some("sbx_demo"),
            payload_json: serde_json::json!({}),
            idempotency: None,
            traceparent: None,
        })
        .await?
    else {
        panic!("non-idempotent create should create operation");
    };

    let events = store
        .fetch_operation_events(OrgId::from_uuid(org_id), operation_id, None, 10)
        .await?
        .expect("owning tenant should see operation events");
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].sequence, 1);

    assert!(
        store
            .fetch_operation_events(OrgId::from_uuid(other_org_id), operation_id, None, 10)
            .await?
            .is_none()
    );
    Ok(())
}

#[tokio::test]
async fn claimed_operation_returns_the_traceparent_it_was_created_with() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let org_id = OrgId::from_uuid(seed_org(&db.pool).await?);
    let store = PublicOperationStore::new(db.pool.clone());
    let traceparent = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";
    let traced_target = PublicSandboxId::new().to_string();
    let invalid_target = PublicSandboxId::new().to_string();
    let traced = store
        .create_operation(CreatePublicOperation {
            traceparent: Some(traceparent.to_string()),
            ..delete_operation_input(org_id, &traced_target)
        })
        .await?
        .expect_created();
    // Trace context is validated before it is stored, not by the column.
    let invalid = store
        .create_operation(CreatePublicOperation {
            traceparent: Some("00-not-a-traceparent".to_string()),
            ..delete_operation_input(org_id, &invalid_target)
        })
        .await?
        .expect_created();

    let claim = store
        .claim_operations(ClaimPublicOperations {
            limits: ClaimLimits::any(10),
            claim_stale_after: Duration::from_secs(60),
            claimable_kinds: &[OperationKind::DeleteSandbox],
        })
        .await?;

    let claimed = |id| {
        claim
            .operations
            .iter()
            .find(|operation| operation.id == id)
            .expect("both operations are claimed")
            .traceparent
            .as_deref()
    };
    assert_eq!(claimed(traced), Some(traceparent));
    assert_eq!(claimed(invalid), None);
    Ok(())
}

#[tokio::test]
async fn claim_operations_filters_kind_and_marks_running() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let org_id = seed_org(&db.pool).await?;
    let store = PublicOperationStore::new(db.pool.clone());
    let wake_operation =
        create_test_operation(&store, OrgId::from_uuid(org_id), OperationKind::WakeSandbox)
            .await?
            .expect_created();
    let _create_operation = create_test_operation(
        &store,
        OrgId::from_uuid(org_id),
        OperationKind::CreateSandbox,
    )
    .await?
    .expect_created();

    let claim = store
        .claim_operations(ClaimPublicOperations {
            limits: ClaimLimits::any(10),
            claim_stale_after: Duration::from_secs(60),
            claimable_kinds: &[OperationKind::WakeSandbox],
        })
        .await?;

    assert_eq!(claim.operations.len(), 1);
    assert_eq!(claim.operations[0].id, wake_operation);
    assert_eq!(claim.operations[0].kind, OperationKind::WakeSandbox);
    assert_eq!(claim.operations[0].attempts, 1);

    let operation = store
        .fetch_operation(OrgId::from_uuid(org_id), wake_operation)
        .await?
        .expect("claimed operation should remain visible");
    assert_eq!(operation.state, OperationState::Running);

    let events = store
        .fetch_operation_events(OrgId::from_uuid(org_id), wake_operation, None, 10)
        .await?
        .expect("owning tenant should see operation events");
    assert_eq!(events.len(), 2);
    assert_eq!(events[1].event_type, PublicOperationEventType::Claimed);
    Ok(())
}

#[tokio::test]
async fn claimed_payload_updates_require_the_active_claim_generation() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let org_id = seed_org(&db.pool).await?;
    let store = PublicOperationStore::new(db.pool.clone());
    let operation_id =
        create_test_operation(&store, OrgId::from_uuid(org_id), OperationKind::ForkSandbox)
            .await?
            .expect_created();
    let claim = store
        .claim_operations(ClaimPublicOperations {
            limits: ClaimLimits::any(1),
            claim_stale_after: Duration::from_secs(60),
            claimable_kinds: &[OperationKind::ForkSandbox],
        })
        .await?;
    let operation = claim.operations.first().expect("operation is claimed");

    let wrong_generation = store
        .update_claimed_payload(UpdateClaimedPublicOperationPayload {
            claim_token: claim.claim_token.clone(),
            operation_id,
            claim_generation: operation.claim_generation + 1,
            payload_json: serde_json::json!({"phase": "wrong-generation"}),
        })
        .await?;
    assert!(!wrong_generation);

    let wrong_token = store
        .update_claimed_payload(UpdateClaimedPublicOperationPayload {
            claim_token: "stale-claim-token".to_string(),
            operation_id,
            claim_generation: operation.claim_generation,
            payload_json: serde_json::json!({"phase": "wrong-token"}),
        })
        .await?;
    assert!(!wrong_token);

    let updated_payload = serde_json::json!({"phase": "disk-captured"});
    let updated = store
        .update_claimed_payload(UpdateClaimedPublicOperationPayload {
            claim_token: claim.claim_token,
            operation_id,
            claim_generation: operation.claim_generation,
            payload_json: updated_payload.clone(),
        })
        .await?;
    assert!(updated);

    let Json(stored_payload): Json<serde_json::Value> = sqlx::query_scalar(sql!(
        "SELECT payload_json FROM public_operations WHERE operation_id = $1"
    ))
    .bind(operation_id.as_uuid())
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(stored_payload, updated_payload);
    Ok(())
}

#[tokio::test]
async fn complete_claimed_operation_terminalizes_and_unclaims() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let org_id = seed_org(&db.pool).await?;
    let store = PublicOperationStore::new(db.pool.clone());
    let operation_id =
        create_test_operation(&store, OrgId::from_uuid(org_id), OperationKind::WakeSandbox)
            .await?
            .expect_created();
    let claim = store
        .claim_operations(ClaimPublicOperations {
            limits: ClaimLimits::any(1),
            claim_stale_after: Duration::from_secs(60),
            claimable_kinds: &[OperationKind::WakeSandbox],
        })
        .await?;
    let result = PublicOperationResult::Sandbox(PublicSandboxOperationResult {
        sandbox_id: PublicSandboxId::new(),
        state: SandboxLifecycleState::Running,
        action: None,
        controller_response: None,
    });

    assert!(
        store
            .complete_claimed_operation(CompleteClaimedPublicOperation {
                claim_token: claim.claim_token,
                operation_id,
                progress_message: "done".to_string(),
                result_json: serde_json::to_value(&result)?,
            })
            .await?
    );

    let operation = store
        .fetch_operation(OrgId::from_uuid(org_id), operation_id)
        .await?
        .expect("completed operation should remain visible");
    assert_eq!(operation.state, OperationState::Succeeded);
    assert_eq!(operation.progress_message, "done");
    assert_eq!(operation.result, Some(result));

    let empty_claim = store
        .claim_operations(ClaimPublicOperations {
            limits: ClaimLimits::any(1),
            claim_stale_after: Duration::from_secs(60),
            claimable_kinds: &[OperationKind::WakeSandbox],
        })
        .await?;
    assert!(empty_claim.operations.is_empty());
    Ok(())
}

#[tokio::test]
async fn release_claimed_operation_requeues_with_backoff() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let org_id = seed_org(&db.pool).await?;
    let store = PublicOperationStore::new(db.pool.clone());
    let operation_id =
        create_test_operation(&store, OrgId::from_uuid(org_id), OperationKind::WakeSandbox)
            .await?
            .expect_created();
    let claim = store
        .claim_operations(ClaimPublicOperations {
            limits: ClaimLimits::any(1),
            claim_stale_after: Duration::from_secs(60),
            claimable_kinds: &[OperationKind::WakeSandbox],
        })
        .await?;

    assert_eq!(
        store
            .release_claimed_operation(ReleaseClaimedPublicOperation {
                claim_token: claim.claim_token,
                operation_id,
                progress_message: "retry later".to_string(),
                // Sub-second precision is kept, so jitter is not rounded off.
                retry_after: Duration::from_millis(300_500),
                count_attempt: true,
                undo_first_start: false,
                cancel_if_superseded: false,
                carried_error: None,
            })
            .await?,
        ReleasedClaim::Requeued
    );
    let delay_secs: f64 = sqlx::query_scalar(sql!(
        "SELECT EXTRACT(EPOCH FROM next_attempt_at - updated_at)::DOUBLE PRECISION
         FROM public_operations
         WHERE operation_id = $1"
    ))
    .bind(operation_id.as_uuid())
    .fetch_one(&db.pool)
    .await?;
    assert!((delay_secs - 300.5).abs() < 0.001, "{delay_secs}");

    let operation = store
        .fetch_operation(OrgId::from_uuid(org_id), operation_id)
        .await?
        .expect("released operation should remain visible");
    assert_eq!(operation.state, OperationState::Queued);
    assert_eq!(operation.progress_message, "retry later");

    let immediate_claim = store
        .claim_operations(ClaimPublicOperations {
            limits: ClaimLimits::any(1),
            claim_stale_after: Duration::from_secs(60),
            claimable_kinds: &[OperationKind::WakeSandbox],
        })
        .await?;
    assert!(immediate_claim.operations.is_empty());
    Ok(())
}

#[tokio::test]
async fn backpressure_release_rolls_back_claim_attempt() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let org_id = seed_org(&db.pool).await?;
    let store = PublicOperationStore::new(db.pool.clone());
    let operation_id =
        create_test_operation(&store, OrgId::from_uuid(org_id), OperationKind::WakeSandbox)
            .await?
            .expect_created();

    // First claim increments attempts to 1.
    let claim = store
        .claim_operations(ClaimPublicOperations {
            limits: ClaimLimits::any(1),
            claim_stale_after: Duration::from_secs(60),
            claimable_kinds: &[OperationKind::WakeSandbox],
        })
        .await?;
    assert_eq!(claim.operations[0].attempts, 1);

    // A backpressure release rolls that increment back.
    assert_eq!(
        store
            .release_claimed_operation(ReleaseClaimedPublicOperation {
                claim_token: claim.claim_token,
                operation_id,
                progress_message: "cluster admission rate limited".to_string(),
                retry_after: Duration::ZERO,
                count_attempt: false,
                undo_first_start: false,
                cancel_if_superseded: false,
                carried_error: None,
            })
            .await?,
        ReleasedClaim::Requeued
    );

    // Re-claim: attempts goes 0 -> 1, not 1 -> 2, proving the rollback so a
    // long limiter wait never inflates a later ordinary error's budget.
    let reclaim = store
        .claim_operations(ClaimPublicOperations {
            limits: ClaimLimits::any(1),
            claim_stale_after: Duration::from_secs(60),
            claimable_kinds: &[OperationKind::WakeSandbox],
        })
        .await?;
    assert_eq!(reclaim.operations.len(), 1);
    assert_eq!(
        reclaim.operations[0].attempts, 1,
        "backpressure release must roll back the claim's attempt increment"
    );
    Ok(())
}

#[tokio::test]
async fn admin_operation_listing_can_filter_across_tenants() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let first_org = seed_org(&db.pool).await?;
    let second_org = seed_org(&db.pool).await?;
    let store = PublicOperationStore::new(db.pool.clone());
    let first = create_test_operation(
        &store,
        OrgId::from_uuid(first_org),
        OperationKind::CreateSandbox,
    )
    .await?
    .expect_created();
    let second = create_test_operation(
        &store,
        OrgId::from_uuid(second_org),
        OperationKind::WakeSandbox,
    )
    .await?
    .expect_created();

    let all = store
        .list_operations_admin(ListAdminPublicOperations {
            limit: 10,
            org_id: None,
            state: None,
            kind: None,
            target_type: None,
            target_id: None,
        })
        .await?;
    assert!(all.iter().any(|operation| operation.id == first));
    assert!(all.iter().any(|operation| operation.id == second));

    let filtered = store
        .list_operations_admin(ListAdminPublicOperations {
            limit: 10,
            org_id: Some(OrgId::from_uuid(second_org)),
            state: Some(OperationState::Queued),
            kind: Some(OperationKind::WakeSandbox),
            target_type: Some(OperationTargetType::Sandbox),
            target_id: Some("sbx_demo".to_string()),
        })
        .await?;
    assert_eq!(filtered.len(), 1);
    assert_eq!(filtered[0].id, second);
    assert_eq!(filtered[0].org_id, OrgId::from_uuid(second_org));
    assert_eq!(filtered[0].kind, OperationKind::WakeSandbox);

    let fetched = store
        .fetch_operation_admin(first)
        .await?
        .expect("admin should fetch an operation without tenant scoping");
    assert_eq!(fetched.id, first);
    assert_eq!(fetched.org_id, OrgId::from_uuid(first_org));
    Ok(())
}

#[tokio::test]
async fn gc_expired_removes_terminal_operations_and_idempotency_records() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let org_id = seed_org(&db.pool).await?;
    let store = PublicOperationStore::new(db.pool.clone());
    let terminal_operation =
        create_test_operation(&store, OrgId::from_uuid(org_id), OperationKind::WakeSandbox)
            .await?
            .expect_created();
    let queued_operation = create_test_operation(
        &store,
        OrgId::from_uuid(org_id),
        OperationKind::PauseSandbox,
    )
    .await?
    .expect_created();
    let key = IdempotencyKey::parse("retention-key")?;
    let fingerprint = RequestFingerprint::parse("c".repeat(64))?;
    let idempotent_operation = store
        .create_operation(CreatePublicOperation {
            org_id: OrgId::from_uuid(org_id),
            actor_user_id: None,
            api_product: ApiProduct::Native,
            kind: OperationKind::CreateSandbox,
            target_type: Some(OperationTargetType::Sandbox),
            target_id: Some("sbx_retention"),
            payload_json: serde_json::json!({}),
            idempotency: Some(CreatePublicOperationIdempotency {
                method: HttpOperationMethod::Post,
                path_template: OperationPathTemplate::new("/v1/sandboxes"),
                key: &key,
                request_fingerprint: &fingerprint,
            }),
            traceparent: None,
        })
        .await?
        .expect_created();

    sqlx::query(sql!(
        "UPDATE public_operations
         SET state = $2, gc_after = NOW() - INTERVAL '1 second'
         WHERE operation_id = $1"
    ))
    .bind(terminal_operation.as_uuid())
    .bind(OperationState::Succeeded.as_str())
    .execute(&db.pool)
    .await?;

    sqlx::query(sql!(
        "UPDATE public_operations
         SET gc_after = NOW() - INTERVAL '1 second'
         WHERE operation_id IN ($1, $2)"
    ))
    .bind(queued_operation.as_uuid())
    .bind(idempotent_operation.as_uuid())
    .execute(&db.pool)
    .await?;
    sqlx::query(sql!(
        "UPDATE public_idempotency_records SET expires_at = NOW() - INTERVAL '1 second'"
    ))
    .execute(&db.pool)
    .await?;

    let report = store
        .gc_expired(PublicOperationGcRequest {
            max_records: 100,
            payload_scrub_rules: &[],
        })
        .await?;

    assert_eq!(
        report,
        PublicOperationGcReport {
            operations_deleted: 1,
            idempotency_records_deleted: 1,
        }
    );
    assert!(
        store
            .fetch_operation(OrgId::from_uuid(org_id), terminal_operation)
            .await?
            .is_none()
    );
    assert!(
        store
            .fetch_operation(OrgId::from_uuid(org_id), queued_operation)
            .await?
            .is_some()
    );
    assert!(
        store
            .fetch_operation(OrgId::from_uuid(org_id), idempotent_operation)
            .await?
            .is_some()
    );

    let event_count = sqlx::query_scalar::<_, i64>(sql!(
        "SELECT COUNT(*)
         FROM public_operation_events
         WHERE operation_id = $1"
    ))
    .bind(terminal_operation.as_uuid())
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(event_count, 0);

    let idempotency_count =
        sqlx::query_scalar::<_, i64>(sql!("SELECT COUNT(*) FROM public_idempotency_records"))
            .fetch_one(&db.pool)
            .await?;
    assert_eq!(idempotency_count, 0);
    Ok(())
}

#[tokio::test]
async fn gc_expired_removes_live_idempotency_references_before_operations() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let org_id = seed_org(&db.pool).await?;
    let store = PublicOperationStore::new(db.pool.clone());
    let key = IdempotencyKey::parse("live-retention-key")?;
    let fingerprint = RequestFingerprint::parse("d".repeat(64))?;
    let operation_id = store
        .create_operation(CreatePublicOperation {
            org_id: OrgId::from_uuid(org_id),
            actor_user_id: None,
            api_product: ApiProduct::Native,
            kind: OperationKind::WakeSandbox,
            target_type: Some(OperationTargetType::Sandbox),
            target_id: Some("sbx_live_retention"),
            payload_json: serde_json::json!({}),
            idempotency: Some(CreatePublicOperationIdempotency {
                method: HttpOperationMethod::Post,
                path_template: OperationPathTemplate::new("/v1/sandboxes/{sandbox_id}/wake"),
                key: &key,
                request_fingerprint: &fingerprint,
            }),
            traceparent: None,
        })
        .await?
        .expect_created();

    sqlx::query(sql!(
        "UPDATE public_operations
         SET state = $2, gc_after = NOW() - INTERVAL '1 second'
         WHERE operation_id = $1"
    ))
    .bind(operation_id.as_uuid())
    .bind(OperationState::Succeeded.as_str())
    .execute(&db.pool)
    .await?;

    let report = store
        .gc_expired(PublicOperationGcRequest {
            max_records: 100,
            payload_scrub_rules: &[],
        })
        .await?;

    assert_eq!(
        report,
        PublicOperationGcReport {
            operations_deleted: 1,
            idempotency_records_deleted: 1,
        }
    );
    assert!(
        store
            .fetch_operation(OrgId::from_uuid(org_id), operation_id)
            .await?
            .is_none()
    );
    let idempotency_count =
        sqlx::query_scalar::<_, i64>(sql!("SELECT COUNT(*) FROM public_idempotency_records"))
            .fetch_one(&db.pool)
            .await?;
    assert_eq!(idempotency_count, 0);
    Ok(())
}

trait ExpectCreated {
    fn expect_created(self) -> OperationId;
}

impl ExpectCreated for CreatePublicOperationResult {
    fn expect_created(self) -> OperationId {
        match self {
            CreatePublicOperationResult::Created(operation_id) => operation_id,
            other => panic!("expected created operation, got {other:?}"),
        }
    }
}

async fn create_test_operation(
    store: &PublicOperationStore,
    org_id: OrgId,
    kind: OperationKind,
) -> Result<CreatePublicOperationResult, PublicOperationStoreError> {
    create_test_operation_for_target(store, org_id, kind, "sbx_demo").await
}

async fn create_test_operation_for_target(
    store: &PublicOperationStore,
    org_id: OrgId,
    kind: OperationKind,
    target_id: &str,
) -> Result<CreatePublicOperationResult, PublicOperationStoreError> {
    store
        .create_operation(CreatePublicOperation {
            org_id,
            actor_user_id: None,
            api_product: ApiProduct::Native,
            kind,
            target_type: Some(OperationTargetType::Sandbox),
            target_id: Some(target_id),
            payload_json: serde_json::json!({}),
            idempotency: None,
            traceparent: None,
        })
        .await
}

async fn create_idempotent_test_operation(
    store: PublicOperationStore,
    org_id: OrgId,
) -> Result<CreatePublicOperationResult, PublicOperationStoreError> {
    let key = IdempotencyKey::parse("concurrent-create-sandbox")
        .expect("test idempotency key should be valid");
    let fingerprint =
        RequestFingerprint::parse("d".repeat(64)).expect("test fingerprint should be valid");
    store
        .create_operation(CreatePublicOperation {
            org_id,
            actor_user_id: None,
            api_product: ApiProduct::Native,
            kind: OperationKind::CreateSandbox,
            target_type: Some(OperationTargetType::Sandbox),
            target_id: Some("sbx_concurrent"),
            payload_json: serde_json::json!({"template": "seed"}),
            idempotency: Some(CreatePublicOperationIdempotency {
                method: HttpOperationMethod::Post,
                path_template: OperationPathTemplate::new("/v1/sandboxes"),
                key: &key,
                request_fingerprint: &fingerprint,
            }),
            traceparent: None,
        })
        .await
}

#[tokio::test]
async fn canceled_fork_operation_is_terminal() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let org_id = seed_org(&db.pool).await?;
    let actor_id = seed_user(&db.pool, OrgId::from_uuid(org_id)).await?;
    let operation_id = create_fork_operation(&db.pool, OrgId::from_uuid(org_id), actor_id).await?;

    sqlx::query(sql!(
        "UPDATE public_operations
             SET state = 'canceled',
                 terminal_error_json = '{\"message\":\"canceled\"}'::jsonb
             WHERE operation_id = $1"
    ))
    .bind(operation_id.as_uuid())
    .execute(&db.pool)
    .await?;

    let outcome = PublicOperationStore::new(db.pool.clone())
        .fetch_fork_outcome(OrgId::from_uuid(org_id), operation_id)
        .await?
        .ok_or("fork operation was not found")?;
    assert!(
        matches!(outcome, ForkOperationOutcome::Failed { .. }),
        "canceled fork operations must not remain pending"
    );
    Ok(())
}

#[tokio::test]
async fn completed_fork_result_uses_the_public_operation_contract() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let org_id = seed_org(&db.pool).await?;
    let actor_id = seed_user(&db.pool, OrgId::from_uuid(org_id)).await?;
    let operation_id = create_fork_operation(&db.pool, OrgId::from_uuid(org_id), actor_id).await?;

    sqlx::query(sql!(
            "UPDATE public_operations
             SET state = 'succeeded',
                 result_json = '{\"type\":\"fork\",\"results\":[{\"sandboxID\":\"sbx_123\"}]}'::jsonb
             WHERE operation_id = $1"
        ))
        .bind(operation_id.as_uuid())
        .execute(&db.pool)
        .await?;

    let store = PublicOperationStore::new(db.pool.clone());
    let operation = store
        .fetch_operation(OrgId::from_uuid(org_id), operation_id)
        .await?
        .ok_or("fork operation was not found")?;
    assert!(
        operation.result.is_some(),
        "completed fork results must decode through the generic operation endpoint"
    );
    let outcome = store
        .fetch_fork_outcome(OrgId::from_uuid(org_id), operation_id)
        .await?
        .ok_or("fork operation outcome was not found")?;
    assert_eq!(
        outcome,
        ForkOperationOutcome::Succeeded(serde_json::json!([
            {"sandboxID": "sbx_123"}
        ])),
        "the E2B outcome path must decode the same tagged result"
    );
    Ok(())
}

#[tokio::test]
async fn completed_fork_result_accepts_the_legacy_array_shape() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let org_id = seed_org(&db.pool).await?;
    let actor_id = seed_user(&db.pool, OrgId::from_uuid(org_id)).await?;
    let operation_id = create_fork_operation(&db.pool, OrgId::from_uuid(org_id), actor_id).await?;

    sqlx::query(sql!(
        "UPDATE public_operations
             SET state = 'succeeded',
                 result_json = '[{\"sandboxID\":\"sbx_legacy\"}]'::jsonb
             WHERE operation_id = $1"
    ))
    .bind(operation_id.as_uuid())
    .execute(&db.pool)
    .await?;

    let outcome = PublicOperationStore::new(db.pool.clone())
        .fetch_fork_outcome(OrgId::from_uuid(org_id), operation_id)
        .await?
        .ok_or("fork operation outcome was not found")?;
    assert_eq!(
        outcome,
        ForkOperationOutcome::Succeeded(serde_json::json!([
            {"sandboxID": "sbx_legacy"}
        ])),
        "rolling deploys must replay results written by the prior executor"
    );
    Ok(())
}

async fn create_fork_operation(
    pool: &PgPool,
    org_id: OrgId,
    actor_id: Uuid,
) -> TestResult<OperationId> {
    let source_id = Uuid::now_v7();
    let source_id_string = source_id.to_string();
    let store = PublicOperationStore::new(pool.clone());
    let result = store
        .create_operation(CreatePublicOperation {
            org_id,
            actor_user_id: Some(UserId::from_uuid(actor_id)),
            api_product: ApiProduct::Native,
            kind: OperationKind::ForkSandbox,
            target_type: Some(OperationTargetType::Sandbox),
            target_id: Some(&source_id_string),
            payload_json: serde_json::json!({
                "source_sandbox_id": source_id,
                "requested_timeout_seconds": 300,
                "requested_count": 1,
            }),
            idempotency: None,
            traceparent: None,
        })
        .await?;
    match result {
        CreatePublicOperationResult::Created(operation_id) => Ok(operation_id),
        CreatePublicOperationResult::Existing(_) => {
            Err("unexpected existing fork operation".into())
        }
        CreatePublicOperationResult::IdempotencyConflict { .. } => {
            Err("unexpected fork operation idempotency conflict".into())
        }
        CreatePublicOperationResult::AdmissionDenied => {
            Err("unexpected denied fork operation admission".into())
        }
    }
}

#[tokio::test]
async fn fork_hard_deadline_uses_the_persisted_operation_deadline() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let org_id = seed_org(&db.pool).await?;
    let actor_id = seed_user(&db.pool, OrgId::from_uuid(org_id)).await?;
    let operation_id = create_fork_operation(&db.pool, OrgId::from_uuid(org_id), actor_id).await?;
    let payload = crate::fork_operations::ForkSandboxPayload::v1(
        crate::fork_operations::ForkSandboxPayloadV1::new_with_deadline(
            SandboxId::new(),
            crate::fork_operations::ForkRequest::from_wire(Some(60), Some(1))?,
            "e2b.example.test".to_string(),
            Duration::from_secs(42),
        ),
    );
    sqlx::query(sql!(
        "UPDATE public_operations SET payload_json = $2 WHERE operation_id = $1"
    ))
    .bind(operation_id.as_uuid())
    .bind(Json(serde_json::to_value(payload)?))
    .execute(&db.pool)
    .await?;

    let timeout = PublicOperationStore::new(db.pool.clone())
        .fetch_fork_hard_deadline(OrgId::from_uuid(org_id), operation_id)
        .await?;

    assert_eq!(timeout, Some(Duration::from_secs(42)));
    Ok(())
}

#[tokio::test]
async fn idempotency_observation_is_read_only_when_the_key_is_missing() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let org_id = seed_org(&db.pool).await?;
    let store = PublicOperationStore::new(db.pool.clone());
    let key = IdempotencyKey::parse("read-only-operation-observation")?;
    let fingerprint = RequestFingerprint::parse("d".repeat(64))?;
    let idempotency = CreatePublicOperationIdempotency {
        method: HttpOperationMethod::Post,
        path_template: OperationPathTemplate::new("/sandboxes/{sandboxID}/fork"),
        key: &key,
        request_fingerprint: &fingerprint,
    };

    assert_eq!(
        store
            .observe_operation_idempotency(
                OrgId::from_uuid(org_id),
                ApiProduct::Native,
                &idempotency
            )
            .await?,
        ObservePublicOperationIdempotency::Missing
    );
    let records = sqlx::query_scalar::<_, i64>(sql!(
        "SELECT COUNT(*)
         FROM public_idempotency_records
         WHERE org_id = $1 AND idempotency_key = $2"
    ))
    .bind(org_id)
    .bind(key.as_str())
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(records, 0, "observation must not reserve a missing key");
    Ok(())
}

#[tokio::test]
async fn operation_admission_serializes_the_pre_insert_idempotency_window() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let org_id = seed_org(&db.pool).await?;
    let store = PublicOperationStore::new(db.pool.clone());
    let key = IdempotencyKey::parse("serialized-operation-admission")?;
    let fingerprint = RequestFingerprint::parse("c".repeat(64))?;
    let idempotency = CreatePublicOperationIdempotency {
        method: HttpOperationMethod::Post,
        path_template: OperationPathTemplate::new("/sandboxes/{sandboxID}/fork"),
        key: &key,
        request_fingerprint: &fingerprint,
    };

    let first = store
        .begin_operation_admission(OrgId::from_uuid(org_id), ApiProduct::Native, &idempotency)
        .await?;
    assert_eq!(
        first.observation(),
        &ObservePublicOperationIdempotency::Missing
    );

    let competing = tokio::time::timeout(
        Duration::from_millis(150),
        store.begin_operation_admission(OrgId::from_uuid(org_id), ApiProduct::Native, &idempotency),
    )
    .await;
    assert!(
        competing.is_err(),
        "same-key admission must wait until the first request inserts or releases"
    );

    let created = first
        .create(CreatePublicOperation {
            org_id: OrgId::from_uuid(org_id),
            actor_user_id: None,
            api_product: ApiProduct::Native,
            kind: OperationKind::ForkSandbox,
            target_type: Some(OperationTargetType::Sandbox),
            target_id: Some("source-sandbox"),
            payload_json: serde_json::json!({}),
            idempotency: Some(idempotency.clone()),
            traceparent: None,
        })
        .await?;
    let CreatePublicOperationResult::Created(operation_id) = created else {
        panic!("first locked admission should create the operation: {created:?}");
    };

    let replay = store
        .begin_operation_admission(OrgId::from_uuid(org_id), ApiProduct::Native, &idempotency)
        .await?;
    assert_eq!(
        replay.observation(),
        &ObservePublicOperationIdempotency::Existing(operation_id)
    );
    replay.finish().await?;
    Ok(())
}

#[tokio::test]
async fn renewing_a_claim_prevents_stale_dispatcher_takeover() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let org_id = seed_org(&db.pool).await?;
    let store = PublicOperationStore::new(db.pool.clone());
    let target_id = PublicSandboxId::new().to_string();
    let CreateClaimedPublicOperationResult::Claimed {
        claim_token,
        operation,
    } = store
        .create_claimed_operation(delete_operation_input(OrgId::from_uuid(org_id), &target_id))
        .await?
    else {
        panic!("operation should be claimed");
    };

    sqlx::query(sql!(
        "UPDATE public_operations
             SET claimed_at = NOW() - INTERVAL '2 minutes'
             WHERE operation_id = $1"
    ))
    .bind(operation.id.as_uuid())
    .execute(&db.pool)
    .await?;

    assert!(
        store
            .renew_claimed_operation(&claim_token, &operation)
            .await?
    );
    let competing = store
        .claim_operations(ClaimPublicOperations {
            limits: ClaimLimits::any(1),
            claim_stale_after: Duration::from_secs(60),
            claimable_kinds: &[OperationKind::DeleteSandbox],
        })
        .await?;
    assert!(competing.operations.is_empty());
    Ok(())
}

#[tokio::test]
async fn terminal_fork_failure_scrubs_the_claimed_federated_grant() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let org_id = seed_org(&db.pool).await?;
    let store = PublicOperationStore::new(db.pool.clone());
    let source_id = Uuid::now_v7().to_string();
    let created = store
        .create_claimed_operation(CreatePublicOperation {
            org_id: OrgId::from_uuid(org_id),
            actor_user_id: None,
            api_product: ApiProduct::Native,
            kind: OperationKind::ForkSandbox,
            target_type: Some(OperationTargetType::Sandbox),
            target_id: Some(&source_id),
            payload_json: serde_json::json!({
                "version": "1",
                "payload": {
                    "federated_create": {
                        "owner": {"issuer": "example-issuer"},
                        "service_grant_base64": "sensitive-grant"
                    }
                }
            }),
            idempotency: None,
            traceparent: None,
        })
        .await?;
    let CreateClaimedPublicOperationResult::Claimed {
        claim_token,
        operation,
    } = created
    else {
        panic!("fork operation should be created and claimed: {created:?}");
    };

    assert!(
        store
            .scrub_claimed_fork_federated_service_grant(&claim_token, &operation)
            .await?,
        "the active claim should own terminal credential scrubbing"
    );

    let stored = sqlx::query_scalar::<_, Json<serde_json::Value>>(sql!(
        "SELECT payload_json
             FROM public_operations
             WHERE org_id = $1 AND operation_id = $2"
    ))
    .bind(org_id)
    .bind(operation.id.as_uuid())
    .fetch_one(&db.pool)
    .await?
    .0;
    assert_eq!(
        stored.pointer("/payload/federated_create/owner"),
        Some(&serde_json::json!({"issuer": "example-issuer"})),
        "terminal scrubbing must preserve non-secret federated ownership"
    );
    assert_eq!(
        stored.pointer("/payload/federated_create/service_grant_base64"),
        None,
        "terminal completion must not retain the federated service grant"
    );
    Ok(())
}

#[tokio::test]
async fn gc_scrubs_canceled_fork_credentials_before_row_retention_expires() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let org_id = seed_org(&db.pool).await?;
    let actor_id = seed_user(&db.pool, OrgId::from_uuid(org_id)).await?;
    let operation_id = create_fork_operation(&db.pool, OrgId::from_uuid(org_id), actor_id).await?;
    sqlx::query(sql!(
        "UPDATE public_operations
             SET state = 'canceled',
                 gc_after = NOW() + INTERVAL '1 day',
                 payload_json = '{
                   \"version\": \"1\",
                   \"payload\": {
                     \"federated_create\": {
                       \"owner\": {\"issuer\": \"example-issuer\"},
                       \"service_grant_base64\": \"sensitive-grant\"
                     }
                   }
                 }'::jsonb
             WHERE operation_id = $1"
    ))
    .bind(operation_id.as_uuid())
    .execute(&db.pool)
    .await?;

    PublicOperationStore::new(db.pool.clone())
        .gc_expired(PublicOperationGcRequest {
            max_records: 100,
            payload_scrub_rules: crate::fork_operations::FORK_OPERATION_PAYLOAD_SCRUB_RULES,
        })
        .await?;

    let payload = sqlx::query_scalar::<_, Json<serde_json::Value>>(sql!(
        "SELECT payload_json FROM public_operations WHERE operation_id = $1"
    ))
    .bind(operation_id.as_uuid())
    .fetch_one(&db.pool)
    .await?
    .0;
    assert_eq!(
        payload.pointer("/payload/federated_create/service_grant_base64"),
        None,
        "terminal retention must not retain a federated service grant"
    );
    assert_eq!(
        payload.pointer("/payload/federated_create/owner"),
        Some(&serde_json::json!({"issuer": "example-issuer"}))
    );
    Ok(())
}

/// `lock_target_lifecycle` gates fork admission on the target sandbox: it
/// must refuse a mid-delete target and a missing one, and admit a live one.
///
/// Pinned because the predicate's shape changed when it moved onto
/// `sandbox_store::get` (`SELECT EXISTS(.. AND lifecycle != 'deleting')`
/// became `get(..).is_some_and(..)`): equivalent today, and this test is what
/// keeps "equivalent" from silently becoming "almost" under a later refactor
/// — the same drift class as the wake-completion CAS.
#[tokio::test]
async fn lock_target_lifecycle_refuses_deleting_and_missing_targets() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let org_id = seed_org(&db.pool).await?;
    let actor_id = seed_user(&db.pool, OrgId::from_uuid(org_id)).await?;
    let store = PublicOperationStore::new(db.pool.clone());
    let key = IdempotencyKey::parse("fork-target-lifecycle-1")?;
    let fingerprint = RequestFingerprint::parse("f".repeat(64))?;
    let idempotency = CreatePublicOperationIdempotency {
        method: HttpOperationMethod::Post,
        path_template: OperationPathTemplate::new("/sandboxes/{sandboxID}/fork"),
        key: &key,
        request_fingerprint: &fingerprint,
    };

    for (lifecycle, expected, case) in [
        (Some("started"), true, "live target admits"),
        (Some("suspended"), true, "suspended target admits"),
        (Some("deleting"), false, "mid-delete target refuses"),
        (None, false, "missing target refuses"),
    ] {
        let target_id = SandboxId::new();
        if let Some(lifecycle) = lifecycle {
            seed_sandbox(
                &db.pool,
                target_id,
                UserId::from_uuid(actor_id),
                OrgId::from_uuid(org_id),
                lifecycle,
            )
            .await?;
        }
        let mut admission = store
            .begin_operation_admission(OrgId::from_uuid(org_id), ApiProduct::Native, &idempotency)
            .await?;
        assert_eq!(
            admission.lock_target_lifecycle(target_id).await?,
            expected,
            "{case}"
        );
    }
    Ok(())
}

async fn seed_sandbox(
    pool: &PgPool,
    id: SandboxId,
    user_id: UserId,
    org_id: OrgId,
    lifecycle: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query(sql!(
        "INSERT INTO sandboxes
             (id, user_id, org_id, managed_by, lifecycle, network_policy_generation)
         VALUES ($1, $2, $3, 'k8s', $4, 1)"
    ))
    .bind(id)
    .bind(user_id)
    .bind(org_id)
    .bind(lifecycle)
    .execute(pool)
    .await?;
    Ok(())
}

async fn bind_file_key(
    pool: &PgPool,
    org_id: OrgId,
    path_template: &'static str,
    key: &str,
    fingerprint: char,
) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
    let key = IdempotencyKey::parse(key)?;
    let fingerprint = RequestFingerprint::parse(fingerprint.to_string().repeat(64))?;
    Ok(bind_idempotency_key(
        pool,
        org_id,
        &CreatePublicOperationIdempotency {
            method: HttpOperationMethod::Post,
            path_template: OperationPathTemplate::new(path_template),
            key: &key,
            request_fingerprint: &fingerprint,
        },
    )
    .await?)
}

async fn expire_keys(pool: &PgPool, org_id: OrgId, keys: &[&str]) -> Result<(), sqlx::Error> {
    sqlx::query(sql!(
        "UPDATE public_idempotency_records
         SET expires_at = NOW() - INTERVAL '1 second'
         WHERE org_id = $1 AND idempotency_key = ANY($2)"
    ))
    .bind(org_id)
    .bind(keys)
    .execute(pool)
    .await?;
    Ok(())
}

/// An expired binding takes the next request's fingerprint, and from then on
/// refuses the first one.
#[tokio::test]
async fn an_expired_key_binding_takes_the_next_request() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let org_id = OrgId::from_uuid(seed_org(&db.pool).await?);

    assert!(bind_file_key(&db.pool, org_id, FILE_WRITE, "key", 'a').await?);
    assert!(bind_file_key(&db.pool, org_id, FILE_WRITE, "key", 'a').await?);
    assert!(!bind_file_key(&db.pool, org_id, FILE_WRITE, "key", 'b').await?);
    expire_keys(&db.pool, org_id, &["key"]).await?;
    assert!(bind_file_key(&db.pool, org_id, FILE_WRITE, "key", 'b').await?);
    assert!(!bind_file_key(&db.pool, org_id, FILE_WRITE, "key", 'a').await?);
    Ok(())
}

const FILE_WRITE: &str = "/v1/sandboxes/{sandbox_id}/files:write";

/// Hold `renew` uncommitted on an expired binding, start `sweep` so it
/// selects that binding as expired and waits on it, then commit the renewal
/// and let the sweep finish. Returns whether the renewed binding, with
/// fingerprint `b`, survived: it still refuses fingerprint `c`.
async fn renewal_survives_a_waiting_sweep<R, S>(
    db: &pplx_space_db_test_fixtures::TestDb,
    renew: R,
    sweep: S,
) -> Result<bool, Box<dyn std::error::Error + Send + Sync>>
where
    R: for<'c> FnOnce(
        &'c mut sqlx::PgConnection,
        OrgId,
    ) -> futures_util::future::BoxFuture<'c, Result<(), sqlx::Error>>,
    S: std::future::Future<Output = Result<(), sqlx::Error>> + Send + 'static,
{
    let org_id = OrgId::from_uuid(seed_org(&db.pool).await?);
    assert!(bind_file_key(&db.pool, org_id, FILE_WRITE, "key", 'a').await?);
    expire_keys(&db.pool, org_id, &["key"]).await?;

    let mut renewal = db.pool.begin().await?;
    renew(&mut renewal, org_id).await?;
    let sweep = tokio::spawn(sweep);
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        let waiting: bool = sqlx::query_scalar(sql!(
            "SELECT EXISTS (
                 SELECT 1 FROM pg_stat_activity
                 WHERE datname = current_database()
                   AND wait_event_type = 'Lock'
                   AND query LIKE '%DELETE FROM public_idempotency_records%'
             )"
        ))
        .fetch_one(&db.pool)
        .await?;
        if waiting {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the sweep never waited on the renewed binding"
        );
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    renewal.commit().await?;
    sweep.await??;

    Ok(!bind_file_key(&db.pool, org_id, FILE_WRITE, "key", 'c').await?)
}

/// A controller released before the sweep rechecked `expires_at` deletes
/// whatever row still has the `id` it selected. A binding renewed while that
/// sweep waits on it survives anyway, because the renewal gives it a new id.
#[tokio::test]
async fn a_renewed_key_binding_survives_a_sweep_that_deletes_by_id() -> TestResult {
    let Some(db) = try_pg_test_db_with_max_connections(3).await? else {
        return Ok(());
    };
    let pool = db.pool.clone();
    let sweep = async move {
        // The retention DELETE as such a controller runs it.
        sqlx::query(sql!(
            "DELETE FROM public_idempotency_records
             WHERE id IN (
                 SELECT id
                 FROM public_idempotency_records
                 WHERE expires_at <= NOW()
                 ORDER BY expires_at ASC, id ASC
                 LIMIT 100
             )"
        ))
        .execute(&pool)
        .await
        .map(drop)
    };
    let survived = renewal_survives_a_waiting_sweep(
        &db,
        |conn, org_id| {
            Box::pin(async move {
                let key = IdempotencyKey::parse("key").expect("valid key");
                let fingerprint = RequestFingerprint::parse("b".repeat(64)).expect("valid");
                let bound = bind_idempotency_key(
                    conn,
                    org_id,
                    &CreatePublicOperationIdempotency {
                        method: HttpOperationMethod::Post,
                        path_template: OperationPathTemplate::new(FILE_WRITE),
                        key: &key,
                        request_fingerprint: &fingerprint,
                    },
                )
                .await?;
                assert!(bound);
                Ok(())
            })
        },
        sweep,
    )
    .await?;
    assert!(survived, "the sweep deleted the renewed binding");
    Ok(())
}

/// The sweep rechecks expiry on the row it deletes, so a record that a
/// writer extends in place while the sweep waits on it is kept.
#[tokio::test]
async fn the_retention_sweep_keeps_a_record_renewed_while_it_waits() -> TestResult {
    let Some(db) = try_pg_test_db_with_max_connections(3).await? else {
        return Ok(());
    };
    let store = PublicOperationStore::new(db.pool.clone());
    let sweep = async move {
        store
            .gc_expired(PublicOperationGcRequest {
                max_records: 100,
                payload_scrub_rules: &[],
            })
            .await
            .map(drop)
            .map_err(|error| sqlx::Error::Protocol(error.to_string()))
    };
    let survived = renewal_survives_a_waiting_sweep(
        &db,
        |conn, org_id| {
            Box::pin(async move {
                sqlx::query(sql!(
                    "UPDATE public_idempotency_records
                     SET request_fingerprint = $2,
                         expires_at = NOW() + INTERVAL '10 minutes'
                     WHERE org_id = $1 AND idempotency_key = 'key'"
                ))
                .bind(org_id)
                .bind("b".repeat(64))
                .execute(conn)
                .await?;
                Ok(())
            })
        },
        sweep,
    )
    .await?;
    assert!(survived, "the sweep deleted the renewed record");
    Ok(())
}

async fn seed_org(pool: &PgPool) -> Result<Uuid, sqlx::Error> {
    let id = Uuid::now_v7();
    sqlx::query(sql!(
        "INSERT INTO organizations (
             id,
             name,
             slug,
             byok_enabled
         )
         VALUES ($1, $2, $3, false)"
    ))
    .bind(id)
    .bind(format!("Org {id}"))
    .bind(format!("org-{}", id.simple()))
    .execute(pool)
    .await?;
    Ok(id)
}

async fn seed_user(pool: &PgPool, org_id: OrgId) -> Result<Uuid, sqlx::Error> {
    let id = Uuid::now_v7();
    sqlx::query(sql!(
        "INSERT INTO users (
             id,
             email,
             password_hash,
             display_name,
             org_id,
             role
         )
         VALUES ($1, $2, 'hash', 'Test User', $3, 'member')"
    ))
    .bind(id)
    .bind(format!("{id}@example.test"))
    .bind(org_id)
    .execute(pool)
    .await?;
    Ok(id)
}

/// Adding or dropping a `public_operations` column must not break the
/// operation reads on a connection that already ran them. A `SELECT *` would:
/// the column change alters its result type, and Postgres then refuses the
/// connection's cached statement with `0A000` ("cached plan must not change
/// result type") until the pool recycles the connection. One connection runs
/// every read, the table gains and then loses a column, and the same
/// connection runs every read again and gets the same records back.
#[tokio::test]
async fn operation_reads_survive_a_column_change_on_the_same_connection() -> TestResult {
    let Some(db) = try_pg_test_db_with_max_connections(1).await? else {
        return Ok(());
    };
    let org_id = OrgId::from_uuid(seed_org(&db.pool).await?);
    let actor_id = UserId::from_uuid(seed_user(&db.pool, org_id).await?);
    let store = PublicOperationStore::new(db.pool.clone());
    let created = store
        .create_operation(CreatePublicOperation {
            org_id,
            actor_user_id: Some(actor_id),
            api_product: ApiProduct::Native,
            kind: OperationKind::ForkSandbox,
            target_type: Some(OperationTargetType::Sandbox),
            target_id: Some("column-change-probe"),
            payload_json: serde_json::json!({}),
            idempotency: None,
            traceparent: None,
        })
        .await?;
    let CreatePublicOperationResult::Created(operation_id) = created else {
        return Err(format!("operation was not created: {created:?}").into());
    };
    let backend = || async {
        sqlx::query_scalar::<_, i32>("SELECT pg_backend_pid()")
            .fetch_one(&db.pool)
            .await
    };
    let read_all = || async {
        let public = store.fetch_operation(org_id, operation_id).await?;
        let admin = store.fetch_operation_admin(operation_id).await?;
        let listed = store
            .list_operations_admin(ListAdminPublicOperations {
                limit: 10,
                org_id: Some(org_id),
                state: None,
                kind: None,
                target_type: None,
                target_id: Some("column-change-probe".to_string()),
            })
            .await?;
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>((
            public,
            serde_json::to_value(admin)?,
            serde_json::to_value(listed)?,
        ))
    };
    let connection = backend().await?;
    let before = read_all().await?;
    assert_eq!(
        before.0.as_ref().map(|record| record.id),
        Some(operation_id)
    );
    assert_eq!(
        before.1["id"],
        serde_json::to_value(operation_id)?,
        "the admin read finds the operation"
    );
    assert_eq!(
        before.2.as_array().map(Vec::len),
        Some(1),
        "the list finds the operation"
    );

    sqlx::query("ALTER TABLE public_operations ADD COLUMN column_change_probe INTEGER")
        .execute(&db.pool)
        .await?;
    assert_eq!(read_all().await?, before, "after a column is added");

    sqlx::query("ALTER TABLE public_operations DROP COLUMN column_change_probe")
        .execute(&db.pool)
        .await?;
    assert_eq!(read_all().await?, before, "after a column is dropped");
    assert_eq!(
        backend().await?,
        connection,
        "every read ran on the one connection, so it reused its cached statements"
    );
    Ok(())
}
