use pplx_space_common_core::types::{
    AutoLifecycleConditionSet, AutoLifecycleConditionSetId, AutoLifecycleIdleAction,
    AutoLifecycleStage,
};
use pplx_space_common_core::types::{NodeId, SandboxId};
use pplx_space_common_core::types::{OrgId, UserId};
use uuid::Uuid;

use super::*;
use crate::public_operations::SubjectSandboxQuota;
use pplx_space_db_test_fixtures::{
    SEED_ORG_ID, SEED_USER_ID, TestDbResult, insert_ready_worker_node, pg_test_db,
    pg_test_db_with_max_connections,
};

mod substrate_scope;

const INTERNAL_FAILURE_REASON: &str = "guest health probe timed out: internal socket detail";
const CURRENT_EXECUTION_GENERATION: i64 = 7;
const CURRENT_POD_UID: &str = "pod-attempt-a";
const REPLACEMENT_POD_UID: &str = "pod-attempt-b";

fn create_request_with_auto_lifecycle(
    auto_lifecycle: Option<AutoLifecyclePolicy>,
) -> CreateSandboxRequest {
    CreateSandboxRequest {
        template: "seed:latest".parse().unwrap(),
        runtime: None,
        name: None,
        env: Default::default(),
        metadata: Default::default(),
        resources: None,
        network_policy: None,
        auto_lifecycle,
        idle_timeout: None,
        idle_action: None,
    }
}

#[test]
fn native_create_defaults_to_suspending_after_six_hours_idle() {
    let requests = [
        serde_json::json!({ "template": "seed:latest" }),
        serde_json::json!({ "template": "seed:latest", "auto_lifecycle": null }),
    ];

    for request in requests {
        let request: CreateSandboxRequest = serde_json::from_value(request).unwrap();
        let request = with_native_create_defaults(request);
        let policy = request.auto_lifecycle.unwrap();

        assert_eq!(policy.wake, Default::default());
        assert_eq!(
            policy.condition_sets,
            std::collections::BTreeMap::from([(
                AutoLifecycleConditionSetId::AccessIdle,
                AutoLifecycleConditionSet::access_idle(),
            )])
        );
        assert_eq!(
            policy.stages,
            vec![AutoLifecycleStage {
                condition_set: AutoLifecycleConditionSetId::AccessIdle,
                at: DEFAULT_NATIVE_IDLE_TIMEOUT_SECS,
                action: AutoLifecycleIdleAction::Suspend,
            }]
        );
    }
}

#[test]
fn native_create_preserves_explicit_auto_lifecycle_policy() {
    let explicit_policy = AutoLifecyclePolicy::disabled();
    let (request, _) = prepare_native_create_request(create_request_with_auto_lifecycle(Some(
        explicit_policy.clone(),
    )))
    .unwrap();

    assert_eq!(request.auto_lifecycle, Some(explicit_policy));
}

#[test]
fn native_create_default_preserves_pre_deploy_idempotency_fingerprint() {
    let request = create_request_with_auto_lifecycle(None);
    let pre_deploy_fingerprint = request_fingerprint(&request).unwrap();
    let legacy_request = serde_json::json!({
        "template": "seed:latest", "runtime": null, "name": null, "env": {},
        "metadata": {}, "resources": null, "network_policy": null, "auto_lifecycle": null
    });
    assert_eq!(serde_json::to_value(&request).unwrap(), legacy_request);

    let (_, post_deploy_fingerprint) = prepare_native_create_request(request).unwrap();

    assert_eq!(post_deploy_fingerprint, pre_deploy_fingerprint);
}

#[tokio::test]
async fn native_create_idle_settings_validate_before_admission_with_typed_errors() {
    use axum::response::IntoResponse;
    use http_body_util::BodyExt;
    use pplx_space_public_api::PublicErrorEnvelope;

    for (timeout, action, policy, accepted) in [
        (None, None, None, true),
        (Some("600s"), None, None, true),
        (Some("1.5m"), Some(SandboxIdleAction::Suspend), None, true),
        (Some("600s"), Some(SandboxIdleAction::Delete), None, true),
        (None, Some(SandboxIdleAction::Suspend), None, false),
        (None, Some(SandboxIdleAction::Delete), None, false),
        (None, None, Some(AutoLifecyclePolicy::disabled()), true),
        (
            Some("600s"),
            None,
            Some(AutoLifecyclePolicy::disabled()),
            false,
        ),
        (
            None,
            Some(SandboxIdleAction::Delete),
            Some(AutoLifecyclePolicy::disabled()),
            false,
        ),
        (
            Some("600s"),
            Some(SandboxIdleAction::Suspend),
            Some(AutoLifecyclePolicy::disabled()),
            false,
        ),
        (
            Some("600s"),
            Some(SandboxIdleAction::Other("hibernate".to_owned())),
            None,
            false,
        ),
        (Some("1.5s"), None, None, false),
        (Some("9223372036854775808s"), None, None, false),
    ] {
        let mut request = create_request_with_auto_lifecycle(policy);
        request.idle_timeout = timeout.map(|value| value.parse().unwrap());
        request.idle_action = action;
        match prepare_native_create_request(request.clone()) {
            Ok(_) => assert!(accepted, "{request:?}"),
            Err(error) => {
                assert!(!accepted, "{request:?}");
                let response = error.into_response();
                assert_eq!(response.status(), StatusCode::BAD_REQUEST);
                let body = response.into_body().collect().await.unwrap().to_bytes();
                let envelope: PublicErrorEnvelope = serde_json::from_slice(&body).unwrap();
                assert_eq!(envelope.code, PublicErrorCode::InvalidRequest);
            }
        }
    }
}

#[test]
fn native_create_idle_policies_match_compatibility_policies() {
    for (timeout, action, seconds, auto_pause) in [
        ("600s", None, 600, true),
        ("1.5m", Some(SandboxIdleAction::Suspend), 90, true),
        ("600s", Some(SandboxIdleAction::Delete), 600, false),
    ] {
        let mut request = create_request_with_auto_lifecycle(None);
        request.idle_timeout = Some(timeout.parse().unwrap());
        request.idle_action = action;
        let fingerprint = request_fingerprint(&request).unwrap();
        let (prepared, prepared_fingerprint) = prepare_native_create_request(request).unwrap();
        assert_eq!(prepared_fingerprint, fingerprint);
        let expected = crate::e2b::auto_lifecycle_policy(seconds, auto_pause, auto_pause);
        assert_eq!(
            prepared.auto_lifecycle.as_ref().unwrap().wake_on_access(),
            auto_pause
        );
        assert_eq!(prepared.auto_lifecycle, Some(expected));
        assert!(prepared.idle_timeout.is_none());
        assert!(prepared.idle_action.is_none());
    }
}

#[test]
fn native_create_rejects_unsupported_disk_size_before_operation_admission() {
    for (disk_mib, accepted) in [
        (0, false),
        (1, true),
        (128 * 1024, true),
        (128 * 1024 + 1, false),
        (u32::MAX, false),
    ] {
        let mut request = create_request_with_auto_lifecycle(None);
        request.resources = Some(pplx_space_public_api::SandboxResources {
            disk_mib: Some(disk_mib),
            vcpu: None,
            memory_mib: None,
        });
        assert_eq!(
            prepare_native_create_request(request).is_ok(),
            accepted,
            "{disk_mib}"
        );
    }
}

#[test]
fn native_create_rejects_zero_vcpu_before_operation_admission() {
    for (vcpu, accepted) in [(None, true), (Some(0), false), (Some(1), true)] {
        let mut request = create_request_with_auto_lifecycle(None);
        request.resources = Some(SandboxResources {
            vcpu,
            memory_mib: None,
            disk_mib: None,
        });
        assert_eq!(
            prepare_native_create_request(request).is_ok(),
            accepted,
            "{vcpu:?}"
        );
    }
}

#[tokio::test]
async fn native_create_persists_default_policy_in_operation_payload() -> TestDbResult<()> {
    let db = pg_test_db().await?;
    let context: crate::fork_operations::ForkFederatedCreateContext =
        serde_json::from_value(serde_json::json!({
            "issuer": "example-issuer", "org_id": "example-org", "subject_id": "example-user"
        }))?;
    let selections = [
        None, // Payloads admitted before profile selection was introduced.
        Some(crate::egress_profiles::EgressProfileSelection::default()),
        Some(serde_json::from_value(serde_json::json!(["sensor"]))?),
        Some(serde_json::from_value(serde_json::json!(["audit"]))?),
    ];
    for (initial, selection) in selections.iter().enumerate() {
        let idempotency_key =
            IdempotencyKey::parse(format!("native-default-policy-persistence-{initial}"))?;
        let mut admitted = context.clone();
        admitted.egress_profiles = selection.clone();
        let response = create_native_sandbox_operation(
            &db.pool,
            OrgId::from_uuid(SEED_ORG_ID),
            UserId::from_uuid(SEED_USER_ID),
            &idempotency_key,
            create_request_with_auto_lifecycle(None),
            Some(admitted),
            None,
            None,
        )
        .await
        .expect("native create should persist an operation");
        for (retry, selection) in selections.iter().enumerate() {
            let mut retried = context.clone();
            retried.egress_profiles = selection.clone();
            let result = create_native_sandbox_operation(
                &db.pool,
                OrgId::from_uuid(SEED_ORG_ID),
                UserId::from_uuid(SEED_USER_ID),
                &idempotency_key,
                create_request_with_auto_lifecycle(None),
                Some(retried),
                None,
                None,
            )
            .await;
            if initial == retry || (initial < 2 && retry < 2) {
                assert_eq!(
                    result
                        .expect("equivalent selections must reuse the operation")
                        .operation_id,
                    response.operation_id,
                );
            } else {
                assert_eq!(
                    axum::response::IntoResponse::into_response(
                        result.expect_err("different nonempty selections must conflict")
                    )
                    .status(),
                    axum::http::StatusCode::CONFLICT,
                );
            }
        }

        let SqlJson(payload): SqlJson<serde_json::Value> = sqlx::query_scalar(sql!(
            "SELECT payload_json FROM public_operations WHERE operation_id = $1"
        ))
        .bind(response.operation_id.as_uuid())
        .fetch_one(&db.pool)
        .await?;
        let payload: CreateSandboxOperationPayload = serde_json::from_value(payload)?;
        assert_eq!(
            &payload
                .federated_create
                .expect("federated context is durable")
                .egress_profiles,
            selection,
            "explicitly empty selection must survive serialization and retries",
        );
        let policy = payload
            .request
            .auto_lifecycle
            .expect("native default must be durable in the operation payload");

        assert_eq!(
            policy.stages,
            vec![AutoLifecycleStage {
                condition_set: AutoLifecycleConditionSetId::AccessIdle,
                at: DEFAULT_NATIVE_IDLE_TIMEOUT_SECS,
                action: AutoLifecycleIdleAction::Suspend,
            }]
        );
    }
    Ok(())
}

/// A native create past a limit is refused with a non-retryable 429 naming
/// that limit and records nothing; a replay of an admitted create still
/// answers, and an uncapped caller (the default) is never refused.
#[tokio::test]
async fn native_create_refuses_a_new_create_at_the_org_quota() -> TestDbResult<()> {
    use http_body_util::BodyExt;
    use pplx_space_public_api::PublicErrorEnvelope;

    let db = pg_test_db().await?;
    let (org_id, user_id) = seed_quota_org(&db.pool).await?;
    let create = |key: &'static str, quota: Option<SandboxQuota>| {
        let pool = db.pool.clone();
        async move {
            create_native_sandbox_operation(
                &pool,
                org_id,
                user_id,
                &IdempotencyKey::parse(key).expect("test key parses"),
                create_request_with_auto_lifecycle(None),
                None,
                None,
                quota,
            )
            .await
        }
    };
    let org_limit = |limit| {
        Some(SandboxQuota {
            org_id,
            org_limit: Some(limit),
            subject: None,
        })
    };
    let refusal = |result: Result<SandboxOperationResponse, PublicApiError>| async move {
        let refused = result.expect_err("at the limit").into_response();
        assert_eq!(refused.status(), StatusCode::TOO_MANY_REQUESTS);
        let body = refused.into_body().collect().await.unwrap().to_bytes();
        serde_json::from_slice::<PublicErrorEnvelope>(&body).unwrap()
    };
    let create_operations = || async {
        sqlx::query_scalar::<_, i64>(sql!(
            "SELECT COUNT(*) FROM public_operations WHERE org_id = $1"
        ))
        .bind(org_id)
        .fetch_one(&db.pool)
        .await
    };

    let first = create("quota-first", org_limit(2))
        .await
        .expect("under the limit");
    create("quota-second", org_limit(2))
        .await
        .expect("reaches the limit");

    let envelope = refusal(create("quota-third", org_limit(2)).await).await;
    assert_eq!(envelope.code, PublicErrorCode::QuotaExceeded);
    assert!(!envelope.retryable, "a delay does not lift the limit");
    assert!(
        envelope
            .message
            .starts_with("organization sandbox limit of 2 reached"),
        "{}",
        envelope.message
    );
    assert_eq!(create_operations().await?, 2, "a refusal records nothing");

    let replay = create("quota-first", org_limit(2))
        .await
        .expect("a replay of an admitted create is not counted again");
    assert_eq!(replay.operation_id, first.operation_id);

    create("quota-uncapped", None)
        .await
        .expect("an uncapped caller is never refused");
    assert_eq!(create_operations().await?, 3);

    let per_user = Some(SandboxQuota {
        org_id,
        org_limit: None,
        subject: Some(SubjectSandboxQuota { user_id, limit: 3 }),
    });
    let envelope = refusal(create("quota-per-user", per_user).await).await;
    assert!(!envelope.retryable);
    assert!(
        envelope
            .message
            .starts_with("sandbox limit of 3 per user reached"),
        "{}",
        envelope.message
    );
    Ok(())
}

/// A create that waits out its quota lock answers the retryable 503 instead
/// of holding its connection until the holder commits.
#[tokio::test]
async fn a_native_create_that_waits_out_its_quota_lock_is_retryable() -> TestDbResult<()> {
    use http_body_util::BodyExt;
    use pplx_space_public_api::PublicErrorEnvelope;

    let db = pg_test_db_with_max_connections(3).await?;
    let (org_id, user_id) = seed_quota_org(&db.pool).await?;
    let holder = hold_org_quota_lock(&db.pool, org_id).await?;
    let refused = create_native_sandbox_operation(
        &db.pool,
        org_id,
        user_id,
        &IdempotencyKey::parse("quota-contended").expect("test key parses"),
        create_request_with_auto_lifecycle(None),
        None,
        None,
        Some(SandboxQuota {
            org_id,
            org_limit: Some(1),
            subject: None,
        }),
    )
    .await
    .expect_err("the quota lock stays held")
    .into_response();
    drop(holder);

    assert_eq!(refused.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert!(
        refused
            .headers()
            .contains_key(axum::http::header::RETRY_AFTER)
    );
    let body = refused.into_body().collect().await?.to_bytes();
    let envelope = serde_json::from_slice::<PublicErrorEnvelope>(&body)?;
    assert_eq!(envelope.code, PublicErrorCode::ServiceUnavailable);
    assert!(envelope.retryable);
    Ok(())
}

async fn seed_quota_org(pool: &PgPool) -> TestDbResult<(OrgId, UserId)> {
    let org_id = OrgId::new();
    let user_id = UserId::new();
    sqlx::query(sql!(
        "INSERT INTO organizations (id, name, slug) VALUES ($1, $2, $2)"
    ))
    .bind(org_id)
    .bind(format!("quota-{org_id}"))
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO users (id, email, password_hash, display_name, org_id, role)
         VALUES ($1, $2, 'hash', 'Quota', $3, 'member')"
    ))
    .bind(user_id)
    .bind(format!("quota-{user_id}@example.com"))
    .bind(org_id)
    .execute(pool)
    .await?;
    Ok((org_id, user_id))
}

/// Hold `org_id`'s quota lock exclusively, as an admitted create under a
/// limit of one does until its transaction ends. Dropping it rolls back.
async fn hold_org_quota_lock(
    pool: &PgPool,
    org_id: OrgId,
) -> TestDbResult<PublicOperationAdmission> {
    let key = IdempotencyKey::parse("quota-lock-holder").expect("test key parses");
    let fingerprint = RequestFingerprint::parse("e".repeat(64)).expect("valid fingerprint");
    let mut admission = PublicOperationStore::new(pool.clone())
        .begin_operation_admission(
            org_id,
            ApiProduct::Native,
            &CreatePublicOperationIdempotency {
                method: HttpOperationMethod::Post,
                path_template: OperationPathTemplate::new("/sandboxes"),
                key: &key,
                request_fingerprint: &fingerprint,
            },
        )
        .await?;
    let outcome = admission
        .admit_new_create(SandboxQuota {
            org_id,
            org_limit: Some(1),
            subject: None,
        })
        .await?;
    assert_eq!(outcome, SandboxQuotaOutcome::Admit);
    Ok(admission)
}

/// Wait until some backend waits on an advisory lock, as a create queued
/// on a held quota lock does.
async fn wait_for_advisory_lock_waiter(pool: &PgPool) -> TestDbResult<()> {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        let waiting: bool = sqlx::query_scalar(sql!(
            "SELECT EXISTS (
                 SELECT 1 FROM pg_stat_activity
                  WHERE datname = current_database()
                    AND wait_event_type = 'Lock'
                    AND wait_event = 'advisory'
             )"
        ))
        .fetch_one(pool)
        .await?;
        if waiting {
            return Ok(());
        }
        if std::time::Instant::now() >= deadline {
            return Err("no create waited on the quota lock".into());
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
}

#[test]
fn sandbox_lifecycle_mapping_uses_public_vocabulary() {
    assert_eq!(
        public_lifecycle_state("started").unwrap(),
        SandboxLifecycleState::Running
    );
    assert_eq!(
        public_lifecycle_state("checkpointing").unwrap(),
        SandboxLifecycleState::Suspending
    );
    assert_eq!(
        public_lifecycle_state("paused").unwrap(),
        SandboxLifecycleState::Paused
    );
    public_lifecycle_state("renamed").unwrap_err();
}

#[test]
fn sandbox_idle_deadline_is_nullable_and_serialized_as_rfc3339() {
    for (deadline, expected) in [
        (None, serde_json::Value::Null),
        (Some(0), serde_json::json!("1970-01-01T00:00:00Z")),
        (
            Some(1_700_000_000),
            serde_json::json!("2023-11-14T22:13:20Z"),
        ),
    ] {
        let mut row = public_sandbox_row(OffsetDateTime::UNIX_EPOCH, SandboxId::new());
        row.next_auto_event_at = deadline;
        let sandbox = public_sandbox_from_row(row).unwrap();
        assert_eq!(
            sandbox.expires_at.map(|time| time.unix_timestamp()),
            deadline
        );
        assert_eq!(
            serde_json::to_value(sandbox).unwrap()["expires_at"],
            expected
        );
    }
}

#[test]
fn sandbox_idle_configuration_requires_one_access_idle_delete_or_suspend_stage() {
    let default_policy = with_native_create_defaults(create_request_with_auto_lifecycle(None))
        .auto_lifecycle
        .unwrap();
    let mut cases = vec![
        (
            default_policy.clone(),
            Some("21600s".to_owned()),
            Some(SandboxIdleAction::Suspend),
        ),
        (AutoLifecyclePolicy::disabled(), None, None),
    ];
    for (action, expected_action) in [
        (AutoLifecycleIdleAction::None, None),
        (AutoLifecycleIdleAction::Pause, None),
        (
            AutoLifecycleIdleAction::Suspend,
            Some(SandboxIdleAction::Suspend),
        ),
        (AutoLifecycleIdleAction::Stop, None),
        (
            AutoLifecycleIdleAction::Delete,
            Some(SandboxIdleAction::Delete),
        ),
    ] {
        let mut policy = default_policy.clone();
        policy.stages[0].at = 600;
        policy.stages[0].action = action;
        cases.push((
            policy,
            expected_action.as_ref().map(|_| "600s".to_owned()),
            expected_action,
        ));
    }
    let mut command_policy = default_policy.clone();
    command_policy.condition_sets = std::collections::BTreeMap::from([(
        AutoLifecycleConditionSetId::CommandIdle,
        AutoLifecycleConditionSet::command_idle(),
    )]);
    command_policy.stages[0].condition_set = AutoLifecycleConditionSetId::CommandIdle;
    cases.push((command_policy.clone(), None, None));
    for second_stage in [
        default_policy.stages[0].clone(),
        command_policy.stages[0].clone(),
    ] {
        let mut policy = default_policy.clone();
        policy
            .condition_sets
            .extend(command_policy.condition_sets.clone());
        policy.stages.push(second_stage);
        cases.push((policy, None, None));
    }
    let mut non_access_policy = default_policy.clone();
    non_access_policy.condition_sets.insert(
        AutoLifecycleConditionSetId::AccessIdle,
        AutoLifecycleConditionSet::command_idle(),
    );
    cases.push((non_access_policy, None, None));
    let mut long_policy = default_policy;
    long_policy.stages[0].at = u64::from(u32::MAX) + 1;
    cases.push((
        long_policy,
        Some("4294967296s".to_owned()),
        Some(SandboxIdleAction::Suspend),
    ));

    for (policy, expected_timeout, expected_action) in cases {
        let mut row = public_sandbox_row(OffsetDateTime::UNIX_EPOCH, SandboxId::new());
        row.auto_lifecycle = SqlJson(policy.clone());
        let sandbox = public_sandbox_from_row(row).unwrap();
        assert_eq!(sandbox.idle_timeout, expected_timeout, "{policy:?}");
        assert_eq!(sandbox.idle_action, expected_action, "{policy:?}");
    }
}

#[test]
fn sandbox_list_limit_defaults_and_caps() {
    assert_eq!(
        ListSandboxesQuery {
            cursor: None,
            limit: None,
        }
        .limit(),
        DEFAULT_SANDBOX_LIST_LIMIT
    );
    assert_eq!(
        ListSandboxesQuery {
            cursor: None,
            limit: Some(MAX_SANDBOX_LIST_LIMIT + 1),
        }
        .limit(),
        MAX_SANDBOX_LIST_LIMIT
    );
}

#[test]
fn sandbox_cursor_round_trips() {
    let created_at = OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap();
    let id = SandboxId::new();
    let cursor = sandbox_cursor(created_at, id).unwrap();
    let payload = decode_cursor(CursorKind::Sandbox, cursor.as_str()).unwrap();
    assert_eq!(parse_sandbox_cursor(&payload).unwrap(), (created_at, id));
}

#[test]
fn sandboxes_page_uses_limit_plus_one_sentinel() {
    let first_created_at = OffsetDateTime::from_unix_timestamp(1_700_000_002).unwrap();
    let second_created_at = OffsetDateTime::from_unix_timestamp(1_700_000_001).unwrap();
    let third_created_at = OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap();
    let first_id = SandboxId::new();
    let second_id = SandboxId::new();
    let rows = vec![
        public_sandbox_row(first_created_at, first_id),
        public_sandbox_row(second_created_at, second_id),
        public_sandbox_row(third_created_at, SandboxId::new()),
    ];

    let page = sandboxes_page(rows, 2).unwrap();

    assert_eq!(page.data.len(), 2);
    assert_eq!(
        page.next_cursor
            .as_ref()
            .and_then(|cursor| decode_cursor(CursorKind::Sandbox, cursor.as_str()).ok())
            .and_then(|payload| parse_sandbox_cursor(&payload).ok()),
        Some((second_created_at, second_id))
    );

    let exact_page = sandboxes_page(
        vec![
            public_sandbox_row(first_created_at, first_id),
            public_sandbox_row(second_created_at, second_id),
        ],
        2,
    )
    .unwrap();
    assert_eq!(exact_page.data.len(), 2);
    assert!(exact_page.next_cursor.is_none());
}

#[tokio::test]
async fn delete_replay_survives_target_removal_without_crossing_tenant_or_request_scope()
-> TestDbResult<()> {
    use crate::public_operations::{
        CompleteClaimedPublicOperation, CreateClaimedPublicOperationResult,
    };

    let db = pg_test_db().await?;
    let org_id = OrgId::from_uuid(SEED_ORG_ID);
    let actor = UserId::from_uuid(SEED_USER_ID);
    let sandbox_id = PublicSandboxId::from(SandboxId::new());
    let key = IdempotencyKey::parse("completed-delete")?;
    let action = SandboxLifecycleAction::Delete;
    let fingerprint = lifecycle_request_fingerprint(sandbox_id, action, None).unwrap();
    let target = sandbox_id.to_string();
    let store = PublicOperationStore::new(db.pool.clone());
    let CreateClaimedPublicOperationResult::Claimed {
        claim_token,
        operation,
    } = store
        .create_claimed_operation(CreatePublicOperation {
            org_id,
            actor_user_id: Some(actor),
            api_product: ApiProduct::Native,
            kind: action.operation_kind(),
            target_type: Some(OperationTargetType::Sandbox),
            target_id: Some(&target),
            payload_json: SandboxLifecycleOperationPayload {
                sandbox_id,
                action,
                rollback: None,
                wake_activity: None,
            }
            .into_json(),
            idempotency: Some(CreatePublicOperationIdempotency {
                method: lifecycle_method(action),
                path_template: lifecycle_path_template(action),
                key: &key,
                request_fingerprint: &fingerprint,
            }),
            traceparent: None,
        })
        .await?
    else {
        panic!("fresh operation must be claimed")
    };
    assert!(
        store
            .complete_claimed_operation(CompleteClaimedPublicOperation {
                claim_token,
                operation_id: operation.id,
                progress_message: "deleted".into(),
                result_json: serde_json::to_value(
                    pplx_space_public_api::PublicOperationResult::Sandbox(
                        pplx_space_public_api::PublicSandboxOperationResult {
                            sandbox_id,
                            state: SandboxLifecycleState::Deleted,
                            action: Some(action),
                            controller_response: None,
                        },
                    )
                )?,
            })
            .await?
    );

    // The operation outlives its target row. Only an exact, tenant-scoped
    // replay can recover it; a new request must still prove target ownership.
    for (org, target, key, expected) in [
        (
            org_id,
            target.clone(),
            "completed-delete",
            StatusCode::ACCEPTED,
        ),
        (org_id, target.clone(), "new-delete", StatusCode::NOT_FOUND),
        (
            OrgId::from_uuid(Uuid::now_v7()),
            target.clone(),
            "completed-delete",
            StatusCode::NOT_FOUND,
        ),
        (
            org_id,
            PublicSandboxId::from(SandboxId::new()).to_string(),
            "completed-delete",
            StatusCode::CONFLICT,
        ),
    ] {
        let mut headers = HeaderMap::new();
        headers.insert(IDEMPOTENCY_KEY_HEADER, key.parse()?);
        let result =
            enqueue_lifecycle_operation(&db.pool, org, actor, headers, &target, action, None).await;
        match result {
            Ok((status, Json(response))) => {
                assert_eq!(status, expected);
                assert_eq!(response.operation_id, operation.id);
                assert_eq!(response.sandbox_id, sandbox_id);
            }
            Err(error) => assert_eq!(error.into_response().status(), expected),
        }
    }
    Ok(())
}

/// Record a native create of a new sandbox in the seed org, queued and
/// without its sandbox row.
async fn queued_native_create(pool: &PgPool) -> TestDbResult<SandboxOperationResponse> {
    Ok(create_native_sandbox_operation(
        pool,
        OrgId::from_uuid(SEED_ORG_ID),
        UserId::from_uuid(SEED_USER_ID),
        &IdempotencyKey::parse("queued-native-create")?,
        create_request_with_auto_lifecycle(None),
        None,
        None,
        None,
    )
    .await
    .expect("native create should be admitted"))
}

fn delete_headers() -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(IDEMPOTENCY_KEY_HEADER, "early-delete".parse().unwrap());
    headers
}

/// The create records its row and finishes after the delete's admission has
/// missed the row but before it looks for the create, so it finds no create
/// in flight either. It reads the row again and admits the delete.
#[tokio::test]
async fn a_delete_admitted_while_its_create_records_the_row_is_not_lost() -> TestDbResult<()> {
    let db = pg_test_db().await?;
    let created = queued_native_create(&db.pool).await?;
    let pool = db.pool.clone();
    let result = crate::test_support::finish_create_while_a_request_reads_operations(
        &db.pool,
        created.operation_id,
        created.sandbox_id,
        async move {
            enqueue_lifecycle_operation(
                &pool,
                OrgId::from_uuid(SEED_ORG_ID),
                UserId::from_uuid(SEED_USER_ID),
                delete_headers(),
                &created.sandbox_id.to_string(),
                SandboxLifecycleAction::Delete,
                None,
            )
            .await
        },
    )
    .await?;
    let (status, Json(deleted)) = result
        .map_err(|error| format!("the delete was refused: {}", error.into_response().status()))?;
    assert_eq!(status, StatusCode::ACCEPTED);
    assert_eq!(deleted.sandbox_id, created.sandbox_id);
    assert_ne!(deleted.operation_id, created.operation_id);
    Ok(())
}

/// Another organization's create in flight is not a delete target: the
/// delete is the unknown sandbox's 404, and the create stays queued.
#[tokio::test]
async fn a_delete_never_sees_another_orgs_create_in_flight() -> TestDbResult<()> {
    let db = pg_test_db().await?;
    let created = queued_native_create(&db.pool).await?;
    let result = enqueue_lifecycle_operation(
        &db.pool,
        OrgId::from_uuid(Uuid::now_v7()),
        UserId::from_uuid(SEED_USER_ID),
        delete_headers(),
        &created.sandbox_id.to_string(),
        SandboxLifecycleAction::Delete,
        None,
    )
    .await;
    let status = result.map_or_else(|error| error.into_response().status(), |(status, _)| status);
    assert_eq!(status, StatusCode::NOT_FOUND);
    let state: String = sqlx::query_scalar(sql!(
        "SELECT state FROM public_operations WHERE operation_id = $1"
    ))
    .bind(created.operation_id.as_uuid())
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(state, "queued");
    Ok(())
}

#[tokio::test]
async fn sandbox_reads_exclude_deleted_tombstones_before_pagination() -> TestDbResult<()> {
    let db = pg_test_db().await?;
    let mut visible = Vec::new();
    for (offset, lifecycle) in ["started", "failed", "quarantined", "deleted", "deleted"]
        .into_iter()
        .enumerate()
    {
        let id = SandboxId::new();
        let created_at = OffsetDateTime::from_unix_timestamp(1_800_000_000 + offset as i64)?;
        let deadline = (offset % 2 == 0).then_some(1_800_021_600 + offset as i64);
        let policy = with_native_create_defaults(create_request_with_auto_lifecycle(None))
            .auto_lifecycle
            .unwrap();
        sqlx::query(pplx_space_common_core::sql!(
            "INSERT INTO sandboxes (
                 id, user_id, org_id, lifecycle, created_at,
                 network_policy_override_json, network_policy_generation,
                 next_auto_event_at, auto_lifecycle_json
             ) VALUES ($1, $2, $3, $4, $5, $6, 1, $7, $8)"
        ))
        .bind(id)
        .bind(SEED_USER_ID)
        .bind(SEED_ORG_ID)
        .bind(lifecycle)
        .bind(created_at)
        .bind(SqlJson(&pplx_space_net_common::NetworkPolicyV1::default()))
        .bind(deadline)
        .bind(SqlJson(&policy))
        .execute(&db.pool)
        .await?;

        let row = fetch_public_sandbox(
            &db.pool,
            OrgId::from_uuid(SEED_ORG_ID),
            PublicSandboxId::from(id),
        )
        .await?;
        if lifecycle == "deleted" {
            // The GET handler maps this absence to NotFound, not a lifecycle
            // decode failure. The durable tombstone itself must remain intact.
            assert!(row.is_none());
            assert!(fetch_sandbox(&db.pool, id).await?.is_some());
        } else {
            let row = row.expect("visible sandbox");
            assert_eq!(row.lifecycle, lifecycle);
            let sandbox = public_sandbox_from_row(row).unwrap();
            assert_eq!(
                sandbox.expires_at.map(|time| time.unix_timestamp()),
                deadline
            );
            assert_eq!(sandbox.idle_timeout.as_deref(), Some("21600s"));
            assert_eq!(sandbox.idle_action, Some(SandboxIdleAction::Suspend));
            visible.push((created_at, id));
        }
    }
    visible.reverse();

    let rows = fetch_sandboxes_page(&db.pool, OrgId::from_uuid(SEED_ORG_ID), None, None, 1).await?;
    assert_eq!(
        rows.iter().map(|row| row.id).collect::<Vec<_>>(),
        vec![visible[0].1, visible[1].1],
        "deleted rows must not consume the requested page or lookahead"
    );
    let page = sandboxes_page(rows, 1).expect("visible lifecycles project normally");
    assert_eq!(page.data.len(), 1);
    assert_eq!(
        page.data[0].expires_at.unwrap().unix_timestamp(),
        1_800_021_602
    );
    assert_eq!(page.data[0].idle_timeout.as_deref(), Some("21600s"));
    assert_eq!(page.data[0].idle_action, Some(SandboxIdleAction::Suspend));
    assert!(page.next_cursor.is_some());

    let next = fetch_sandboxes_page(
        &db.pool,
        OrgId::from_uuid(SEED_ORG_ID),
        None,
        Some(visible[0]),
        1,
    )
    .await?;
    assert_eq!(
        next.iter().map(|row| row.id).collect::<Vec<_>>(),
        vec![visible[1].1, visible[2].1]
    );
    Ok(())
}

#[tokio::test]
async fn sandbox_reads_expose_only_the_current_failure_episode() -> TestDbResult<()> {
    let db = pg_test_db().await?;
    let sandbox_id = SandboxId::new();
    let node_id = NodeId::new();
    let route_activation_id = Uuid::now_v7();
    insert_ready_worker_node(&db.pool, node_id).await?;
    sqlx::query(pplx_space_common_core::sql!(
        "INSERT INTO sandboxes (
             id, user_id, org_id, lifecycle, placement, managed_by, active_node_id,
             network_policy_override_json, network_policy_generation
         ) VALUES ($1, $2, $3, 'failed', 'on_node', 'k8s', $4, $5, 1)"
    ))
    .bind(sandbox_id)
    .bind(SEED_USER_ID)
    .bind(SEED_ORG_ID)
    .bind(node_id)
    .bind(sqlx::types::Json(
        &pplx_space_net_common::NetworkPolicyV1::default(),
    ))
    .execute(&db.pool)
    .await?;
    sqlx::query(pplx_space_common_core::sql!(
        "INSERT INTO sandbox_k8s_executions (
             sandbox_id, generation, pod_name, pod_namespace, pod_uid, state, active,
             route_activation_id
         ) VALUES
             ($1, $2 - 1, $3 || '-old', 'space', 'pod-old', 'failed', FALSE, NULL),
             ($1, $2, $3 || '-current', 'space', $4, 'failed', TRUE, $5)"
    ))
    .bind(sandbox_id)
    .bind(CURRENT_EXECUTION_GENERATION)
    .bind(format!("space-sbx-{}", sandbox_id.as_uuid().simple()))
    .bind(CURRENT_POD_UID)
    .bind(route_activation_id)
    .execute(&db.pool)
    .await?;
    sqlx::query(pplx_space_common_core::sql!(
        "INSERT INTO sandbox_observed_states (
             sandbox_id, node_id, status, failure_reason
         ) VALUES ($1, $2, 'failed', $3)"
    ))
    .bind(sandbox_id)
    .bind(node_id)
    .bind(INTERNAL_FAILURE_REASON)
    .execute(&db.pool)
    .await?;
    sqlx::query(pplx_space_common_core::sql!(
        "INSERT INTO node_facet_observed_status (
             node_id, facet, element, observed_payload, observed_at
         ) VALUES (
             $1,
             $2,
             $3,
             jsonb_build_object(
                 $4::TEXT,
                 jsonb_build_object(
                     $5::TEXT, $6::BIGINT,
                     $7::TEXT, $8::TEXT,
                     $9::TEXT, $10::TEXT
                 )
             ),
             NOW()
         )"
    ))
    .bind(node_id)
    .bind(FACET_SANDBOX_DESIRED_STATE)
    .bind(sandbox_id.to_string())
    .bind(SANDBOX_FAILURE_EPISODE_KEY)
    .bind(SANDBOX_FAILURE_EXECUTION_GENERATION_KEY)
    .bind(CURRENT_EXECUTION_GENERATION - 1)
    .bind(SANDBOX_FAILURE_POD_UID_KEY)
    .bind(CURRENT_POD_UID)
    .bind(SANDBOX_FAILURE_ACTIVATION_ATTEMPT_ID_KEY)
    .bind(route_activation_id.to_string())
    .execute(&db.pool)
    .await?;
    let public_id = PublicSandboxId::from(sandbox_id);

    let stale_generation = fetch_public_sandbox(&db.pool, OrgId::from_uuid(SEED_ORG_ID), public_id)
        .await?
        .expect("sandbox");
    let stale_generation = public_sandbox_from_row(stale_generation).expect("valid public sandbox");
    assert!(stale_generation.failure_code.is_none());

    sqlx::query(pplx_space_common_core::sql!(
        "UPDATE node_facet_observed_status
         SET observed_payload = jsonb_build_object(
                 $4::TEXT,
                 jsonb_build_object(
                     $5::TEXT, $6::BIGINT,
                     $7::TEXT, $8::TEXT,
                     $9::TEXT, $10::TEXT
                 )
             )
         WHERE node_id = $1 AND facet = $2 AND element = $3"
    ))
    .bind(node_id)
    .bind(FACET_SANDBOX_DESIRED_STATE)
    .bind(sandbox_id.to_string())
    .bind(SANDBOX_FAILURE_EPISODE_KEY)
    .bind(SANDBOX_FAILURE_EXECUTION_GENERATION_KEY)
    .bind(CURRENT_EXECUTION_GENERATION)
    .bind(SANDBOX_FAILURE_POD_UID_KEY)
    .bind(CURRENT_POD_UID)
    .bind(SANDBOX_FAILURE_ACTIVATION_ATTEMPT_ID_KEY)
    .bind(route_activation_id.to_string())
    .execute(&db.pool)
    .await?;
    let current = fetch_public_sandbox(&db.pool, OrgId::from_uuid(SEED_ORG_ID), public_id)
        .await?
        .expect("sandbox");
    let current = public_sandbox_from_row(current).expect("valid public sandbox");
    assert_eq!(
        current.failure_code,
        Some(SandboxFailureCode::RuntimeFailure)
    );

    sqlx::query(pplx_space_common_core::sql!(
        "UPDATE node_facet_observed_status
         SET observed_payload = jsonb_set(
             observed_payload,
             ARRAY[$4::TEXT],
             (observed_payload -> $4::TEXT) - $5::TEXT
         )
         WHERE node_id = $1 AND facet = $2 AND element = $3"
    ))
    .bind(node_id)
    .bind(FACET_SANDBOX_DESIRED_STATE)
    .bind(sandbox_id.to_string())
    .bind(SANDBOX_FAILURE_EPISODE_KEY)
    .bind(SANDBOX_FAILURE_ACTIVATION_ATTEMPT_ID_KEY)
    .execute(&db.pool)
    .await?;
    let missing_attempt = fetch_public_sandbox(&db.pool, OrgId::from_uuid(SEED_ORG_ID), public_id)
        .await?
        .expect("sandbox");
    let missing_attempt = public_sandbox_from_row(missing_attempt).expect("valid public sandbox");
    assert!(missing_attempt.failure_code.is_none());
    sqlx::query(pplx_space_common_core::sql!(
        "UPDATE node_facet_observed_status
         SET observed_payload = jsonb_set(
             observed_payload,
             ARRAY[$4::TEXT, $5::TEXT],
             to_jsonb($6::TEXT),
             TRUE
         )
         WHERE node_id = $1 AND facet = $2 AND element = $3"
    ))
    .bind(node_id)
    .bind(FACET_SANDBOX_DESIRED_STATE)
    .bind(sandbox_id.to_string())
    .bind(SANDBOX_FAILURE_EPISODE_KEY)
    .bind(SANDBOX_FAILURE_ACTIVATION_ATTEMPT_ID_KEY)
    .bind(route_activation_id.to_string())
    .execute(&db.pool)
    .await?;

    let replacement_route_activation_id = Uuid::now_v7();
    sqlx::query(pplx_space_common_core::sql!(
        "UPDATE sandbox_k8s_executions
         SET route_activation_id = $2
         WHERE sandbox_id = $1 AND active"
    ))
    .bind(sandbox_id)
    .bind(replacement_route_activation_id)
    .execute(&db.pool)
    .await?;
    let replacement_route =
        fetch_public_sandbox(&db.pool, OrgId::from_uuid(SEED_ORG_ID), public_id)
            .await?
            .expect("sandbox");
    let replacement_route =
        public_sandbox_from_row(replacement_route).expect("valid public sandbox");
    assert!(replacement_route.failure_code.is_none());
    sqlx::query(pplx_space_common_core::sql!(
        "UPDATE sandbox_k8s_executions
         SET route_activation_id = NULL
         WHERE sandbox_id = $1 AND active"
    ))
    .bind(sandbox_id)
    .execute(&db.pool)
    .await?;
    let missing_route = fetch_public_sandbox(&db.pool, OrgId::from_uuid(SEED_ORG_ID), public_id)
        .await?
        .expect("sandbox");
    let missing_route = public_sandbox_from_row(missing_route).expect("valid public sandbox");
    assert!(missing_route.failure_code.is_none());
    sqlx::query(pplx_space_common_core::sql!(
        "UPDATE sandbox_k8s_executions
         SET route_activation_id = $2
         WHERE sandbox_id = $1 AND active"
    ))
    .bind(sandbox_id)
    .bind(route_activation_id)
    .execute(&db.pool)
    .await?;

    sqlx::query(pplx_space_common_core::sql!(
        "UPDATE sandbox_k8s_executions
         SET pod_uid = $2
         WHERE sandbox_id = $1 AND active"
    ))
    .bind(sandbox_id)
    .bind(REPLACEMENT_POD_UID)
    .execute(&db.pool)
    .await?;
    let replacement_pod = fetch_public_sandbox(&db.pool, OrgId::from_uuid(SEED_ORG_ID), public_id)
        .await?
        .expect("sandbox");
    let replacement_pod = public_sandbox_from_row(replacement_pod).expect("valid public sandbox");
    assert!(replacement_pod.failure_code.is_none());
    sqlx::query(pplx_space_common_core::sql!(
        "UPDATE sandbox_k8s_executions
         SET pod_uid = $2
         WHERE sandbox_id = $1 AND active"
    ))
    .bind(sandbox_id)
    .bind(CURRENT_POD_UID)
    .execute(&db.pool)
    .await?;
    let listed =
        fetch_sandboxes_page(&db.pool, OrgId::from_uuid(SEED_ORG_ID), None, None, 100).await?;
    assert_eq!(listed.len(), 1);
    let cursor = (listed[0].created_at, listed[0].id);
    let listed = sandboxes_page(listed, 100).expect("valid sandbox page");
    assert_eq!(listed.data, vec![current]);
    assert!(
        fetch_sandboxes_page(&db.pool, OrgId::new(), None, None, 100)
            .await?
            .is_empty()
    );
    assert!(
        fetch_sandboxes_page(
            &db.pool,
            OrgId::from_uuid(SEED_ORG_ID),
            None,
            Some(cursor),
            100
        )
        .await?
        .is_empty()
    );
    assert!(
        fetch_public_sandbox(&db.pool, OrgId::new(), public_id)
            .await?
            .is_none()
    );

    let direct_attempt_id = Uuid::now_v7();
    sqlx::query(pplx_space_common_core::sql!(
        "INSERT INTO sandbox_direct_route_identities (
             sandbox_id, route_generation, current_node_id, substrate_kind,
             execution_generation, activation_attempt_id
         ) VALUES ($1, 1, $2, 'space', $3, $4)"
    ))
    .bind(sandbox_id)
    .bind(node_id)
    .bind(CURRENT_EXECUTION_GENERATION)
    .bind(direct_attempt_id)
    .execute(&db.pool)
    .await?;
    sqlx::query(pplx_space_common_core::sql!(
        "UPDATE sandboxes SET managed_by = 'space' WHERE id = $1"
    ))
    .bind(sandbox_id)
    .execute(&db.pool)
    .await?;
    sqlx::query(pplx_space_common_core::sql!(
        "UPDATE node_facet_observed_status
         SET observed_payload = jsonb_build_object(
                 $4::TEXT,
                 jsonb_build_object(
                     $5::TEXT, $6::BIGINT,
                     $7::TEXT, $8::TEXT
                 )
             )
         WHERE node_id = $1 AND facet = $2 AND element = $3"
    ))
    .bind(node_id)
    .bind(FACET_SANDBOX_DESIRED_STATE)
    .bind(sandbox_id.to_string())
    .bind(SANDBOX_FAILURE_EPISODE_KEY)
    .bind(SANDBOX_FAILURE_EXECUTION_GENERATION_KEY)
    .bind(CURRENT_EXECUTION_GENERATION)
    .bind(SANDBOX_FAILURE_ACTIVATION_ATTEMPT_ID_KEY)
    .bind(direct_attempt_id.to_string())
    .execute(&db.pool)
    .await?;
    let direct = fetch_public_sandbox(&db.pool, OrgId::from_uuid(SEED_ORG_ID), public_id)
        .await?
        .expect("sandbox");
    let direct = public_sandbox_from_row(direct).expect("valid public sandbox");
    assert_eq!(
        direct.failure_code,
        Some(SandboxFailureCode::RuntimeFailure)
    );

    let replacement_attempt_id = Uuid::now_v7();
    sqlx::query(pplx_space_common_core::sql!(
        "UPDATE sandbox_direct_route_identities
         SET activation_attempt_id = $2
         WHERE sandbox_id = $1"
    ))
    .bind(sandbox_id)
    .bind(replacement_attempt_id)
    .execute(&db.pool)
    .await?;
    let replacement_attempt =
        fetch_public_sandbox(&db.pool, OrgId::from_uuid(SEED_ORG_ID), public_id)
            .await?
            .expect("sandbox");
    let replacement_attempt =
        public_sandbox_from_row(replacement_attempt).expect("valid public sandbox");
    assert!(replacement_attempt.failure_code.is_none());
    sqlx::query(pplx_space_common_core::sql!(
        "UPDATE sandbox_direct_route_identities
         SET activation_attempt_id = $2
         WHERE sandbox_id = $1"
    ))
    .bind(sandbox_id)
    .bind(direct_attempt_id)
    .execute(&db.pool)
    .await?;

    sqlx::query(pplx_space_common_core::sql!(
        "UPDATE sandbox_direct_route_identities
         SET execution_generation = $2
         WHERE sandbox_id = $1"
    ))
    .bind(sandbox_id)
    .bind(CURRENT_EXECUTION_GENERATION - 1)
    .execute(&db.pool)
    .await?;
    let stale_direct = fetch_public_sandbox(&db.pool, OrgId::from_uuid(SEED_ORG_ID), public_id)
        .await?
        .expect("sandbox");
    let stale_direct = public_sandbox_from_row(stale_direct).expect("valid public sandbox");
    assert!(stale_direct.failure_code.is_none());

    sqlx::query(pplx_space_common_core::sql!(
        "UPDATE sandbox_direct_route_identities
         SET execution_generation = $2
         WHERE sandbox_id = $1"
    ))
    .bind(sandbox_id)
    .bind(CURRENT_EXECUTION_GENERATION)
    .execute(&db.pool)
    .await?;

    sqlx::query(pplx_space_common_core::sql!(
        "UPDATE sandbox_observed_states SET status = 'started' WHERE sandbox_id = $1"
    ))
    .bind(sandbox_id)
    .execute(&db.pool)
    .await?;
    let mismatched_status =
        fetch_public_sandbox(&db.pool, OrgId::from_uuid(SEED_ORG_ID), public_id)
            .await?
            .expect("sandbox");
    let mismatched_status =
        public_sandbox_from_row(mismatched_status).expect("valid public sandbox");
    assert!(mismatched_status.failure_code.is_none());

    let replacement_node_id = NodeId::new();
    insert_ready_worker_node(&db.pool, replacement_node_id).await?;
    sqlx::query(pplx_space_common_core::sql!(
        "UPDATE sandbox_observed_states SET status = 'failed' WHERE sandbox_id = $1"
    ))
    .bind(sandbox_id)
    .execute(&db.pool)
    .await?;
    sqlx::query(pplx_space_common_core::sql!(
        "UPDATE sandboxes SET active_node_id = $2 WHERE id = $1"
    ))
    .bind(sandbox_id)
    .bind(replacement_node_id)
    .execute(&db.pool)
    .await?;
    let direct_with_stale_sandbox_node =
        fetch_public_sandbox(&db.pool, OrgId::from_uuid(SEED_ORG_ID), public_id)
            .await?
            .expect("sandbox");
    let direct_with_stale_sandbox_node =
        public_sandbox_from_row(direct_with_stale_sandbox_node).expect("valid public sandbox");
    assert_eq!(
        direct_with_stale_sandbox_node.failure_code,
        Some(SandboxFailureCode::RuntimeFailure)
    );

    sqlx::query(pplx_space_common_core::sql!(
        "UPDATE sandboxes
         SET lifecycle = 'started', active_node_id = $2
         WHERE id = $1"
    ))
    .bind(sandbox_id)
    .bind(node_id)
    .execute(&db.pool)
    .await?;
    let recovered = fetch_public_sandbox(&db.pool, OrgId::from_uuid(SEED_ORG_ID), public_id)
        .await?
        .expect("sandbox");
    let recovered = public_sandbox_from_row(recovered).expect("valid public sandbox");
    assert_eq!(recovered.state, SandboxLifecycleState::Running);
    assert!(recovered.failure_code.is_none());
    Ok(())
}

#[test]
fn action_segment_parses_colon_custom_method_suffix() {
    let sandbox_id = PublicSandboxId::new().to_string();
    let segment = format!("{sandbox_id}:pause");
    let (parsed_id, action) = parse_action_segment(&segment, |_| true).unwrap();

    assert_eq!(parsed_id, sandbox_id);
    assert_eq!(action, SandboxLifecycleAction::Pause);
    parse_action_segment(&sandbox_id, |_| true).unwrap_err();
    parse_action_segment(&format!("{sandbox_id}:delete"), |_| true).unwrap_err();
}

#[test]
fn lifecycle_path_template_uses_explicit_custom_methods() {
    assert_eq!(
        lifecycle_path_template(SandboxLifecycleAction::Pause).as_str(),
        "/v1/sandboxes/{sandbox_id}:pause"
    );
    assert_eq!(
        lifecycle_path_template(SandboxLifecycleAction::Checkpoint).as_str(),
        "/v1/sandboxes/{sandbox_id}:checkpoint"
    );
    assert_eq!(
        lifecycle_path_template(SandboxLifecycleAction::Delete).as_str(),
        "/v1/sandboxes/{sandbox_id}"
    );
}

#[test]
fn rollback_request_is_only_required_for_rollback_action() {
    let ignored = rollback_request(SandboxLifecycleAction::Pause, br#"not-json"#).unwrap();
    assert!(ignored.is_none());

    rollback_request(SandboxLifecycleAction::Rollback, b"").unwrap_err();

    let parsed = rollback_request(
        SandboxLifecycleAction::Rollback,
        br#"{"snapshot_name":"snap-a","auto_restart":true}"#,
    )
    .unwrap()
    .unwrap();
    assert_eq!(parsed.snapshot_name, "snap-a");
    assert!(parsed.auto_restart);
}

#[test]
fn public_stats_carry_every_sandbox_manager_value_under_the_public_id() {
    let sandbox_id = PublicSandboxId::new();
    let response = public_stats_response(
        sandbox_id,
        pplx_space_sbx_mgr_client::proto::StatsResponse {
            sandbox_id: "node-local-id".to_string(),
            cpu_max: "200000 100000".to_string(),
            cpu_usage_usec: 42,
            tcp_denied_connections: 3,
            ..Default::default()
        },
    );
    assert_eq!(response.sandbox_id, sandbox_id);
    assert_eq!(response.cpu_max, "200000 100000");
    assert_eq!(response.cpu_usage_usec, 42);
    assert_eq!(response.tcp_denied_connections, 3);
}

#[test]
fn update_request_rejects_requires_restart_disk_resize() {
    let request = UpdateSandboxRequest {
        metadata: None,
        resources: Some(SandboxResources {
            vcpu: None,
            memory_mib: None,
            disk_mib: Some(4096),
        }),
        network_policy: None,
        auto_lifecycle: None,
    };

    assert!(validate_update_request(&request).is_err());
}

#[test]
fn update_request_fingerprint_includes_sandbox_id() {
    let first = PublicSandboxId::new();
    let second = PublicSandboxId::new();
    let request = UpdateSandboxRequest {
        metadata: None,
        resources: Some(SandboxResources {
            vcpu: Some(2),
            memory_mib: None,
            disk_mib: None,
        }),
        network_policy: None,
        auto_lifecycle: None,
    };

    assert_ne!(
        update_request_fingerprint(first, &request).unwrap(),
        update_request_fingerprint(second, &request).unwrap()
    );
}

fn public_sandbox_row(created_at: OffsetDateTime, id: SandboxId) -> PublicSandboxRow {
    PublicSandboxRow {
        id,
        name: None,
        lifecycle: "started".to_string(),
        has_current_failure: false,
        placement: "on_node".to_string(),
        active_node_id: Some(NodeId::from_uuid(Uuid::nil())),
        template_id: Some("seed".to_string()),
        content_hash: Some("hash".to_string()),
        vcpu: Some(2),
        memory_mib: Some(1024),
        disk_mib: Some(4096),
        metadata: SqlJson(Default::default()),
        next_auto_event_at: None,
        auto_lifecycle: SqlJson(AutoLifecyclePolicy::disabled()),
        created_at,
        updated_at: created_at,
        has_next: false,
    }
}

/// Rejections from the cluster activation limiter reach clients exactly as
/// the contract promises: `rate_limited`, retryable, with a whole-second
/// `Retry-After`. A substrate 503 is `capacity_unavailable` and retryable,
/// and carries a delay only when the backend supplied one.
#[tokio::test]
async fn admission_rejections_are_retryable_with_a_retry_after_hint() {
    use axum::http::{StatusCode, header};
    use axum::response::IntoResponse;
    use http_body_util::BodyExt;
    use pplx_space_public_api::{PublicErrorCode, PublicErrorEnvelope};

    let throttled = controller_error(
        ControllerError::rate_limited("cluster activation bucket is empty")
            .with_retry_after_header(std::time::Duration::from_millis(1500)),
    )
    .into_response();
    assert_eq!(throttled.status(), StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(throttled.headers().get(header::RETRY_AFTER).unwrap(), "2");
    let envelope: PublicErrorEnvelope =
        serde_json::from_slice(&throttled.into_body().collect().await.unwrap().to_bytes()).unwrap();
    assert_eq!(envelope.code, PublicErrorCode::RateLimited);
    assert!(envelope.retryable);

    let exhausted = controller_error(ControllerError::service_unavailable(
        "no direct worker node-api nodes accepted create",
    ))
    .into_response();
    assert_eq!(exhausted.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert!(exhausted.headers().get(header::RETRY_AFTER).is_none());
    let envelope: PublicErrorEnvelope =
        serde_json::from_slice(&exhausted.into_body().collect().await.unwrap().to_bytes()).unwrap();
    assert_eq!(envelope.code, PublicErrorCode::CapacityUnavailable);
    assert!(envelope.retryable);
}

/// Admission-time warm-pool claims on `POST /v1/sandboxes`, against the real
/// Kubernetes substrate's pool store and synthetic slots.
#[cfg(feature = "kubernetes")]
mod admission_pool_claim {
    use pplx_space_feature_flags::{FeatureFlagClient, SPACE_NATIVE_CREATE_POOL_CLAIM_FLAG};
    use pplx_space_public_api::PublicSandboxId;

    use super::super::admission_test_fixture::AdmissionPoolFixture;
    use super::*;
    use crate::controllers::sandbox::AdmissionPoolPlan;

    /// The `outcome` tags of every admission-claim counter in `counters`.
    fn admission_outcomes(
        counters: &[pplx_space_metrics::test_capture::CapturedCounter],
    ) -> Vec<String> {
        counters
            .iter()
            .filter(|counter| counter.name == "warm_pool.admission_claim")
            .map(|counter| counter.tags.join(","))
            .collect()
    }

    #[test]
    fn the_gate_is_off_unless_both_the_env_master_and_the_flag_are_on() {
        let flag_on =
            FeatureFlagClient::fixed_bools([(SPACE_NATIVE_CREATE_POOL_CLAIM_FLAG.key(), true)]);
        let flag_off =
            FeatureFlagClient::fixed_bools([(SPACE_NATIVE_CREATE_POOL_CLAIM_FLAG.key(), false)]);
        let no_provider = FeatureFlagClient::disabled();

        const { assert!(!DEFAULT_NATIVE_CREATE_POOL_CLAIM_ENABLED) };
        assert!(native_create_pool_claim_gate(true, &flag_on));
        assert!(!native_create_pool_claim_gate(false, &flag_on));
        assert!(!native_create_pool_claim_gate(true, &flag_off));
        // A rollout gate: no provider means no rollout.
        assert!(!native_create_pool_claim_gate(true, &no_provider));
        assert!(!native_create_pool_claim_gate(false, &no_provider));
    }

    /// Off is today's path: a fresh ID, no claim in the payload, and the
    /// pool untouched.
    #[tokio::test(flavor = "current_thread")]
    async fn disabled_admission_takes_a_fresh_id_and_leaves_the_pool_alone() -> TestDbResult<()> {
        let fixture = AdmissionPoolFixture::new(1).await?;
        let capture = pplx_space_metrics::test_capture::capture_counters_guard();
        let response = fixture
            .create(None, "admission-disabled", fixture.request())
            .await
            .expect("create is recorded");
        let counters = capture.finish();

        let slot = &fixture.slots[0];
        assert_ne!(
            response.sandbox_id,
            PublicSandboxId::from(slot.reserved_sandbox_id)
        );
        let payload = fixture.payload(response.operation_id).await?;
        assert_eq!(payload.pool_claim, None);
        assert_eq!(payload.sandbox_id, response.sandbox_id);
        assert_eq!(
            fixture.slot_state(slot.id).await?,
            ("ready".to_string(), None)
        );
        assert_eq!(admission_outcomes(&counters), vec!["outcome:disabled"]);
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn eligible_admission_takes_the_ready_slot_and_its_reserved_id() -> TestDbResult<()> {
        let fixture = AdmissionPoolFixture::new(1).await?;
        let controller = fixture.controller();
        let capture = pplx_space_metrics::test_capture::capture_counters_guard();
        let response = fixture
            .create(Some(&controller), "admission-hit", fixture.request())
            .await
            .expect("create is recorded");
        let counters = capture.finish();

        let slot = &fixture.slots[0];
        assert_eq!(
            response.sandbox_id,
            PublicSandboxId::from(slot.reserved_sandbox_id)
        );
        // The 202 body's ID is still a valid public sandbox ID.
        let rendered = response.sandbox_id.to_string();
        assert!(rendered.starts_with("sbx_"), "{rendered}");
        assert_eq!(
            rendered.parse::<PublicSandboxId>().unwrap(),
            response.sandbox_id
        );
        assert_eq!(fixture.target_id(response.operation_id).await?, rendered);

        let payload = fixture.payload(response.operation_id).await?;
        assert_eq!(payload.sandbox_id, response.sandbox_id);
        let claim = payload.pool_claim.expect("payload carries the claim");
        assert_eq!(claim.pool_slot_id, slot.id);
        assert_eq!(
            fixture.slot_state(slot.id).await?,
            ("claimed".to_string(), Some(claim.claim_id))
        );
        assert_eq!(admission_outcomes(&counters), vec!["outcome:hit"]);
        Ok(())
    }

    /// The quota check runs before the claim: a create waiting for its quota
    /// lock holds no slot, so another create takes the ready one meanwhile,
    /// and the waiting create is then refused without claiming.
    #[tokio::test(flavor = "current_thread")]
    async fn a_create_waiting_on_the_quota_holds_no_slot() -> TestDbResult<()> {
        let fixture = AdmissionPoolFixture::with_max_connections(1, 5).await?;
        let controller = fixture.controller();
        let holder = hold_org_quota_lock(&fixture.db.pool, fixture.org_id).await?;
        let capped_key = IdempotencyKey::parse("quota-claim-capped").expect("test key parses");
        let capped = create_native_sandbox_operation(
            &fixture.db.pool,
            fixture.org_id,
            fixture.user_id,
            &capped_key,
            fixture.request(),
            None,
            Some(NativeCreatePoolClaims {
                controller: &controller,
                inline: None,
            }),
            Some(SandboxQuota {
                org_id: fixture.org_id,
                org_limit: Some(1),
                subject: None,
            }),
        );
        let meanwhile = async {
            wait_for_advisory_lock_waiter(&fixture.db.pool).await?;
            let uncapped = fixture
                .create(Some(&controller), "quota-claim-uncapped", fixture.request())
                .await
                .expect("an uncapped create is recorded");
            assert_eq!(
                uncapped.sandbox_id,
                PublicSandboxId::from(fixture.slots[0].reserved_sandbox_id),
                "the create waiting on the quota must not hold the slot"
            );
            drop(holder);
            TestDbResult::Ok(())
        };
        let (capped, meanwhile) = tokio::join!(capped, meanwhile);
        meanwhile?;
        let refused = capped.expect_err("the uncapped create filled the org's limit");
        assert_eq!(
            refused.into_response().status(),
            StatusCode::TOO_MANY_REQUESTS
        );
        assert_eq!(fixture.create_operations().await?, 1);
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn an_idempotent_retry_returns_the_claimed_id_without_claiming_again() -> TestDbResult<()>
    {
        let fixture = AdmissionPoolFixture::new(2).await?;
        let controller = fixture.controller();
        let first = fixture
            .create(Some(&controller), "admission-retry", fixture.request())
            .await
            .expect("create is recorded");
        assert_eq!(fixture.slot_count("ready").await?, 1);

        let capture = pplx_space_metrics::test_capture::capture_counters_guard();
        let retry = fixture
            .create(Some(&controller), "admission-retry", fixture.request())
            .await
            .expect("an identical retry reuses the operation");
        let counters = capture.finish();

        assert_eq!(retry.operation_id, first.operation_id);
        assert_eq!(retry.sandbox_id, first.sandbox_id);
        assert_eq!(fixture.slot_count("ready").await?, 1);
        assert_eq!(fixture.slot_count("claimed").await?, 1);
        assert_eq!(fixture.create_operations().await?, 1);
        assert!(
            admission_outcomes(&counters).is_empty(),
            "a retry records no new create"
        );
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn concurrent_same_key_creates_keep_exactly_one_claim() -> TestDbResult<()> {
        let fixture = AdmissionPoolFixture::new(2).await?;
        let controller = fixture.controller();
        let (a, b) = tokio::join!(
            fixture.create(Some(&controller), "admission-race", fixture.request()),
            fixture.create(Some(&controller), "admission-race", fixture.request()),
        );
        let (a, b) = (a.expect("first racer"), b.expect("second racer"));

        assert_eq!(a.operation_id, b.operation_id);
        assert_eq!(a.sandbox_id, b.sandbox_id);
        assert_eq!(fixture.create_operations().await?, 1);
        assert_eq!(fixture.slot_count("claimed").await?, 1);
        assert_eq!(fixture.slot_count("ready").await?, 1);
        let payload = fixture.payload(a.operation_id).await?;
        let claim = payload
            .pool_claim
            .expect("the surviving create carries a claim");
        let claimed = fixture
            .slots
            .iter()
            .find(|slot| slot.id == claim.pool_slot_id)
            .expect("the claim names a fixture slot");
        assert_eq!(
            a.sandbox_id,
            PublicSandboxId::from(claimed.reserved_sandbox_id)
        );
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn an_idempotency_conflict_leaves_the_slot_ready() -> TestDbResult<()> {
        let fixture = AdmissionPoolFixture::new(2).await?;
        let controller = fixture.controller();
        fixture
            .create(Some(&controller), "admission-conflict", fixture.request())
            .await
            .expect("create is recorded");
        let mut different = fixture.request();
        different.name = Some("a-different-body".to_string());

        let error = fixture
            .create(Some(&controller), "admission-conflict", different)
            .await
            .expect_err("same key, different body");

        assert_eq!(
            axum::response::IntoResponse::into_response(error).status(),
            axum::http::StatusCode::CONFLICT
        );
        assert_eq!(fixture.slot_count("ready").await?, 1);
        assert_eq!(fixture.slot_count("claimed").await?, 1);
        assert_eq!(fixture.create_operations().await?, 1);
        Ok(())
    }

    /// Every slot is plaintext, so an org with encryption enabled never gets
    /// one, whatever the cluster's encryption opt-in says.
    #[tokio::test(flavor = "current_thread")]
    async fn an_encrypted_org_never_gets_a_plaintext_slot() -> TestDbResult<()> {
        let fixture = AdmissionPoolFixture::new(1).await?;
        fixture.enable_org_encryption().await?;
        let controller = fixture.controller();
        let capture = pplx_space_metrics::test_capture::capture_counters_guard();
        let response = fixture
            .create(Some(&controller), "admission-encrypted", fixture.request())
            .await
            .expect("create is recorded");
        let counters = capture.finish();

        let slot = &fixture.slots[0];
        assert_ne!(
            response.sandbox_id,
            PublicSandboxId::from(slot.reserved_sandbox_id)
        );
        assert_eq!(
            fixture.payload(response.operation_id).await?.pool_claim,
            None
        );
        assert_eq!(
            fixture.slot_state(slot.id).await?,
            ("ready".to_string(), None)
        );
        assert_eq!(admission_outcomes(&counters), vec!["outcome:ineligible"]);
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_custom_shape_is_admitted_cold() -> TestDbResult<()> {
        let fixture = AdmissionPoolFixture::new(1).await?;
        let controller = fixture.controller();
        for (index, resources) in [
            pplx_space_public_api::SandboxResources {
                vcpu: Some(4),
                memory_mib: None,
                disk_mib: None,
            },
            pplx_space_public_api::SandboxResources {
                vcpu: None,
                memory_mib: Some(4096),
                disk_mib: None,
            },
            pplx_space_public_api::SandboxResources {
                vcpu: None,
                memory_mib: None,
                disk_mib: Some(4096),
            },
        ]
        .into_iter()
        .enumerate()
        {
            let mut request = fixture.request();
            request.resources = Some(resources);
            let capture = pplx_space_metrics::test_capture::capture_counters_guard();
            let response = fixture
                .create(
                    Some(&controller),
                    &format!("admission-shape-{index}"),
                    request,
                )
                .await
                .expect("create is recorded");
            let counters = capture.finish();
            assert_eq!(
                fixture.payload(response.operation_id).await?.pool_claim,
                None
            );
            assert_eq!(admission_outcomes(&counters), vec!["outcome:ineligible"]);
        }
        assert_eq!(fixture.slot_count("ready").await?, 1);
        Ok(())
    }

    /// The public route never pins, but the shared predicate refuses a pin
    /// for every caller, and an unknown org fails closed.
    #[tokio::test(flavor = "current_thread")]
    async fn the_admission_plan_refuses_a_node_pool_pin_and_an_unknown_org() -> TestDbResult<()> {
        let fixture = AdmissionPoolFixture::new(1).await?;
        let controller = fixture.controller();
        let request =
            crate::public_sandbox_operations::controller_create_request(fixture.request(), None);
        assert_eq!(
            controller
                .plan_admission_pool_claim(&fixture.db.pool, fixture.org_id, &request)
                .await,
            AdmissionPoolPlan::Eligible {
                template_release_id: fixture.release_id
            },
            "positive control"
        );

        let pinned = crate::public_sandbox_operations::controller_create_request(
            fixture.request(),
            Some(pplx_space_common_core::types::NodePoolName::parse("isolated-test-pool").unwrap()),
        );
        assert_eq!(
            controller
                .plan_admission_pool_claim(&fixture.db.pool, fixture.org_id, &pinned)
                .await,
            AdmissionPoolPlan::Ineligible
        );
        assert_eq!(
            controller
                .plan_admission_pool_claim(&fixture.db.pool, OrgId::new(), &request)
                .await,
            AdmissionPoolPlan::Ineligible
        );
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_pool_miss_is_admitted_cold() -> TestDbResult<()> {
        let fixture = AdmissionPoolFixture::new(0).await?;
        let controller = fixture.controller();
        let capture = pplx_space_metrics::test_capture::capture_counters_guard();
        let response = fixture
            .create(Some(&controller), "admission-miss", fixture.request())
            .await
            .expect("create is recorded");
        let counters = capture.finish();

        let payload = fixture.payload(response.operation_id).await?;
        assert_eq!(payload.pool_claim, None);
        assert_eq!(payload.sandbox_id, response.sandbox_id);
        assert_eq!(admission_outcomes(&counters), vec!["outcome:miss"]);
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_template_miss_is_admitted_cold() -> TestDbResult<()> {
        let fixture = AdmissionPoolFixture::new(1).await?;
        let controller = fixture.controller();
        let mut request = fixture.request();
        request.template = "no-such-template:latest".parse().unwrap();
        let capture = pplx_space_metrics::test_capture::capture_counters_guard();
        let response = fixture
            .create(Some(&controller), "admission-template-miss", request)
            .await
            .expect("create is recorded; execution reports the missing template");
        let counters = capture.finish();

        assert_eq!(
            fixture.payload(response.operation_id).await?.pool_claim,
            None
        );
        assert_eq!(fixture.slot_count("ready").await?, 1);
        assert_eq!(admission_outcomes(&counters), vec!["outcome:miss"]);
        Ok(())
    }
}

async fn error_envelope(
    error: PublicApiError,
) -> (
    axum::http::StatusCode,
    pplx_space_public_api::PublicErrorEnvelope,
) {
    use axum::response::IntoResponse;
    use http_body_util::BodyExt;

    let response = error.into_response();
    let status = response.status();
    let body = response.into_body().collect().await.unwrap().to_bytes();
    (status, serde_json::from_slice(&body).unwrap())
}

fn field_detail(field: &str) -> Vec<pplx_space_public_api::PublicErrorDetail> {
    vec![pplx_space_public_api::PublicErrorDetail {
        key: pplx_space_public_api::INVALID_REQUEST_FIELD_DETAIL_KEY.to_string(),
        value: field.to_string(),
    }]
}

/// Create and update reject out-of-limit fields with a non-retryable
/// `invalid_request` that names the field, before anything is admitted.
#[tokio::test]
async fn create_and_update_name_the_field_outside_its_limit() {
    use pplx_space_public_api::{PublicErrorCode, PublicSandboxMetadataValue};

    let mut big_metadata = create_request_with_auto_lifecycle(None);
    big_metadata.metadata = (0..65)
        .map(|index| (format!("k{index}"), PublicSandboxMetadataValue::Bool(true)))
        .collect();
    let mut nul_name = create_request_with_auto_lifecycle(None);
    nul_name.name = Some("a\0b".into());
    let mut wrapping_vcpu = create_request_with_auto_lifecycle(None);
    wrapping_vcpu.resources = Some(SandboxResources {
        vcpu: Some(3_000_000_000),
        memory_mib: None,
        disk_mib: None,
    });
    for (request, field) in [
        (big_metadata, "metadata"),
        (nul_name, "name"),
        (wrapping_vcpu, "resources.vcpu"),
    ] {
        let (status, envelope) =
            error_envelope(prepare_native_create_request(request).unwrap_err()).await;
        assert_eq!(status, axum::http::StatusCode::BAD_REQUEST, "{field}");
        assert_eq!(envelope.code, PublicErrorCode::InvalidRequest, "{field}");
        assert!(!envelope.retryable, "{field}");
        assert_eq!(envelope.details, field_detail(field));
    }

    let update = UpdateSandboxRequest {
        metadata: None,
        resources: Some(SandboxResources {
            vcpu: None,
            memory_mib: Some(16),
            disk_mib: None,
        }),
        network_policy: None,
        auto_lifecycle: None,
    };
    let (status, envelope) = error_envelope(validate_update_request(&update).unwrap_err()).await;
    assert_eq!(status, axum::http::StatusCode::BAD_REQUEST);
    assert_eq!(envelope.details, field_detail("resources.memory_mib"));
}

#[tokio::test]
async fn idempotency_key_distinguishes_missing_from_invalid() {
    use axum::http::HeaderValue;

    let with_key = |value: HeaderValue| {
        let mut headers = HeaderMap::new();
        headers.insert(IDEMPOTENCY_KEY_HEADER, value);
        headers
    };
    let max = "k".repeat(pplx_space_public_api::limits::MAX_IDEMPOTENCY_KEY_BYTES);
    assert_eq!(
        idempotency_key(&with_key(HeaderValue::from_str(&max).unwrap()))
            .unwrap()
            .as_str(),
        max
    );

    for accepted in ["two words", "tab\tseparated"] {
        assert_eq!(
            idempotency_key(&with_key(HeaderValue::from_static(accepted)))
                .unwrap()
                .as_str(),
            accepted
        );
    }

    let cases = [
        (HeaderMap::new(), "Idempotency-Key header is required"),
        (
            with_key(HeaderValue::from_str(&format!("{max}k")).unwrap()),
            "Idempotency-Key exceeds 1024 bytes",
        ),
        (
            with_key(HeaderValue::from_bytes("clé".as_bytes()).unwrap()),
            "Idempotency-Key must contain only printable ASCII characters, spaces and tabs",
        ),
        (
            with_key(HeaderValue::from_static("")),
            "Idempotency-Key must not be empty",
        ),
    ];
    for (headers, message) in cases {
        let (status, envelope) = error_envelope(idempotency_key(&headers).unwrap_err()).await;
        assert_eq!(status, axum::http::StatusCode::BAD_REQUEST, "{message}");
        assert_eq!(envelope.message, message);
    }
}

#[test]
fn sandboxes_page_continues_after_a_budget_cut() {
    let created_at = OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap();
    let last_id = SandboxId::new();
    let rows = |has_next| {
        vec![
            public_sandbox_row(created_at, SandboxId::new()),
            PublicSandboxRow {
                has_next,
                ..public_sandbox_row(created_at - time::Duration::seconds(1), last_id)
            },
        ]
    };

    let cut = sandboxes_page(rows(true), 100).unwrap();
    assert_eq!(cut.data.len(), 2);
    let cursor = cut.next_cursor.expect("a cut page continues");
    assert_eq!(
        parse_sandbox_cursor(&decode_cursor(CursorKind::Sandbox, cursor.as_str()).unwrap())
            .unwrap(),
        (created_at - time::Duration::seconds(1), last_id)
    );
    assert!(
        sandboxes_page(rows(false), 100)
            .unwrap()
            .next_cursor
            .is_none()
    );
}

/// Inserts a sandbox directly, as one stored before the input limits could
/// be, with a single metadata string of `metadata_bytes` bytes.
async fn insert_legacy_sandbox(
    pool: &PgPool,
    created_at: OffsetDateTime,
    metadata_bytes: usize,
) -> TestDbResult<SandboxId> {
    let id = SandboxId::new();
    let metadata = serde_json::json!({ "legacy": "m".repeat(metadata_bytes) });
    sqlx::query(pplx_space_common_core::sql!(
        "INSERT INTO sandboxes (
             id, user_id, org_id, lifecycle, created_at, metadata,
             network_policy_override_json, network_policy_generation
         ) VALUES ($1, $2, $3, 'stopped', $4, $5, $6, 1)"
    ))
    .bind(id)
    .bind(SEED_USER_ID)
    .bind(SEED_ORG_ID)
    .bind(created_at)
    .bind(SqlJson(metadata))
    .bind(SqlJson(&pplx_space_net_common::NetworkPolicyV1::default()))
    .execute(pool)
    .await?;
    Ok(id)
}

/// Rows stored before the metadata limits still page within the byte
/// budget: Postgres returns no more than the budget (or one oversized row),
/// and following the cursor visits every row once.
#[tokio::test]
async fn sandbox_list_query_bounds_the_bytes_it_returns() -> TestDbResult<()> {
    const MIB: usize = 1024 * 1024;
    let db = pg_test_db().await?;
    let org_id = OrgId::from_uuid(SEED_ORG_ID);
    let newest = OffsetDateTime::from_unix_timestamp(1_800_000_000)?;
    // Newest first: one row over the whole budget, twelve 1 MiB rows, then
    // three small ones.
    let sizes = std::iter::once(9 * MIB)
        .chain(std::iter::repeat_n(MIB, 12))
        .chain(std::iter::repeat_n(16, 3));
    let mut expected = Vec::new();
    for (offset, size) in sizes.enumerate() {
        let created_at = newest - time::Duration::seconds(offset as i64);
        expected.push(insert_legacy_sandbox(&db.pool, created_at, size).await?);
    }

    let mut pages = Vec::new();
    let mut cursor = None;
    loop {
        let rows =
            fetch_sandboxes_page(&db.pool, org_id, None, cursor, MAX_SANDBOX_LIST_LIMIT).await?;
        let metadata_bytes = rows
            .iter()
            .map(|row| serde_json::to_vec(&row.metadata.0).unwrap().len())
            .sum::<usize>();
        if rows.len() > 1 {
            assert!(
                metadata_bytes <= MAX_SANDBOX_LIST_PAGE_BYTES as usize,
                "{metadata_bytes}"
            );
        }
        let ids = rows.iter().map(|row| row.id).collect::<Vec<_>>();
        let page = sandboxes_page(rows, MAX_SANDBOX_LIST_LIMIT).unwrap();
        pages.push(ids);
        let Some(next) = page.next_cursor else {
            break;
        };
        cursor = Some(
            parse_sandbox_cursor(&decode_cursor(CursorKind::Sandbox, next.as_str()).unwrap())
                .unwrap(),
        );
    }

    // The oversized row comes back alone; 1 MiB rows fit seven to a page.
    assert_eq!(
        pages.iter().map(Vec::len).collect::<Vec<_>>(),
        vec![1, 7, 8]
    );
    assert_eq!(pages.concat(), expected);

    // Within the budget, the walk stops at the limit's lookahead row.
    let after_oversized = Some((newest, expected[0]));
    let rows = fetch_sandboxes_page(&db.pool, org_id, None, after_oversized, 2).await?;
    assert_eq!(
        rows.iter().map(|row| row.id).collect::<Vec<_>>(),
        expected[1..4]
    );
    Ok(())
}

/// The public sandbox has no env, so env bytes do not count toward a native
/// page's budget.
#[tokio::test]
async fn sandbox_list_budget_ignores_env() -> TestDbResult<()> {
    let db = pg_test_db().await?;
    let newest = OffsetDateTime::from_unix_timestamp(1_800_000_000)?;
    let mut expected = Vec::new();
    for offset in 0..3 {
        let created_at = newest - time::Duration::seconds(offset);
        let id = insert_legacy_sandbox(&db.pool, created_at, 16).await?;
        sqlx::query(pplx_space_common_core::sql!(
            "UPDATE sandboxes SET env = $2 WHERE id = $1"
        ))
        .bind(id)
        .bind(SqlJson(
            serde_json::json!({ "LARGE": "e".repeat(3 * 1024 * 1024) }),
        ))
        .execute(&db.pool)
        .await?;
        expected.push(id);
    }

    let rows = fetch_sandboxes_page(
        &db.pool,
        OrgId::from_uuid(SEED_ORG_ID),
        None,
        None,
        MAX_SANDBOX_LIST_LIMIT,
    )
    .await?;
    assert_eq!(rows.iter().map(|row| row.id).collect::<Vec<_>>(), expected);
    let page = sandboxes_page(rows, MAX_SANDBOX_LIST_LIMIT).unwrap();
    assert!(page.next_cursor.is_none());
    Ok(())
}
