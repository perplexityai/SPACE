use pplx_space_common_core::types::{NodeId, SandboxId};
use pplx_space_common_core::types::{OrgId, UserId};
use uuid::Uuid;

use super::*;
use pplx_space_db_test_fixtures::{
    SEED_ORG_ID, SEED_USER_ID, TestDbResult, insert_ready_worker_node, pg_test_db,
};

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
    let request = with_native_create_defaults(create_request_with_auto_lifecycle(Some(
        explicit_policy.clone(),
    )));

    assert_eq!(request.auto_lifecycle, Some(explicit_policy));
}

#[test]
fn native_create_default_preserves_pre_deploy_idempotency_fingerprint() {
    let request = create_request_with_auto_lifecycle(None);
    let pre_deploy_fingerprint = request_fingerprint(&request).unwrap();

    let (_, post_deploy_fingerprint) = prepare_native_create_request(request).unwrap();

    assert_eq!(post_deploy_fingerprint, pre_deploy_fingerprint);
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
    let idempotency_key = IdempotencyKey::parse("native-default-policy-persistence")?;
    let response = create_native_sandbox_operation(
        &db.pool,
        OrgId::from_uuid(SEED_ORG_ID),
        UserId::from_uuid(SEED_USER_ID),
        &idempotency_key,
        create_request_with_auto_lifecycle(None),
        None,
    )
    .await
    .expect("native create should persist an operation");
    let retry_response = create_native_sandbox_operation(
        &db.pool,
        OrgId::from_uuid(SEED_ORG_ID),
        UserId::from_uuid(SEED_USER_ID),
        &idempotency_key,
        create_request_with_auto_lifecycle(None),
        None,
    )
    .await
    .expect("an identical retry should reuse the operation");

    assert_eq!(retry_response.operation_id, response.operation_id);

    let SqlJson(payload): SqlJson<serde_json::Value> = sqlx::query_scalar(sql!(
        "SELECT payload_json FROM public_operations WHERE operation_id = $1"
    ))
    .bind(response.operation_id.as_uuid())
    .fetch_one(&db.pool)
    .await?;
    let payload: CreateSandboxOperationPayload = serde_json::from_value(payload)?;
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
    Ok(())
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
            }
            .into_json(),
            idempotency: Some(CreatePublicOperationIdempotency {
                method: lifecycle_method(action),
                path_template: lifecycle_path_template(action),
                key: &key,
                request_fingerprint: &fingerprint,
            }),
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
        sqlx::query(pplx_space_common_core::sql!(
            "INSERT INTO sandboxes (
                 id, user_id, org_id, lifecycle, created_at,
                 network_policy_override_json, network_policy_generation
             ) VALUES ($1, $2, $3, $4, $5, $6, 1)"
        ))
        .bind(id)
        .bind(SEED_USER_ID)
        .bind(SEED_ORG_ID)
        .bind(lifecycle)
        .bind(created_at)
        .bind(SqlJson(&pplx_space_net_common::NetworkPolicyV1::default()))
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
            assert_eq!(row.expect("visible sandbox").lifecycle, lifecycle);
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
    let (parsed_id, action) = parse_action_segment(&segment).unwrap();

    assert_eq!(parsed_id, sandbox_id);
    assert_eq!(action, SandboxLifecycleAction::Pause);
    parse_action_segment(&sandbox_id).unwrap_err();
    parse_action_segment(&format!("{sandbox_id}:delete")).unwrap_err();
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
        created_at,
        updated_at: created_at,
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
