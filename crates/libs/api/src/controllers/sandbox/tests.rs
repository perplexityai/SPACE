use super::*;
use pplx_space_common_core::types::{PodUid, SandboxActivationAttemptId, TemplateId};

mod activation_timeout_tests {
    use super::*;
    use crate::models::SandboxActivationRequest;

    #[test]
    fn caller_ready_timeout_is_clamped_to_alb_budget() {
        // A caller-supplied wait above the budget must be capped, or the
        // restore/wake/start handler could run past the gateway ceiling.
        let request = SandboxActivationRequest {
            blocking: Some(true),
            ready_timeout_secs: Some(600),
        };
        let options = resolve_activation_options(Some(&request), true).unwrap();
        assert_eq!(options.ready_timeout, DEFAULT_BLOCKING_WAIT_TIMEOUT);
    }

    #[test]
    fn caller_ready_timeout_below_budget_is_preserved() {
        let request = SandboxActivationRequest {
            blocking: Some(true),
            ready_timeout_secs: Some(30),
        };
        let options = resolve_activation_options(Some(&request), true).unwrap();
        assert_eq!(options.ready_timeout, Duration::from_secs(30));
    }

    #[test]
    fn missing_ready_timeout_defaults_to_alb_budget() {
        let options = resolve_activation_options(None, true).unwrap();
        assert_eq!(
            DEFAULT_BLOCKING_WAIT_TIMEOUT,
            crate::controllers::substrate::NODE_RPC_BLOCKING_BUDGET
        );
        assert_eq!(options.ready_timeout, DEFAULT_BLOCKING_WAIT_TIMEOUT);
    }

    #[test]
    fn zero_ready_timeout_is_rejected() {
        let request = SandboxActivationRequest {
            blocking: Some(true),
            ready_timeout_secs: Some(0),
        };
        resolve_activation_options(Some(&request), true).unwrap_err();
    }
}

mod touch_auto_lifecycle_tests {
    use super::*;

    #[test]
    fn deadline_generation_requires_current_authoritative_runtime_identity() {
        use pplx_space_sbx_mgr_client::proto::{SandboxInfo, SandboxLifecycleState, StateResponse};

        let sandbox_id = SandboxId::new();
        let observed = SandboxInfo {
            sandbox_id: sandbox_id.to_string(),
            lifecycle: Some(SandboxLifecycleState::Started as i32),
            execution_generation: Some(7),
            // Historical provenance must never substitute for current identity.
            route_provenance_execution_generation: Some(42),
            ..Default::default()
        };
        #[derive(Clone, Copy, Debug, PartialEq, Eq)]
        enum Observation {
            Current,
            Missing,
            Foreign,
            Duplicate,
            UnexpectedSibling,
            ContradictoryAbsence,
            Degraded,
            MissingLifecycle,
            InvalidLifecycle,
            NotStarted,
            MissingGeneration,
            ZeroGeneration,
            NegativeGeneration,
        }
        use Observation::*;
        for scenario in [
            Current,
            Missing,
            Foreign,
            Duplicate,
            UnexpectedSibling,
            ContradictoryAbsence,
            Degraded,
            MissingLifecycle,
            InvalidLifecycle,
            NotStarted,
            MissingGeneration,
            ZeroGeneration,
            NegativeGeneration,
        ] {
            let mut response = StateResponse {
                sandboxes: vec![observed.clone()],
                ..Default::default()
            };
            match scenario {
                Current => {}
                Missing => response.sandboxes.clear(),
                Foreign => response.sandboxes[0].sandbox_id = SandboxId::new().to_string(),
                Duplicate => response.sandboxes.push(observed.clone()),
                UnexpectedSibling => response.sandboxes.push(SandboxInfo {
                    sandbox_id: SandboxId::new().to_string(),
                    ..observed.clone()
                }),
                ContradictoryAbsence => response.missing_ids.push(sandbox_id.to_string()),
                Degraded => response.sandboxes[0].state_degraded = true,
                MissingLifecycle => response.sandboxes[0].lifecycle = None,
                InvalidLifecycle => response.sandboxes[0].lifecycle = Some(i32::MAX),
                NotStarted => {
                    response.sandboxes[0].lifecycle = Some(SandboxLifecycleState::Starting as i32)
                }
                MissingGeneration => response.sandboxes[0].execution_generation = None,
                ZeroGeneration => response.sandboxes[0].execution_generation = Some(0),
                NegativeGeneration => response.sandboxes[0].execution_generation = Some(-1),
            }
            let result = updates::observed_access_idle_generation(sandbox_id, response);
            assert_eq!(
                result.ok().map(|generation| generation.get()),
                (scenario == Current).then_some(7),
                "{scenario:?}"
            );
        }
    }

    #[test]
    fn deadline_success_requires_specific_acknowledgement() {
        for (result, successful) in [
            (AutoLifecycleTouchResult::Unspecified as i32, false),
            (AutoLifecycleTouchResult::Applied as i32, true),
            (AutoLifecycleTouchResult::AlreadyCurrent as i32, true),
            (AutoLifecycleTouchResult::NotStarted as i32, false),
            (AutoLifecycleTouchResult::Disabled as i32, false),
            (i32::MAX, false),
        ] {
            for acknowledgement in [None, Some("1"), Some("0"), Some("")] {
                let mut response = tonic::Response::new(TouchAutoLifecycleResponse { result });
                if let Some(value) = acknowledgement {
                    response.metadata_mut().insert(
                        pplx_space_sbx_mgr_client::ACCESS_IDLE_UPDATE_ACK_METADATA,
                        tonic::metadata::MetadataValue::from_static(value),
                    );
                }
                let outcome = updates::require_access_idle_update_acknowledgement(response);
                assert_eq!(
                    outcome.is_ok(),
                    acknowledgement == Some("1") && successful,
                    "result={result}, acknowledgement={acknowledgement:?}"
                );
            }
        }
    }

    #[test]
    fn touch_response_returns_typed_result() {
        let outcome = updates::touch_auto_lifecycle_response_outcome(Ok(tonic::Response::new(
            TouchAutoLifecycleResponse {
                result: AutoLifecycleTouchResult::AlreadyCurrent as i32,
            },
        )))
        .unwrap();

        assert_eq!(outcome, AutoLifecycleTouchOutcome::AlreadyCurrent);
    }

    #[test]
    fn touch_response_rejects_an_unknown_result() {
        for result in [AutoLifecycleTouchResult::Unspecified as i32, i32::MAX] {
            updates::touch_auto_lifecycle_response_outcome(Ok(tonic::Response::new(
                TouchAutoLifecycleResponse { result },
            )))
            .unwrap_err();
        }
    }

    #[test]
    fn touch_response_classifies_transport_error_separately() {
        let outcome = updates::touch_auto_lifecycle_response_outcome(Err(tonic::Status::unknown(
            "transport error",
        )))
        .unwrap();

        assert_eq!(outcome, AutoLifecycleTouchOutcome::TransportUnavailable);
    }

    #[test]
    fn touch_response_propagates_application_rpc_error() {
        let result = updates::touch_auto_lifecycle_response_outcome(Err(
            tonic::Status::failed_precondition("sandbox is not started"),
        ));

        result.unwrap_err();
    }
}

mod policy_tests {

    use pplx_space_common_core::types::SandboxId;
    use std::collections::BTreeMap;

    use pplx_space_common_core::types::{
        AutoLifecycleConditionKind, AutoLifecycleConditionSet, AutoLifecycleConditionSetId,
        AutoLifecycleIdleAction, AutoLifecycleStage, AutoLifecycleWakePolicy,
        AutoLifecycleWakeSource,
    };

    use super::*;
    use crate::test_support::command_idle_suspend_policy;

    /// The policy ECW create synthesizes from a request timeout.
    fn synthesized_access_idle_policy(timeout_secs: u32) -> AutoLifecyclePolicy {
        crate::e2b::auto_lifecycle_policy(timeout_secs, false, false).normalized()
    }

    fn guest_activity_suspend_policy(timeout_secs: u64) -> AutoLifecyclePolicy {
        AutoLifecyclePolicy {
            wake: AutoLifecycleWakePolicy::default(),
            condition_sets: BTreeMap::from([(
                AutoLifecycleConditionSetId::GuestActivity,
                AutoLifecycleConditionSet::guest_activity(Some("30s".parse().unwrap())),
            )]),
            stages: vec![AutoLifecycleStage {
                condition_set: AutoLifecycleConditionSetId::GuestActivity,
                at: timeout_secs,
                action: AutoLifecycleIdleAction::Suspend,
            }],
        }
        .normalized()
    }

    #[test]
    fn command_idle_template_default_wins_over_synthesized_access_idle() {
        // ECW create always fabricates (synthesizes) an AccessIdle policy from the request
        // timeout. For a template whose default is CommandIdle, that default must win so
        // the sandbox keeps its command-driven idle lifecycle instead of being
        // downgraded.
        let request = synthesized_access_idle_policy(300);
        let template_default = command_idle_suspend_policy(600);

        let resolved = resolve_create_auto_lifecycle(
            Some(&request),
            RequestAutoLifecycleOrigin::SynthesizedDefault,
            &template_default,
        )
        .unwrap();

        assert_eq!(resolved, template_default);
    }

    #[test]
    fn synthesized_request_wins_when_template_default_is_ecw_managed() {
        // A template default ECW manages (here none) yields to the synthesized policy.
        let request = synthesized_access_idle_policy(300);
        let template_default = AutoLifecyclePolicy::disabled();

        let resolved = resolve_create_auto_lifecycle(
            Some(&request),
            RequestAutoLifecycleOrigin::SynthesizedDefault,
            &template_default,
        )
        .unwrap();

        assert_eq!(resolved, request);
    }

    #[test]
    fn explicit_access_idle_request_wins_over_command_idle_template_default() {
        // A native caller's explicit policy is authoritative and must not be silently dropped
        // (or skip validation) just because the template default is non-access.
        let request = synthesized_access_idle_policy(120);
        let template_default = command_idle_suspend_policy(600);

        let resolved = resolve_create_auto_lifecycle(
            Some(&request),
            RequestAutoLifecycleOrigin::Explicit,
            &template_default,
        )
        .unwrap();

        assert_eq!(resolved, request);
    }

    #[test]
    fn template_default_used_when_request_is_absent() {
        let template_default = command_idle_suspend_policy(600);

        let resolved = resolve_create_auto_lifecycle(
            None,
            RequestAutoLifecycleOrigin::SynthesizedDefault,
            &template_default,
        )
        .unwrap();

        assert_eq!(resolved, template_default);
    }

    #[test]
    fn guest_activity_template_default_wins_over_synthesized_access_idle() {
        // The rule generalizes beyond CommandIdle: any template default ECW doesn't manage
        // (here GuestActivity) is preserved against a synthesized AccessIdle request.
        let template_default = guest_activity_suspend_policy(600);
        let request = synthesized_access_idle_policy(300);

        let resolved = resolve_create_auto_lifecycle(
            Some(&request),
            RequestAutoLifecycleOrigin::SynthesizedDefault,
            &template_default,
        )
        .unwrap();

        assert_eq!(resolved, template_default);
    }

    #[test]
    fn create_auto_lifecycle_policy_rejects_invalid_policy() {
        let policy = AutoLifecyclePolicy {
            condition_sets: BTreeMap::from([(
                AutoLifecycleConditionSetId::AccessIdle,
                AutoLifecycleConditionSet {
                    kind: AutoLifecycleConditionKind::GuestActivity,
                    activity_source: None,
                    max_guest_idle_extension: None,
                },
            )]),
            ..Default::default()
        };

        let error = create_auto_lifecycle_policy(Some(policy)).unwrap_err();
        assert!(error.to_string().contains("access_idle"));
    }

    #[test]
    fn create_auto_lifecycle_policy_normalizes_policy() {
        let policy = AutoLifecyclePolicy {
            wake: AutoLifecycleWakePolicy {
                on: vec![
                    AutoLifecycleWakeSource::Command,
                    AutoLifecycleWakeSource::Access,
                    AutoLifecycleWakeSource::Command,
                ],
            },
            ..Default::default()
        };

        let policy = create_auto_lifecycle_policy(Some(policy)).unwrap();
        assert_eq!(
            policy.wake.on,
            vec![
                AutoLifecycleWakeSource::Access,
                AutoLifecycleWakeSource::Command
            ]
        );
    }

    #[test]
    fn sandbox_port_exposure_ready_requires_matching_node_and_bridge_observation() {
        let node_id = NodeId::from_uuid(Uuid::from_u128(1));
        let other_node_id = NodeId::from_uuid(Uuid::from_u128(2));
        let exposure = SandboxPortExposure {
            sandbox_id: SandboxId::from_uuid(Uuid::nil()),
            protocol: "tcp".to_string(),
            guest_port: 3000,
            visibility: "private".to_string(),
            source: "ecw".to_string(),
            desired_state: SANDBOX_PORT_DESIRED_OPEN.to_string(),
            apply_state: SANDBOX_PORT_APPLY_APPLIED.to_string(),
            last_applied_node_id: Some(node_id),
            last_error: None,
            last_bridge_status: Some("loopback_forwarded".to_string()),
            last_applied_bridge_generation: Some("generation-1".to_string()),
            last_observed_at: Some(time::OffsetDateTime::now_utc()),
            created_at: time::OffsetDateTime::UNIX_EPOCH,
            updated_at: time::OffsetDateTime::UNIX_EPOCH,
        };

        assert!(sandbox_port_exposure_ready_on_node(
            &exposure,
            Some(node_id)
        ));
        assert!(!sandbox_port_exposure_ready_on_node(
            &exposure,
            Some(other_node_id)
        ));
        assert!(!sandbox_port_exposure_ready_on_node(&exposure, None));
    }

    #[test]
    fn sandbox_port_exposure_missing_bridge_observation_is_not_ready() {
        let node_id = NodeId::from_uuid(Uuid::from_u128(1));
        let exposure = SandboxPortExposure {
            sandbox_id: SandboxId::from_uuid(Uuid::nil()),
            protocol: "tcp".to_string(),
            guest_port: 3000,
            visibility: "private".to_string(),
            source: "ecw".to_string(),
            desired_state: SANDBOX_PORT_DESIRED_OPEN.to_string(),
            apply_state: SANDBOX_PORT_APPLY_APPLIED.to_string(),
            last_applied_node_id: Some(node_id),
            last_error: None,
            last_bridge_status: None,
            last_applied_bridge_generation: None,
            last_observed_at: None,
            created_at: time::OffsetDateTime::UNIX_EPOCH,
            updated_at: time::OffsetDateTime::UNIX_EPOCH,
        };

        assert!(!sandbox_port_exposure_ready_on_node(
            &exposure,
            Some(node_id)
        ));
    }

    #[test]
    fn sandbox_port_exposure_pending_is_not_applied() {
        let node_id = NodeId::from_uuid(Uuid::from_u128(1));
        let exposure = SandboxPortExposure {
            sandbox_id: SandboxId::from_uuid(Uuid::nil()),
            protocol: "tcp".to_string(),
            guest_port: 3000,
            visibility: "private".to_string(),
            source: "ecw".to_string(),
            desired_state: SANDBOX_PORT_DESIRED_OPEN.to_string(),
            apply_state: "pending".to_string(),
            last_applied_node_id: Some(node_id),
            last_error: None,
            last_bridge_status: None,
            last_applied_bridge_generation: None,
            last_observed_at: None,
            created_at: time::OffsetDateTime::UNIX_EPOCH,
            updated_at: time::OffsetDateTime::UNIX_EPOCH,
        };

        assert!(!sandbox_port_exposure_ready_on_node(
            &exposure,
            Some(node_id)
        ));
    }

    #[test]
    fn proxy_failure_bridge_close_skips_dormant_and_terminal_lifecycles() {
        assert!(super::ports::sandbox_lifecycle_may_have_live_loopback_bridge("started"));
        assert!(super::ports::sandbox_lifecycle_may_have_live_loopback_bridge("restoring"));
        assert!(!super::ports::sandbox_lifecycle_may_have_live_loopback_bridge("paused"));
        assert!(!super::ports::sandbox_lifecycle_may_have_live_loopback_bridge("suspended"));
        assert!(!super::ports::sandbox_lifecycle_may_have_live_loopback_bridge("stopped"));
    }

    #[test]
    fn sandbox_port_exposure_admission_enforces_guest_listener_budget() {
        super::ports::enforce_sandbox_port_exposure_capacity(
            pplx_space_common_core::types::MAX_SANDBOX_PORT_EXPOSURES as i64 - 1,
            false,
        )
        .unwrap();
        let error = super::ports::enforce_sandbox_port_exposure_capacity(
            pplx_space_common_core::types::MAX_SANDBOX_PORT_EXPOSURES as i64,
            false,
        )
        .unwrap_err();
        assert_eq!(error.status(), axum::http::StatusCode::CONFLICT);
        super::ports::enforce_sandbox_port_exposure_capacity(
            pplx_space_common_core::types::MAX_SANDBOX_PORT_EXPOSURES as i64,
            true,
        )
        .expect("existing desired-open exposure remains idempotent above the new admission limit");
    }

    #[test]
    fn validate_create_template_reference_accepts_tags_and_full_digest() {
        let template = TemplateReference::latest(TemplateName::parse("seed").unwrap());
        validate_create_template_reference(&template).unwrap();
        assert!(template.uses_default_selector());
        assert_eq!(template.artifact_digest(), None);

        let template = "seed:prod".parse::<TemplateReference>().unwrap();
        validate_create_template_reference(&template).unwrap();
        assert!(!template.uses_default_selector());

        let hash = "sha256:1111111111111111111111111111111111111111111111111111111111111111";
        let template = format!("seed@{hash}").parse::<TemplateReference>().unwrap();
        validate_create_template_reference(&template).unwrap();
        assert_eq!(
            template.artifact_digest().map(|digest| digest.to_string()),
            Some(hash.to_string())
        );
    }

    #[test]
    fn validate_create_template_reference_rejects_digest_prefixes() {
        let template = "seed@sha256:111111111111"
            .parse::<TemplateReference>()
            .unwrap();

        let error = validate_create_template_reference(&template).unwrap_err();
        assert!(error.to_string().contains("digest prefixes"));
    }
}

mod template_release_resolution_postgres_tests {
    use pplx_space_db_test_fixtures::pg_test_db;
    use sqlx::PgPool;

    use super::*;

    const TEMPLATE_COMMIT_SHA_TAG: &str = "62f56c72e6af1173d3e6445c778be4a392d9e442";

    /// Seeded org ids from the baseline migration.
    const TENANT_ORG: &str = "00000000-0000-0000-0000-000000000003";
    const OTHER_ORG: &str = "00000000-0000-0000-0000-000000000002";

    fn org(id: &str) -> OrgId {
        OrgId::from_uuid(Uuid::parse_str(id).expect("valid org UUID"))
    }

    use pplx_space_test_env::TestResult;

    #[tokio::test(flavor = "current_thread")]
    async fn resolves_custom_template_commit_sha_tag_to_distinct_content_hash() -> TestResult {
        let db = pg_test_db().await?;
        let release_id = Uuid::now_v7();
        let content_hash =
            "sha256:1111111111111111111111111111111111111111111111111111111111111111";

        sqlx::query(
            "INSERT INTO templates (id, name, description, org_id, network_policy_generation)
             VALUES ($1, 'custom-template', NULL, NULL, 1)",
        )
        .bind(TemplateId::new().as_uuid())
        .execute(&db.pool)
        .await?;
        sqlx::query(
            "INSERT INTO template_releases (id, template_name, type, hash, org_id)
             VALUES ($1, 'custom-template', 'fc', $2, NULL)",
        )
        .bind(release_id)
        .bind(content_hash)
        .execute(&db.pool)
        .await?;
        sqlx::query(
            "INSERT INTO template_release_tags (template_release_id, template_name, tag)
             VALUES ($1, 'custom-template', $2)",
        )
        .bind(release_id)
        .bind(TEMPLATE_COMMIT_SHA_TAG)
        .execute(&db.pool)
        .await?;

        insert_release_with_latest_tag(
            &db.pool,
            "custom-template",
            Uuid::now_v7(),
            "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            None,
        )
        .await?;

        let template = format!("custom-template:{TEMPLATE_COMMIT_SHA_TAG}")
            .parse::<TemplateReference>()
            .unwrap();
        assert_eq!(
            template.tag().map(|tag| tag.as_str()),
            Some(TEMPLATE_COMMIT_SHA_TAG)
        );
        assert_eq!(template.artifact_digest(), None);

        let resolved =
            resolve_template_release(&db.pool, &template, RuntimeKind::Firecracker, OrgId::new())
                .await?;

        assert_eq!(resolved, (release_id, content_hash.to_string()));
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn missing_explicit_tag_falls_back_to_latest() -> TestResult {
        let db = pg_test_db().await?;
        let release_id = Uuid::now_v7();
        let content_hash =
            "sha256:2222222222222222222222222222222222222222222222222222222222222222";
        sqlx::query(sql!(
            "INSERT INTO templates (id, name, description, org_id, network_policy_generation)
             VALUES ($1, 'custom-template', NULL, NULL, 1)"
        ))
        .bind(TemplateId::new().as_uuid())
        .execute(&db.pool)
        .await?;
        let caller_org = OrgId::new();
        let missing = "custom-template:v123".parse::<TemplateReference>()?;
        let error =
            resolve_template_release(&db.pool, &missing, RuntimeKind::Firecracker, caller_org)
                .await
                .unwrap_err();
        assert_eq!(error.status(), axum::http::StatusCode::NOT_FOUND);

        insert_release_with_latest_tag(&db.pool, "custom-template", release_id, content_hash, None)
            .await?;
        for reference in [
            "custom-template:v123",
            "custom-template:latest",
            "custom-template",
        ] {
            let template = reference.parse::<TemplateReference>()?;

            let resolved =
                resolve_template_release(&db.pool, &template, RuntimeKind::Firecracker, caller_org)
                    .await?;

            assert_eq!(resolved, (release_id, content_hash.to_string()));
        }
        let missing_digest = "custom-template@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            .parse::<TemplateReference>()?;
        let error = resolve_template_release(
            &db.pool,
            &missing_digest,
            RuntimeKind::Firecracker,
            caller_org,
        )
        .await
        .unwrap_err();
        assert_eq!(error.status(), axum::http::StatusCode::NOT_FOUND);
        Ok(())
    }

    /// Pins the backfill policy: an org-owned template left at the default
    /// `generic` visibility stays usable by every other org, so adding the
    /// visibility column is a no-op for existing workloads.
    #[tokio::test(flavor = "current_thread")]
    async fn create_resolution_allows_org_owned_template_releases_during_rollout() -> TestResult {
        let db = pg_test_db().await?;
        let tenant_org_id = Uuid::parse_str(TENANT_ORG)?;
        let cross_org_caller = org(OTHER_ORG);
        let release_id = Uuid::now_v7();
        let content_hash =
            "sha256:3333333333333333333333333333333333333333333333333333333333333333";

        sqlx::query(
            "INSERT INTO templates (id, name, description, org_id, network_policy_generation)
             VALUES ($1, 'tenant-private', NULL, $2, 1)",
        )
        .bind(TemplateId::new().as_uuid())
        .bind(tenant_org_id)
        .execute(&db.pool)
        .await?;
        insert_release_with_latest_tag(
            &db.pool,
            "tenant-private",
            release_id,
            content_hash,
            Some(tenant_org_id),
        )
        .await?;

        let tagged_template = "tenant-private:latest"
            .parse::<TemplateReference>()
            .unwrap();
        let resolved_tag = resolve_template_release(
            &db.pool,
            &tagged_template,
            RuntimeKind::Firecracker,
            cross_org_caller,
        )
        .await?;
        assert_eq!(resolved_tag, (release_id, content_hash.to_string()));

        let digest_template = format!("tenant-private@{content_hash}")
            .parse::<TemplateReference>()
            .unwrap();
        let resolved_digest = resolve_template_release(
            &db.pool,
            &digest_template,
            RuntimeKind::Firecracker,
            cross_org_caller,
        )
        .await?;
        assert_eq!(resolved_digest, (release_id, content_hash.to_string()));

        let wrong_runtime = resolve_template_release(
            &db.pool,
            &digest_template,
            RuntimeKind::CloudHypervisor,
            cross_org_caller,
        )
        .await
        .unwrap_err();
        assert_eq!(wrong_runtime.status(), axum::http::StatusCode::NOT_FOUND);

        Ok(())
    }

    use crate::controllers::test_fixtures::insert_org_specific_template;

    async fn insert_release_with_latest_tag(
        pool: &PgPool,
        name: &str,
        release_id: Uuid,
        content_hash: &str,
        owner_org: Option<Uuid>,
    ) -> TestResult {
        sqlx::query(
            "INSERT INTO template_releases (id, template_name, type, hash, org_id)
             VALUES ($1, $2, 'fc', $3, $4)",
        )
        .bind(release_id)
        .bind(name)
        .bind(content_hash)
        .bind(owner_org)
        .execute(pool)
        .await?;
        sqlx::query(
            "INSERT INTO template_release_tags (template_release_id, template_name, tag)
             VALUES ($1, $2, 'latest')",
        )
        .bind(release_id)
        .bind(name)
        .execute(pool)
        .await?;
        Ok(())
    }

    async fn insert_builder_observed_template(
        pool: &PgPool,
        name: &str,
        content_hash: &str,
    ) -> TestResult {
        let builder_id = Uuid::now_v7();
        sqlx::query(
            "INSERT INTO nodes (id, address, role, managed_by, state)
             VALUES ($1, '10.0.0.10'::INET, 'builder', 'space', 'ready')",
        )
        .bind(builder_id)
        .execute(pool)
        .await?;
        sqlx::query(
            "INSERT INTO node_template_inventory (node_id, template_name, content_hash, runtime)
             VALUES ($1, $2, $3, 'fc')",
        )
        .bind(builder_id)
        .bind(name)
        .bind(content_hash)
        .execute(pool)
        .await?;
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn same_org_caller_resolves_org_specific_template() -> TestResult {
        let db = pg_test_db().await?;
        let release_id = Uuid::now_v7();
        let content_hash =
            "sha256:4444444444444444444444444444444444444444444444444444444444444444";

        insert_org_specific_template(&db.pool, "tenant-secret", Uuid::parse_str(TENANT_ORG)?)
            .await?;
        insert_release_with_latest_tag(
            &db.pool,
            "tenant-secret",
            release_id,
            content_hash,
            Some(Uuid::parse_str(TENANT_ORG)?),
        )
        .await?;

        let template = "tenant-secret:latest".parse::<TemplateReference>().unwrap();
        let resolved = resolve_template_release(
            &db.pool,
            &template,
            RuntimeKind::Firecracker,
            org(TENANT_ORG),
        )
        .await?;

        assert_eq!(resolved, (release_id, content_hash.to_string()));
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn cross_org_caller_gets_not_found_indistinguishable_from_absent() -> TestResult {
        let db = pg_test_db().await?;
        let release_id = Uuid::now_v7();
        let content_hash =
            "sha256:5555555555555555555555555555555555555555555555555555555555555555";

        insert_org_specific_template(&db.pool, "tenant-secret", Uuid::parse_str(TENANT_ORG)?)
            .await?;
        insert_release_with_latest_tag(
            &db.pool,
            "tenant-secret",
            release_id,
            content_hash,
            Some(Uuid::parse_str(TENANT_ORG)?),
        )
        .await?;

        let denied_template = "tenant-secret:latest".parse::<TemplateReference>().unwrap();
        let denied = resolve_template_release(
            &db.pool,
            &denied_template,
            RuntimeKind::Firecracker,
            org(OTHER_ORG),
        )
        .await
        .unwrap_err();
        assert_eq!(denied.status(), axum::http::StatusCode::NOT_FOUND);

        // The denial must be byte-identical (modulo the requested name) to a
        // truly-absent template so a caller can't probe which template names
        // exist in other orgs.
        let absent_template = "tenant-absent:latest".parse::<TemplateReference>().unwrap();
        let absent = resolve_template_release(
            &db.pool,
            &absent_template,
            RuntimeKind::Firecracker,
            org(OTHER_ORG),
        )
        .await
        .unwrap_err();
        assert_eq!(absent.status(), axum::http::StatusCode::NOT_FOUND);
        assert_eq!(
            denied.to_string().replace("tenant-secret", "{name}"),
            absent.to_string().replace("tenant-absent", "{name}"),
        );

        // The digest path is gated too, with the digest-shaped message.
        let denied_digest_template = format!("tenant-secret@{content_hash}")
            .parse::<TemplateReference>()
            .unwrap();
        let denied_digest = resolve_template_release(
            &db.pool,
            &denied_digest_template,
            RuntimeKind::Firecracker,
            org(OTHER_ORG),
        )
        .await
        .unwrap_err();
        assert_eq!(denied_digest.status(), axum::http::StatusCode::NOT_FOUND);
        assert_eq!(
            denied_digest.to_string(),
            format!("no release found for template 'tenant-secret' hash '{content_hash}'"),
        );

        Ok(())
    }

    #[rstest::rstest]
    #[case::generic(None, "generic", OTHER_ORG)]
    #[case::private_owner(Some(TENANT_ORG), "org_specific", TENANT_ORG)]
    #[case::private_foreign(Some(TENANT_ORG), "org_specific", OTHER_ORG)]
    #[tokio::test]
    async fn inventory_cannot_mint_releases_or_tags(
        #[case] owner: Option<&str>,
        #[case] visibility: &str,
        #[case] caller: &str,
    ) -> TestResult {
        let db = pg_test_db().await?;
        let name = "observed-tpl";
        let hash = "sha256:6666666666666666666666666666666666666666666666666666666666666666";
        sqlx::query(sql!(
            "INSERT INTO templates (id, name, org_id, visibility) VALUES ($1, $2, $3, $4)"
        ))
        .bind(TemplateId::new().as_uuid())
        .bind(name)
        .bind(owner.map(Uuid::parse_str).transpose()?)
        .bind(visibility)
        .execute(&db.pool)
        .await?;
        insert_builder_observed_template(&db.pool, name, hash).await?;

        for reference in [
            format!("{name}:latest"),
            format!("{name}:missing"),
            format!("{name}@{hash}"),
        ] {
            let error = resolve_template_release(
                &db.pool,
                &reference.parse()?,
                RuntimeKind::Firecracker,
                org(caller),
            )
            .await
            .expect_err("inventory cannot publish a release");
            assert_eq!(error.status(), axum::http::StatusCode::NOT_FOUND);
        }

        let counts: (i64, i64) = sqlx::query_as(sql!(
            "SELECT (SELECT COUNT(*) FROM template_releases WHERE template_name = $1),
                    (SELECT COUNT(*) FROM template_release_tags WHERE template_name = $1)"
        ))
        .bind(name)
        .fetch_one(&db.pool)
        .await?;
        assert_eq!(counts, (0, 0));
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn published_latest_wins_over_observed_inventory() -> TestResult {
        let db = pg_test_db().await?;
        let template_name = "observed-retag";
        let republished_release_id = Uuid::now_v7();
        let newer_release_id = Uuid::now_v7();
        let republished_hash =
            "sha256:1212121212121212121212121212121212121212121212121212121212121212";
        let newer_hash = "sha256:1313131313131313131313131313131313131313131313131313131313131313";

        sqlx::query(
            "INSERT INTO templates (id, name, description, org_id, network_policy_generation)
             VALUES ($1, $2, NULL, NULL, 1)",
        )
        .bind(TemplateId::new().as_uuid())
        .bind(template_name)
        .execute(&db.pool)
        .await?;
        sqlx::query(
            "INSERT INTO template_releases (id, template_name, type, hash, created_at)
             VALUES
                 ($1, $3, 'fc', $4, NOW() - INTERVAL '2 days'),
                 ($2, $3, 'fc', $5, NOW() - INTERVAL '1 day')",
        )
        .bind(republished_release_id)
        .bind(newer_release_id)
        .bind(template_name)
        .bind(republished_hash)
        .bind(newer_hash)
        .execute(&db.pool)
        .await?;
        sqlx::query(
            "INSERT INTO template_release_tags (
                 template_release_id, template_name, tag, created_at, modified_at
             ) VALUES ($1, $2, 'latest', NOW() - INTERVAL '1 day', NOW() - INTERVAL '1 day')",
        )
        .bind(newer_release_id)
        .bind(template_name)
        .execute(&db.pool)
        .await?;
        sqlx::query(
            "INSERT INTO warm_pool_policies (
                 template_name, runtime, vcpu, memory_mib, min_ready_slots,
                 max_ready_slots, retain_releases
             ) VALUES ($1, 'fc', 2, 1024, 1, 10, 1)",
        )
        .bind(template_name)
        .execute(&db.pool)
        .await?;
        insert_builder_observed_template(&db.pool, template_name, republished_hash).await?;

        let before_modified_at = sqlx::query_scalar::<_, time::OffsetDateTime>(
            "SELECT modified_at
             FROM template_release_tags
             WHERE template_name = $1
               AND tag = 'latest'",
        )
        .bind(template_name)
        .fetch_one(&db.pool)
        .await?;
        let resolved = resolve_template_release(
            &db.pool,
            &format!("{template_name}:latest").parse()?,
            RuntimeKind::Firecracker,
            org(OTHER_ORG),
        )
        .await?;
        let latest = sqlx::query_as::<_, (Uuid, time::OffsetDateTime)>(
            "SELECT template_release_id, modified_at
             FROM template_release_tags
             WHERE template_name = $1
               AND tag = 'latest'",
        )
        .bind(template_name)
        .fetch_one(&db.pool)
        .await?;
        let protected = pplx_space_cluster_pool::WarmPoolStore::new(db.pool.clone())
            .protected_template_releases()
            .await?;

        assert_eq!(resolved, (newer_release_id, newer_hash.to_string()));
        assert_eq!(latest.0, newer_release_id);
        assert_eq!(latest.1, before_modified_at);
        assert!(!protected.iter().any(|release| {
            release.template_name == template_name && release.content_hash == republished_hash
        }));
        assert!(protected.iter().any(|release| {
            release.template_name == template_name && release.content_hash == newer_hash
        }));

        Ok(())
    }

    /// The release lookup itself must be visibility-constrained in SQL, not
    /// just hidden behind the up-front gate: a template published or flipped
    /// to org_specific between the gate read and the release lookup (separate
    /// READ COMMITTED statements) must still not resolve cross-org. Calling
    /// the lookup directly simulates that race window.
    #[tokio::test(flavor = "current_thread")]
    async fn release_lookup_rechecks_visibility_in_sql() -> TestResult {
        let db = pg_test_db().await?;
        let release_id = Uuid::now_v7();
        let content_hash =
            "sha256:8888888888888888888888888888888888888888888888888888888888888888";

        insert_org_specific_template(&db.pool, "tenant-secret", Uuid::parse_str(TENANT_ORG)?)
            .await?;
        insert_release_with_latest_tag(
            &db.pool,
            "tenant-secret",
            release_id,
            content_hash,
            Some(Uuid::parse_str(TENANT_ORG)?),
        )
        .await?;

        let cross_org = resolve_template_release_by_tag(
            &db.pool,
            &TemplateName::parse("tenant-secret")?,
            &TemplateTag::latest(),
            RuntimeKind::Firecracker,
            org(OTHER_ORG),
        )
        .await?;
        assert_eq!(cross_org, None, "cross-org caller must not see the release");

        let same_org = resolve_template_release_by_tag(
            &db.pool,
            &TemplateName::parse("tenant-secret")?,
            &TemplateTag::latest(),
            RuntimeKind::Firecracker,
            org(TENANT_ORG),
        )
        .await?;
        assert_eq!(same_org, Some((release_id, content_hash.to_string())));

        for (caller, expected) in [
            (org(OTHER_ORG), None),
            (
                org(TENANT_ORG),
                Some((release_id, content_hash.to_string())),
            ),
        ] {
            let resolved = crate::template_releases_store::resolve_template_release_by_digest(
                &db.pool,
                &TemplateName::parse("tenant-secret")?,
                content_hash.parse()?,
                RuntimeKind::Firecracker,
                caller,
            )
            .await?;
            assert_eq!(resolved, expected);
        }

        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn org_specific_template_without_owner_is_rejected_by_check() -> TestResult {
        let db = pg_test_db().await?;

        let error = sqlx::query(
            "INSERT INTO templates (id, name, description, org_id, visibility, network_policy_generation)
             VALUES ($1, 'unowned-secret', NULL, NULL, 'org_specific', 1)",
        )
        .bind(TemplateId::new().as_uuid())
        .execute(&db.pool)
        .await
        .unwrap_err();

        assert!(
            error.to_string().contains("org_specific_requires_owner"),
            "unexpected error: {error}",
        );
        Ok(())
    }
}

mod response_tests {

    use super::*;
    use pplx_space_common_core::types::{NodeId, NodeManagedBy, SandboxId, SandboxOwnerSource};
    use pplx_space_common_core::types::{OrgId, UserId};
    use pplx_space_common_svc::space_api_route_ed25519::{
        SPACE_API_ROUTE_ED25519_ISSUER_ENV, SPACE_API_ROUTE_ED25519_SIGNING_KEY_ID_ENV,
        SPACE_API_ROUTE_ED25519_SIGNING_KEY_PATH_ENV, SpaceApiRouteEd25519Authority,
        decode_unverified_space_api_route_ed25519_claims,
    };

    use sqlx::types::Json;

    fn sandbox_with_network_policy(
        override_policy: Option<NetworkPolicyV1>,
        override_generation: i64,
        template_policy: Option<NetworkPolicyV1>,
        template_generation: Option<i64>,
    ) -> Sandbox {
        Sandbox {
            id: SandboxId::from_uuid(Uuid::nil()),
            name: None,
            user_id: UserId::from_uuid(Uuid::nil()),
            org_id: OrgId::from_uuid(Uuid::nil()),
            owner_source: SandboxOwnerSource::Local,
            upstream_issuer: None,
            upstream_org_id: None,
            upstream_subject_id: None,
            managed_by: "direct".to_string(),
            lifecycle: "running".to_string(),
            placement: "on_node".to_string(),
            active_node_id: Some(NodeId::from_uuid(Uuid::nil())),
            active_node_name: None,
            active_node_role: None,
            active_node_address: None,
            active_node_managed_by: None,
            active_execution_generation: None,
            active_pod_uid: None,
            active_activation_attempt_id: None,
            ecw_credential_epoch: pplx_space_common_core::types::EcwCredentialEpoch::new(1)
                .expect("credential epoch is valid"),
            target_node_id: None,
            placement_changed_at: None,
            template_id: Some("base".to_string()),
            content_hash: None,
            release_tags: Vec::new(),
            vcpu: None,
            memory_mib: None,
            disk_mib: None,
            disk_io_max_operations_per_sec: 1_000,
            disk_io_max_bandwidth_mib_per_sec: 100,
            env: Json(Default::default()),
            metadata: Json(Default::default()),
            auto_lifecycle: Json(AutoLifecyclePolicy::disabled()),
            auto_lifecycle_state: Json(AutoLifecycleRuntimeState::default()),
            network_policy: override_policy.map(Json),
            network_policy_generation: override_generation,
            template_network_policy: template_policy.map(Json),
            template_network_policy_generation: template_generation,
            next_auto_event_at: None,
            snapshots_enabled_desired: false,
            volume_encryption: false,
            command_ready_pending: false,
            last_used_at: None,
            last_access_at: None,
            last_command_at: None,
            created_at: time::OffsetDateTime::UNIX_EPOCH,
            updated_at: time::OffsetDateTime::UNIX_EPOCH,
        }
    }

    #[test]
    fn tracked_response_serves_owner_from_cluster_not_node_echo() {
        let sandbox = sandbox_with_network_policy(None, 0, None, None);
        let mut observed = observed_network_policy("cluster", 1);
        // A node echoing a divergent owner (the orphan-adoption artifact)
        // must not override the cluster's immutable birth fact.
        observed.sandbox_owner = Some("orphaned@local".to_string());

        let response = sandbox_to_cached_response(&sandbox, Some(&observed));

        assert_eq!(
            response.user.as_deref(),
            Some("user:00000000-0000-0000-0000-000000000000")
        );
    }

    fn observed_network_policy(source: &str, generation: i64) -> SandboxObservedState {
        SandboxObservedState {
            sandbox_id: SandboxId::from_uuid(Uuid::nil()),
            node_id: Some(NodeId::from_uuid(Uuid::nil())),
            node_name: None,
            node_role: None,
            node_managed_by: None,
            node_api_endpoint: None,
            substrate_node_id: None,
            template: None,
            status: None,
            spaced_version: None,
            pid: None,
            namespace: None,
            uptime: None,
            sandbox_owner: None,
            image_hash: None,
            snapshots_enabled: false,
            disk_usage_bytes: None,
            pool_template: None,
            failure_reason: None,
            observed_created_at: None,
            observed_at: time::OffsetDateTime::UNIX_EPOCH,
            release_tags: Vec::new(),
            observed_network_policy: None,
            observed_network_policy_generation: Some(generation),
            observed_network_policy_source: Some(source.to_string()),
            observed_network_policy_authority: Some("cluster".to_string()),
            observed_network_policy_at: None,
            network_policy_conditions: Json(Vec::new()),
        }
    }

    #[test]
    fn network_policy_status_compares_source_and_generation() {
        let sandbox = sandbox_with_network_policy(
            Some(NetworkPolicyV1::deny_all()),
            7,
            Some(NetworkPolicyV1::allow_all()),
            Some(7),
        );
        let observed = observed_network_policy("template", 7);

        let status = network_policy_status_for(Some(&sandbox), Some(&observed), None).unwrap();

        let stale = status
            .conditions
            .iter()
            .find(|condition| condition.condition_type == "Stale")
            .expect("source mismatch should be stale");
        assert_eq!(stale.status, "True");
        assert_eq!(
            stale.reason.as_deref(),
            Some("ObservedSourceOrGenerationDiffers")
        );
    }

    #[test]
    fn db_backed_response_is_sandbox_resource() {
        let sandbox = sandbox_with_network_policy(None, 1, None, None);

        let response = sandbox_to_response(&sandbox, None);

        assert_eq!(response.resource_type, SandboxResourceType::Sandbox);
    }

    #[test]
    fn db_backed_response_includes_content_hash() {
        let mut sandbox = sandbox_with_network_policy(None, 1, None, None);
        sandbox.content_hash = Some("sha256:db".to_string());

        let response = sandbox_to_response(&sandbox, None);

        assert_eq!(response.content_hash.as_deref(), Some("sha256:db"));
    }

    #[test]
    fn db_backed_response_includes_release_tags() {
        let mut sandbox = sandbox_with_network_policy(None, 1, None, None);
        sandbox.release_tags = vec!["latest".to_string(), "release-2026-05-28".to_string()];

        let response = sandbox_to_response(&sandbox, None);

        assert_eq!(response.release_tags, sandbox.release_tags);
    }

    #[test]
    fn observed_pool_response_is_pool_slot_resource() {
        let mut observed = observed_network_policy("template", 1);
        observed.pool_template = Some("seed".to_string());

        let response = observed_only_sandbox_response(&observed);

        assert_eq!(response.resource_type, SandboxResourceType::PoolSlot);
    }

    #[test]
    fn observed_only_response_uses_image_hash_as_content_hash() {
        let mut observed = observed_network_policy("template", 1);
        observed.image_hash = Some("sha256:observed".to_string());

        let response = observed_only_sandbox_response(&observed);

        assert_eq!(response.content_hash.as_deref(), Some("sha256:observed"));
    }

    #[test]
    fn observed_only_response_includes_release_tags() {
        let mut observed = observed_network_policy("template", 1);
        observed.release_tags = vec!["latest".to_string(), "stable".to_string()];

        let response = observed_only_sandbox_response(&observed);

        assert_eq!(response.release_tags, observed.release_tags);
    }

    #[test]
    fn observed_non_pool_response_is_observed_local_resource() {
        let mut observed = observed_network_policy("template", 1);
        observed.observed_created_at = Some("60".to_string());

        let response = observed_only_sandbox_response(&observed);

        assert_eq!(response.resource_type, SandboxResourceType::ObservedLocal);
        assert_eq!(
            response.created_at,
            time::OffsetDateTime::from_unix_timestamp(60).ok()
        );
    }

    #[test]
    fn space_api_bridge_route_token_uses_active_execution_identity() {
        {
            let mut env = pplx_space_test_env::TestEnv::default();
            env.set("SPACE_CLUSTER_ID", "unit-test");
            env.set(
                SPACE_API_ROUTE_ED25519_SIGNING_KEY_PATH_ENV,
                pplx_space_common_svc::space_api_route_ed25519::test_signing_key_path()
                    .to_str()
                    .unwrap(),
            );
            env.set(
                SPACE_API_ROUTE_ED25519_SIGNING_KEY_ID_ENV,
                "unit-test-route-key",
            );
            env.set(SPACE_API_ROUTE_ED25519_ISSUER_ENV, "space-api");
            let authority =
                SpaceApiRouteEd25519Authority::load_required_from_environment(&env).unwrap();
            let active_node_id = NodeId::new();
            let mut sandbox = sandbox_with_network_policy(None, 1, None, None);
            sandbox.id = SandboxId::new();
            sandbox.org_id = OrgId::new();
            sandbox.active_node_id = Some(active_node_id);
            sandbox.active_node_managed_by = Some(NodeManagedBy::Kubernetes);
            sandbox.active_execution_generation = Some(42);
            sandbox.active_pod_uid = Some(PodUid::parse("pod-uid-42").unwrap());
            sandbox.active_activation_attempt_id = Some(SandboxActivationAttemptId::new());

            let token = crate::controllers::sandbox::spaced::space_api_bridge_route_token(
                Some(&authority),
                &sandbox,
            )
            .expect("token mint succeeds")
            .expect("route signing authority is configured");
            let claims = decode_unverified_space_api_route_ed25519_claims(&token)
                .expect("token claims decode");
            let identity = claims.route_identity;

            assert_eq!(identity.sandbox_id(), sandbox.id);
            assert_eq!(identity.current_node_id(), active_node_id);
            assert_eq!(identity.route_generation().get(), 42);
            assert_eq!(identity.execution_generation().get(), 42);
            assert_eq!(identity.pod_uid().map(PodUid::as_str), Some("pod-uid-42"));
            assert_eq!(
                Some(identity.activation_attempt_id()),
                sandbox.active_activation_attempt_id
            );
        };
    }
}
