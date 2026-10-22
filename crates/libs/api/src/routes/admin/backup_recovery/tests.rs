use super::*;
use crate::{models::User, user_role::UserRole};
use pplx_space_common_core::{
    sql,
    types::{Arch, ExecutionMetadata, SandboxId, SnapshotExecutionMetadata},
};

#[tokio::test]
async fn recovery_admin_authority_and_rollout_gate() -> pplx_space_test_env::TestResult {
    if !pplx_space_test_env::enter_test_process(&[(
        "SPACE_K8S_CONTROL_PLANE",
        Some(std::ffi::OsStr::new("false")),
    )]) {
        return Ok(());
    }
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db().await? else {
        return Ok(());
    };
    let mut state = AppState::from_pool(db.pool.clone()).await?;
    state.feature_flags = pplx_space_feature_flags::FeatureFlagClient::disabled();
    let admin = sqlx::query_as::<_, User>(sql!("SELECT u.* FROM users u JOIN clusters c ON c.managing_org_id=u.org_id WHERE c.singleton AND u.role='organization_admin' ORDER BY u.id LIMIT 1"))
        .fetch_one(&db.pool).await?;
    let sandbox = PublicSandboxId::from(SandboxId::new());
    for (role, enabled, expected) in [
        (UserRole::Member, true, StatusCode::FORBIDDEN),
        (
            UserRole::OrganizationAdmin,
            false,
            StatusCode::SERVICE_UNAVAILABLE,
        ),
        (UserRole::OrganizationAdmin, true, StatusCode::NOT_FOUND),
    ] {
        let mut actor = admin.clone();
        actor.role = role;
        state.feature_flags = pplx_space_feature_flags::FeatureFlagClient::fixed_bools([(
            pplx_space_feature_flags::BACKUP_RECOVERY_GRANTS_ENABLED.key(),
            enabled,
        )]);
        let approval = Approval {
            local_artifact_id: LocalArtifactId::new(),
            execution_generation: ExecutionGeneration::new(1)?,
            uploader_node_id: NodeId::new(),
            uploader_node_uid: "synthetic-node".parse()?,
            captured_at: "2026-01-01T00:00:00Z".into(),
            expires_at: "2026-01-01T01:00:00Z".into(),
            capture_evidence: recovery::CaptureTimeEvidence::RecordedCapture {
                reference: "synthetic capture".into(),
            },
            reason: "synthetic approval".into(),
            manifest: RecoveryManifest {
                validation: recovery::SealedValidation {
                    sealed_full_suspend: true,
                    evidence_reference: "synthetic validation".into(),
                },
                execution: SnapshotExecutionMetadata::new(ExecutionMetadata::initial_firecracker(
                    Arch::Aarch64,
                    vec![],
                ))?,
                metadata_binding_digest: format!("sha256:{}", "11".repeat(32)).parse()?,
                vcpu: 2,
                memory_mib: 1024,
                template_id: "synthetic-template".parse()?,
                content_hash: None,
                artifacts: vec![],
            },
        };
        let response = approve(
            State(state.clone()),
            Extension(AuthUser(actor.clone())),
            Path(sandbox),
            Json(approval),
        )
        .await;
        assert_eq!(response.status(), expected);
        let response = list(
            State(state.clone()),
            Extension(AuthUser(actor.clone())),
            Path(sandbox),
        )
        .await;
        assert_eq!(
            response.status(),
            if role == UserRole::Member {
                StatusCode::FORBIDDEN
            } else {
                StatusCode::OK
            }
        );
        let response = revoke(
            State(state.clone()),
            Extension(AuthUser(actor)),
            Path((sandbox, RecoveryGrantId::new())),
            Json(Revocation {
                reason: "synthetic revocation".into(),
            }),
        )
        .await;
        assert_eq!(
            response.status(),
            if role == UserRole::Member {
                StatusCode::FORBIDDEN
            } else {
                StatusCode::NOT_FOUND
            }
        );
    }
    Ok(())
}
