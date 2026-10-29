//! The warm-pool step charges activation admission only on a miss, so a warm
//! hit leaves both the cluster bucket and the client's bucket untouched.
//!
//! Driven through `warm_pool_step_then_charge` with the scripted pool
//! substrate and a Redis-backed admission that has a per-client rate.

use super::*;
use crate::cluster_admission::{
    ClientActivationRate, ClusterSandboxAdmission, ClusterSandboxAdmissionConfig,
};
use crate::controllers::sandbox::queries::pool_step::{
    PoolPath, PoolStep, PoolStepInput, warm_pool_step_then_charge,
};
use crate::test_support::try_redis_admission;
use pplx_space_common_core::types::ClientKey;
use pplx_space_common_core::types::RuntimeKind;

const CLUSTER_BURST: f64 = 10.0;
const CLIENT_BURST: f64 = 5.0;

/// A Redis-backed admission with a per-client rate and full buckets, on its
/// own logical Redis DB, and a fresh client key.
async fn charged_admission(
    pool: &PgPool,
    redis_db: u8,
) -> TestResult<
    Option<(
        Option<testcontainers::ContainerAsync<testcontainers_modules::redis::Redis>>,
        ClusterSandboxAdmission,
        ClientKey,
    )>,
> {
    let config = ClusterSandboxAdmissionConfig {
        enabled: true,
        refill_rps: 0.001,
        burst: CLUSTER_BURST,
        redis_url: None,
        client_rate: ClientActivationRate::new(0.001, Some(CLIENT_BURST)),
    };
    let Some((node, admission)) = try_redis_admission(
        pool,
        redis_db,
        config,
        pplx_space_feature_flags::FeatureFlagClient::disabled(),
    )
    .await?
    else {
        return Ok(None);
    };
    let client = ClientKey::allowlisted(&format!("pool-step-{}", Uuid::now_v7()))
        .expect("a label-safe client id");
    Ok(Some((node, admission, client)))
}

/// Run the warm-pool step for a pool-eligible create against `substrate`.
async fn run_step(
    db: &pplx_space_db_test_fixtures::TestDb,
    substrate: &RetryingPoolSubstrate,
    release_id: Uuid,
    admission: &ClusterSandboxAdmission,
    client: &ClientKey,
    boot_mode: CreateBootMode,
) -> Result<PoolStep, ControllerError> {
    let actor = insert_actor(&db.pool).await.expect("actor");
    let auto_lifecycle = AutoLifecyclePolicy::disabled();
    let mut req = pooled_create_request("pool-step", None);
    req.boot_mode = None;
    let template_network_policy = NetworkPolicyV1::deny_all();
    let requested_template = TemplateReference::latest(TemplateName::parse("seed").unwrap());
    let create_owner = SandboxCreateOwner::local(&actor);
    let mut pooled = pooled_create_input(
        &db.pool,
        &req,
        &requested_template,
        &auto_lifecycle,
        &template_network_policy,
        &create_owner,
    );
    pooled.template_release_id = release_id;
    pooled.boot_mode = boot_mode;
    pooled.client_key = client;
    let template = TemplateName::parse("seed").unwrap();
    let claim_duration_nanos = Arc::new(AtomicU64::new(0));
    let mut create_path = CreatePathEmitter {
        template: "seed",
        content_hash: "sha256:pooled",
        surface: "native",
        started_at: Instant::now(),
        observation: PoolCreateObservation::error(),
        claim_duration_nanos: Arc::clone(&claim_duration_nanos),
        request_succeeded: false,
    };
    warm_pool_step_then_charge(
        substrate,
        Some(admission),
        PoolStepInput {
            pool: &db.pool,
            pooled,
            path: PoolPath::Claim,
            cold_shape: ColdCreateShapeInput {
                capabilities: substrate.capabilities(),
                runtime: RuntimeKind::Firecracker,
                boot_mode,
                template: &template,
                content_hash: "sha256:pooled",
                template_release_id: release_id,
                requested: SandboxResources {
                    vcpu: None,
                    memory_mib: None,
                    disk_mib: None,
                },
                restoring: false,
            },
            client_key: client,
        },
        &mut create_path,
        claim_duration_nanos,
    )
    .await
}

/// A template release with an enabled pool config, and a node for claims.
async fn pooled_release(pool: &PgPool) -> TestResult<(Uuid, Uuid, NodeId)> {
    let release_id = insert_template_release(pool, "seed", "sha256:pooled").await?;
    let pool_config_id = Uuid::now_v7();
    let node_id = NodeId::new();
    insert_node(pool, node_id).await?;
    sqlx::query(sql!(
        "INSERT INTO kubernetes_pool_configs (
             id, template_release_id, vcpu, memory_mib, target_ready_slots,
             refill_per_tick, max_warming_slots, namespace, enabled
         )
         VALUES ($1, $2, 4, 2048, 3, 1, 1, 'space', true)"
    ))
    .bind(pool_config_id)
    .bind(release_id)
    .execute(pool)
    .await?;
    Ok((release_id, pool_config_id, node_id))
}

#[tokio::test(flavor = "current_thread")]
async fn a_warm_hit_leaves_both_activation_buckets_untouched() -> TestResult {
    let db = pg_test_db().await?;
    let Some((_node, admission, client)) = charged_admission(&db.pool, 6).await? else {
        return Ok(());
    };
    let (release_id, pool_config_id, node_id) = pooled_release(&db.pool).await?;
    let sandbox_id = SandboxId::new();
    let substrate = RetryingPoolSubstrate::new(
        db.pool.clone(),
        pool_config_id,
        release_id,
        vec![pooled_claim(sandbox_id, node_id)],
        vec![true],
    );

    let step = run_step(
        &db,
        &substrate,
        release_id,
        &admission,
        &client,
        CreateBootMode::Fallthrough,
    )
    .await?;

    let PoolStep::Served(response) = step else {
        panic!("the ready slot serves the create");
    };
    assert_eq!(response.id, sandbox_id);
    assert_eq!(substrate.claim_calls(), 1);
    // No consume wrote either key, so neither bucket lost a token.
    assert_eq!(admission.stored_tokens(&client).await?, (None, None));
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn a_warm_miss_charges_both_activation_buckets() -> TestResult {
    let db = pg_test_db().await?;
    let Some((_node, admission, client)) = charged_admission(&db.pool, 7).await? else {
        return Ok(());
    };
    let (release_id, pool_config_id, _) = pooled_release(&db.pool).await?;
    let substrate = RetryingPoolSubstrate::new(
        db.pool.clone(),
        pool_config_id,
        release_id,
        Vec::new(),
        Vec::new(),
    );

    let step = run_step(
        &db,
        &substrate,
        release_id,
        &admission,
        &client,
        CreateBootMode::Fallthrough,
    )
    .await?;

    assert!(
        matches!(step, PoolStep::Cold(_)),
        "an empty pool builds cold"
    );
    assert_eq!(substrate.claim_calls(), 1, "the pool was tried first");
    let (cluster, client_tokens) = admission.stored_tokens(&client).await?;
    for (what, tokens, burst) in [
        ("cluster", cluster, CLUSTER_BURST),
        ("client", client_tokens, CLIENT_BURST),
    ] {
        let tokens = tokens.unwrap_or_else(|| panic!("the {what} bucket was charged"));
        assert!(
            (tokens - (burst - 1.0)).abs() < 0.05,
            "{what} bucket holds {tokens}, expected one token below its {burst} burst"
        );
    }
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn a_create_refused_before_it_builds_cold_charges_no_bucket() -> TestResult {
    let db = pg_test_db().await?;
    let Some((_node, admission, client)) = charged_admission(&db.pool, 8).await? else {
        return Ok(());
    };
    let (release_id, pool_config_id, _) = pooled_release(&db.pool).await?;
    let substrate = RetryingPoolSubstrate::new(
        db.pool.clone(),
        pool_config_id,
        release_id,
        Vec::new(),
        Vec::new(),
    );

    // `boot_mode=pool` with no ready slot is refused after the pool step and
    // before the cold path, so the activation it never ran costs nothing.
    let refused = run_step(
        &db,
        &substrate,
        release_id,
        &admission,
        &client,
        CreateBootMode::Pool,
    )
    .await;

    let Err(error) = refused else {
        panic!("a strict pool create with no slot is refused");
    };
    assert_eq!(error.status(), StatusCode::SERVICE_UNAVAILABLE, "{error}");
    assert!(
        error.to_string().starts_with("boot_mode=pool requested"),
        "refused by the strict-pool check, not by setup: {error}"
    );
    assert_eq!(substrate.claim_calls(), 1, "the pool was tried first");
    assert_eq!(admission.stored_tokens(&client).await?, (None, None));
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn a_create_whose_cold_shape_fails_to_resolve_charges_no_bucket() -> TestResult {
    let db = pg_test_db().await?;
    let Some((_node, admission, client)) = charged_admission(&db.pool, 9).await? else {
        return Ok(());
    };
    let (release_id, pool_config_id, _) = pooled_release(&db.pool).await?;
    // One MiB past the rootfs cap: the cold fallthrough create inherits the
    // pool's shape, and resolving it refuses the disk.
    sqlx::query(sql!(
        "UPDATE kubernetes_pool_configs SET disk_mib = 131073 WHERE id = $1"
    ))
    .bind(pool_config_id)
    .execute(&db.pool)
    .await?;
    let substrate = RetryingPoolSubstrate::new(
        db.pool.clone(),
        pool_config_id,
        release_id,
        Vec::new(),
        Vec::new(),
    );

    let refused = run_step(
        &db,
        &substrate,
        release_id,
        &admission,
        &client,
        CreateBootMode::Fallthrough,
    )
    .await;

    let Err(error) = refused else {
        panic!("a cold shape over the rootfs cap is refused");
    };
    assert!(
        error.to_string().contains("disk_mib is unsupported"),
        "refused while resolving the cold shape: {error}"
    );
    assert_eq!(substrate.claim_calls(), 1, "the pool was tried first");
    // Charging before the shape resolved would have spent a token from each.
    assert_eq!(admission.stored_tokens(&client).await?, (None, None));
    Ok(())
}
