use pplx_space_common_core::types::{OrgId, UserId};

use std::time::Duration;

use pplx_space_common_core::types::{
    ContentHash, Digest, MemorySize, SandboxId, SandboxLifecycle, TemplateName, VcpuCount,
};
use pplx_space_db_test_fixtures::{TestDb, pg_test_db};
use sqlx::PgPool;
use uuid::Uuid;

use crate::{
    ManualPoolConfigSpec, NewPoolSlot, OpenPoolSlotDeleteLock, PoolConfigOwner, PoolError,
    PoolSlot, PoolSlotState, WarmPoolStore,
};

type TestResult<T = ()> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[test]
fn parses_slot_state() {
    // arrange
    let state = "ready";

    // act
    let parsed = state.parse::<PoolSlotState>().unwrap();

    // assert
    assert_eq!(parsed, PoolSlotState::Ready);
    assert_eq!(parsed.as_str(), state);
}

#[test]
fn rejects_unknown_slot_state() {
    // arrange
    let state = "lost";

    // act
    let err = state.parse::<PoolSlotState>().unwrap_err();

    // assert
    assert!(matches!(err, PoolError::InvalidSlotState(value) if value == state));
}

const ALL_SLOT_STATES: [PoolSlotState; 5] = [
    PoolSlotState::Warming,
    PoolSlotState::Ready,
    PoolSlotState::Claimed,
    PoolSlotState::Bound,
    PoolSlotState::CleanupPending,
];

#[test]
fn slot_state_round_trips_through_strings() {
    for state in ALL_SLOT_STATES {
        // act
        let parsed = state.as_str().parse::<PoolSlotState>().unwrap();

        // assert
        assert_eq!(parsed, state);
        assert_eq!(state.to_string(), state.as_str());
    }
}

#[test]
fn slot_state_transitions_enforce_legal_edges() {
    for state in ALL_SLOT_STATES {
        // act + assert: each verb succeeds only from its legal source state.
        assert_eq!(state.mark_ready().is_ok(), state == PoolSlotState::Warming);
        assert_eq!(state.claim().is_ok(), state == PoolSlotState::Ready);
        assert_eq!(state.bind().is_ok(), state == PoolSlotState::Claimed);
        assert_eq!(state.release().is_ok(), state == PoolSlotState::Claimed);
        assert_eq!(
            state.cleanup_pending().is_ok(),
            matches!(state, PoolSlotState::Claimed | PoolSlotState::Bound)
        );
    }
}

#[test]
fn slot_state_transitions_produce_target_states() {
    assert_eq!(
        PoolSlotState::Warming.mark_ready().unwrap(),
        PoolSlotState::Ready
    );
    assert_eq!(
        PoolSlotState::Ready.claim().unwrap(),
        PoolSlotState::Claimed
    );
    assert_eq!(PoolSlotState::Claimed.bind().unwrap(), PoolSlotState::Bound);
    assert_eq!(
        PoolSlotState::Claimed.release().unwrap(),
        PoolSlotState::Ready
    );
    assert_eq!(
        PoolSlotState::Claimed.cleanup_pending().unwrap(),
        PoolSlotState::CleanupPending
    );
    assert_eq!(
        PoolSlotState::Bound.cleanup_pending().unwrap(),
        PoolSlotState::CleanupPending
    );
}

#[test]
fn illegal_transition_reports_from_and_to_states() {
    // act
    let transition = PoolSlotState::CleanupPending.mark_ready();

    // assert
    assert!(matches!(
        transition,
        Err(PoolError::InvalidSlotTransition {
            from: "cleanup_pending",
            to: "ready"
        })
    ));
}

struct PostgresFixture {
    store: WarmPoolStore,
    pool: PgPool,
    _db: TestDb,
}

async fn postgres_fixture() -> TestResult<PostgresFixture> {
    let db = pg_test_db().await?;
    let pool = db.pool.clone();

    Ok(PostgresFixture {
        store: WarmPoolStore::new(pool.clone()),
        pool,
        _db: db,
    })
}

async fn insert_pool_config(
    pool: &PgPool,
    template_release_id: Uuid,
    config_id: Uuid,
    content_hash: ContentHash,
) -> TestResult {
    let user_id = UserId::new();
    let org_id = OrgId::new();

    sqlx::query(
        "INSERT INTO organizations (id, name, slug)
         VALUES ($1, 'Pool Test', $2)",
    )
    .bind(org_id)
    .bind(format!("pool-test-{org_id}"))
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO users (id, email, password_hash, display_name, org_id, role)
         VALUES ($1, $2, 'unused', 'Pool Test', $3, 'member')",
    )
    .bind(user_id)
    .bind(format!("pool-test-{user_id}@example.com"))
    .bind(org_id)
    .execute(pool)
    .await?;
    sqlx::query("INSERT INTO templates (id, name, org_id) VALUES ($1, 'seed', $2)")
        .bind(Uuid::now_v7())
        .bind(org_id)
        .execute(pool)
        .await?;
    sqlx::query(
        "INSERT INTO template_releases (id, template_name, type, hash)
         VALUES ($1, 'seed', 'fc', $2)",
    )
    .bind(template_release_id)
    .bind(content_hash.to_string())
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO kubernetes_pool_configs (
             id, template_release_id, vcpu, memory_mib, target_ready_slots,
             refill_per_tick, max_warming_slots, namespace, enabled
         )
         VALUES ($1, $2, 2, 1024, 2, 1, 2, 'space', true)",
    )
    .bind(config_id)
    .bind(template_release_id)
    .execute(pool)
    .await?;

    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn warming_slot_ids_are_uuid_v7() -> TestResult {
    let fixture = postgres_fixture().await?;
    let (release_id, config_id, content_hash) = seed_pool_config(&fixture.pool, 0x71).await?;
    let (vcpu, memory) = default_shape()?;
    let template_name = TemplateName::parse("seed")?;
    let slot = fixture
        .store
        .insert_warming_slot(NewPoolSlot {
            pod_namespace: "sandboxes",
            ..new_slot(
                config_id,
                release_id,
                &template_name,
                content_hash,
                vcpu,
                memory,
                "space-sbx-v7-slot-g1",
            )
        })
        .await?;

    assert_eq!(slot.id.get_version_num(), 7);
    Ok(())
}

async fn insert_template(pool: &PgPool, template_name: &str) -> TestResult {
    let user_id = UserId::new();
    let org_id = OrgId::new();

    sqlx::query(
        "INSERT INTO organizations (id, name, slug)
         VALUES ($1, 'Pool Test', $2)",
    )
    .bind(org_id)
    .bind(format!("pool-test-{org_id}"))
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO users (id, email, password_hash, display_name, org_id, role)
         VALUES ($1, $2, 'unused', 'Pool Test', $3, 'member')",
    )
    .bind(user_id)
    .bind(format!("pool-test-{user_id}@example.com"))
    .bind(org_id)
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO templates (id, name, org_id)
         VALUES ($1, $2, $3)
         ON CONFLICT (name) DO NOTHING",
    )
    .bind(Uuid::now_v7())
    .bind(template_name)
    .bind(org_id)
    .execute(pool)
    .await?;

    Ok(())
}

async fn insert_template_release(
    pool: &PgPool,
    template_name: &str,
    template_release_id: Uuid,
    content_hash: ContentHash,
) -> TestResult {
    let content_hash = content_hash.to_string();
    insert_template_release_with_hash(pool, template_name, template_release_id, &content_hash).await
}

async fn insert_template_release_with_hash(
    pool: &PgPool,
    template_name: &str,
    template_release_id: Uuid,
    content_hash: &str,
) -> TestResult {
    insert_template(pool, template_name).await?;
    sqlx::query(
        "INSERT INTO template_releases (id, template_name, type, hash)
         VALUES ($1, $2, 'fc', $3)",
    )
    .bind(template_release_id)
    .bind(template_name)
    .bind(content_hash.to_string())
    .execute(pool)
    .await?;

    Ok(())
}

/// Seeds a fresh release for `template_name` with a hash derived from
/// `hash_byte`, returning the ids most autoscaler tests need to seed against
/// and assert on afterward.
async fn seed_release(
    pool: &PgPool,
    template_name: &str,
    hash_byte: u8,
) -> TestResult<(Uuid, ContentHash)> {
    let release_id = Uuid::now_v7();
    let content_hash = ContentHash::new(Digest::sha256([hash_byte; 32]));
    insert_template_release(pool, template_name, release_id, content_hash).await?;
    Ok((release_id, content_hash))
}

async fn upsert_warm_pool_policy(
    pool: &PgPool,
    template_name: &str,
    min_ready_slots: i32,
    max_ready_slots: i32,
    retain_releases: i32,
) -> TestResult {
    upsert_warm_pool_policy_with_pacing(
        pool,
        template_name,
        min_ready_slots,
        max_ready_slots,
        5,
        10,
        retain_releases,
    )
    .await
}

async fn upsert_warm_pool_policy_with_pacing(
    pool: &PgPool,
    template_name: &str,
    min_ready_slots: i32,
    max_ready_slots: i32,
    refill_per_tick: i32,
    max_warming_slots: i32,
    retain_releases: i32,
) -> TestResult {
    sqlx::query(
        "INSERT INTO warm_pool_policies (
             template_name, runtime, vcpu, memory_mib, min_ready_slots,
             max_ready_slots, refill_per_tick, max_warming_slots, retain_releases
         )
         VALUES ($1, 'fc', 2, 1024, $2, $3, $4, $5, $6)
         ON CONFLICT (template_name, runtime, vcpu, memory_mib) DO UPDATE
         SET min_ready_slots = EXCLUDED.min_ready_slots,
             max_ready_slots = EXCLUDED.max_ready_slots,
             refill_per_tick = EXCLUDED.refill_per_tick,
             max_warming_slots = EXCLUDED.max_warming_slots,
             retain_releases = EXCLUDED.retain_releases,
             updated_at = NOW()",
    )
    .bind(template_name)
    .bind(min_ready_slots)
    .bind(max_ready_slots)
    .bind(refill_per_tick)
    .bind(max_warming_slots)
    .bind(retain_releases)
    .execute(pool)
    .await?;

    Ok(())
}

struct PoolConfigSeed<'a> {
    template_name: &'a str,
    template_release_id: Uuid,
    config_id: Uuid,
    content_hash: ContentHash,
    target_ready_slots: i32,
    refill_per_tick: i32,
    max_warming_slots: i32,
    namespace: &'a str,
    enabled: bool,
    managed_by: &'a str,
}

async fn insert_pool_config_seed(pool: &PgPool, seed: PoolConfigSeed<'_>) -> TestResult {
    insert_template_release(
        pool,
        seed.template_name,
        seed.template_release_id,
        seed.content_hash,
    )
    .await?;
    sqlx::query(
        "INSERT INTO kubernetes_pool_configs (
             id, template_release_id, vcpu, memory_mib, target_ready_slots,
             refill_per_tick, max_warming_slots, namespace, enabled, managed_by
         )
         VALUES ($1, $2, 2, 1024, $3, $4, $5, $6, $7, $8)",
    )
    .bind(seed.config_id)
    .bind(seed.template_release_id)
    .bind(seed.target_ready_slots)
    .bind(seed.refill_per_tick)
    .bind(seed.max_warming_slots)
    .bind(seed.namespace)
    .bind(seed.enabled)
    .bind(seed.managed_by)
    .execute(pool)
    .await?;

    Ok(())
}

/// The `2 / 1 / 2` target/refill/max-warming shape, owned by the
/// autoscaler in the "sandboxes" namespace — the combination every
/// autoscale-owned-config fixture in this file seeds unless the test is
/// specifically about a different shape.
fn autoscaler_seed(
    template_name: &str,
    template_release_id: Uuid,
    config_id: Uuid,
    content_hash: ContentHash,
    (target_ready_slots, refill_per_tick, max_warming_slots): (i32, i32, i32),
) -> PoolConfigSeed<'_> {
    PoolConfigSeed {
        template_name,
        template_release_id,
        config_id,
        content_hash,
        target_ready_slots,
        refill_per_tick,
        max_warming_slots,
        namespace: "sandboxes",
        enabled: true,
        managed_by: "autoscaler",
    }
}

/// Seeds an enabled, manually-owned pool config (`managed_by: "manual"`,
/// `enabled: true`) at the given shape in the "space" namespace, for tests asserting the
/// autoscaler leaves manual configs alone.
async fn seed_manual_pool_config(
    pool: &PgPool,
    hash_byte: u8,
    (target_ready_slots, refill_per_tick, max_warming_slots): (i32, i32, i32),
) -> TestResult<(Uuid, Uuid, ContentHash)> {
    let release_id = Uuid::now_v7();
    let config_id = Uuid::now_v7();
    let content_hash = ContentHash::new(Digest::sha256([hash_byte; 32]));
    insert_pool_config_seed(
        pool,
        PoolConfigSeed {
            template_name: "seed",
            template_release_id: release_id,
            config_id,
            content_hash,
            target_ready_slots,
            refill_per_tick,
            max_warming_slots,
            namespace: "space",
            enabled: true,
            managed_by: "manual",
        },
    )
    .await?;
    Ok((release_id, config_id, content_hash))
}

async fn insert_k8s_sandbox(
    pool: &PgPool,
    content_hash: ContentHash,
    vcpu: Option<i32>,
    memory_mib: Option<i32>,
) -> TestResult {
    insert_k8s_sandbox_for_template(pool, "seed", content_hash, vcpu, memory_mib).await
}

async fn insert_k8s_sandbox_for_template(
    pool: &PgPool,
    template_name: &str,
    content_hash: ContentHash,
    vcpu: Option<i32>,
    memory_mib: Option<i32>,
) -> TestResult {
    let content_hash = content_hash.to_string();
    insert_sandbox_for_template(pool, template_name, "k8s", &content_hash, vcpu, memory_mib).await
}

async fn insert_sandbox_for_template(
    pool: &PgPool,
    template_name: &str,
    managed_by: &str,
    content_hash: &str,
    vcpu: Option<i32>,
    memory_mib: Option<i32>,
) -> TestResult {
    sqlx::query(
        "INSERT INTO sandboxes (
             id, user_id, org_id, managed_by, lifecycle, placement,
             template_id, content_hash, vcpu, memory_mib,
             network_policy_generation
         )
         VALUES (
             $1,
             '00000000-0000-0000-0000-000000000010',
             '00000000-0000-0000-0000-000000000001',
             $6,
             'started',
             'on_node',
             $5,
             $2,
             $3,
             $4,
             1
         )",
    )
    .bind(Uuid::now_v7())
    .bind(content_hash.to_string())
    .bind(vcpu)
    .bind(memory_mib)
    .bind(template_name)
    .bind(managed_by)
    .execute(pool)
    .await?;

    Ok(())
}

async fn insert_sandbox_with_lifecycle(
    pool: &PgPool,
    sandbox_id: SandboxId,
    lifecycle: SandboxLifecycle,
) -> TestResult {
    sqlx::query(
        "INSERT INTO sandboxes (
             id, user_id, org_id, managed_by, lifecycle, placement,
             network_policy_generation
         )
         VALUES (
             $1,
             '00000000-0000-0000-0000-000000000010',
             '00000000-0000-0000-0000-000000000001',
             'k8s',
             $2,
             'unplaced',
             1
         )",
    )
    .bind(sandbox_id)
    .bind(lifecycle.as_str())
    .execute(pool)
    .await?;

    Ok(())
}

/// Force a slot into `claimed` with a chosen claim age, bypassing the
/// ready/claim dance — the drain tests exercise the batch delete's
/// predicate, not the claim path.
async fn force_slot_claimed(pool: &PgPool, slot_id: Uuid, claimed_ago: &str) -> TestResult {
    force_slot_state(pool, slot_id, "claimed", claimed_ago).await
}

/// [`force_slot_claimed`] for the post-bind `bound` stamp.
async fn force_slot_bound(pool: &PgPool, slot_id: Uuid, claimed_ago: &str) -> TestResult {
    force_slot_state(pool, slot_id, "bound", claimed_ago).await
}

async fn force_slot_state(
    pool: &PgPool,
    slot_id: Uuid,
    state: &str,
    claimed_ago: &str,
) -> TestResult {
    sqlx::query(
        "UPDATE kubernetes_pool_slots
         SET state = $4,
             claim_id = $2,
             claimed_at = NOW() - $3::INTERVAL,
             updated_at = NOW()
         WHERE id = $1",
    )
    .bind(slot_id)
    .bind(Uuid::now_v7())
    .bind(claimed_ago)
    .bind(state)
    .execute(pool)
    .await?;

    Ok(())
}

/// Seeds a fresh "seed" template release with a pool config row (fixed at
/// 2 vCPU / 1024 MiB by `insert_pool_config`), returning the ids and hash
/// every `store_*` warming-slot test seeds against.
async fn seed_pool_config(pool: &PgPool, hash_byte: u8) -> TestResult<(Uuid, Uuid, ContentHash)> {
    let template_release_id = Uuid::now_v7();
    let config_id = Uuid::now_v7();
    let content_hash = ContentHash::new(Digest::sha256([hash_byte; 32]));
    insert_pool_config(pool, template_release_id, config_id, content_hash).await?;
    Ok((template_release_id, config_id, content_hash))
}

/// The 2 vCPU / 1024 MiB shape most warming-slot tests seed and claim
/// against; tests exercising a mismatched shape build their own instead.
fn default_shape() -> TestResult<(VcpuCount, MemorySize)> {
    Ok((VcpuCount::try_from_u32(2)?, MemorySize::mib(1024)?))
}

/// Builds a `NewPoolSlot` in the "space" namespace with a fresh
/// `reserved_sandbox_id`, the shape every `store_*` warming-slot test uses
/// unless it needs to assert on the reserved sandbox id later.
fn new_slot<'a>(
    config_id: Uuid,
    template_release_id: Uuid,
    template_name: &'a TemplateName,
    content_hash: ContentHash,
    vcpu: VcpuCount,
    memory: MemorySize,
    pod_name: &'a str,
) -> NewPoolSlot<'a> {
    NewPoolSlot {
        config_id,
        template_release_id,
        reserved_sandbox_id: SandboxId::new(),
        template_name,
        content_hash,
        vcpu,
        memory,
        pod_name,
        pod_namespace: "space",
    }
}

/// Inserts a warming slot and immediately marks it ready. Callers build the
/// slot via `new_slot(...)`, optionally overriding fields (e.g.
/// `reserved_sandbox_id`) with struct-update syntax.
async fn insert_ready_slot(
    fixture: &PostgresFixture,
    slot: NewPoolSlot<'_>,
    pod_ip: &str,
) -> TestResult<PoolSlot> {
    let slot = fixture.store.insert_warming_slot(slot).await?;
    fixture
        .store
        .mark_slot_ready(slot.id, "pod-uid", pod_ip, "worker-1")
        .await?;
    Ok(slot)
}

#[tokio::test(flavor = "current_thread")]
async fn autoscale_configs_prepares_new_release_without_observed_demand() -> TestResult {
    let fixture = postgres_fixture().await?;
    let (release_id, _content_hash) = seed_release(&fixture.pool, "seed", 0x45).await?;
    upsert_warm_pool_policy(&fixture.pool, "seed", 2, 10, 1).await?;

    fixture.store.autoscale_configs("sandboxes").await?;

    let config = sqlx::query_as::<_, (i32, i32, i32, String, bool, String)>(
        "SELECT target_ready_slots, refill_per_tick, max_warming_slots,
                namespace, enabled, managed_by
         FROM kubernetes_pool_configs
         WHERE template_release_id = $1",
    )
    .bind(release_id)
    .fetch_one(&fixture.pool)
    .await?;
    assert_eq!(
        config,
        (
            2,
            5,
            10,
            "sandboxes".to_string(),
            true,
            "autoscaler".to_string()
        )
    );

    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn autoscale_configs_materializes_warm_pool_disk_io_policy() -> TestResult {
    let fixture = postgres_fixture().await?;
    let (release_id, _content_hash) = seed_release(&fixture.pool, "custom-runtime", 0x46).await?;
    upsert_warm_pool_policy(&fixture.pool, "custom-runtime", 2, 10, 1).await?;
    sqlx::query(
        "UPDATE warm_pool_policies
         SET disk_mib = 32768,
             disk_io_max_operations_per_sec = 5000,
             disk_io_max_bandwidth_mib_per_sec = 200
         WHERE template_name = 'custom-runtime'",
    )
    .execute(&fixture.pool)
    .await?;

    fixture.store.autoscale_configs("sandboxes").await?;

    let limits = sqlx::query_as::<_, (Option<i32>, i64, i64)>(
        "SELECT disk_mib, disk_io_max_operations_per_sec,
                disk_io_max_bandwidth_mib_per_sec
         FROM kubernetes_pool_configs
         WHERE template_release_id = $1",
    )
    .bind(release_id)
    .fetch_one(&fixture.pool)
    .await?;
    assert_eq!(limits, (Some(32768), 5_000, 200));

    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn autoscale_configs_prepares_republished_exact_release_without_observed_demand() -> TestResult
{
    let fixture = postgres_fixture().await?;
    let (republished_release_id, republished_hash) =
        seed_release(&fixture.pool, "seed", 0x60).await?;
    let (previous_release_id, previous_hash) = seed_release(&fixture.pool, "seed", 0x61).await?;
    let (current_release_id, current_hash) = seed_release(&fixture.pool, "seed", 0x62).await?;
    sqlx::query(
        "UPDATE template_releases
         SET created_at = CASE id
             WHEN $1 THEN NOW() - INTERVAL '2 days'
             WHEN $2 THEN NOW() - INTERVAL '1 day'
             ELSE NOW()
         END
         WHERE id IN ($1, $2, $3)",
    )
    .bind(republished_release_id)
    .bind(previous_release_id)
    .bind(current_release_id)
    .execute(&fixture.pool)
    .await?;
    sqlx::query(
        "INSERT INTO template_release_tags (
             template_release_id, template_name, tag, created_at, modified_at
         ) VALUES
             ($1, 'seed', $2, NOW() - INTERVAL '2 days', NOW() - INTERVAL '2 days'),
             ($3, 'seed', $4, NOW() - INTERVAL '1 day', NOW() - INTERVAL '1 day'),
             ($5, 'seed', $6, NOW(), NOW())",
    )
    .bind(republished_release_id)
    .bind(republished_hash.to_string())
    .bind(previous_release_id)
    .bind(previous_hash.to_string())
    .bind(current_release_id)
    .bind(current_hash.to_string())
    .execute(&fixture.pool)
    .await?;
    upsert_warm_pool_policy(&fixture.pool, "seed", 1, 10, 2).await?;

    sqlx::query(
        "INSERT INTO kubernetes_pool_configs (
             template_release_id, vcpu, memory_mib, target_ready_slots,
             refill_per_tick, max_warming_slots, namespace, enabled, managed_by
         ) VALUES ($1, 2, 1024, 1, 5, 10, 'sandboxes', TRUE, 'autoscaler')",
    )
    .bind(previous_release_id)
    .execute(&fixture.pool)
    .await?;
    fixture.store.autoscale_configs("sandboxes").await?;
    let previous_config_id = sqlx::query_scalar::<_, Uuid>(
        "SELECT id
         FROM kubernetes_pool_configs
         WHERE template_release_id = $1",
    )
    .bind(previous_release_id)
    .fetch_one(&fixture.pool)
    .await?;
    let template_name = TemplateName::parse("seed")?;
    let (vcpu, memory) = default_shape()?;
    let previous_slot = fixture
        .store
        .insert_warming_slot(NewPoolSlot {
            pod_namespace: "sandboxes",
            ..new_slot(
                previous_config_id,
                previous_release_id,
                &template_name,
                previous_hash,
                vcpu,
                memory,
                "space-sbx-previous-release-g1",
            )
        })
        .await?;
    fixture
        .store
        .mark_slot_ready(previous_slot.id, "pod-uid", "10.90.0.30", "worker-1")
        .await?;

    sqlx::query(
        "UPDATE template_release_tags
         SET modified_at = NOW()
         WHERE template_release_id = $1
           AND tag = $2",
    )
    .bind(republished_release_id)
    .bind(republished_hash.to_string())
    .execute(&fixture.pool)
    .await?;
    fixture.store.autoscale_configs("sandboxes").await?;

    let configs = sqlx::query_as::<_, (Uuid, i32, bool, String)>(
        "SELECT template_release_id, target_ready_slots, enabled, managed_by
         FROM kubernetes_pool_configs
         ORDER BY template_release_id",
    )
    .fetch_all(&fixture.pool)
    .await?;
    assert_eq!(configs.len(), 3);
    assert!(configs.contains(&(republished_release_id, 1, true, "autoscaler".to_string(),)));
    assert!(configs.contains(&(previous_release_id, 0, false, "autoscaler".to_string(),)));
    assert!(configs.contains(&(current_release_id, 1, true, "autoscaler".to_string(),)));

    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn autoscale_configs_uses_policy_refill_and_warming_slots() -> TestResult {
    let fixture = postgres_fixture().await?;
    let (release_id, _content_hash) = seed_release(&fixture.pool, "seed", 0x46).await?;
    upsert_warm_pool_policy_with_pacing(&fixture.pool, "seed", 5, 100, 20, 40, 1).await?;

    fixture.store.autoscale_configs("sandboxes").await?;

    let config = sqlx::query_as::<_, (i32, i32, i32)>(
        "SELECT target_ready_slots, refill_per_tick, max_warming_slots
         FROM kubernetes_pool_configs
         WHERE template_release_id = $1",
    )
    .bind(release_id)
    .fetch_one(&fixture.pool)
    .await?;

    assert_eq!(config, (5, 20, 40));

    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn autoscale_ignores_templates_without_policy() -> TestResult {
    let fixture = postgres_fixture().await?;
    let (release_id, config_id, content_hash) =
        seed_manual_pool_config(&fixture.pool, 0x40, (2, 1, 2)).await?;

    for _ in 0..10 {
        insert_k8s_sandbox(&fixture.pool, content_hash, None, None).await?;
    }

    let before = sqlx::query_as::<_, (i32, i32, i32, String, bool, String)>(
        "SELECT target_ready_slots, refill_per_tick, max_warming_slots,
                namespace, enabled, managed_by
         FROM kubernetes_pool_configs
         WHERE id = $1",
    )
    .bind(config_id)
    .fetch_one(&fixture.pool)
    .await?;
    fixture.store.autoscale_configs("sandboxes").await?;

    let config_count = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*)::BIGINT
         FROM kubernetes_pool_configs
         WHERE template_release_id = $1",
    )
    .bind(release_id)
    .fetch_one(&fixture.pool)
    .await?;
    let after = sqlx::query_as::<_, (i32, i32, i32, String, bool, String)>(
        "SELECT target_ready_slots, refill_per_tick, max_warming_slots,
                namespace, enabled, managed_by
         FROM kubernetes_pool_configs
         WHERE id = $1",
    )
    .bind(config_id)
    .fetch_one(&fixture.pool)
    .await?;

    assert_eq!(config_count, 1);
    assert_eq!(
        before,
        (2, 1, 2, "space".to_string(), true, "manual".to_string())
    );
    assert_eq!(after, before);

    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn autoscale_uses_per_template_min_and_max_when_set() -> TestResult {
    let fixture = postgres_fixture().await?;
    let (release_id, content_hash) = seed_release(&fixture.pool, "seed", 0x41).await?;
    upsert_warm_pool_policy(&fixture.pool, "seed", 5, 8, 1).await?;

    for _ in 0..3 {
        insert_k8s_sandbox(&fixture.pool, content_hash, None, None).await?;
    }

    fixture.store.autoscale_configs("sandboxes").await?;

    let target_ready_slots = sqlx::query_scalar::<_, i32>(
        "SELECT target_ready_slots
         FROM kubernetes_pool_configs
         WHERE template_release_id = $1",
    )
    .bind(release_id)
    .fetch_one(&fixture.pool)
    .await?;
    assert_eq!(target_ready_slots, 5);

    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn autoscale_prepares_multiple_retained_releases_without_demand() -> TestResult {
    let fixture = postgres_fixture().await?;
    seed_release(&fixture.pool, "seed", 0x42).await?;
    seed_release(&fixture.pool, "seed", 0x43).await?;
    upsert_warm_pool_policy(&fixture.pool, "seed", 4, 7, 2).await?;

    fixture.store.autoscale_configs("sandboxes").await?;

    let targets = sqlx::query_scalar::<_, i32>(
        "SELECT target_ready_slots
         FROM kubernetes_pool_configs
         ORDER BY target_ready_slots",
    )
    .fetch_all(&fixture.pool)
    .await?;
    assert_eq!(targets, vec![4, 4]);

    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn autoscale_keeps_unretained_release_with_recent_demand() -> TestResult {
    // arrange
    let fixture = postgres_fixture().await?;
    let (old_release_id, old_hash) = seed_release(&fixture.pool, "seed", 0x44).await?;
    seed_release(&fixture.pool, "seed", 0x45).await?;
    upsert_warm_pool_policy(&fixture.pool, "seed", 2, 10, 1).await?;
    for _ in 0..10 {
        insert_k8s_sandbox(&fixture.pool, old_hash, None, None).await?;
    }
    // act
    fixture.store.autoscale_configs("sandboxes").await?;

    // assert
    let old_target = sqlx::query_scalar::<_, i32>(
        "SELECT target_ready_slots
         FROM kubernetes_pool_configs
         WHERE template_release_id = $1",
    )
    .bind(old_release_id)
    .fetch_optional(&fixture.pool)
    .await?;
    assert_eq!(old_target, Some(2));

    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn autoscale_requires_sustained_demand_for_unretained_release() -> TestResult {
    let fixture = postgres_fixture().await?;
    let (old_release_id, old_hash) = seed_release(&fixture.pool, "seed", 0x65).await?;
    seed_release(&fixture.pool, "seed", 0x66).await?;
    upsert_warm_pool_policy(&fixture.pool, "seed", 2, 10, 1).await?;

    insert_k8s_sandbox(&fixture.pool, old_hash, None, None).await?;
    fixture.store.autoscale_configs("sandboxes").await?;
    let config_below_threshold = sqlx::query_scalar::<_, Uuid>(
        "SELECT id
         FROM kubernetes_pool_configs
         WHERE template_release_id = $1",
    )
    .bind(old_release_id)
    .fetch_optional(&fixture.pool)
    .await?;

    for _ in 1..10 {
        insert_k8s_sandbox(&fixture.pool, old_hash, None, None).await?;
    }
    fixture.store.autoscale_configs("sandboxes").await?;
    let target_at_threshold = sqlx::query_scalar::<_, i32>(
        "SELECT target_ready_slots
         FROM kubernetes_pool_configs
         WHERE template_release_id = $1",
    )
    .bind(old_release_id)
    .fetch_one(&fixture.pool)
    .await?;

    assert_eq!(config_below_threshold, None);
    assert_eq!(target_at_threshold, 2);

    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn autoscale_ignores_unretained_demand_for_unsupported_shape() -> TestResult {
    let fixture = postgres_fixture().await?;
    let (old_release_id, old_hash) = seed_release(&fixture.pool, "seed", 0x6c).await?;
    seed_release(&fixture.pool, "seed", 0x6d).await?;
    upsert_warm_pool_policy(&fixture.pool, "seed", 2, 10, 1).await?;
    for _ in 0..10 {
        insert_k8s_sandbox(&fixture.pool, old_hash, Some(4), Some(2048)).await?;
    }

    fixture.store.autoscale_configs("sandboxes").await?;

    let old_config = sqlx::query_scalar::<_, Uuid>(
        "SELECT id
         FROM kubernetes_pool_configs
         WHERE template_release_id = $1",
    )
    .bind(old_release_id)
    .fetch_optional(&fixture.pool)
    .await?;
    assert_eq!(old_config, None);

    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn autoscale_bounds_demand_selected_releases_per_policy() -> TestResult {
    let fixture = postgres_fixture().await?;
    let (first_demanded_id, first_demanded_hash) =
        seed_release(&fixture.pool, "seed", 0x67).await?;
    let (second_demanded_id, second_demanded_hash) =
        seed_release(&fixture.pool, "seed", 0x68).await?;
    let (retained_id, _retained_hash) = seed_release(&fixture.pool, "seed", 0x69).await?;
    upsert_warm_pool_policy(&fixture.pool, "seed", 1, 10, 1).await?;
    for _ in 0..20 {
        insert_k8s_sandbox(&fixture.pool, first_demanded_hash, None, None).await?;
    }
    for _ in 0..10 {
        insert_k8s_sandbox(&fixture.pool, second_demanded_hash, None, None).await?;
    }

    fixture.store.autoscale_configs("sandboxes").await?;

    let selected_releases = sqlx::query_scalar::<_, Uuid>(
        "SELECT template_release_id
         FROM kubernetes_pool_configs
         ORDER BY template_release_id",
    )
    .fetch_all(&fixture.pool)
    .await?;
    assert_eq!(selected_releases.len(), 2);
    assert!(selected_releases.contains(&retained_id));
    assert!(selected_releases.contains(&first_demanded_id));
    assert!(!selected_releases.contains(&second_demanded_id));

    let first_config_id = sqlx::query_scalar::<_, Uuid>(
        "SELECT id
         FROM kubernetes_pool_configs
         WHERE template_release_id = $1",
    )
    .bind(first_demanded_id)
    .fetch_one(&fixture.pool)
    .await?;
    let template_name = TemplateName::parse("seed")?;
    let (vcpu, memory) = default_shape()?;
    fixture
        .store
        .insert_warming_slot(NewPoolSlot {
            pod_namespace: "sandboxes",
            ..new_slot(
                first_config_id,
                first_demanded_id,
                &template_name,
                first_demanded_hash,
                vcpu,
                memory,
                "space-sbx-demand-cap-draining-g1",
            )
        })
        .await?;

    for _ in 0..11 {
        insert_k8s_sandbox(&fixture.pool, second_demanded_hash, None, None).await?;
    }
    fixture.store.autoscale_configs("sandboxes").await?;
    let configs_after_demand_shift = sqlx::query_as::<_, (Uuid, bool, i32)>(
        "SELECT template_release_id, enabled, target_ready_slots
         FROM kubernetes_pool_configs
         WHERE template_release_id IN ($1, $2)
         ORDER BY template_release_id",
    )
    .bind(first_demanded_id)
    .bind(second_demanded_id)
    .fetch_all(&fixture.pool)
    .await?;
    assert!(configs_after_demand_shift.contains(&(first_demanded_id, false, 0)));
    assert!(
        configs_after_demand_shift
            .iter()
            .any(|config| config.0 == second_demanded_id && config.1)
    );

    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn autoscale_counts_fallthrough_space_sandboxes_as_recent_demand() -> TestResult {
    // arrange
    let fixture = postgres_fixture().await?;
    let (old_release_id, old_hash) = seed_release(&fixture.pool, "seed", 0x56).await?;
    seed_release(&fixture.pool, "seed", 0x57).await?;
    upsert_warm_pool_policy(&fixture.pool, "seed", 2, 10, 1).await?;
    for _ in 0..10 {
        insert_sandbox_for_template(
            &fixture.pool,
            "seed",
            "space",
            &old_hash.to_string(),
            None,
            None,
        )
        .await?;
    }

    // act
    fixture.store.autoscale_configs("sandboxes").await?;

    // assert
    let old_target = sqlx::query_scalar::<_, i32>(
        "SELECT target_ready_slots
         FROM kubernetes_pool_configs
         WHERE template_release_id = $1",
    )
    .bind(old_release_id)
    .fetch_optional(&fixture.pool)
    .await?;
    assert_eq!(old_target, Some(2));

    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn autoscale_matches_prefixed_sandbox_demand_to_bare_release_hash() -> TestResult {
    // arrange
    let fixture = postgres_fixture().await?;
    let old_release_id = Uuid::now_v7();
    let old_hash = ContentHash::new(Digest::sha256([0x58; 32]));
    let old_hash_prefixed = old_hash.to_string();
    let old_hash_bare = old_hash_prefixed
        .strip_prefix("sha256:")
        .expect("test hash should use sha256");
    insert_template_release_with_hash(&fixture.pool, "seed", old_release_id, old_hash_bare).await?;
    seed_release(&fixture.pool, "seed", 0x59).await?;
    upsert_warm_pool_policy(&fixture.pool, "seed", 2, 10, 1).await?;
    for _ in 0..10 {
        insert_sandbox_for_template(
            &fixture.pool,
            "seed",
            "space",
            &old_hash_prefixed,
            None,
            None,
        )
        .await?;
    }

    // act
    fixture.store.autoscale_configs("sandboxes").await?;

    // assert
    let old_target = sqlx::query_scalar::<_, i32>(
        "SELECT target_ready_slots
         FROM kubernetes_pool_configs
         WHERE template_release_id = $1",
    )
    .bind(old_release_id)
    .fetch_optional(&fixture.pool)
    .await?;
    let configs = fixture.store.reconcilable_configs().await?;
    let old_config = configs
        .iter()
        .find(|config| config.template_release_id == old_release_id)
        .expect("demanded bare-hash release should be reconcilable");

    assert_eq!(old_target, Some(2));
    assert_eq!(old_config.content_hash.to_string(), old_hash_prefixed);

    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn autoscale_deletes_empty_policy_owned_pool_when_policy_is_removed() -> TestResult {
    let fixture = postgres_fixture().await?;
    let release_id = Uuid::now_v7();
    let config_id = Uuid::now_v7();
    let content_hash = ContentHash::new(Digest::sha256([0x43; 32]));
    insert_pool_config_seed(
        &fixture.pool,
        PoolConfigSeed {
            template_name: "seed",
            template_release_id: release_id,
            config_id,
            content_hash,
            target_ready_slots: 4,
            refill_per_tick: 2,
            max_warming_slots: 4,
            namespace: "sandboxes",
            enabled: true,
            managed_by: "autoscaler",
        },
    )
    .await?;

    let retired_configs = fixture.store.autoscale_configs("sandboxes").await?;

    let config_count = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*)::BIGINT
         FROM kubernetes_pool_configs
         WHERE id = $1",
    )
    .bind(config_id)
    .fetch_one(&fixture.pool)
    .await?;
    assert_eq!(config_count, 0);
    assert_eq!(retired_configs.len(), 1);
    assert_eq!(retired_configs[0].id, config_id);
    assert!(!retired_configs[0].enabled);
    assert_eq!(retired_configs[0].target_ready_slots, 0);
    assert_eq!(retired_configs[0].managed_by, PoolConfigOwner::Autoscaler);

    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn recently_disabled_configs_only_returns_configs_within_grace_period() -> TestResult {
    let fixture = postgres_fixture().await?;
    let recent_id = Uuid::now_v7();
    let old_id = Uuid::now_v7();
    for (config_id, hash_byte) in [(recent_id, 0x61), (old_id, 0x62)] {
        insert_pool_config_seed(
            &fixture.pool,
            PoolConfigSeed {
                template_name: "seed",
                template_release_id: Uuid::now_v7(),
                config_id,
                content_hash: ContentHash::new(Digest::sha256([hash_byte; 32])),
                target_ready_slots: 0,
                refill_per_tick: 1,
                max_warming_slots: 1,
                namespace: "sandboxes",
                enabled: false,
                managed_by: "manual",
            },
        )
        .await?;
    }
    sqlx::query(
        "UPDATE kubernetes_pool_configs
         SET updated_at = NOW() - INTERVAL '1 hour'
         WHERE id = $1",
    )
    .bind(old_id)
    .execute(&fixture.pool)
    .await?;

    let configs = fixture
        .store
        .recently_disabled_configs(Duration::from_secs(10 * 60))
        .await?;

    assert_eq!(configs.len(), 1);
    assert_eq!(configs[0].id, recent_id);
    assert!(!configs[0].enabled);
    assert_eq!(configs[0].managed_by, PoolConfigOwner::Manual);

    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn autoscale_keeps_retired_config_until_open_slots_are_drained() -> TestResult {
    // arrange
    let fixture = postgres_fixture().await?;
    let release_id = Uuid::now_v7();
    let config_id = Uuid::now_v7();
    let template = TemplateName::parse("seed")?;
    let content_hash = ContentHash::new(Digest::sha256([0x54; 32]));
    let (vcpu, memory) = default_shape()?;
    insert_pool_config_seed(
        &fixture.pool,
        PoolConfigSeed {
            template_name: "seed",
            template_release_id: release_id,
            config_id,
            content_hash,
            target_ready_slots: 1,
            refill_per_tick: 1,
            max_warming_slots: 1,
            namespace: "sandboxes",
            enabled: true,
            managed_by: "autoscaler",
        },
    )
    .await?;
    let slot = fixture
        .store
        .insert_warming_slot(NewPoolSlot {
            pod_namespace: "sandboxes",
            ..new_slot(
                config_id,
                release_id,
                &template,
                content_hash,
                vcpu,
                memory,
                "space-sbx-retired-draining-g1",
            )
        })
        .await?;
    // act
    fixture.store.autoscale_configs("sandboxes").await?;
    let config_enabled = sqlx::query_scalar::<_, bool>(
        "SELECT enabled
         FROM kubernetes_pool_configs
         WHERE id = $1",
    )
    .bind(config_id)
    .fetch_one(&fixture.pool)
    .await?;
    fixture.store.delete_open_slot(slot.id).await?;
    fixture.store.autoscale_configs("sandboxes").await?;

    // assert
    assert!(!config_enabled);
    let config_count = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*)::BIGINT
         FROM kubernetes_pool_configs
         WHERE id = $1",
    )
    .bind(config_id)
    .fetch_one(&fixture.pool)
    .await?;
    assert_eq!(config_count, 0);

    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn autoscale_disables_all_autoscaler_configs_when_all_policies_are_disabled() -> TestResult {
    // arrange
    let fixture = postgres_fixture().await?;
    let seed_release_id = Uuid::now_v7();
    let app_release_id = Uuid::now_v7();
    let seed_config_id = Uuid::now_v7();
    let app_config_id = Uuid::now_v7();
    let seed_template = TemplateName::parse("seed")?;
    let app_template = TemplateName::parse("app-runtime")?;
    let seed_hash = ContentHash::new(Digest::sha256([0x51; 32]));
    let app_hash = ContentHash::new(Digest::sha256([0x52; 32]));
    let (vcpu, memory) = default_shape()?;
    for (template_name, release_id, config_id, content_hash) in [
        ("seed", seed_release_id, seed_config_id, seed_hash),
        ("app-runtime", app_release_id, app_config_id, app_hash),
    ] {
        insert_pool_config_seed(
            &fixture.pool,
            autoscaler_seed(
                template_name,
                release_id,
                config_id,
                content_hash,
                (2, 1, 2),
            ),
        )
        .await?;
    }
    for (template, release_id, config_id, content_hash, pod_name) in [
        (
            &seed_template,
            seed_release_id,
            seed_config_id,
            seed_hash,
            "space-sbx-seed-disabled-policy-g1",
        ),
        (
            &app_template,
            app_release_id,
            app_config_id,
            app_hash,
            "space-sbx-app-disabled-policy-g1",
        ),
    ] {
        fixture
            .store
            .insert_warming_slot(NewPoolSlot {
                pod_namespace: "sandboxes",
                ..new_slot(
                    config_id,
                    release_id,
                    template,
                    content_hash,
                    vcpu,
                    memory,
                    pod_name,
                )
            })
            .await?;
    }
    upsert_warm_pool_policy(&fixture.pool, "seed", 1, 10, 1).await?;
    upsert_warm_pool_policy(&fixture.pool, "app-runtime", 1, 10, 1).await?;
    sqlx::query("UPDATE warm_pool_policies SET enabled = FALSE")
        .execute(&fixture.pool)
        .await?;
    // act
    fixture.store.autoscale_configs("sandboxes").await?;

    // assert
    let configs = sqlx::query_as::<_, (bool, i32)>(
        "SELECT enabled, target_ready_slots
         FROM kubernetes_pool_configs
         ORDER BY id",
    )
    .fetch_all(&fixture.pool)
    .await?;
    assert_eq!(configs, vec![(false, 0), (false, 0)]);

    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn autoscale_drains_old_shape_when_policy_shape_changes() -> TestResult {
    // arrange
    let fixture = postgres_fixture().await?;
    let template = TemplateName::parse("seed")?;
    let (release_id, content_hash) = seed_release(&fixture.pool, "seed", 0x53).await?;
    upsert_warm_pool_policy(&fixture.pool, "seed", 2, 10, 1).await?;
    fixture.store.autoscale_configs("sandboxes").await?;
    let old_config_id = sqlx::query_scalar::<_, Uuid>(
        "SELECT id
         FROM kubernetes_pool_configs
         WHERE template_release_id = $1
           AND vcpu = 2
           AND memory_mib = 1024",
    )
    .bind(release_id)
    .fetch_one(&fixture.pool)
    .await?;
    let (vcpu, memory) = default_shape()?;
    fixture
        .store
        .insert_warming_slot(NewPoolSlot {
            pod_namespace: "sandboxes",
            ..new_slot(
                old_config_id,
                release_id,
                &template,
                content_hash,
                vcpu,
                memory,
                "space-sbx-old-shape-g1",
            )
        })
        .await?;
    sqlx::query(
        "UPDATE warm_pool_policies
         SET vcpu = 4,
             memory_mib = 2048,
             updated_at = NOW()
         WHERE template_name = 'seed'
           AND runtime = 'fc'
           AND vcpu = 2
           AND memory_mib = 1024",
    )
    .execute(&fixture.pool)
    .await?;

    // act
    fixture.store.autoscale_configs("sandboxes").await?;

    // assert
    let configs = sqlx::query_as::<_, (i32, i32, bool, i32)>(
        "SELECT vcpu, memory_mib, enabled, target_ready_slots
         FROM kubernetes_pool_configs
         WHERE template_release_id = $1
         ORDER BY vcpu",
    )
    .bind(release_id)
    .fetch_all(&fixture.pool)
    .await?;
    assert_eq!(configs, vec![(2, 1024, false, 0), (4, 2048, true, 2)]);

    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn warm_pool_policies_table_round_trip() -> TestResult {
    let fixture = postgres_fixture().await?;

    upsert_warm_pool_policy(&fixture.pool, "app-runtime", 5, 30, 2).await?;
    let inserted = sqlx::query_as::<_, (i32, i32, i32)>(
        "SELECT min_ready_slots, max_ready_slots, retain_releases
         FROM warm_pool_policies
         WHERE template_name = 'app-runtime'",
    )
    .fetch_one(&fixture.pool)
    .await?;
    assert_eq!(inserted, (5, 30, 2));

    upsert_warm_pool_policy(&fixture.pool, "app-runtime", 6, 24, 3).await?;
    let updated = sqlx::query_as::<_, (i32, i32, i32)>(
        "SELECT min_ready_slots, max_ready_slots, retain_releases
         FROM warm_pool_policies
         WHERE template_name = 'app-runtime'",
    )
    .fetch_one(&fixture.pool)
    .await?;
    assert_eq!(updated, (6, 24, 3));

    let negative_min_result =
        upsert_warm_pool_policy(&fixture.pool, "invalid-negative-min", -1, 24, 1).await;
    assert!(negative_min_result.is_err());

    let negative_max_result =
        upsert_warm_pool_policy(&fixture.pool, "invalid-negative-max", 1, -1, 1).await;
    assert!(negative_max_result.is_err());

    let inverted_bounds_result =
        upsert_warm_pool_policy(&fixture.pool, "invalid-inverted", 8, 4, 1).await;
    assert!(inverted_bounds_result.is_err());

    let invalid_retention_result =
        upsert_warm_pool_policy(&fixture.pool, "invalid-retention", 1, 4, 0).await;
    assert!(invalid_retention_result.is_err());

    let invalid_runtime_result = sqlx::query(
        "INSERT INTO warm_pool_policies (
             template_name, runtime, vcpu, memory_mib, min_ready_slots,
             max_ready_slots, retain_releases
         )
         VALUES ('invalid-runtime', 'unknown', 2, 1024, 1, 4, 1)",
    )
    .execute(&fixture.pool)
    .await;
    invalid_runtime_result.unwrap_err();

    sqlx::query("DELETE FROM warm_pool_policies WHERE template_name = 'app-runtime'")
        .execute(&fixture.pool)
        .await?;
    let count = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*)::BIGINT
         FROM warm_pool_policies
         WHERE template_name = 'app-runtime'",
    )
    .fetch_one(&fixture.pool)
    .await?;
    assert_eq!(count, 0);

    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn autoscale_configs_grows_target_when_traffic_increases() -> TestResult {
    let fixture = postgres_fixture().await?;
    let release_id = Uuid::now_v7();
    let config_id = Uuid::now_v7();
    let content_hash = ContentHash::new(Digest::sha256([0x46; 32]));
    insert_pool_config_seed(
        &fixture.pool,
        PoolConfigSeed {
            template_name: "seed",
            template_release_id: release_id,
            config_id,
            content_hash,
            target_ready_slots: 1,
            refill_per_tick: 2,
            max_warming_slots: 4,
            namespace: "sandboxes",
            enabled: true,
            managed_by: "autoscaler",
        },
    )
    .await?;
    upsert_warm_pool_policy(&fixture.pool, "seed", 1, 10, 1).await?;

    for _ in 0..41 {
        insert_k8s_sandbox(&fixture.pool, content_hash, None, None).await?;
    }

    fixture.store.autoscale_configs("sandboxes").await?;

    let target_ready_slots = sqlx::query_scalar::<_, i32>(
        "SELECT target_ready_slots
         FROM kubernetes_pool_configs
         WHERE template_release_id = $1",
    )
    .bind(release_id)
    .fetch_one(&fixture.pool)
    .await?;

    assert_eq!(target_ready_slots, 5);

    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn autoscale_configs_shrinks_target_when_traffic_decreases_but_keeps_min() -> TestResult {
    let fixture = postgres_fixture().await?;
    let release_id = Uuid::now_v7();
    let config_id = Uuid::now_v7();
    let content_hash = ContentHash::new(Digest::sha256([0x47; 32]));
    insert_pool_config_seed(
        &fixture.pool,
        PoolConfigSeed {
            template_name: "seed",
            template_release_id: release_id,
            config_id,
            content_hash,
            target_ready_slots: 9,
            refill_per_tick: 2,
            max_warming_slots: 4,
            namespace: "sandboxes",
            enabled: true,
            managed_by: "autoscaler",
        },
    )
    .await?;
    upsert_warm_pool_policy(&fixture.pool, "seed", 3, 10, 1).await?;

    insert_k8s_sandbox(&fixture.pool, content_hash, None, None).await?;

    fixture.store.autoscale_configs("sandboxes").await?;

    let target_ready_slots = sqlx::query_scalar::<_, i32>(
        "SELECT target_ready_slots
         FROM kubernetes_pool_configs
         WHERE id = $1",
    )
    .bind(config_id)
    .fetch_one(&fixture.pool)
    .await?;

    assert_eq!(target_ready_slots, 3);

    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn autoscale_configs_deletes_empty_unretained_pool_after_lookback_with_no_traffic()
-> TestResult {
    let fixture = postgres_fixture().await?;
    let release_id = Uuid::now_v7();
    let config_id = Uuid::now_v7();
    let content_hash = ContentHash::new(Digest::sha256([0x48; 32]));
    insert_pool_config_seed(
        &fixture.pool,
        PoolConfigSeed {
            template_name: "seed",
            template_release_id: release_id,
            config_id,
            content_hash,
            target_ready_slots: 4,
            refill_per_tick: 2,
            max_warming_slots: 4,
            namespace: "sandboxes",
            enabled: true,
            managed_by: "autoscaler",
        },
    )
    .await?;
    seed_release(&fixture.pool, "seed", 0x58).await?;
    upsert_warm_pool_policy(&fixture.pool, "seed", 1, 10, 1).await?;

    fixture.store.autoscale_configs("sandboxes").await?;

    let config_count = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*)::BIGINT
         FROM kubernetes_pool_configs
         WHERE id = $1",
    )
    .bind(config_id)
    .fetch_one(&fixture.pool)
    .await?;

    assert_eq!(config_count, 0);

    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn autoscale_disables_demand_selected_pool_after_demand_expires() -> TestResult {
    let fixture = postgres_fixture().await?;
    let old_release_id = Uuid::now_v7();
    let old_config_id = Uuid::now_v7();
    let old_hash = ContentHash::new(Digest::sha256([0x6a; 32]));
    let template_name = TemplateName::parse("seed")?;
    insert_pool_config_seed(
        &fixture.pool,
        autoscaler_seed("seed", old_release_id, old_config_id, old_hash, (2, 1, 2)),
    )
    .await?;
    seed_release(&fixture.pool, "seed", 0x6b).await?;
    upsert_warm_pool_policy(&fixture.pool, "seed", 2, 10, 1).await?;
    for _ in 0..10 {
        insert_k8s_sandbox(&fixture.pool, old_hash, None, None).await?;
    }
    let (vcpu, memory) = default_shape()?;
    let slot = fixture
        .store
        .insert_warming_slot(NewPoolSlot {
            pod_namespace: "sandboxes",
            ..new_slot(
                old_config_id,
                old_release_id,
                &template_name,
                old_hash,
                vcpu,
                memory,
                "space-sbx-expired-demand-g1",
            )
        })
        .await?;

    fixture.store.autoscale_configs("sandboxes").await?;
    sqlx::query(
        "UPDATE sandboxes
         SET created_at = NOW() - INTERVAL '25 hours'
         WHERE template_id = 'seed'
           AND content_hash = $1",
    )
    .bind(old_hash.to_string())
    .execute(&fixture.pool)
    .await?;
    fixture.store.autoscale_configs("sandboxes").await?;

    let retired = sqlx::query_as::<_, (bool, i32)>(
        "SELECT enabled, target_ready_slots
         FROM kubernetes_pool_configs
         WHERE id = $1",
    )
    .bind(old_config_id)
    .fetch_one(&fixture.pool)
    .await?;
    assert_eq!(retired, (false, 0));

    fixture.store.delete_open_slot(slot.id).await?;
    fixture.store.autoscale_configs("sandboxes").await?;
    let config_count = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*)::BIGINT
         FROM kubernetes_pool_configs
         WHERE id = $1",
    )
    .bind(old_config_id)
    .fetch_one(&fixture.pool)
    .await?;
    assert_eq!(config_count, 0);

    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn autoscale_configs_does_not_touch_manual_configs() -> TestResult {
    let fixture = postgres_fixture().await?;
    let (_release_id, config_id, content_hash) =
        seed_manual_pool_config(&fixture.pool, 0x49, (2, 1, 2)).await?;
    upsert_warm_pool_policy(&fixture.pool, "seed", 1, 10, 1).await?;

    for _ in 0..3 {
        insert_k8s_sandbox(&fixture.pool, content_hash, None, None).await?;
    }

    let before = sqlx::query_as::<_, (i32, i32, i32, String, bool, String)>(
        "SELECT target_ready_slots, refill_per_tick, max_warming_slots,
                namespace, enabled, managed_by
         FROM kubernetes_pool_configs
         WHERE id = $1",
    )
    .bind(config_id)
    .fetch_one(&fixture.pool)
    .await?;

    fixture.store.autoscale_configs("sandboxes").await?;

    let after = sqlx::query_as::<_, (i32, i32, i32, String, bool, String)>(
        "SELECT target_ready_slots, refill_per_tick, max_warming_slots,
                namespace, enabled, managed_by
         FROM kubernetes_pool_configs
         WHERE id = $1",
    )
    .bind(config_id)
    .fetch_one(&fixture.pool)
    .await?;

    assert_eq!(
        before,
        (2, 1, 2, "space".to_string(), true, "manual".to_string())
    );
    assert_eq!(after, before);

    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn store_claims_oldest_ready_slot_once() -> TestResult {
    let fixture = postgres_fixture().await?;
    let first_sandbox_id = SandboxId::new();
    let second_sandbox_id = SandboxId::new();
    let claim_id = Uuid::now_v7();
    let template = TemplateName::parse("seed")?;
    let (template_release_id, config_id, content_hash) =
        seed_pool_config(&fixture.pool, 0x11).await?;
    let (vcpu, memory) = default_shape()?;

    let first = fixture
        .store
        .insert_warming_slot(NewPoolSlot {
            reserved_sandbox_id: first_sandbox_id,
            ..new_slot(
                config_id,
                template_release_id,
                &template,
                content_hash,
                vcpu,
                memory,
                "space-sbx-first-g1",
            )
        })
        .await?;
    fixture
        .store
        .insert_warming_slot(NewPoolSlot {
            reserved_sandbox_id: second_sandbox_id,
            ..new_slot(
                config_id,
                template_release_id,
                &template,
                content_hash,
                vcpu,
                memory,
                "space-sbx-second-g1",
            )
        })
        .await?;
    sqlx::query(
        "UPDATE kubernetes_pool_slots
         SET state = 'ready',
             ready_at = CASE id WHEN $1 THEN NOW() - INTERVAL '1 second' ELSE NOW() END,
             pod_uid = 'pod-uid',
             pod_ip = '10.90.0.7',
             k8s_node_name = 'worker-1'",
    )
    .bind(first.id)
    .execute(&fixture.pool)
    .await?;

    let mut tx = fixture.pool.begin().await?;
    let claimed = WarmPoolStore::claim_ready_slot(
        &mut tx,
        template_release_id,
        vcpu,
        memory,
        "space",
        claim_id,
        false,
    )
    .await?
    .expect("ready slot");
    tx.commit().await?;

    assert_eq!(claimed.id, first.id);
    assert_eq!(claimed.reserved_sandbox_id, first_sandbox_id);
    assert_eq!(claimed.vcpu, vcpu);
    assert_eq!(claimed.memory, memory);
    assert_eq!(claimed.pod_ip.as_deref(), Some("10.90.0.7"));

    let states: Vec<(String, Option<Uuid>)> = sqlx::query_as(
        "SELECT state, claim_id
         FROM kubernetes_pool_slots
         ORDER BY ready_at ASC",
    )
    .fetch_all(&fixture.pool)
    .await?;
    assert_eq!(
        states,
        vec![
            ("claimed".to_string(), Some(claim_id)),
            ("ready".to_string(), None)
        ]
    );

    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn store_claims_exclude_oversized_disks_without_consuming_slots() -> TestResult {
    let fixture = postgres_fixture().await?;
    let template = TemplateName::parse("seed")?;
    let (release_id, config_id, content_hash) = seed_pool_config(&fixture.pool, 0x72).await?;
    let (vcpu, memory) = default_shape()?;
    let slot = insert_ready_slot(
        &fixture,
        new_slot(
            config_id,
            release_id,
            &template,
            content_hash,
            vcpu,
            memory,
            "disk-policy",
        ),
        "10.90.0.7",
    )
    .await?;
    for (disk_mib, allowed) in [
        (Some(131073_i32), false),
        (None, true),
        (Some(131072), true),
    ] {
        sqlx::query(pplx_space_common_core::sql!(
            "UPDATE kubernetes_pool_configs SET disk_mib = $2 WHERE id = $1"
        ))
        .bind(config_id)
        .bind(disk_mib)
        .execute(&fixture.pool)
        .await?;
        sqlx::query(pplx_space_common_core::sql!(
            "UPDATE kubernetes_pool_slots SET disk_mib = $2 WHERE id = $1"
        ))
        .bind(slot.id)
        .bind(disk_mib)
        .execute(&fixture.pool)
        .await?;
        for exact_shape in [true, false] {
            let mut tx = fixture.pool.begin().await?;
            let claim = if exact_shape {
                WarmPoolStore::claim_ready_slot(
                    &mut tx,
                    release_id,
                    vcpu,
                    memory,
                    "space",
                    Uuid::now_v7(),
                    false,
                )
                .await?
            } else {
                WarmPoolStore::claim_ready_slot_for_release(
                    &mut tx,
                    release_id,
                    "space",
                    Uuid::now_v7(),
                    false,
                )
                .await?
            };
            assert_eq!(
                claim.is_some(),
                allowed,
                "{disk_mib:?}, exact={exact_shape}"
            );
            let state: String = sqlx::query_scalar(pplx_space_common_core::sql!(
                "SELECT state FROM kubernetes_pool_slots WHERE id = $1"
            ))
            .bind(slot.id)
            .fetch_one(&mut *tx)
            .await?;
            assert_eq!(state, if allowed { "claimed" } else { "ready" });
            tx.rollback().await?;
        }
    }
    Ok(())
}

/// Claims partition in both directions: an encrypted claim never drains a
/// plaintext pool, a plaintext claim never drains an encryptable pool, and
/// each side sees only its own capacity.
#[tokio::test(flavor = "current_thread")]
async fn store_claim_routes_by_encryptable_partition() -> TestResult {
    let fixture = postgres_fixture().await?;
    let sandbox_id = SandboxId::new();
    let template = TemplateName::parse("seed")?;
    let (template_release_id, config_id, content_hash) =
        seed_pool_config(&fixture.pool, 0x13).await?;
    let (vcpu, memory) = default_shape()?;
    insert_ready_slot(
        &fixture,
        NewPoolSlot {
            reserved_sandbox_id: sandbox_id,
            ..new_slot(
                config_id,
                template_release_id,
                &template,
                content_hash,
                vcpu,
                memory,
                "space-sbx-encryptable-g1",
            )
        },
        "10.90.0.7",
    )
    .await?;

    // While the config is plaintext, an encrypted claim must not touch it.
    let mut tx = fixture.pool.begin().await?;
    let encrypted_claim_on_plaintext = WarmPoolStore::claim_ready_slot(
        &mut tx,
        template_release_id,
        vcpu,
        memory,
        "space",
        Uuid::now_v7(),
        true,
    )
    .await?;
    assert!(
        encrypted_claim_on_plaintext.is_none(),
        "encrypted claim must not drain a plaintext pool"
    );
    let encrypted_shape_on_plaintext = WarmPoolStore::preferred_enabled_shape_for_release(
        &mut tx,
        template_release_id,
        "space",
        None,
        true,
    )
    .await?;
    assert!(encrypted_shape_on_plaintext.is_none());
    tx.commit().await?;

    sqlx::query("UPDATE kubernetes_pool_configs SET encryptable = TRUE WHERE id = $1")
        .bind(config_id)
        .execute(&fixture.pool)
        .await?;

    let mut tx = fixture.pool.begin().await?;
    let plaintext_claim = WarmPoolStore::claim_ready_slot(
        &mut tx,
        template_release_id,
        vcpu,
        memory,
        "space",
        Uuid::now_v7(),
        false,
    )
    .await?;
    assert!(
        plaintext_claim.is_none(),
        "plaintext claim must not see the encryptable pool"
    );
    let plaintext_release_claim = WarmPoolStore::claim_ready_slot_for_release(
        &mut tx,
        template_release_id,
        "space",
        Uuid::now_v7(),
        false,
    )
    .await?;
    assert!(plaintext_release_claim.is_none());
    let plaintext_shape = WarmPoolStore::preferred_enabled_shape_for_release(
        &mut tx,
        template_release_id,
        "space",
        None,
        false,
    )
    .await?;
    assert!(
        plaintext_shape.is_none(),
        "shape resolution must respect the partition"
    );

    let encrypted_shape = WarmPoolStore::preferred_enabled_shape_for_release(
        &mut tx,
        template_release_id,
        "space",
        None,
        true,
    )
    .await?;
    assert!(encrypted_shape.is_some());
    let encrypted_claim = WarmPoolStore::claim_ready_slot(
        &mut tx,
        template_release_id,
        vcpu,
        memory,
        "space",
        Uuid::now_v7(),
        true,
    )
    .await?
    .expect("encrypted claim must draw from the encryptable pool");
    tx.commit().await?;
    assert_eq!(encrypted_claim.reserved_sandbox_id, sandbox_id);

    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn manual_pool_creation_uses_platform_limits_and_updates_preserve_assignments() -> TestResult
{
    let fixture = postgres_fixture().await?;
    let (release_id, _, _) = seed_pool_config(&fixture.pool, 0x22).await?;
    let spec = ManualPoolConfigSpec {
        vcpu: 8,
        memory_mib: 4096,
        disk_mib: None,
        reset_disk_to_template_default: false,
        target_ready_slots: 2,
        refill_per_tick: 1,
        max_warming_slots: 1,
        namespace: "space",
        enabled: true,
        encryptable: false,
    };
    let mut tx = fixture.pool.begin().await?;
    let id = WarmPoolStore::upsert_manual_config(&mut tx, release_id, &spec).await?;
    tx.commit().await?;
    let created = fixture.store.config_with_counts(id).await?.unwrap();
    let defaults = pplx_space_common_core::types::DiskIoLimits::default();
    assert_eq!(
        created.disk_io_max_operations_per_sec,
        i64::try_from(defaults.max_operations_per_sec())?
    );
    assert_eq!(
        created.disk_io_max_bandwidth_mib_per_sec,
        i64::try_from(defaults.max_bandwidth_mib_per_sec())?
    );

    sqlx::query(pplx_space_common_core::sql!(
        "UPDATE kubernetes_pool_configs
         SET disk_io_max_operations_per_sec = $2,
             disk_io_max_bandwidth_mib_per_sec = $3
         WHERE id = $1"
    ))
    .bind(id)
    .bind(7_000_i64)
    .bind(50_i64)
    .execute(&fixture.pool)
    .await?;
    let mut tx = fixture.pool.begin().await?;
    assert_eq!(
        WarmPoolStore::upsert_manual_config(&mut tx, release_id, &spec).await?,
        id
    );
    tx.commit().await?;
    let updated = fixture.store.config_with_counts(id).await?.unwrap();
    assert_eq!(
        (
            updated.disk_io_max_operations_per_sec,
            updated.disk_io_max_bandwidth_mib_per_sec
        ),
        (7_000, 50)
    );
    Ok(())
}

/// The store itself fences encryptable seeding while the legacy 3-column
/// index is live, so a direct store caller cannot persist a row the
/// autoscaler's partition-scoped upsert would collide with.
#[tokio::test(flavor = "current_thread")]
async fn store_manual_upsert_fences_encryptable_partition() -> TestResult {
    let fixture = postgres_fixture().await?;
    let (template_release_id, _config_id, _content_hash) =
        seed_pool_config(&fixture.pool, 0x21).await?;
    let spec = ManualPoolConfigSpec {
        vcpu: 8,
        memory_mib: 4096,
        disk_mib: None,
        reset_disk_to_template_default: false,
        target_ready_slots: 2,
        refill_per_tick: 1,
        max_warming_slots: 1,
        namespace: "space",
        enabled: false,
        encryptable: true,
    };

    let mut tx = fixture.pool.begin().await?;
    let err = WarmPoolStore::upsert_manual_config(&mut tx, template_release_id, &spec)
        .await
        .expect_err("encryptable specs must be fenced in the store");
    assert!(matches!(err, PoolError::EncryptableConfigFenced));
    tx.commit().await?;

    let rows: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM kubernetes_pool_configs WHERE encryptable")
            .fetch_one(&fixture.pool)
            .await?;
    assert_eq!(rows, 0, "no encryptable row may be persisted");

    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn store_claim_skips_slot_with_stale_disk_io_assignment() -> TestResult {
    let fixture = postgres_fixture().await?;
    let first_sandbox_id = SandboxId::new();
    let second_sandbox_id = SandboxId::new();
    let claim_id = Uuid::now_v7();
    let template = TemplateName::parse("seed")?;
    let (template_release_id, config_id, content_hash) =
        seed_pool_config(&fixture.pool, 0x11).await?;
    let (vcpu, memory) = default_shape()?;

    let first = fixture
        .store
        .insert_warming_slot(NewPoolSlot {
            reserved_sandbox_id: first_sandbox_id,
            ..new_slot(
                config_id,
                template_release_id,
                &template,
                content_hash,
                vcpu,
                memory,
                "space-sbx-first-g1",
            )
        })
        .await?;
    sqlx::query(
        "UPDATE kubernetes_pool_configs
         SET disk_io_max_operations_per_sec = 6000,
             disk_io_max_bandwidth_mib_per_sec = 300
         WHERE id = $1",
    )
    .bind(config_id)
    .execute(&fixture.pool)
    .await?;
    fixture
        .store
        .insert_warming_slot(NewPoolSlot {
            reserved_sandbox_id: second_sandbox_id,
            ..new_slot(
                config_id,
                template_release_id,
                &template,
                content_hash,
                vcpu,
                memory,
                "space-sbx-second-g1",
            )
        })
        .await?;
    sqlx::query(
        "UPDATE kubernetes_pool_slots
         SET state = 'ready',
             ready_at = CASE id WHEN $1 THEN NOW() - INTERVAL '1 second' ELSE NOW() END,
             pod_uid = 'pod-uid',
             pod_ip = '10.90.0.7',
             k8s_node_name = 'worker-1'",
    )
    .bind(first.id)
    .execute(&fixture.pool)
    .await?;

    let mut tx = fixture.pool.begin().await?;
    let claimed = WarmPoolStore::claim_ready_slot(
        &mut tx,
        template_release_id,
        vcpu,
        memory,
        "space",
        claim_id,
        false,
    )
    .await?
    .expect("ready slot");
    tx.commit().await?;

    assert_ne!(claimed.id, first.id);
    assert_eq!(claimed.reserved_sandbox_id, second_sandbox_id);
    assert_eq!(claimed.vcpu, vcpu);
    assert_eq!(claimed.memory, memory);
    assert_eq!(claimed.disk_io_limits.max_operations_per_sec(), 6_000);
    assert_eq!(claimed.disk_io_limits.max_bandwidth_mib_per_sec(), 300);
    assert_eq!(claimed.pod_ip.as_deref(), Some("10.90.0.7"));

    let states: Vec<(String, Option<Uuid>)> = sqlx::query_as(
        "SELECT state, claim_id
         FROM kubernetes_pool_slots
         ORDER BY ready_at ASC",
    )
    .fetch_all(&fixture.pool)
    .await?;
    assert_eq!(
        states,
        vec![
            ("ready".to_string(), None),
            ("claimed".to_string(), Some(claim_id))
        ]
    );

    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn store_claim_ready_slot_skips_ready_slot_without_pod_ip() -> TestResult {
    // arrange
    let fixture = postgres_fixture().await?;
    let template = TemplateName::parse("seed")?;
    let (template_release_id, config_id, content_hash) =
        seed_pool_config(&fixture.pool, 0x12).await?;
    let vcpu = VcpuCount::try_from_u32(4)?;
    let memory = MemorySize::mib(2048)?;
    let claim_id = Uuid::now_v7();
    let missing_ip = fixture
        .store
        .insert_warming_slot(new_slot(
            config_id,
            template_release_id,
            &template,
            content_hash,
            vcpu,
            memory,
            "space-sbx-missing-ip-g1",
        ))
        .await?;
    let ready = insert_ready_slot(
        &fixture,
        new_slot(
            config_id,
            template_release_id,
            &template,
            content_hash,
            vcpu,
            memory,
            "space-sbx-ready-g1",
        ),
        "10.90.0.10",
    )
    .await?;
    sqlx::query(
        "UPDATE kubernetes_pool_slots
         SET state = 'ready',
             ready_at = NOW() - INTERVAL '1 second',
             pod_uid = 'pod-uid',
             pod_ip = NULL,
             k8s_node_name = 'worker-1'
         WHERE id = $1",
    )
    .bind(missing_ip.id)
    .execute(&fixture.pool)
    .await?;

    // act
    let mut tx = fixture.pool.begin().await?;
    let claimed = WarmPoolStore::claim_ready_slot(
        &mut tx,
        template_release_id,
        vcpu,
        memory,
        "space",
        claim_id,
        false,
    )
    .await?
    .expect("ready slot with pod IP");
    tx.commit().await?;

    // assert
    assert_eq!(claimed.id, ready.id);
    assert_eq!(claimed.pod_ip.as_deref(), Some("10.90.0.10"));
    let missing_ip_state: (String, Option<Uuid>) = sqlx::query_as(
        "SELECT state, claim_id
         FROM kubernetes_pool_slots
         WHERE id = $1",
    )
    .bind(missing_ip.id)
    .fetch_one(&fixture.pool)
    .await?;
    assert_eq!(missing_ip_state, ("ready".to_string(), None));

    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn store_claim_ready_slot_skips_other_namespaces() -> TestResult {
    // arrange
    let fixture = postgres_fixture().await?;
    let template = TemplateName::parse("seed")?;
    let (template_release_id, config_id, content_hash) =
        seed_pool_config(&fixture.pool, 0x12).await?;
    let vcpu = VcpuCount::try_from_u32(4)?;
    let memory = MemorySize::mib(2048)?;
    let claim_id = Uuid::now_v7();
    let slot = insert_ready_slot(
        &fixture,
        new_slot(
            config_id,
            template_release_id,
            &template,
            content_hash,
            vcpu,
            memory,
            "space-sbx-wrong-namespace-g1",
        ),
        "10.90.0.10",
    )
    .await?;

    // act
    let mut tx = fixture.pool.begin().await?;
    let claimed = WarmPoolStore::claim_ready_slot(
        &mut tx,
        template_release_id,
        vcpu,
        memory,
        "sandboxes",
        claim_id,
        false,
    )
    .await?;
    tx.commit().await?;

    // assert
    assert!(claimed.is_none());
    let slot_state: (String, Option<Uuid>) = sqlx::query_as(
        "SELECT state, claim_id
         FROM kubernetes_pool_slots
         WHERE id = $1",
    )
    .bind(slot.id)
    .fetch_one(&fixture.pool)
    .await?;
    assert_eq!(slot_state, ("ready".to_string(), None));

    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn store_claim_ready_slot_for_release_skips_ready_slot_without_pod_ip() -> TestResult {
    // arrange
    let fixture = postgres_fixture().await?;
    let template = TemplateName::parse("seed")?;
    let (template_release_id, config_id, content_hash) =
        seed_pool_config(&fixture.pool, 0x12).await?;
    let vcpu = VcpuCount::try_from_u32(4)?;
    let memory = MemorySize::mib(2048)?;
    let claim_id = Uuid::now_v7();
    let missing_ip = fixture
        .store
        .insert_warming_slot(new_slot(
            config_id,
            template_release_id,
            &template,
            content_hash,
            vcpu,
            memory,
            "space-sbx-release-missing-ip-g1",
        ))
        .await?;
    let ready = insert_ready_slot(
        &fixture,
        new_slot(
            config_id,
            template_release_id,
            &template,
            content_hash,
            vcpu,
            memory,
            "space-sbx-release-ready-g1",
        ),
        "10.90.0.10",
    )
    .await?;
    sqlx::query(
        "UPDATE kubernetes_pool_slots
         SET state = 'ready', ready_at = NOW() - INTERVAL '1 second',
             pod_uid = 'pod-uid', pod_ip = NULL, k8s_node_name = 'worker-1'
         WHERE id = $1",
    )
    .bind(missing_ip.id)
    .execute(&fixture.pool)
    .await?;

    // act
    let mut tx = fixture.pool.begin().await?;
    let claimed = WarmPoolStore::claim_ready_slot_for_release(
        &mut tx,
        template_release_id,
        "space",
        claim_id,
        false,
    )
    .await?
    .expect("ready slot with pod IP");
    tx.commit().await?;

    // assert
    assert_eq!(claimed.id, ready.id);
    assert_eq!(claimed.pod_ip.as_deref(), Some("10.90.0.10"));
    let missing_ip_state: (String, Option<Uuid>) =
        sqlx::query_as("SELECT state, claim_id FROM kubernetes_pool_slots WHERE id = $1")
            .bind(missing_ip.id)
            .fetch_one(&fixture.pool)
            .await?;
    assert_eq!(missing_ip_state, ("ready".to_string(), None));

    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn store_mark_slot_ready_returns_typed_error_when_slot_not_warming() -> TestResult {
    // arrange
    let fixture = postgres_fixture().await?;
    let template = TemplateName::parse("seed")?;
    let (template_release_id, config_id, content_hash) =
        seed_pool_config(&fixture.pool, 0x22).await?;
    let (vcpu, memory) = default_shape()?;
    let slot = insert_ready_slot(
        &fixture,
        new_slot(
            config_id,
            template_release_id,
            &template,
            content_hash,
            vcpu,
            memory,
            "space-sbx-ready-g1",
        ),
        "10.90.0.8",
    )
    .await?;

    // act
    let err = fixture
        .store
        .mark_slot_ready(slot.id, "pod-uid", "10.90.0.8", "worker-1")
        .await
        .unwrap_err();

    // assert
    assert!(matches!(err, PoolError::SlotNotWarming { slot_id } if slot_id == slot.id));

    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn store_delete_open_slot_skips_claimed_slot() -> TestResult {
    // arrange
    let fixture = postgres_fixture().await?;
    let template = TemplateName::parse("seed")?;
    let (template_release_id, config_id, content_hash) =
        seed_pool_config(&fixture.pool, 0x44).await?;
    let (vcpu, memory) = default_shape()?;
    let claim_id = Uuid::now_v7();
    let slot = insert_ready_slot(
        &fixture,
        new_slot(
            config_id,
            template_release_id,
            &template,
            content_hash,
            vcpu,
            memory,
            "space-sbx-claimed-g1",
        ),
        "10.90.0.8",
    )
    .await?;
    let mut tx = fixture.pool.begin().await?;
    WarmPoolStore::claim_ready_slot(
        &mut tx,
        template_release_id,
        vcpu,
        memory,
        "space",
        claim_id,
        false,
    )
    .await?;
    tx.commit().await?;

    // act
    let deleted = fixture.store.delete_open_slot(slot.id).await?;

    // assert
    assert!(!deleted);
    let state: String = sqlx::query_scalar(
        "SELECT state
         FROM kubernetes_pool_slots
         WHERE id = $1",
    )
    .bind(slot.id)
    .fetch_one(&fixture.pool)
    .await?;
    assert_eq!(state, "claimed");

    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn store_stale_unbound_claimed_slots_can_be_locked_and_deleted() -> TestResult {
    // arrange
    let fixture = postgres_fixture().await?;
    let template = TemplateName::parse("seed")?;
    let (template_release_id, config_id, content_hash) =
        seed_pool_config(&fixture.pool, 0x45).await?;
    let (vcpu, memory) = default_shape()?;
    let claim_id = Uuid::now_v7();
    let slot = insert_ready_slot(
        &fixture,
        new_slot(
            config_id,
            template_release_id,
            &template,
            content_hash,
            vcpu,
            memory,
            "space-sbx-stale-claimed-g1",
        ),
        "10.90.0.9",
    )
    .await?;
    let mut claim_tx = fixture.pool.begin().await?;
    WarmPoolStore::claim_ready_slot(
        &mut claim_tx,
        template_release_id,
        vcpu,
        memory,
        "space",
        claim_id,
        false,
    )
    .await?;
    claim_tx.commit().await?;
    sqlx::query(
        "UPDATE kubernetes_pool_slots
         SET claimed_at = NOW() - INTERVAL '10 minutes'
         WHERE id = $1",
    )
    .bind(slot.id)
    .execute(&fixture.pool)
    .await?;

    // act
    let stale = fixture
        .store
        .stale_unbound_claimed_slots(Duration::from_secs(5 * 60), 100)
        .await?;

    // assert
    assert_eq!(stale.len(), 1);
    assert_eq!(stale[0].id, slot.id);
    assert_eq!(stale[0].state, PoolSlotState::Claimed);

    let mut delete_tx = fixture.pool.begin().await?;
    assert!(
        WarmPoolStore::lock_stale_unbound_claimed_slot_for_delete(
            &mut delete_tx,
            slot.id,
            Duration::from_secs(5 * 60),
        )
        .await?
    );
    assert!(WarmPoolStore::delete_locked_claimed_slot(&mut delete_tx, slot.id).await?);
    delete_tx.commit().await?;

    let exists: bool = sqlx::query_scalar(
        "SELECT EXISTS (
             SELECT 1
             FROM kubernetes_pool_slots
             WHERE id = $1
         )",
    )
    .bind(slot.id)
    .fetch_one(&fixture.pool)
    .await?;
    assert!(!exists);

    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn store_stale_claimed_slot_exists_tracks_staleness_and_bind_stamp() -> TestResult {
    // arrange
    let fixture = postgres_fixture().await?;
    let template = TemplateName::parse("seed")?;
    let (template_release_id, config_id, content_hash) =
        seed_pool_config(&fixture.pool, 0x47).await?;
    let (vcpu, memory) = default_shape()?;
    let claim_id = Uuid::now_v7();
    let timeout = Duration::from_secs(5 * 60);
    let slot = insert_ready_slot(
        &fixture,
        new_slot(
            config_id,
            template_release_id,
            &template,
            content_hash,
            vcpu,
            memory,
            "space-sbx-exists-check-g1",
        ),
        "10.90.0.10",
    )
    .await?;
    let mut claim_tx = fixture.pool.begin().await?;
    WarmPoolStore::claim_ready_slot(
        &mut claim_tx,
        template_release_id,
        vcpu,
        memory,
        "space",
        claim_id,
        false,
    )
    .await?;
    claim_tx.commit().await?;

    // act + assert: a fresh claim is not reapable
    assert!(
        !fixture
            .store
            .stale_unbound_claimed_slot_exists(slot.id, timeout)
            .await?
    );

    // act + assert: an aged, unbound claim is reapable
    sqlx::query(
        "UPDATE kubernetes_pool_slots
         SET claimed_at = NOW() - INTERVAL '10 minutes'
         WHERE id = $1",
    )
    .bind(slot.id)
    .execute(&fixture.pool)
    .await?;
    assert!(
        fixture
            .store
            .stale_unbound_claimed_slot_exists(slot.id, timeout)
            .await?
    );

    // act + assert: a `sandboxes` row owning the reserved id protects the
    // aged claim. A claimed row with a live sandbox is a pre-stamp legacy
    // receipt (or a bind from a rolled-back pre-stamp binary), never a dead
    // handoff: the bind transaction inserts the sandbox row and stamps the
    // slot atomically, so a handoff that died leaves no sandbox row. This
    // guard is what makes the state-only predicate safe on every
    // environment regardless of deploy timing or rollbacks.
    sqlx::query(
        "INSERT INTO sandboxes (
             id, user_id, org_id, managed_by, lifecycle, placement,
             template_id, content_hash, vcpu, memory_mib,
             network_policy_generation
         )
         VALUES (
             $1,
             '00000000-0000-0000-0000-000000000010',
             '00000000-0000-0000-0000-000000000001',
             'k8s',
             'started',
             'on_node',
             'seed',
             $2,
             2,
             1024,
             1
         )",
    )
    .bind(slot.reserved_sandbox_id)
    .bind(content_hash.to_string())
    .execute(&fixture.pool)
    .await?;
    assert!(
        !fixture
            .store
            .stale_unbound_claimed_slot_exists(slot.id, timeout)
            .await?
    );
    let batch = fixture
        .store
        .stale_unbound_claimed_slots(timeout, 10)
        .await?;
    assert!(batch.iter().all(|candidate| candidate.id != slot.id));

    // act + assert: the bind stamp is what makes the slot permanently
    // invisible to the sweep.
    let mut bind_tx = fixture.pool.begin().await?;
    assert!(
        WarmPoolStore::bind_claimed_slot(&mut bind_tx, claim_id, slot.reserved_sandbox_id).await?
    );
    bind_tx.commit().await?;
    assert!(
        !fixture
            .store
            .stale_unbound_claimed_slot_exists(slot.id, timeout)
            .await?
    );

    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn store_cleanup_pending_slots_can_be_locked_and_deleted() -> TestResult {
    // arrange
    let fixture = postgres_fixture().await?;
    let template = TemplateName::parse("seed")?;
    let (template_release_id, config_id, content_hash) =
        seed_pool_config(&fixture.pool, 0x46).await?;
    let (vcpu, memory) = default_shape()?;
    let claim_id = Uuid::now_v7();
    let reserved_sandbox_id = SandboxId::new();
    let slot = insert_ready_slot(
        &fixture,
        NewPoolSlot {
            reserved_sandbox_id,
            ..new_slot(
                config_id,
                template_release_id,
                &template,
                content_hash,
                vcpu,
                memory,
                "space-sbx-cleanup-pending-g1",
            )
        },
        "10.90.0.10",
    )
    .await?;
    let mut claim_tx = fixture.pool.begin().await?;
    WarmPoolStore::claim_ready_slot(
        &mut claim_tx,
        template_release_id,
        vcpu,
        memory,
        "space",
        claim_id,
        false,
    )
    .await?;
    assert!(
        WarmPoolStore::mark_claimed_slot_cleanup_pending(
            &mut claim_tx,
            claim_id,
            reserved_sandbox_id,
        )
        .await?
    );
    claim_tx.commit().await?;

    // act
    let cleanup_pending = fixture.store.cleanup_pending_slots(100).await?;

    // assert
    assert_eq!(cleanup_pending.len(), 1);
    assert_eq!(cleanup_pending[0].id, slot.id);
    assert_eq!(cleanup_pending[0].state, PoolSlotState::CleanupPending);

    let mut delete_tx = fixture.pool.begin().await?;
    assert!(WarmPoolStore::lock_cleanup_pending_slot_for_delete(&mut delete_tx, slot.id).await?);
    assert!(WarmPoolStore::delete_locked_cleanup_pending_slot(&mut delete_tx, slot.id).await?);
    delete_tx.commit().await?;

    let exists: bool = sqlx::query_scalar(
        "SELECT EXISTS (
             SELECT 1
             FROM kubernetes_pool_slots
             WHERE id = $1
         )",
    )
    .bind(slot.id)
    .fetch_one(&fixture.pool)
    .await?;
    assert!(!exists);

    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn store_delete_claimed_slot_for_retired_execution_deletes_claimed_slot() -> TestResult {
    // arrange
    let fixture = postgres_fixture().await?;
    let template = TemplateName::parse("seed")?;
    let (template_release_id, config_id, content_hash) =
        seed_pool_config(&fixture.pool, 0x47).await?;
    let (vcpu, memory) = default_shape()?;
    let claim_id = Uuid::now_v7();
    let reserved_sandbox_id = SandboxId::new();
    let slot = insert_ready_slot(
        &fixture,
        NewPoolSlot {
            reserved_sandbox_id,
            ..new_slot(
                config_id,
                template_release_id,
                &template,
                content_hash,
                vcpu,
                memory,
                "space-sbx-retired-claim-g1",
            )
        },
        "10.90.0.11",
    )
    .await?;
    let mut claim_tx = fixture.pool.begin().await?;
    WarmPoolStore::claim_ready_slot(
        &mut claim_tx,
        template_release_id,
        vcpu,
        memory,
        "space",
        claim_id,
        false,
    )
    .await?;
    claim_tx.commit().await?;

    // act
    let mut retire_tx = fixture.pool.begin().await?;
    let deleted = WarmPoolStore::delete_claimed_slot_for_retired_execution(
        &mut retire_tx,
        reserved_sandbox_id,
    )
    .await?;
    let unknown_deleted =
        WarmPoolStore::delete_claimed_slot_for_retired_execution(&mut retire_tx, SandboxId::new())
            .await?;
    retire_tx.commit().await?;

    // assert
    assert!(deleted);
    assert!(!unknown_deleted);
    let exists: bool = sqlx::query_scalar(
        "SELECT EXISTS (
             SELECT 1
             FROM kubernetes_pool_slots
             WHERE id = $1
         )",
    )
    .bind(slot.id)
    .fetch_one(&fixture.pool)
    .await?;
    assert!(!exists);

    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn store_delete_claimed_slot_for_retired_execution_skips_open_and_cleanup_pending_slots()
-> TestResult {
    // arrange: slot A ends up cleanup_pending, slot B stays ready. Neither
    // state may be touched by the retire-path delete: an open slot still
    // owns its pod, and a cleanup_pending slot is a durable work item.
    let fixture = postgres_fixture().await?;
    let template = TemplateName::parse("seed")?;
    let (template_release_id, config_id, content_hash) =
        seed_pool_config(&fixture.pool, 0x48).await?;
    let (vcpu, memory) = default_shape()?;
    let claim_id = Uuid::now_v7();
    let cleanup_pending_sandbox_id = SandboxId::new();
    let ready_sandbox_id = SandboxId::new();
    let cleanup_pending_slot = insert_ready_slot(
        &fixture,
        NewPoolSlot {
            reserved_sandbox_id: cleanup_pending_sandbox_id,
            ..new_slot(
                config_id,
                template_release_id,
                &template,
                content_hash,
                vcpu,
                memory,
                "space-sbx-retire-guard-g1",
            )
        },
        "10.90.0.12",
    )
    .await?;
    let mut claim_tx = fixture.pool.begin().await?;
    WarmPoolStore::claim_ready_slot(
        &mut claim_tx,
        template_release_id,
        vcpu,
        memory,
        "space",
        claim_id,
        false,
    )
    .await?;
    assert!(
        WarmPoolStore::mark_claimed_slot_cleanup_pending(
            &mut claim_tx,
            claim_id,
            cleanup_pending_sandbox_id,
        )
        .await?
    );
    claim_tx.commit().await?;
    let ready_slot = insert_ready_slot(
        &fixture,
        NewPoolSlot {
            reserved_sandbox_id: ready_sandbox_id,
            ..new_slot(
                config_id,
                template_release_id,
                &template,
                content_hash,
                vcpu,
                memory,
                "space-sbx-retire-guard-g2",
            )
        },
        "10.90.0.13",
    )
    .await?;

    // act
    let mut retire_tx = fixture.pool.begin().await?;
    let cleanup_pending_deleted = WarmPoolStore::delete_claimed_slot_for_retired_execution(
        &mut retire_tx,
        cleanup_pending_sandbox_id,
    )
    .await?;
    let ready_deleted =
        WarmPoolStore::delete_claimed_slot_for_retired_execution(&mut retire_tx, ready_sandbox_id)
            .await?;
    retire_tx.commit().await?;

    // assert
    assert!(!cleanup_pending_deleted);
    assert!(!ready_deleted);
    let remaining: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)
         FROM kubernetes_pool_slots
         WHERE id = ANY($1)",
    )
    .bind(vec![cleanup_pending_slot.id, ready_slot.id])
    .fetch_one(&fixture.pool)
    .await?;
    assert_eq!(remaining, 2);

    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn store_delete_claimed_slots_for_retired_executions_deletes_receipts_in_batch() -> TestResult
{
    // arrange: one bound receipt, one pre-stamp claimed receipt, and one
    // ready slot. The batch retire must delete exactly the two receipts;
    // the ready slot still owns its pod and an unknown id is a no-op.
    let fixture = postgres_fixture().await?;
    let template = TemplateName::parse("seed")?;
    let (template_release_id, config_id, content_hash) =
        seed_pool_config(&fixture.pool, 0x4a).await?;
    let (vcpu, memory) = default_shape()?;
    for (generation, pod_ip) in [(1, "10.90.0.14"), (2, "10.90.0.15"), (3, "10.90.0.16")] {
        insert_ready_slot(
            &fixture,
            new_slot(
                config_id,
                template_release_id,
                &template,
                content_hash,
                vcpu,
                memory,
                &format!("space-sbx-batch-retire-g{generation}"),
            ),
            pod_ip,
        )
        .await?;
    }
    let mut claim_tx = fixture.pool.begin().await?;
    let bound_claim_id = Uuid::now_v7();
    let bound_slot = WarmPoolStore::claim_ready_slot(
        &mut claim_tx,
        template_release_id,
        vcpu,
        memory,
        "space",
        bound_claim_id,
        false,
    )
    .await?
    .expect("bound slot claim must hit");
    assert!(
        WarmPoolStore::bind_claimed_slot(
            &mut claim_tx,
            bound_claim_id,
            bound_slot.reserved_sandbox_id
        )
        .await?
    );
    let claimed_slot = WarmPoolStore::claim_ready_slot(
        &mut claim_tx,
        template_release_id,
        vcpu,
        memory,
        "space",
        Uuid::now_v7(),
        false,
    )
    .await?
    .expect("claimed slot claim must hit");
    claim_tx.commit().await?;

    // act
    let mut conn = fixture.pool.acquire().await?;
    let no_ids_deleted =
        WarmPoolStore::delete_claimed_slots_for_retired_executions(&mut conn, &[]).await?;
    let deleted = WarmPoolStore::delete_claimed_slots_for_retired_executions(
        &mut conn,
        &[
            bound_slot.reserved_sandbox_id,
            claimed_slot.reserved_sandbox_id,
            SandboxId::new(),
        ],
    )
    .await?;
    drop(conn);

    // assert
    assert_eq!(no_ids_deleted, 0);
    assert_eq!(deleted, 2);
    let remaining_states: Vec<String> = sqlx::query_scalar(
        "SELECT state
         FROM kubernetes_pool_slots
         WHERE config_id = $1",
    )
    .bind(config_id)
    .fetch_all(&fixture.pool)
    .await?;
    assert_eq!(remaining_states, vec!["ready".to_string()]);

    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn store_dead_claimed_drain_deletes_only_suspended_sandbox_claims() -> TestResult {
    // arrange: three aged claims — suspended sandbox (drainable), started
    // sandbox (claimed pod still live), and no sandboxes row (potential
    // crash orphan owned by the stale-unbound sweep, which kills pods).
    let fixture = postgres_fixture().await?;
    let template = TemplateName::parse("seed")?;
    let (template_release_id, config_id, content_hash) =
        seed_pool_config(&fixture.pool, 0x49).await?;
    let (vcpu, memory) = default_shape()?;

    let mut slots = Vec::new();
    for suffix in ["g1", "g2", "g3"] {
        let slot = fixture
            .store
            .insert_warming_slot(new_slot(
                config_id,
                template_release_id,
                &template,
                content_hash,
                vcpu,
                memory,
                &format!("space-sbx-dead-drain-{suffix}"),
            ))
            .await?;
        force_slot_claimed(&fixture.pool, slot.id, "10 minutes").await?;
        slots.push(slot);
    }
    insert_sandbox_with_lifecycle(
        &fixture.pool,
        slots[0].reserved_sandbox_id,
        SandboxLifecycle::Suspended,
    )
    .await?;
    insert_sandbox_with_lifecycle(
        &fixture.pool,
        slots[1].reserved_sandbox_id,
        SandboxLifecycle::Started,
    )
    .await?;

    // act
    let drained = fixture
        .store
        .sweep_dead_claimed_slots_window(
            Duration::from_secs(5 * 60),
            Uuid::nil(),
            fixture.store.newest_slot_id().await?.expect("slots seeded"),
            10_000,
        )
        .await?;

    // assert
    assert_eq!(drained.deleted, 1);
    let remaining: Vec<Uuid> =
        sqlx::query_scalar("SELECT id FROM kubernetes_pool_slots WHERE id = ANY($1) ORDER BY id")
            .bind(slots.iter().map(|slot| slot.id).collect::<Vec<_>>())
            .fetch_all(&fixture.pool)
            .await?;
    let mut expected = vec![slots[1].id, slots[2].id];
    expected.sort();
    assert_eq!(remaining, expected);

    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn store_dead_claimed_drain_respects_claim_grace_and_limit() -> TestResult {
    // arrange: three suspended-sandbox claims — one inside the claim grace,
    // two aged past it.
    let fixture = postgres_fixture().await?;
    let template = TemplateName::parse("seed")?;
    let (template_release_id, config_id, content_hash) =
        seed_pool_config(&fixture.pool, 0x4a).await?;
    let (vcpu, memory) = default_shape()?;

    let mut slots = Vec::new();
    for (suffix, claimed_ago) in [
        ("g1", "1 second"),
        ("g2", "10 minutes"),
        ("g3", "10 minutes"),
    ] {
        let slot = fixture
            .store
            .insert_warming_slot(new_slot(
                config_id,
                template_release_id,
                &template,
                content_hash,
                vcpu,
                memory,
                &format!("space-sbx-dead-grace-{suffix}"),
            ))
            .await?;
        force_slot_claimed(&fixture.pool, slot.id, claimed_ago).await?;
        insert_sandbox_with_lifecycle(
            &fixture.pool,
            slot.reserved_sandbox_id,
            SandboxLifecycle::Suspended,
        )
        .await?;
        slots.push(slot);
    }

    // act: sweep in single-row windows across the whole table, following the
    // cursor exactly as the reconciler does. The two aged claims are deleted
    // in whichever windows contain them; the in-grace claim survives; the
    // sweep reports the wrap once the cursor passes the end of the table.
    let mut deleted = 0;
    let mut cursor = Uuid::nil();
    let mut windows = 0;
    let high_water = fixture.store.newest_slot_id().await?.expect("slots seeded");
    loop {
        let outcome = fixture
            .store
            .sweep_dead_claimed_slots_window(Duration::from_secs(5 * 60), cursor, high_water, 1)
            .await?;
        deleted += outcome.deleted;
        windows += 1;
        match outcome.next_cursor {
            Some(next) => {
                assert!(next > cursor, "cursor must advance");
                cursor = next;
            }
            None => break,
        }
        assert!(windows < 100, "sweep must wrap after passing the table end");
    }

    // assert: 3 one-row windows examined the 3 slots, then one empty window
    // reported the wrap.
    assert_eq!(deleted, 2);
    assert_eq!(windows, 4);
    let remaining: Vec<Uuid> =
        sqlx::query_scalar("SELECT id FROM kubernetes_pool_slots WHERE id = ANY($1)")
            .bind(slots.iter().map(|slot| slot.id).collect::<Vec<_>>())
            .fetch_all(&fixture.pool)
            .await?;
    assert_eq!(remaining, vec![slots[0].id]);

    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn store_orphaned_receipt_drain_deletes_only_aged_bound_receipts_without_sandbox()
-> TestResult {
    // arrange: four receipts — aged bound with no sandbox row (drainable),
    // aged bound with a live sandbox row (owned by the suspend/delete
    // flows), aged pre-bind claimed with no sandbox row (crash orphan the
    // stale-unbound sweep finds through this row), and a bound orphan still
    // inside the grace.
    let fixture = postgres_fixture().await?;
    let template = TemplateName::parse("seed")?;
    let (template_release_id, config_id, content_hash) =
        seed_pool_config(&fixture.pool, 0x4b).await?;
    let (vcpu, memory) = default_shape()?;

    let mut slots = Vec::new();
    for suffix in ["g1", "g2", "g3", "g4"] {
        let slot = fixture
            .store
            .insert_warming_slot(new_slot(
                config_id,
                template_release_id,
                &template,
                content_hash,
                vcpu,
                memory,
                &format!("space-sbx-orphan-drain-{suffix}"),
            ))
            .await?;
        slots.push(slot);
    }
    force_slot_bound(&fixture.pool, slots[0].id, "10 minutes").await?;
    force_slot_bound(&fixture.pool, slots[1].id, "10 minutes").await?;
    insert_sandbox_with_lifecycle(
        &fixture.pool,
        slots[1].reserved_sandbox_id,
        SandboxLifecycle::Started,
    )
    .await?;
    force_slot_claimed(&fixture.pool, slots[2].id, "10 minutes").await?;
    force_slot_bound(&fixture.pool, slots[3].id, "1 second").await?;

    // act
    let drained = fixture
        .store
        .sweep_orphaned_bound_receipts_window(
            Duration::from_secs(5 * 60),
            Uuid::nil(),
            fixture.store.newest_slot_id().await?.expect("slots seeded"),
            10_000,
        )
        .await?;

    // assert
    assert_eq!(drained.deleted, 1);
    let remaining: Vec<Uuid> =
        sqlx::query_scalar("SELECT id FROM kubernetes_pool_slots WHERE id = ANY($1) ORDER BY id")
            .bind(slots.iter().map(|slot| slot.id).collect::<Vec<_>>())
            .fetch_all(&fixture.pool)
            .await?;
    let mut expected = vec![slots[1].id, slots[2].id, slots[3].id];
    expected.sort();
    assert_eq!(remaining, expected);

    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn store_reserved_sandbox_delete_lock_deletes_ready_slot() -> TestResult {
    // arrange
    let fixture = postgres_fixture().await?;
    let reserved_sandbox_id = SandboxId::new();
    let template = TemplateName::parse("seed")?;
    let (template_release_id, config_id, content_hash) =
        seed_pool_config(&fixture.pool, 0x55).await?;
    let (vcpu, memory) = default_shape()?;
    let slot = insert_ready_slot(
        &fixture,
        NewPoolSlot {
            reserved_sandbox_id,
            ..new_slot(
                config_id,
                template_release_id,
                &template,
                content_hash,
                vcpu,
                memory,
                "space-sbx-ready-delete-g1",
            )
        },
        "10.90.0.8",
    )
    .await?;

    // act
    let mut tx = fixture.pool.begin().await?;
    let locked = match WarmPoolStore::lock_open_slot_for_delete_by_reserved_sandbox_id(
        &mut tx,
        reserved_sandbox_id,
    )
    .await?
    {
        OpenPoolSlotDeleteLock::Locked(locked) => locked,
        other => panic!("expected locked slot, got {other:?}"),
    };
    let deleted = WarmPoolStore::delete_locked_open_slot(&mut tx, locked.id).await?;
    tx.commit().await?;

    // assert
    assert!(deleted);
    assert_eq!(locked.id, slot.id);
    let remaining: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)
         FROM kubernetes_pool_slots
         WHERE id = $1",
    )
    .bind(slot.id)
    .fetch_one(&fixture.pool)
    .await?;
    assert_eq!(remaining, 0);

    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn store_claim_ready_slot_only_locks_slots() -> TestResult {
    // arrange
    let fixture = postgres_fixture().await?;
    let template = TemplateName::parse("seed")?;
    let (template_release_id, config_id, content_hash) =
        seed_pool_config(&fixture.pool, 0x33).await?;
    let (vcpu, memory) = default_shape()?;

    let first = fixture
        .store
        .insert_warming_slot(new_slot(
            config_id,
            template_release_id,
            &template,
            content_hash,
            vcpu,
            memory,
            "space-sbx-first-g1",
        ))
        .await?;
    let second = fixture
        .store
        .insert_warming_slot(new_slot(
            config_id,
            template_release_id,
            &template,
            content_hash,
            vcpu,
            memory,
            "space-sbx-second-g1",
        ))
        .await?;
    fixture
        .store
        .mark_slot_ready(first.id, "first-pod-uid", "10.90.0.8", "worker-1")
        .await?;
    fixture
        .store
        .mark_slot_ready(second.id, "second-pod-uid", "10.90.0.9", "worker-1")
        .await?;

    let mut first_tx = fixture.pool.begin().await?;
    let first_claim = WarmPoolStore::claim_ready_slot(
        &mut first_tx,
        template_release_id,
        vcpu,
        memory,
        "space",
        Uuid::now_v7(),
        false,
    )
    .await?
    .expect("first ready slot");

    // act
    let mut second_tx = fixture.pool.begin().await?;
    let second_claim = WarmPoolStore::claim_ready_slot(
        &mut second_tx,
        template_release_id,
        vcpu,
        memory,
        "space",
        Uuid::now_v7(),
        false,
    )
    .await?
    .expect("second ready slot");

    // assert
    assert_ne!(first_claim.id, second_claim.id);
    assert_eq!(first_claim.id, first.id);
    assert_eq!(second_claim.id, second.id);

    second_tx.commit().await?;
    first_tx.commit().await?;

    Ok(())
}

async fn insert_ready_slot_for_bind(
    fixture: &PostgresFixture,
    template_release_id: Uuid,
    config_id: Uuid,
    content_hash: ContentHash,
    reserved_sandbox_id: SandboxId,
    pod_name: &str,
) -> TestResult<Uuid> {
    let template = TemplateName::parse("seed")?;
    let (vcpu, memory) = default_shape()?;
    let slot = insert_ready_slot(
        fixture,
        NewPoolSlot {
            reserved_sandbox_id,
            ..new_slot(
                config_id,
                template_release_id,
                &template,
                content_hash,
                vcpu,
                memory,
                pod_name,
            )
        },
        "10.90.0.21",
    )
    .await?;

    Ok(slot.id)
}

async fn claim_slot_for_bind(
    fixture: &PostgresFixture,
    template_release_id: Uuid,
    claim_id: Uuid,
) -> TestResult {
    let mut claim_tx = fixture.pool.begin().await?;
    WarmPoolStore::claim_ready_slot(
        &mut claim_tx,
        template_release_id,
        VcpuCount::try_from_u32(2)?,
        MemorySize::mib(1024)?,
        "space",
        claim_id,
        false,
    )
    .await?
    .ok_or("expected a claimable ready slot")?;
    claim_tx.commit().await?;

    Ok(())
}

async fn slot_state(pool: &PgPool, slot_id: Uuid) -> TestResult<String> {
    Ok(
        sqlx::query_scalar("SELECT state FROM kubernetes_pool_slots WHERE id = $1")
            .bind(slot_id)
            .fetch_one(pool)
            .await?,
    )
}

#[tokio::test(flavor = "current_thread")]
async fn store_bind_claimed_slot_stamps_bound_exactly_once() -> TestResult {
    // arrange
    let fixture = postgres_fixture().await?;
    let (template_release_id, config_id, content_hash) =
        seed_pool_config(&fixture.pool, 0x51).await?;
    let claim_id = Uuid::now_v7();
    let reserved_sandbox_id = SandboxId::new();
    let slot_id = insert_ready_slot_for_bind(
        &fixture,
        template_release_id,
        config_id,
        content_hash,
        reserved_sandbox_id,
        "space-sbx-bind-stamp-g1",
    )
    .await?;
    claim_slot_for_bind(&fixture, template_release_id, claim_id).await?;

    // act
    let mut bind_tx = fixture.pool.begin().await?;
    let bound =
        WarmPoolStore::bind_claimed_slot(&mut bind_tx, claim_id, reserved_sandbox_id).await?;
    let rebound =
        WarmPoolStore::bind_claimed_slot(&mut bind_tx, claim_id, reserved_sandbox_id).await?;
    bind_tx.commit().await?;

    // assert: the stamp is a CAS — the second attempt finds no claimed row.
    assert!(bound);
    assert!(!rebound);
    assert_eq!(slot_state(&fixture.pool, slot_id).await?, "bound");

    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn store_stale_sweep_ignores_bound_slots() -> TestResult {
    // arrange: a bound slot whose claim is far older than any grace period.
    let fixture = postgres_fixture().await?;
    let (template_release_id, config_id, content_hash) =
        seed_pool_config(&fixture.pool, 0x52).await?;
    let claim_id = Uuid::now_v7();
    let reserved_sandbox_id = SandboxId::new();
    let slot_id = insert_ready_slot_for_bind(
        &fixture,
        template_release_id,
        config_id,
        content_hash,
        reserved_sandbox_id,
        "space-sbx-bound-not-stale-g1",
    )
    .await?;
    claim_slot_for_bind(&fixture, template_release_id, claim_id).await?;
    let mut bind_tx = fixture.pool.begin().await?;
    assert!(WarmPoolStore::bind_claimed_slot(&mut bind_tx, claim_id, reserved_sandbox_id).await?);
    bind_tx.commit().await?;
    sqlx::query(
        "UPDATE kubernetes_pool_slots
         SET claimed_at = NOW() - INTERVAL '1 day'
         WHERE id = $1",
    )
    .bind(slot_id)
    .execute(&fixture.pool)
    .await?;

    // act
    let stale = fixture
        .store
        .stale_unbound_claimed_slots(Duration::from_secs(60), 10)
        .await?;
    let exists = fixture
        .store
        .stale_unbound_claimed_slot_exists(slot_id, Duration::from_secs(60))
        .await?;

    // assert: a bound receipt is never a stale-claim candidate, no matter
    // how old — the sweep cutover to a state-only predicate relies on this.
    assert!(stale.iter().all(|slot| slot.id != slot_id));
    assert!(!exists);

    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn store_cleanup_pending_marks_bound_slot() -> TestResult {
    // arrange
    let fixture = postgres_fixture().await?;
    let (template_release_id, config_id, content_hash) =
        seed_pool_config(&fixture.pool, 0x53).await?;
    let claim_id = Uuid::now_v7();
    let reserved_sandbox_id = SandboxId::new();
    let slot_id = insert_ready_slot_for_bind(
        &fixture,
        template_release_id,
        config_id,
        content_hash,
        reserved_sandbox_id,
        "space-sbx-bound-cleanup-g1",
    )
    .await?;
    claim_slot_for_bind(&fixture, template_release_id, claim_id).await?;
    let mut bind_tx = fixture.pool.begin().await?;
    assert!(WarmPoolStore::bind_claimed_slot(&mut bind_tx, claim_id, reserved_sandbox_id).await?);
    bind_tx.commit().await?;

    // act: a failed prepare parks the bound slot as reconciler-owned work.
    let mut mark_tx = fixture.pool.begin().await?;
    let marked = WarmPoolStore::mark_claimed_slot_cleanup_pending(
        &mut mark_tx,
        claim_id,
        reserved_sandbox_id,
    )
    .await?;
    mark_tx.commit().await?;

    // assert
    assert!(marked);
    assert_eq!(slot_state(&fixture.pool, slot_id).await?, "cleanup_pending");

    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn store_retired_execution_delete_removes_bound_slot() -> TestResult {
    // arrange
    let fixture = postgres_fixture().await?;
    let (template_release_id, config_id, content_hash) =
        seed_pool_config(&fixture.pool, 0x54).await?;
    let claim_id = Uuid::now_v7();
    let reserved_sandbox_id = SandboxId::new();
    let slot_id = insert_ready_slot_for_bind(
        &fixture,
        template_release_id,
        config_id,
        content_hash,
        reserved_sandbox_id,
        "space-sbx-bound-retire-g1",
    )
    .await?;
    claim_slot_for_bind(&fixture, template_release_id, claim_id).await?;
    let mut bind_tx = fixture.pool.begin().await?;
    assert!(WarmPoolStore::bind_claimed_slot(&mut bind_tx, claim_id, reserved_sandbox_id).await?);
    bind_tx.commit().await?;

    // act
    let mut retire_tx = fixture.pool.begin().await?;
    let deleted = WarmPoolStore::delete_claimed_slot_for_retired_execution(
        &mut retire_tx,
        reserved_sandbox_id,
    )
    .await?;
    retire_tx.commit().await?;

    // assert
    assert!(deleted);
    let exists: bool =
        sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM kubernetes_pool_slots WHERE id = $1)")
            .bind(slot_id)
            .fetch_one(&fixture.pool)
            .await?;
    assert!(!exists);

    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn store_dead_claimed_drain_deletes_suspended_bound_slot() -> TestResult {
    // arrange: a bound receipt whose sandbox suspended — the fossil class the
    // drain exists for.
    let fixture = postgres_fixture().await?;
    let (template_release_id, config_id, content_hash) =
        seed_pool_config(&fixture.pool, 0x55).await?;
    let claim_id = Uuid::now_v7();
    let reserved_sandbox_id = SandboxId::new();
    let slot_id = insert_ready_slot_for_bind(
        &fixture,
        template_release_id,
        config_id,
        content_hash,
        reserved_sandbox_id,
        "space-sbx-bound-drain-g1",
    )
    .await?;
    claim_slot_for_bind(&fixture, template_release_id, claim_id).await?;
    let mut bind_tx = fixture.pool.begin().await?;
    assert!(WarmPoolStore::bind_claimed_slot(&mut bind_tx, claim_id, reserved_sandbox_id).await?);
    bind_tx.commit().await?;
    insert_sandbox_with_lifecycle(
        &fixture.pool,
        reserved_sandbox_id,
        SandboxLifecycle::Suspended,
    )
    .await?;
    sqlx::query(
        "UPDATE kubernetes_pool_slots
         SET claimed_at = NOW() - INTERVAL '1 hour'
         WHERE id = $1",
    )
    .bind(slot_id)
    .execute(&fixture.pool)
    .await?;

    // act
    let drained = fixture
        .store
        .sweep_dead_claimed_slots_window(
            Duration::from_secs(300),
            Uuid::nil(),
            fixture.store.newest_slot_id().await?.expect("slots seeded"),
            10_000,
        )
        .await?;

    // assert
    assert_eq!(drained.deleted, 1);
    let exists: bool =
        sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM kubernetes_pool_slots WHERE id = $1)")
            .bind(slot_id)
            .fetch_one(&fixture.pool)
            .await?;
    assert!(!exists);

    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn store_bind_source_reads_locked_identity_and_normalizes_pod_uid() -> TestResult {
    // arrange
    let fixture = postgres_fixture().await?;
    let template = TemplateName::parse("seed")?;
    let (template_release_id, config_id, content_hash) =
        seed_pool_config(&fixture.pool, 0x61).await?;
    let (vcpu, memory) = default_shape()?;
    let claim_id = Uuid::now_v7();
    let reserved_sandbox_id = SandboxId::new();
    let slot = fixture
        .store
        .insert_warming_slot(NewPoolSlot {
            reserved_sandbox_id,
            ..new_slot(
                config_id,
                template_release_id,
                &template,
                content_hash,
                vcpu,
                memory,
                "space-sbx-bind-source-g1",
            )
        })
        .await?;
    fixture
        .store
        .mark_slot_ready(slot.id, "  ", "10.90.0.31", "worker-1")
        .await?;
    let mut claim_tx = fixture.pool.begin().await?;
    WarmPoolStore::claim_ready_slot(
        &mut claim_tx,
        template_release_id,
        vcpu,
        memory,
        "space",
        claim_id,
        false,
    )
    .await?
    .ok_or("expected a claimable ready slot")?;
    claim_tx.commit().await?;

    // act
    let mut bind_tx = fixture.pool.begin().await?;
    let source = WarmPoolStore::bind_source(&mut bind_tx, claim_id, reserved_sandbox_id).await?;
    let missing =
        WarmPoolStore::bind_source(&mut bind_tx, Uuid::now_v7(), reserved_sandbox_id).await?;
    bind_tx.commit().await?;

    // assert: identity columns come back, and a blank pod_uid is normalized
    // to NULL so both sandbox rows bind the same absent value.
    let source = source.ok_or("expected bind source for live claim")?;
    assert_eq!(source.pod_name, "space-sbx-bind-source-g1");
    assert_eq!(source.pod_namespace, "space");
    assert_eq!(source.pod_uid, None);
    assert_eq!(source.pod_ip.as_deref(), Some("10.90.0.31"));
    assert_eq!(source.k8s_node_name.as_deref(), Some("worker-1"));
    assert!(missing.is_none());

    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn store_reserved_sandbox_ids_reports_slots_in_any_state() -> TestResult {
    // arrange: one warming slot; its reserved id must be reported while an
    // unknown id is not.
    let fixture = postgres_fixture().await?;
    let template = TemplateName::parse("seed")?;
    let (template_release_id, config_id, content_hash) =
        seed_pool_config(&fixture.pool, 0x62).await?;
    let (vcpu, memory) = default_shape()?;
    let reserved_sandbox_id = SandboxId::new();
    fixture
        .store
        .insert_warming_slot(NewPoolSlot {
            reserved_sandbox_id,
            ..new_slot(
                config_id,
                template_release_id,
                &template,
                content_hash,
                vcpu,
                memory,
                "space-sbx-reserved-ids-g1",
            )
        })
        .await?;

    // act
    let reserved = fixture
        .store
        .reserved_sandbox_ids(&[reserved_sandbox_id, SandboxId::new()])
        .await?;

    // assert
    assert_eq!(reserved, vec![reserved_sandbox_id]);

    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn live_slot_ids_returns_only_ids_with_rows() -> TestResult {
    // arrange
    let fixture = postgres_fixture().await?;
    let release_id = Uuid::now_v7();
    let config_id = Uuid::now_v7();
    let template = TemplateName::parse("seed")?;
    let content_hash = ContentHash::new(Digest::sha256([0x77; 32]));
    let (vcpu, memory) = default_shape()?;
    insert_pool_config_seed(
        &fixture.pool,
        autoscaler_seed("seed", release_id, config_id, content_hash, (2, 1, 2)),
    )
    .await?;
    let slot = fixture
        .store
        .insert_warming_slot(NewPoolSlot {
            pod_namespace: "sandboxes",
            ..new_slot(
                config_id,
                release_id,
                &template,
                content_hash,
                vcpu,
                memory,
                "space-sbx-live-slot-g1",
            )
        })
        .await?;
    // A slot id the pool never tracked — stands in for an orphan pod whose row
    // has already cascaded away.
    let orphan_slot_id = Uuid::now_v7();

    // act
    let empty = fixture.store.live_slot_ids(&[]).await?;
    let live = fixture
        .store
        .live_slot_ids(&[slot.id, orphan_slot_id])
        .await?;
    fixture.store.delete_open_slot(slot.id).await?;
    let after_delete = fixture.store.live_slot_ids(&[slot.id]).await?;

    // assert
    assert!(empty.is_empty(), "empty input short-circuits to empty set");
    assert!(live.contains(&slot.id), "the live slot is reported");
    assert!(
        !live.contains(&orphan_slot_id),
        "an id with no row is not reported (the orphan case the sweep owns)"
    );
    assert!(
        after_delete.is_empty(),
        "once the row is gone the id is no longer live"
    );

    Ok(())
}

async fn insert_execution(
    pool: &PgPool,
    sandbox_id: SandboxId,
    generation: i64,
    pod_name: &str,
    active: bool,
    state: &str,
) -> TestResult {
    sqlx::query(
        "INSERT INTO sandbox_k8s_executions (
             id, sandbox_id, generation, pod_name, pod_namespace, state, active
         )
         VALUES ($1, $2, $3, $4, 'sandboxes', $5, $6)",
    )
    .bind(Uuid::now_v7())
    .bind(sandbox_id)
    .bind(generation)
    .bind(pod_name)
    .bind(state)
    .bind(active)
    .execute(pool)
    .await?;
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn live_execution_pod_names_matches_active_execution() -> TestResult {
    // arrange: one sandbox with an active execution on its pod (an adopted warm
    // slot), one sandbox that never adopted (no execution row).
    let fixture = postgres_fixture().await?;
    let live_sandbox = SandboxId::new();
    let orphan_sandbox = SandboxId::new();
    let live_pod = "space-sbx-live-g1".to_string();
    let orphan_pod = "space-sbx-orphan-g1".to_string();
    insert_sandbox_with_lifecycle(&fixture.pool, live_sandbox, SandboxLifecycle::Started).await?;
    insert_execution(&fixture.pool, live_sandbox, 1, &live_pod, true, "running").await?;

    // act
    let empty = fixture.store.live_execution_pod_names(&[], &[]).await?;
    let live = fixture
        .store
        .live_execution_pod_names(
            &[live_sandbox, orphan_sandbox],
            &[live_pod.clone(), orphan_pod],
        )
        .await?;

    // assert
    assert!(empty.is_empty(), "empty input short-circuits to empty set");
    assert!(
        live.contains(&live_pod),
        "the pod with an active execution is reported (guarded from the sweep)"
    );
    assert_eq!(live.len(), 1, "the pod with no execution is not reported");

    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn live_execution_pod_names_ignores_suspended_execution() -> TestResult {
    // Regression (codex P1): a suspended pool-adopted sandbox retires its
    // execution (`active = FALSE`). Its leaked terminal pod must NOT be
    // classified as live — keying on mere execution existence would leave it
    // uncollectable forever; keying on the *active* flag collects it.
    let fixture = postgres_fixture().await?;
    let sandbox = SandboxId::new();
    let pod = "space-sbx-suspended-g1".to_string();
    insert_sandbox_with_lifecycle(&fixture.pool, sandbox, SandboxLifecycle::Suspended).await?;
    // Retired execution (active=FALSE): the sandbox is suspended, not live.
    insert_execution(&fixture.pool, sandbox, 1, &pod, false, "suspended").await?;

    // act
    let live = fixture
        .store
        .live_execution_pod_names(&[sandbox], std::slice::from_ref(&pod))
        .await?;

    // assert: the retired execution does not protect the leaked pod, so the
    // sweep can collect it.
    assert!(
        live.is_empty(),
        "a suspended (active=FALSE) execution must not classify the pod as live"
    );

    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn orphan_candidate_still_unowned_checks_both_tables() -> TestResult {
    // arrange: a config so we can insert a real slot row, plus a bound sandbox.
    let fixture = postgres_fixture().await?;
    let release_id = Uuid::now_v7();
    let config_id = Uuid::now_v7();
    let template = TemplateName::parse("seed")?;
    let content_hash = ContentHash::new(Digest::sha256([0x79; 32]));
    let (vcpu, memory) = default_shape()?;
    insert_pool_config_seed(
        &fixture.pool,
        autoscaler_seed("seed", release_id, config_id, content_hash, (2, 1, 2)),
    )
    .await?;
    let slot = fixture
        .store
        .insert_warming_slot(NewPoolSlot {
            pod_namespace: "sandboxes",
            ..new_slot(
                config_id,
                release_id,
                &template,
                content_hash,
                vcpu,
                memory,
                "space-sbx-owned-slot-g1",
            )
        })
        .await?;

    let live_sandbox = SandboxId::new();
    let live_pod = "space-sbx-live-recheck-g1";
    insert_sandbox_with_lifecycle(&fixture.pool, live_sandbox, SandboxLifecycle::Started).await?;
    insert_execution(&fixture.pool, live_sandbox, 1, live_pod, true, "running").await?;

    // A suspended sandbox: retired execution + retained compat row. Must NOT
    // count as owned — the sweep should be able to collect its leaked pod.
    let suspended_sandbox = SandboxId::new();
    let suspended_pod = "space-sbx-suspended-recheck-g1";
    insert_sandbox_with_lifecycle(
        &fixture.pool,
        suspended_sandbox,
        SandboxLifecycle::Suspended,
    )
    .await?;
    insert_execution(
        &fixture.pool,
        suspended_sandbox,
        1,
        suspended_pod,
        false,
        "suspended",
    )
    .await?;

    // act + assert
    // (a) neither a slot row nor a live execution → unowned (deletable).
    assert!(
        fixture
            .store
            .orphan_candidate_still_unowned(Uuid::now_v7(), SandboxId::new(), "space-sbx-orphan-g1")
            .await?,
        "a candidate with no slot row and no live execution is unowned"
    );
    // (b) a live slot row → owned (must not delete).
    assert!(
        !fixture
            .store
            .orphan_candidate_still_unowned(slot.id, SandboxId::new(), "space-sbx-owned-slot-g1")
            .await?,
        "a live kubernetes_pool_slots row means owned"
    );
    // (c) a matching active execution → owned (must not delete).
    assert!(
        !fixture
            .store
            .orphan_candidate_still_unowned(Uuid::now_v7(), live_sandbox, live_pod)
            .await?,
        "a matching active sandbox_k8s_executions row means owned"
    );
    // (d) a suspended (active=FALSE) execution → unowned (collectable).
    assert!(
        fixture
            .store
            .orphan_candidate_still_unowned(Uuid::now_v7(), suspended_sandbox, suspended_pod)
            .await?,
        "a retired (suspended) execution does not count as owned"
    );

    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn store_backstop_lap_terminates_under_continuous_inserts() -> TestResult {
    // arrange: three aged orphaned bound receipts (no sandboxes row). The
    // lap's high-water id is captured before two more matching receipts are
    // inserted mid-lap; UUIDv7 ids sort after it, so a lap without the
    // high-water bound would keep finding new rows.
    let fixture = postgres_fixture().await?;
    let template = TemplateName::parse("seed")?;
    let (template_release_id, config_id, content_hash) =
        seed_pool_config(&fixture.pool, 0x4c).await?;
    let (vcpu, memory) = default_shape()?;
    let insert_orphan = async |suffix: &str| -> TestResult<Uuid> {
        let slot = fixture
            .store
            .insert_warming_slot(new_slot(
                config_id,
                template_release_id,
                &template,
                content_hash,
                vcpu,
                memory,
                &format!("space-sbx-lap-{suffix}"),
            ))
            .await?;
        force_slot_bound(&fixture.pool, slot.id, "10 minutes").await?;
        Ok(slot.id)
    };
    for suffix in ["g1", "g2", "g3"] {
        insert_orphan(suffix).await?;
    }
    let high_water = fixture.store.newest_slot_id().await?.expect("slots seeded");

    // act: walk the lap in single-row windows, inserting new matching
    // orphans after the first window.
    let mut deleted = 0;
    let mut cursor = Uuid::nil();
    let mut windows = 0;
    let mut late_orphans = Vec::new();
    loop {
        let outcome = fixture
            .store
            .sweep_orphaned_bound_receipts_window(
                Duration::from_secs(5 * 60),
                cursor,
                high_water,
                1,
            )
            .await?;
        deleted += outcome.deleted;
        windows += 1;
        if windows == 1 {
            late_orphans.push(insert_orphan("late-1").await?);
            late_orphans.push(insert_orphan("late-2").await?);
        }
        match outcome.next_cursor {
            Some(next) => cursor = next,
            None => break,
        }
        assert!(windows < 10, "lap must terminate at its high-water id");
    }

    // assert: the lap covered exactly the three lap-start rows (three
    // one-row windows, then the wrap) and left the mid-lap inserts for the
    // next lap; a fresh lap with a fresh high-water id then reaps them.
    assert_eq!(deleted, 3);
    assert_eq!(windows, 4);
    let survivors: Vec<Uuid> =
        sqlx::query_scalar("SELECT id FROM kubernetes_pool_slots ORDER BY id")
            .fetch_all(&fixture.pool)
            .await?;
    assert_eq!(survivors, late_orphans);

    let next_high_water = fixture.store.newest_slot_id().await?.expect("late orphans");
    let next_lap = fixture
        .store
        .sweep_orphaned_bound_receipts_window(
            Duration::from_secs(5 * 60),
            Uuid::nil(),
            next_high_water,
            10_000,
        )
        .await?;
    assert_eq!(next_lap.deleted, 2);

    Ok(())
}

/// Demand for one release published under two hash spellings (`sha256:`-
/// prefixed and bare) and two equivalent shape spellings (NULL vcpu/memory
/// vs the explicit defaults) must aggregate into a single demand group.
/// Neither spelling alone reaches AUTOSCALE_MIN_DEMAND_SANDBOXES (10), so a
/// config existing at all proves the merge — this pins the equivalence of
/// the aggregate-before-lateral form of `observed_demand`, whose outer
/// GROUP BY re-merges distinct hash spellings that resolve to one release.
#[tokio::test(flavor = "current_thread")]
async fn autoscale_merges_hash_spellings_and_default_shapes_into_one_demand_group() -> TestResult {
    let fixture = postgres_fixture().await?;
    let release_id = Uuid::now_v7();
    let hash = ContentHash::new(Digest::sha256([0x5a; 32]));
    let hash_prefixed = hash.to_string();
    let hash_bare = hash_prefixed
        .strip_prefix("sha256:")
        .expect("test hash should use sha256");
    insert_template_release_with_hash(&fixture.pool, "seed", release_id, hash_bare).await?;
    upsert_warm_pool_policy(&fixture.pool, "seed", 2, 10, 1).await?;
    // 6 prefixed + 6 bare (each spelling below the 10-sandbox demand
    // threshold), mixing NULL and explicit-default shapes within both.
    for i in 0..6 {
        let shape = if i % 2 == 0 { None } else { Some(2) };
        let memory = if i % 2 == 0 { None } else { Some(1024) };
        insert_sandbox_for_template(
            &fixture.pool,
            "seed",
            "space",
            &hash_prefixed,
            shape,
            memory,
        )
        .await?;
        insert_sandbox_for_template(&fixture.pool, "seed", "space", hash_bare, shape, memory)
            .await?;
    }

    fixture.store.autoscale_configs("sandboxes").await?;

    let target = sqlx::query_scalar::<_, i32>(
        "SELECT target_ready_slots
         FROM kubernetes_pool_configs
         WHERE template_release_id = $1",
    )
    .bind(release_id)
    .fetch_optional(&fixture.pool)
    .await?;
    assert!(
        target.is_some(),
        "12 sandboxes split across hash and shape spellings must merge into \
         one demand group that crosses the 10-sandbox threshold"
    );

    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn config_reads_decode_blake3_release_hashes() -> TestResult {
    // Template-manager registers BLAKE3 identities by default. Every config
    // read the warm-pool pass makes must decode them as stored; one
    // undecodable row fails the whole pass for every template.
    let fixture = postgres_fixture().await?;
    let release_id = Uuid::now_v7();
    let config_id = Uuid::now_v7();
    let content_hash = ContentHash::new(Digest::blake3([0x3b; 32]));
    insert_pool_config(&fixture.pool, release_id, config_id, content_hash).await?;

    let enabled = fixture.store.enabled_configs().await?;
    assert_eq!(enabled.len(), 1);
    assert_eq!(enabled[0].content_hash, content_hash);

    let reconcilable = fixture.store.reconcilable_configs().await?;
    assert_eq!(reconcilable.len(), 1);
    assert_eq!(reconcilable[0].content_hash, content_hash);

    sqlx::query(
        "UPDATE kubernetes_pool_configs
         SET enabled = false, managed_by = 'autoscaler', updated_at = NOW()
         WHERE id = $1",
    )
    .bind(config_id)
    .execute(&fixture.pool)
    .await?;

    let recently_disabled = fixture
        .store
        .recently_disabled_configs(Duration::from_secs(10 * 60))
        .await?;
    assert_eq!(recently_disabled.len(), 1);
    assert_eq!(recently_disabled[0].content_hash, content_hash);

    let retired = fixture.store.autoscale_configs("space").await?;
    assert_eq!(retired.len(), 1);
    assert_eq!(retired[0].id, config_id);
    assert_eq!(retired[0].content_hash, content_hash);

    Ok(())
}
