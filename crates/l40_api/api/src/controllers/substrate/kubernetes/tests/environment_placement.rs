//! The environment a sandbox pod records and, while the environment
//! placement gate is on, requires a node label for: the binding of what it
//! boots.

use pplx_space_accounts::{NodeId, SandboxOwner};
use pplx_space_db_test_fixtures::{SEED_ORG_ID, insert_ready_worker_node};
use pplx_space_execution_spec::EnvironmentReference;
use pplx_space_sandbox_model::{SandboxId, SandboxLifecycle};
use pplx_space_sql_locator::sql;

use super::*;
use crate::controllers::substrate::environment_placement::EnvironmentPlacementGate;
use pplx_space_sandbox_pod::PodEnvironment;

type BoxError = Box<dyn std::error::Error + Send + Sync>;

const E0: &str = "sha256:0000000000000000000000000000000000000000000000000000000000000000";
const E1: &str = "sha256:1111111111111111111111111111111111111111111111111111111111111111";
const HASH: &str = "sha256:seed";
const URI: &str = "s3://synthetic-bucket/checkpoint";

fn reference(value: &str) -> EnvironmentReference {
    value.parse().unwrap()
}

/// `value` as on requires it.
fn required(value: &str) -> Option<PodEnvironment> {
    Some(PodEnvironment {
        reference: reference(value),
        required: true,
    })
}

/// A sandbox row and its in-memory model, on template `seed` at [`HASH`],
/// whose release is bound to `release_environment`, with a ready checkpoint
/// at [`URI`] produced under `checkpoint_environment` and a published
/// execution under `published_environment`.
async fn sandbox(
    pool: &sqlx::PgPool,
    lifecycle: &str,
    release_environment: Option<&str>,
    checkpoint_environment: Option<&str>,
    published_environment: Option<&str>,
) -> Result<Sandbox, BoxError> {
    let id = SandboxId::new();
    insert_test_sandbox_row(pool, id, lifecycle).await?;
    let binding = |environment: Option<&str>| {
        sqlx::types::Json(match environment {
            Some(environment) => serde_json::json!({"environment": environment, "components": []}),
            None => serde_json::json!({"components": []}),
        })
    };
    sqlx::query(sql!(
        "INSERT INTO templates (id, name, org_id) VALUES ($1, 'seed', $2)
         ON CONFLICT (name) DO NOTHING"
    ))
    .bind(uuid::Uuid::now_v7())
    .bind(SEED_ORG_ID)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "DELETE FROM template_releases WHERE template_name = 'seed'"
    ))
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO template_releases (template_name, type, hash, execution_metadata)
         VALUES ('seed', 'fc', $1, $2)"
    ))
    .bind(HASH)
    .bind(binding(release_environment))
    .execute(pool)
    .await?;
    let snapshot_id = pplx_space_snapshot_model::SnapshotId::new();
    sqlx::query(sql!(
        "INSERT INTO snapshots (id, sandbox_id, type, state, timestamp, org_id, execution_metadata)
         VALUES ($1, $2, 'full', 'ready', NOW(), $3, $4)"
    ))
    .bind(snapshot_id)
    .bind(id)
    .bind(SEED_ORG_ID)
    .bind(binding(checkpoint_environment))
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO snapshot_storage_locations (snapshot_id, storage_connection_string, org_id)
         VALUES ($1, $2, $3)"
    ))
    .bind(snapshot_id)
    .bind(URI)
    .bind(SEED_ORG_ID)
    .execute(pool)
    .await?;
    let node = NodeId::new();
    insert_ready_worker_node(pool, node).await?;
    sqlx::query(sql!(
        "INSERT INTO sandbox_observed_states (sandbox_id, node_id, status, runtime, execution_metadata)
         VALUES ($1, $2, 'stopped', 'firecracker', $3)"
    ))
    .bind(id)
    .bind(node)
    .bind(binding(published_environment))
    .execute(pool)
    .await?;
    Ok(Sandbox {
        id,
        content_hash: Some(HASH.to_string()),
        ..test_sandbox_with_lifecycle(lifecycle)
    })
}

/// The id of the release [`sandbox`] registers.
async fn seed_release(pool: &sqlx::PgPool) -> Result<uuid::Uuid, BoxError> {
    Ok(sqlx::query_scalar(sql!(
        "SELECT id FROM template_releases WHERE template_name = 'seed'"
    ))
    .fetch_one(pool)
    .await?)
}

fn restore() -> PodCreateContext<'static> {
    PodCreateContext::Restore(RestoreContext::UserRestore {
        suspend_restore_storage_uri: Some(URI),
        suspend_restore_source_node: None,
        direct_node_name: None,
        preferred_node_name: None,
        runtime_upgrade: None,
    })
}

#[tokio::test(flavor = "current_thread")]
async fn off_renders_no_environment_and_shadow_records_it_without_requiring_it()
-> Result<(), BoxError> {
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let sandbox = sandbox(&db.pool, "suspended", Some(E1), Some(E1), Some(E1)).await?;
    let mut substrate = substrate_with_store(db.pool.clone());
    for (gate, expected) in [
        (EnvironmentPlacementGate::Off, None),
        (
            EnvironmentPlacementGate::Shadow,
            Some(PodEnvironment {
                reference: reference(E1),
                required: false,
            }),
        ),
        (EnvironmentPlacementGate::On, required(E1)),
    ] {
        substrate.k8s.environment_placement = gate;
        for context in [PodCreateContext::OnDemand, restore()] {
            assert_eq!(
                substrate
                    .resolve_pod_environment(&sandbox, &context)
                    .await?,
                expected,
                "{gate}"
            );
        }
    }
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn on_requires_the_binding_of_what_the_pod_boots() -> Result<(), BoxError> {
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let mut substrate = substrate_with_store(db.pool.clone());
    substrate.k8s.environment_placement = EnvironmentPlacementGate::On;

    // Each record is bound to a different environment, so the answer names
    // the one the pod boots.
    let suspended = sandbox(&db.pool, "suspended", Some(E1), Some(E0), None).await?;
    assert_eq!(
        substrate
            .resolve_pod_environment(&suspended, &PodCreateContext::OnDemand)
            .await?,
        required(E1),
        "an on-demand pod boots the template release"
    );
    assert_eq!(
        substrate
            .resolve_pod_environment(&suspended, &restore())
            .await?,
        required(E0),
        "a suspended sandbox restores its checkpoint"
    );
    let stopped = sandbox(&db.pool, "stopped", Some(E1), Some(E1), Some(E0)).await?;
    assert_eq!(
        substrate
            .resolve_pod_environment(&stopped, &restore())
            .await?,
        required(E0),
        "a stopped sandbox cold boots its published execution"
    );
    // The release is still bound to E1 here, so only the missing content
    // hash can make an on-demand pod unbound.
    let no_hash = Sandbox {
        content_hash: None,
        ..suspended
    };
    assert_eq!(
        substrate
            .resolve_pod_environment(&no_hash, &PodCreateContext::OnDemand)
            .await?,
        None,
        "a sandbox with no content hash names no release"
    );

    let unbound = sandbox(&db.pool, "suspended", None, None, None).await?;
    for context in [PodCreateContext::OnDemand, restore()] {
        assert_eq!(
            substrate
                .resolve_pod_environment(&unbound, &context)
                .await?,
            None,
            "unbound work requires no environment"
        );
    }
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn an_invalid_binding_fails_rather_than_placing_as_unbound() -> Result<(), BoxError> {
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let mut substrate = substrate_with_store(db.pool.clone());
    substrate.k8s.environment_placement = EnvironmentPlacementGate::On;
    let sandbox = sandbox(&db.pool, "suspended", Some("E1"), None, None).await?;
    let error = substrate
        .resolve_pod_environment(&sandbox, &PodCreateContext::OnDemand)
        .await
        .expect_err("an unparseable binding");
    assert!(
        matches!(&error, SubstrateError::Failed { message } if message.contains("invalid environment")),
        "{error:?}"
    );
    Ok(())
}

/// Shadow places exactly as off, so a binding it can't parse places the pod
/// as unbound rather than fail it: only on refuses to place bound work it
/// can't name.
#[tokio::test(flavor = "current_thread")]
async fn shadow_places_work_with_an_invalid_binding_as_off_would() -> Result<(), BoxError> {
    use crate::controllers::substrate::environment_placement::pool_pod_environment;
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let mut substrate = substrate_with_store(db.pool.clone());
    substrate.k8s.environment_placement = EnvironmentPlacementGate::Shadow;
    let sandbox = sandbox(&db.pool, "suspended", Some("E1"), Some("E0"), None).await?;
    for context in [PodCreateContext::OnDemand, restore()] {
        assert_eq!(
            substrate
                .resolve_pod_environment(&sandbox, &context)
                .await?,
            None
        );
    }
    let release = seed_release(&db.pool).await?;
    assert_eq!(
        pool_pod_environment(
            EnvironmentPlacementGate::Shadow,
            &db.pool,
            release,
            "seed",
            HASH
        )
        .await?,
        None
    );
    assert!(
        pool_pod_environment(
            EnvironmentPlacementGate::On,
            &db.pool,
            release,
            "seed",
            HASH
        )
        .await
        .is_err(),
        "on fails it"
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn a_pool_pod_records_its_release_binding_and_requires_it_only_when_on()
-> Result<(), BoxError> {
    use crate::controllers::substrate::environment_placement::pool_pod_environment;
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    sandbox(&db.pool, "suspended", Some(E1), None, None).await?;
    let release = seed_release(&db.pool).await?;
    for (gate, expected) in [
        (EnvironmentPlacementGate::Off, None),
        (
            EnvironmentPlacementGate::Shadow,
            Some(PodEnvironment {
                reference: reference(E1),
                required: false,
            }),
        ),
        (EnvironmentPlacementGate::On, required(E1)),
    ] {
        assert_eq!(
            pool_pod_environment(gate, &db.pool, release, "seed", HASH).await?,
            expected,
            "{gate}"
        );
    }
    assert_eq!(
        pool_pod_environment(
            EnvironmentPlacementGate::On,
            &db.pool,
            uuid::Uuid::now_v7(),
            "seed",
            "sha256:other"
        )
        .await?,
        None,
        "no such build"
    );
    Ok(())
}

/// A warm pool of a release's twin runs under the twin's environment, while
/// template work that names no release reads the original's, as before twins.
/// Twins exist only once `idx_template_releases_template_name_hash_unique` is
/// dropped, which the test does to stand in for the contract migration.
#[tokio::test(flavor = "current_thread")]
async fn a_pool_of_a_twin_runs_under_the_twin_binding() -> Result<(), BoxError> {
    use crate::controllers::substrate::environment_placement::pool_pod_environment;
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let mut substrate = substrate_with_store(db.pool.clone());
    substrate.k8s.environment_placement = EnvironmentPlacementGate::On;
    sandbox(&db.pool, "suspended", Some(E1), None, None).await?;
    let original = seed_release(&db.pool).await?;
    sqlx::query(sql!(
        "DROP INDEX IF EXISTS idx_template_releases_template_name_hash_unique"
    ))
    .execute(&db.pool)
    .await?;
    let twin: uuid::Uuid = sqlx::query_scalar(sql!(
        "INSERT INTO template_releases (template_name, type, hash, execution_metadata)
         VALUES ('seed', 'fc', $1, jsonb_build_object('environment', $2::TEXT, 'components', '[]'::jsonb))
         RETURNING id"
    ))
    .bind(HASH)
    .bind(E0)
    .fetch_one(&db.pool)
    .await?;
    // Rewrite the table in environment order, which puts the twin (E0)
    // before the original (E1) in the heap and in every index's tie order,
    // so only an explicit original-first order answers with the original.
    sqlx::query(sql!(
        "CLUSTER template_releases USING template_releases_template_name_hash_environment_key"
    ))
    .execute(&db.pool)
    .await?;

    let gate = EnvironmentPlacementGate::On;
    assert_eq!(
        pool_pod_environment(gate, &db.pool, twin, "seed", HASH).await?,
        required(E0),
        "the twin's pool"
    );
    assert_eq!(
        pool_pod_environment(gate, &db.pool, original, "seed", HASH).await?,
        required(E1),
        "the original's pool"
    );
    assert_eq!(
        substrate.template_pod_environment("seed", HASH).await?,
        required(E1),
        "work that names no release reads the original"
    );
    Ok(())
}

/// A pod of a create renders the binding of the release the create resolves
/// to only with the gate on, when the pod requires it and the worker boots
/// it. In shadow the worker boots the named release's build, so its pod
/// records that release's binding.
#[tokio::test(flavor = "current_thread")]
async fn a_pod_renders_the_resolved_twin_only_with_the_gate_on() -> Result<(), BoxError> {
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let mut substrate = substrate_with_store(db.pool.clone());
    sandbox(&db.pool, "suspended", Some(E1), None, None).await?;
    let original = seed_release(&db.pool).await?;
    // Twins exist only once the `(template_name, hash)` unique index is
    // dropped; the test drops it to stand in for the contract migration.
    sqlx::query(sql!(
        "DROP INDEX IF EXISTS idx_template_releases_template_name_hash_unique"
    ))
    .execute(&db.pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO template_releases (template_name, type, hash, execution_metadata, created_at)
         SELECT template_name, type, hash,
                jsonb_build_object('environment', $2::TEXT, 'components', '[]'::jsonb),
                clock_timestamp()
         FROM template_releases WHERE id = $1"
    ))
    .bind(original)
    .bind(E0)
    .execute(&db.pool)
    .await?;
    crate::test_support::set_default_environment(&db.pool, E0).await?;

    substrate.k8s.environment_placement = EnvironmentPlacementGate::On;
    assert_eq!(
        substrate.template_pod_environment("seed", HASH).await?,
        required(E0),
        "with the gate on, the twin the create resolves to"
    );
    substrate.k8s.environment_placement = EnvironmentPlacementGate::Shadow;
    assert_eq!(
        substrate.template_pod_environment("seed", HASH).await?,
        Some(PodEnvironment {
            reference: reference(E1),
            required: false,
        }),
        "in shadow, the named release's binding"
    );
    Ok(())
}

/// With the gate on, a claimed warm pod is checked against the environment
/// its slot's release runs under, which the pool rendered it for, even once
/// the default moves creates to a twin bound elsewhere: a node that publishes
/// only the twin's environment doesn't vouch for the original's pod. A claim
/// that holds no slot release reads the release a create resolves to, the
/// twin, which the original's pod didn't boot.
#[tokio::test(flavor = "current_thread")]
async fn a_warm_pod_is_checked_against_its_slots_release_after_the_default_moves()
-> Result<(), BoxError> {
    use crate::controllers::substrate::PooledExecutionClaim;
    use crate::controllers::substrate::kubernetes::adapter::PooledPodEnvironment;
    use k8s_openapi::api::core::v1::{Pod, PodSpec};
    use pplx_space_sandbox_pod::{
        ENVIRONMENT_REQUIRED_ANNOTATION_VALUE, ENVIRONMENT_USABLE_LABEL_VALUE,
        SANDBOX_ENVIRONMENT_ANNOTATION, SANDBOX_ENVIRONMENT_REQUIRED_ANNOTATION,
        SandboxPodConfigValues, environment_label_key,
    };
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    sandbox(&db.pool, "suspended", Some(E1), None, None).await?;
    let original = seed_release(&db.pool).await?;
    // Twins exist only once the `(template_name, hash)` unique index is
    // dropped; the test drops it to stand in for the contract migration.
    sqlx::query(sql!(
        "DROP INDEX IF EXISTS idx_template_releases_template_name_hash_unique"
    ))
    .execute(&db.pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO template_releases (template_name, type, hash, execution_metadata, created_at)
         SELECT template_name, type, hash,
                jsonb_build_object('environment', $2::TEXT, 'components', '[]'::jsonb),
                clock_timestamp()
         FROM template_releases WHERE id = $1"
    ))
    .bind(original)
    .bind(E0)
    .execute(&db.pool)
    .await?;
    crate::test_support::set_default_environment(&db.pool, E0).await?;
    let node = |name: &str, publishes: &str| {
        let mut labels = SandboxPodConfigValues::default()
            .base_node_selector()
            .into_labels();
        labels.insert(
            environment_label_key(&reference(publishes)),
            ENVIRONMENT_USABLE_LABEL_VALUE.to_string(),
        );
        (*node_with_network_slots(name, labels, Some(8), None, true, false)).clone()
    };
    let mut substrate = substrate_with_inventory_caches_and_pool(
        Vec::new(),
        vec![node("publishes-e1", E1), node("publishes-e0", E0)],
        db.pool.clone(),
    );
    substrate.k8s.environment_placement = EnvironmentPlacementGate::On;
    let claim = |template_release_id| PooledExecutionClaim {
        claim_id: uuid::Uuid::now_v7(),
        pool_slot_id: Some(uuid::Uuid::now_v7()),
        sandbox_id: SandboxId::new(),
        content_hash: HASH.to_string(),
        template_release_id,
        pod_name: "space-sbx-pooled-e1".to_string(),
        pod_uid: None,
        execution_generation: 1,
        pod_ip: "10.90.0.11".to_string(),
        vcpu: Some(4),
        memory_mib: Some(2048),
        disk_mib: None,
        disk_io_limits: Default::default(),
        active_node_id: None,
        route_identity: None,
    };
    // The pool rendered the original's pod with the gate on, so it tells
    // its shim to boot the original's environment.
    let pod_on = |node_name: &str| {
        SandboxPodView::from(Pod {
            metadata: kube::api::ObjectMeta {
                annotations: Some(
                    [
                        (SANDBOX_ENVIRONMENT_ANNOTATION.to_string(), E1.to_string()),
                        (
                            SANDBOX_ENVIRONMENT_REQUIRED_ANNOTATION.to_string(),
                            ENVIRONMENT_REQUIRED_ANNOTATION_VALUE.to_string(),
                        ),
                    ]
                    .into(),
                ),
                ..Default::default()
            },
            spec: Some(PodSpec {
                node_name: Some(node_name.to_string()),
                ..Default::default()
            }),
            ..Default::default()
        })
    };

    assert_eq!(
        substrate
            .pooled_pod_environment(
                "seed",
                &claim(Some(original)),
                Some(&pod_on("publishes-e1"))
            )
            .await?,
        PooledPodEnvironment::Admitted,
        "the original's pod on a node publishing the original's environment"
    );
    assert_eq!(
        substrate
            .pooled_pod_environment(
                "seed",
                &claim(Some(original)),
                Some(&pod_on("publishes-e0"))
            )
            .await?,
        PooledPodEnvironment::NotPublished,
        "the new default's environment doesn't vouch for the original's pod"
    );
    assert_eq!(
        substrate
            .pooled_pod_environment("seed", &claim(None), Some(&pod_on("publishes-e0")))
            .await?,
        PooledPodEnvironment::NotBooted,
        "a claim with no slot release reads the release creates resolve to, the E0 \
         twin, which the pod didn't boot"
    );
    Ok(())
}

/// A release that records no binding leaves template work the one every
/// node holding the build reports, as for a Direct pull-on-create, rather
/// than passing the gate as unbound.
#[tokio::test(flavor = "current_thread")]
async fn a_release_without_a_binding_takes_its_environment_from_the_build_inventory()
-> Result<(), BoxError> {
    use crate::controllers::substrate::environment_placement::pool_pod_environment;
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let mut substrate = substrate_with_store(db.pool.clone());
    substrate.k8s.environment_placement = EnvironmentPlacementGate::On;
    sandbox(&db.pool, "suspended", None, None, None).await?;
    assert_eq!(
        substrate.template_pod_environment("seed", HASH).await?,
        None,
        "nothing records a binding yet"
    );

    hold_build(&db.pool, E1).await?;
    assert_eq!(
        substrate.template_pod_environment("seed", HASH).await?,
        required(E1),
        "a cold create or an on-demand pod"
    );
    assert_eq!(
        pool_pod_environment(
            EnvironmentPlacementGate::On,
            &db.pool,
            seed_release(&db.pool).await?,
            "seed",
            HASH
        )
        .await?,
        required(E1),
        "a warm-pool pod"
    );

    // A pod requires one label, so holders that disagree leave the work
    // unbound on Kubernetes.
    hold_build(&db.pool, E0).await?;
    assert_eq!(
        substrate.template_pod_environment("seed", HASH).await?,
        None,
        "the holders disagree"
    );
    Ok(())
}

/// Add a ready worker holding the seed build at `HASH`, bound to
/// `environment`.
async fn hold_build(pool: &sqlx::PgPool, environment: &str) -> Result<(), BoxError> {
    hold_build_at(pool, HASH, environment).await
}

async fn hold_build_at(
    pool: &sqlx::PgPool,
    content_hash: &str,
    environment: &str,
) -> Result<(), BoxError> {
    let holder = NodeId::new();
    insert_ready_worker_node(pool, holder).await?;
    sqlx::query(sql!(
        "INSERT INTO node_template_inventory (
             node_id, template_name, content_hash, runtime, arch,
             has_base_snapshot, vcpu, memory_mib, guest_os,
             state_format_name, state_format_version, execution_metadata
         )
         VALUES ($1, 'seed', $2, 'fc', 'aarch64', TRUE, 1, 512, 'linux',
                 'firecracker-snapshot', '9.0.0',
                 jsonb_build_object('environment', $3::TEXT))"
    ))
    .bind(holder)
    .bind(content_hash)
    .bind(environment)
    .execute(pool)
    .await?;
    Ok(())
}

/// A twin's build shares its original's template and hash, so a node
/// holding only the twin's build reports the twin's binding. With the
/// original recording none, that row mustn't bind the original's pool or
/// work that names no release: only the original's own holders can.
#[tokio::test(flavor = "current_thread")]
async fn a_twin_holder_doesnt_bind_an_unbound_original() -> Result<(), BoxError> {
    use crate::controllers::substrate::environment_placement::pool_pod_environment;
    const E2: &str = "sha256:2222222222222222222222222222222222222222222222222222222222222222";
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let mut substrate = substrate_with_store(db.pool.clone());
    substrate.k8s.environment_placement = EnvironmentPlacementGate::On;
    sandbox(&db.pool, "suspended", None, None, None).await?;
    let original = seed_release(&db.pool).await?;
    sqlx::query(sql!(
        "DROP INDEX IF EXISTS idx_template_releases_template_name_hash_unique"
    ))
    .execute(&db.pool)
    .await?;
    let twin: uuid::Uuid = sqlx::query_scalar(sql!(
        "INSERT INTO template_releases (template_name, type, hash, execution_metadata)
         VALUES ('seed', 'fc', $1, jsonb_build_object('environment', $2::TEXT, 'components', '[]'::jsonb))
         RETURNING id"
    ))
    .bind(HASH)
    .bind(E2)
    .fetch_one(&db.pool)
    .await?;
    let gate = EnvironmentPlacementGate::On;

    hold_build(&db.pool, E2).await?;
    assert_eq!(
        pool_pod_environment(gate, &db.pool, original, "seed", HASH).await?,
        None,
        "the original's pool, with only the twin's holder"
    );
    assert_eq!(
        substrate.template_pod_environment("seed", HASH).await?,
        None,
        "work that names no release, with only the twin's holder"
    );

    hold_build(&db.pool, E1).await?;
    assert_eq!(
        pool_pod_environment(gate, &db.pool, original, "seed", HASH).await?,
        required(E1),
        "the original's pool takes its own holder's binding"
    );
    assert_eq!(
        substrate.template_pod_environment("seed", HASH).await?,
        required(E1),
        "work that names no release takes the original's holder's binding"
    );
    assert_eq!(
        pool_pod_environment(gate, &db.pool, twin, "seed", HASH).await?,
        required(E2),
        "the twin's pool reads the twin's own binding"
    );
    Ok(())
}

/// A pool's content hash is its release's stored spelling, which is bare for a
/// release registered before the prefix was required. The holders' exclusion
/// still reads the bindings of the releases under `sha256:`, so a bare holder
/// row of a build bound elsewhere doesn't bind the bare original's pool.
#[tokio::test(flavor = "current_thread")]
async fn a_bare_originals_pool_excludes_bindings_under_the_prefixed_spelling()
-> Result<(), BoxError> {
    use crate::controllers::substrate::environment_placement::pool_pod_environment;
    const E2: &str = "sha256:2222222222222222222222222222222222222222222222222222222222222222";
    let bare = HASH.trim_start_matches("sha256:");
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    sandbox(&db.pool, "suspended", None, None, None).await?;
    let prefixed = seed_release(&db.pool).await?;
    sqlx::query(sql!(
        "UPDATE template_releases
         SET execution_metadata = jsonb_build_object('environment', $1::TEXT, 'components', '[]'::jsonb)
         WHERE id = $2"
    ))
    .bind(E2)
    .bind(prefixed)
    .execute(&db.pool)
    .await?;
    let original: uuid::Uuid = sqlx::query_scalar(sql!(
        "INSERT INTO template_releases (template_name, type, hash)
         VALUES ('seed', 'fc', $1)
         RETURNING id"
    ))
    .bind(bare)
    .fetch_one(&db.pool)
    .await?;

    hold_build_at(&db.pool, bare, E2).await?;
    assert_eq!(
        pool_pod_environment(
            EnvironmentPlacementGate::On,
            &db.pool,
            original,
            "seed",
            bare
        )
        .await?,
        None,
        "the bare original's pool, with only a holder of the prefixed release's build"
    );
    Ok(())
}

/// An unbound release and one bound to the initial environment share the
/// legacy cache partition, so a node holding the build bound to the initial
/// environment holds the unbound release's build. While the old index stands
/// the two coexist only under the digest's two spellings. That holder still
/// binds the unbound original; a twin's holder under the other spelling
/// still doesn't.
#[tokio::test(flavor = "current_thread")]
async fn an_initial_environment_release_doesnt_hide_the_originals_holder() -> Result<(), BoxError> {
    use crate::controllers::substrate::environment_placement::pool_pod_environment;
    const E2: &str = "sha256:2222222222222222222222222222222222222222222222222222222222222222";
    let initial =
        pplx_space_execution_catalog::catalog::initial_environment_reference().to_string();
    let bare = HASH.trim_start_matches("sha256:");
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let mut substrate = substrate_with_store(db.pool.clone());
    substrate.k8s.environment_placement = EnvironmentPlacementGate::On;
    sandbox(&db.pool, "suspended", None, None, None).await?;
    let original = seed_release(&db.pool).await?;
    let insert_bound = |environment: String| {
        let pool = db.pool.clone();
        async move {
            sqlx::query(sql!(
                "INSERT INTO template_releases (template_name, type, hash, execution_metadata)
                 VALUES ('seed', 'fc', $1,
                         jsonb_build_object('environment', $2::TEXT, 'components', '[]'::jsonb))"
            ))
            .bind(bare)
            .bind(environment)
            .execute(&pool)
            .await?;
            Ok::<_, BoxError>(())
        }
    };
    let gate = EnvironmentPlacementGate::On;

    insert_bound(initial.clone()).await?;
    hold_build(&db.pool, &initial).await?;
    assert_eq!(
        pool_pod_environment(gate, &db.pool, original, "seed", HASH).await?,
        required(&initial),
        "the original's pool takes the initial environment's holder"
    );
    assert_eq!(
        substrate.template_pod_environment("seed", HASH).await?,
        required(&initial),
        "work that names no release takes the initial environment's holder"
    );

    sqlx::query(sql!(
        "DROP INDEX IF EXISTS idx_template_releases_template_name_hash_unique"
    ))
    .execute(&db.pool)
    .await?;
    insert_bound(E2.to_owned()).await?;
    hold_build(&db.pool, E2).await?;
    assert_eq!(
        pool_pod_environment(gate, &db.pool, original, "seed", HASH).await?,
        required(&initial),
        "the original's pool, beside a twin's holder under the other spelling"
    );
    assert_eq!(
        substrate.template_pod_environment("seed", HASH).await?,
        required(&initial),
        "work that names no release, beside a twin's holder under the other spelling"
    );
    Ok(())
}

/// The environment has to reach the PodSpec on every context
/// `sandbox_pod_request` renders, as the node-group boundary does
/// (`every_pod_build_path_renders_the_node_group_boundary`): each arm passes
/// `common` through, so a dropped field renders an unbound pod.
#[tokio::test(flavor = "current_thread")]
async fn every_pod_build_path_renders_the_environment() {
    use pplx_space_sandbox_pod::{
        SANDBOX_ENVIRONMENT_ANNOTATION, build_sandbox_pod, environment_label_key,
    };
    let substrate = substrate_with_unreachable_store();
    let sandbox = test_sandbox_with_lifecycle("started");
    let execution = SandboxExecutionRow {
        generation: 1,
        pod_name: "space-sbx-env-g1".to_string(),
        pod_materialized: false,
        state: ActiveExecutionState::Creating,
        create_age_expired: false,
        activation_attempt_id: None,
        route_activation_id: None,
    };
    let owner = SandboxOwner::platform_user(sandbox.user_id);
    for context in [
        PodCreateContext::OnDemand,
        PodCreateContext::Restore(RestoreContext::UserRestore {
            suspend_restore_storage_uri: None,
            suspend_restore_source_node: Some("source-node"),
            direct_node_name: Some("source-node"),
            preferred_node_name: None,
            runtime_upgrade: None,
        }),
        PodCreateContext::Restore(RestoreContext::Evacuation {
            stopped_materialization: None,
            suspend_restore_storage_uri: Some("s3://bucket/key"),
            extra_tolerations: &[],
            extra_required_affinity: None,
        }),
    ] {
        let request = substrate.sandbox_pod_request(
            &sandbox,
            &execution,
            crate::controllers::substrate::kubernetes::pod_ops::SandboxPodRequestInputs {
                sandbox_owner: &owner,
                auto_lifecycle_json: "{}",
                network_policy_json: "{}",
                node_group_placement: None,
                environment: required(E1),
                client_id: None,
            },
            context,
        );
        let pod = build_sandbox_pod(&sandbox_pod_config(&substrate.k8s), request);
        assert_eq!(
            pod.metadata
                .annotations
                .as_ref()
                .unwrap()
                .get(SANDBOX_ENVIRONMENT_ANNOTATION)
                .map(String::as_str),
            Some(E1)
        );
        let key = environment_label_key(&reference(E1));
        let terms = pod
            .spec
            .as_ref()
            .and_then(|spec| spec.affinity.as_ref())
            .and_then(|affinity| affinity.node_affinity.as_ref())
            .and_then(|node_affinity| {
                node_affinity
                    .required_during_scheduling_ignored_during_execution
                    .as_ref()
            })
            .map(|selector| selector.node_selector_terms.clone())
            .unwrap_or_default();
        assert!(!terms.is_empty());
        for term in terms {
            assert!(
                term.match_expressions
                    .unwrap_or_default()
                    .iter()
                    .any(|expression| expression.key == key)
            );
        }
    }
}

/// A drain's stopped re-placement claims the row to `restoring`
/// (`claim_evacuation_restore`), so the lifecycle no longer says the sandbox
/// is stopped while its pod cold boots the pinned disk artifact. The stopped
/// materialization is what names that work: the disk snapshot's binding, or,
/// where it records none, the execution the sandbox last published.
#[tokio::test(flavor = "current_thread")]
async fn a_stopped_replacement_requires_its_pinned_disk_environment_while_restoring()
-> Result<(), BoxError> {
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let mut substrate = substrate_with_store(db.pool.clone());
    substrate.k8s.environment_placement = EnvironmentPlacementGate::On;
    let restoring = sandbox(&db.pool, "restoring", Some(E1), Some(E1), Some(E0)).await?;
    let excluded: Vec<String> = Vec::new();
    let artifact_id = pplx_space_snapshot_model::LocalArtifactId::new();
    let evacuation = |stopped| {
        PodCreateContext::Restore(RestoreContext::Evacuation {
            stopped_materialization: stopped,
            suspend_restore_storage_uri: Some(URI),
            extra_tolerations: &[],
            extra_required_affinity: None,
        })
    };
    let pinned = || {
        evacuation(Some(pplx_space_sandbox_pod::StoppedMaterialization {
            artifact_id,
            excluded_node_names: &excluded,
        }))
    };
    assert_eq!(
        substrate
            .resolve_pod_environment(&restoring, &pinned())
            .await?,
        required(E0),
        "a disk that records no binding boots the published execution"
    );
    assert_eq!(
        substrate
            .resolve_pod_environment(&restoring, &evacuation(None))
            .await?,
        required(E1),
        "a suspended evacuation restores its checkpoint"
    );

    sqlx::query(sql!(
        "INSERT INTO snapshots (id, sandbox_id, type, state, timestamp, org_id,
                                local_artifact_id, artifact_kind, artifact_origin,
                                execution_metadata)
         VALUES ($1, $2, 'disk', 'ready', NOW(), $3, $4, 'disk', 'stop', $5)"
    ))
    .bind(pplx_space_snapshot_model::SnapshotId::new())
    .bind(restoring.id)
    .bind(SEED_ORG_ID)
    .bind(artifact_id.as_uuid())
    .bind(sqlx::types::Json(
        serde_json::json!({"environment": E1, "components": []}),
    ))
    .execute(&db.pool)
    .await?;
    // The source node's deletion takes the published execution with it.
    sqlx::query(sql!(
        "DELETE FROM sandbox_observed_states WHERE sandbox_id = $1"
    ))
    .bind(restoring.id)
    .execute(&db.pool)
    .await?;
    assert_eq!(
        substrate
            .resolve_pod_environment(&restoring, &pinned())
            .await?,
        required(E1),
        "the pinned disk's own binding outlives its source node"
    );
    Ok(())
}

/// Off renders no environment even for a caller that doesn't check the gate
/// first.
#[test]
fn pod_environment_follows_the_gate() {
    use crate::controllers::substrate::environment_placement::pod_environment;
    assert_eq!(
        pod_environment(EnvironmentPlacementGate::Off, reference(E1)),
        None
    );
    assert_eq!(
        pod_environment(EnvironmentPlacementGate::Shadow, reference(E1)),
        Some(PodEnvironment {
            reference: reference(E1),
            required: false,
        })
    );
    assert_eq!(
        pod_environment(EnvironmentPlacementGate::On, reference(E1)),
        required(E1)
    );
}

/// The environment of `pod`, a captured pod create: its annotation, and
/// whether every required node-affinity term requires its label.
pub(in crate::controllers::substrate::kubernetes) fn created_pod_environment(
    pod: &serde_json::Value,
) -> (Option<String>, bool) {
    use pplx_space_sandbox_pod::SANDBOX_ENVIRONMENT_ANNOTATION;
    let annotation = pod
        .pointer("/metadata/annotations")
        .and_then(|annotations| annotations.get(SANDBOX_ENVIRONMENT_ANNOTATION))
        .and_then(serde_json::Value::as_str)
        .map(str::to_string);
    let terms = pod
        .pointer(
            "/spec/affinity/nodeAffinity/requiredDuringSchedulingIgnoredDuringExecution/nodeSelectorTerms",
        )
        .and_then(serde_json::Value::as_array)
        .cloned()
        .unwrap_or_default();
    let required = annotation.as_deref().is_some_and(|recorded| {
        let key = pplx_space_sandbox_pod::environment_label_key(&reference(recorded));
        !terms.is_empty()
            && terms.iter().all(|term| {
                term.get("matchExpressions")
                    .and_then(serde_json::Value::as_array)
                    .is_some_and(|expressions| {
                        expressions.iter().any(|expression| {
                            expression.get("key").and_then(serde_json::Value::as_str)
                                == Some(key.as_str())
                        })
                    })
            })
    });
    (annotation, required)
}

/// `ensure_pod_exists` resolves the environment and hands it to the pod it
/// creates: a started sandbox with no prior state materializes on demand,
/// which boots the template release.
#[tokio::test(flavor = "current_thread")]
async fn a_materialized_on_demand_pod_requires_its_release_environment() -> Result<(), BoxError> {
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let seeded = sandbox(&db.pool, "started", Some(E1), None, None).await?;
    // No prior state, so the recovery materializes on demand rather than
    // restoring: only the release binding is left.
    for statement in [
        sql!(
            "DELETE FROM snapshot_storage_locations
              WHERE snapshot_id IN (SELECT id FROM snapshots WHERE sandbox_id = $1)"
        ),
        sql!("DELETE FROM snapshots WHERE sandbox_id = $1"),
        sql!("DELETE FROM sandbox_observed_states WHERE sandbox_id = $1"),
    ] {
        sqlx::query(statement)
            .bind(seeded.id)
            .execute(&db.pool)
            .await?;
    }
    // `ensure_pod_exists` builds from the row it re-reads.
    sqlx::query(sql!(
        "UPDATE sandboxes SET template_id = 'seed', content_hash = $2 WHERE id = $1"
    ))
    .bind(seeded.id)
    .bind(HASH)
    .execute(&db.pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO sandbox_k8s_executions
             (sandbox_id, generation, pod_name, pod_namespace, state, active)
         VALUES ($1, 2, 'space-sbx-environment-g2', 'space', 'running', TRUE)"
    ))
    .bind(seeded.id)
    .execute(&db.pool)
    .await?;
    let sandbox = crate::controllers::sandbox::fetch_sandbox(&db.pool, seeded.id)
        .await?
        .expect("the seeded sandbox");
    let generation = match substrate_with_store(db.pool.clone())
        .store
        .claim_started_recovery(sandbox.id)
        .await?
    {
        StartedRecoveryClaim::ExistingExecution { generation } => generation,
        claim => panic!("expected the tracked execution's recovery claim, got {claim:?}"),
    };
    let creates = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let mut substrate = substrate_with_store_and_client(
        db.pool.clone(),
        mock_kubernetes_client(std::sync::Arc::clone(&creates)),
    );
    substrate.k8s.environment_placement = EnvironmentPlacementGate::On;

    substrate
        .ensure_pod_exists(
            &sandbox,
            None,
            None,
            None,
            PodMaterializationOwnership::new(
                Some(StartedRecoveryOwnership::ExistingExecution { generation }),
                None,
                None,
            ),
        )
        .await?;

    let creates = creates.lock().unwrap();
    let [pod] = creates.as_slice() else {
        panic!("expected one pod create, got {}", creates.len());
    };
    assert_eq!(
        pod.pointer("/metadata/annotations")
            .and_then(|annotations| {
                annotations.get(pplx_space_sandbox_pod::SANDBOX_CHECKPOINT_ANNOTATION)
            }),
        None,
        "an on-demand pod restores no checkpoint"
    );
    assert_eq!(created_pod_environment(pod), (Some(E1.to_string()), true));
    Ok(())
}

/// The cross-node re-drive builds its successor from scratch, so it resolves
/// the environment itself: the successor of a suspended sandbox's restore
/// requires the checkpoint's environment.
#[tokio::test(flavor = "current_thread")]
async fn a_cross_node_redrive_successor_requires_its_checkpoint_environment() -> Result<(), BoxError>
{
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let sandbox = sandbox(&db.pool, "restoring", Some(E1), Some(E0), Some(E1)).await?;
    let creates = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let mut substrate = substrate_with_store_and_client(
        db.pool.clone(),
        mock_kubernetes_client(std::sync::Arc::clone(&creates)),
    );
    substrate.k8s.environment_placement = EnvironmentPlacementGate::On;
    let claim = substrate
        .store
        .ensure_active_execution_claim(sandbox.id)
        .await?;

    // The pinned predecessor is already gone (the GET answers 404), so the
    // re-drive goes straight to the successor.
    let _ = substrate
        .redrive_unschedulable_restore_cross_node_recorded(
            crate::metrics::sandbox::restore::RedrivePath::Retry,
            &sandbox,
            &claim,
            SandboxLifecycle::Restoring,
            Some("source-node"),
            Some(URI),
            None,
            None,
            None,
        )
        .await;

    let creates = creates.lock().unwrap();
    let [pod] = creates.as_slice() else {
        panic!("expected one successor create, got {}", creates.len());
    };
    assert_eq!(created_pod_environment(pod), (Some(E0.to_string()), true));
    Ok(())
}

/// A failed read of the work's record places the pod as off would in
/// shadow, and fails it on, for template work and a restore alike.
#[tokio::test(flavor = "current_thread")]
async fn shadow_places_work_whose_record_it_cannot_read_as_off_would() {
    let mut substrate = substrate_with_unreachable_store();
    let sandbox = Sandbox {
        content_hash: Some(HASH.to_string()),
        ..test_sandbox_with_lifecycle("suspended")
    };
    for context in [PodCreateContext::OnDemand, restore()] {
        substrate.k8s.environment_placement = EnvironmentPlacementGate::Shadow;
        assert_eq!(
            substrate
                .resolve_pod_environment(&sandbox, &context)
                .await
                .expect("shadow places as off"),
            None
        );
        substrate.k8s.environment_placement = EnvironmentPlacementGate::On;
        assert!(
            substrate
                .resolve_pod_environment(&sandbox, &context)
                .await
                .is_err(),
            "on fails the build"
        );
    }
}

/// A stopped sandbox's own start boots its current disk artifact, whose
/// record outlives the node the sandbox stopped on: deleting that node
/// removes the published execution, and the disk's binding still applies.
#[tokio::test(flavor = "current_thread")]
async fn a_stopped_start_requires_its_disk_environment_after_the_published_execution_is_gone()
-> Result<(), BoxError> {
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let mut substrate = substrate_with_store(db.pool.clone());
    substrate.k8s.environment_placement = EnvironmentPlacementGate::On;
    let stopped = sandbox(&db.pool, "stopped", Some(E0), Some(E0), Some(E0)).await?;
    let artifact_id = pplx_space_snapshot_model::LocalArtifactId::new();
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET current_local_artifact_id = $2,
                current_local_artifact_kind = 'disk',
                current_local_artifact_origin = 'stop'
          WHERE id = $1"
    ))
    .bind(stopped.id)
    .bind(artifact_id.as_uuid())
    .execute(&db.pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO snapshots (id, sandbox_id, type, state, timestamp, org_id,
                                local_artifact_id, artifact_kind, artifact_origin,
                                execution_metadata)
         VALUES ($1, $2, 'disk', 'ready', NOW(), $3, $4, 'disk', 'stop', $5)"
    ))
    .bind(pplx_space_snapshot_model::SnapshotId::new())
    .bind(stopped.id)
    .bind(SEED_ORG_ID)
    .bind(artifact_id.as_uuid())
    .bind(sqlx::types::Json(
        serde_json::json!({"environment": E1, "components": []}),
    ))
    .execute(&db.pool)
    .await?;
    sqlx::query(sql!(
        "DELETE FROM sandbox_observed_states WHERE sandbox_id = $1"
    ))
    .bind(stopped.id)
    .execute(&db.pool)
    .await?;

    assert_eq!(
        substrate
            .resolve_pod_environment(&stopped, &restore())
            .await?,
        required(E1)
    );
    Ok(())
}

/// A restore pinned to its source node skips kube-scheduler, and kubelet's
/// refusal there is terminal, so with the gate on the restore is refused
/// before any pod exists when the reflector shows the source node not
/// publishing the environment. A node the cache doesn't have keeps the pin.
#[tokio::test(flavor = "current_thread")]
async fn a_pinned_restore_is_refused_before_its_pod_when_the_source_node_lacks_the_environment()
-> Result<(), BoxError> {
    use pplx_space_sandbox_pod::{
        ENVIRONMENT_USABLE_LABEL_VALUE, SandboxPodConfigValues, environment_label_key,
    };
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let suspended = sandbox(&db.pool, "suspended", Some(E1), Some(E1), None).await?;
    let node = |name: &str, publishes: Option<&str>| {
        let mut labels = SandboxPodConfigValues::default()
            .base_node_selector()
            .into_labels();
        if let Some(environment) = publishes {
            labels.insert(
                environment_label_key(&reference(environment)),
                ENVIRONMENT_USABLE_LABEL_VALUE.to_string(),
            );
        }
        (*node_with_network_slots(name, labels, Some(8), None, true, false)).clone()
    };
    let mut substrate = substrate_with_inventory_caches_and_pool(
        Vec::new(),
        vec![
            node("publishes-e1", Some(E1)),
            node("publishes-e0", Some(E0)),
        ],
        db.pool.clone(),
    );
    substrate.k8s.environment_placement = EnvironmentPlacementGate::On;
    let pinned = |node_name| {
        PodCreateContext::Restore(RestoreContext::UserRestore {
            suspend_restore_storage_uri: Some(URI),
            suspend_restore_source_node: Some(node_name),
            direct_node_name: Some(node_name),
            preferred_node_name: None,
            runtime_upgrade: None,
        })
    };

    assert_eq!(
        substrate
            .resolve_pod_environment(&suspended, &pinned("publishes-e1"))
            .await?,
        required(E1)
    );
    let refused = substrate
        .resolve_pod_environment(&suspended, &pinned("publishes-e0"))
        .await
        .expect_err("the source node doesn't publish E1");
    assert!(
        matches!(&refused, SubstrateError::Conflict { message } if message.contains("publishes-e0")),
        "{refused:?}"
    );
    assert_eq!(
        substrate
            .resolve_pod_environment(&suspended, &pinned("not-in-the-cache"))
            .await?,
        required(E1),
        "kubelet stays the backstop for a node the cache doesn't have"
    );
    substrate.k8s.environment_placement = EnvironmentPlacementGate::Shadow;
    assert!(
        substrate
            .resolve_pod_environment(&suspended, &pinned("publishes-e0"))
            .await
            .is_ok(),
        "shadow places as off"
    );
    Ok(())
}

/// Startup checks the gate against the substrate's own resume-time upgrade
/// switch: on runs while upgrades are off, and fails startup with them on.
#[tokio::test(flavor = "current_thread")]
async fn startup_refuses_on_only_alongside_resume_time_upgrades() -> Result<(), BoxError> {
    use crate::controllers::substrate::runtime_upgrade::RuntimeUpgradeAtActivation;
    // The check reads only the two switches, so the store never connects.
    let mut substrate = substrate_with_unreachable_store();
    substrate.k8s.environment_placement = EnvironmentPlacementGate::On;
    substrate.k8s.runtime_upgrade_at_activation = RuntimeUpgradeAtActivation::Disabled;
    substrate.k8s.check_environment_placement_support()?;
    substrate.k8s.runtime_upgrade_at_activation = RuntimeUpgradeAtActivation::Enabled;
    assert!(matches!(
        substrate.k8s.check_environment_placement_support(),
        Err(crate::controllers::substrate::SubstrateError::Unsupported { .. })
    ));
    substrate.k8s.environment_placement = EnvironmentPlacementGate::Shadow;
    substrate.k8s.check_environment_placement_support()?;
    Ok(())
}
