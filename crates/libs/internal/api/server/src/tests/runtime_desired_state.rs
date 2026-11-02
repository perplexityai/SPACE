//! `GetNodeRuntimeDesiredState` serves the stored execution policy while its
//! setting is on, and the unretired catalog as a recipe to nodes in the
//! provisioning cohort, each independently, to registered nodes only. A
//! policy it can't read fails the call; a catalog it can't read only omits
//! the recipe.

use super::*;
use std::net::Ipv4Addr;

use crate::proto::{RuntimeEnvironmentDesiredPolicy, RuntimeProvisioningRecipe};
use crate::runtime_desired_state::RuntimeDesiredState;
use crate::runtime_provisioning_cohort::{RuntimeProvisioningCohort, hash_bucket};
use pplx_space_common_core::types::{Digest, EnvironmentReference};
use pplx_space_node_api_client::MAX_RECIPE_ENVIRONMENTS;
use pplx_space_runtime_environment_policy_store::{
    self as policy_store, PolicyChange, PolicyChangeReason, RuntimeEnvironmentState,
};

const NODE_IP: std::net::Ipv4Addr = std::net::Ipv4Addr::new(10, 0, 0, 40);

fn environment(byte: u8) -> EnvironmentReference {
    EnvironmentReference::new(Digest::sha256([byte; 32]))
}

fn change() -> PolicyChange {
    PolicyChange {
        actor: None,
        reason: PolicyChangeReason::new("runtime desired state test").expect("valid reason"),
    }
}

async fn insert_worker(pool: &sqlx::PgPool) -> TestResult<NodeId> {
    let id = NodeId::new();
    insert_data_plane_worker_node(pool, id, &NODE_IP.to_string()).await?;
    Ok(id)
}

async fn insert_node(pool: &sqlx::PgPool, role: &str) -> TestResult<NodeId> {
    Ok(NodeId::from_uuid(
        insert_k8s_node(pool, NODE_IP, role).await?,
    ))
}

fn request(node_id: NodeId) -> Request<GetNodeRuntimeDesiredStateRequest> {
    request_as(node_id, Some(ProtoNodeRole::Worker))
}

fn request_as(
    node_id: NodeId,
    role: Option<ProtoNodeRole>,
) -> Request<GetNodeRuntimeDesiredStateRequest> {
    request_from_node(
        GetNodeRuntimeDesiredStateRequest {
            node_id: node_id.to_string(),
            role: role.map(|role| role as i32),
        },
        NODE_IP.into(),
    )
}

/// A catalog row as a node publish would write it.
async fn insert_catalog(pool: &sqlx::PgPool, environment: &str, retired: bool) -> TestResult {
    sqlx::query(sql!(
        "INSERT INTO runtime_environments (
             environment_ref, runtime, runtime_version, version_sort_key,
             kernel_version, state_format_name, state_format_version, retired_at
         )
         VALUES ($1, 'firecracker', '1.17.0', '{1,17,0}', '6.1.155',
                 'firecracker-snapshot', '12.0.0',
                 CASE WHEN $2 THEN now() END)"
    ))
    .bind(environment)
    .bind(retired)
    .execute(pool)
    .await?;
    Ok(())
}

/// e2, e5 and e7 certified, e6 retired. `write_policy` blocks e2 and gives e5
/// and e7 no state; installation ignores both.
async fn write_catalog(pool: &sqlx::PgPool) -> TestResult {
    insert_catalog(pool, &environment(7).to_string(), false).await?;
    insert_catalog(pool, &environment(6).to_string(), true).await?;
    insert_catalog(pool, &environment(5).to_string(), false).await?;
    insert_catalog(pool, &environment(2).to_string(), false).await?;
    Ok(())
}

fn catalog_recipe() -> Option<RuntimeProvisioningRecipe> {
    Some(RuntimeProvisioningRecipe {
        required_environments: vec![
            environment(2).to_string(),
            environment(5).to_string(),
            environment(7).to_string(),
        ],
    })
}

fn cohort(canary_nodes: &[&str], percent: u8) -> RuntimeProvisioningCohort {
    RuntimeProvisioningCohort::Enabled {
        canary_nodes: canary_nodes.iter().map(|node| (*node).to_owned()).collect(),
        percent,
    }
}

fn admit_every_node() -> RuntimeProvisioningCohort {
    cohort(&[], 100)
}

/// A catalog row this binary can't parse. The schema refuses one, so drop its
/// CHECK in this isolated database to stand in for it.
async fn insert_unreadable_catalog(pool: &sqlx::PgPool) -> TestResult {
    sqlx::query(sql!(
        "ALTER TABLE runtime_environments
         DROP CONSTRAINT runtime_environments_environment_ref_check"
    ))
    .execute(pool)
    .await?;
    insert_catalog(pool, "sha256:not-a-digest", false).await
}

/// Default e0, deprecated e1, blocked e2, legacy off.
async fn write_policy(pool: &sqlx::PgPool) -> TestResult {
    let mut tx = pool.begin().await?;
    policy_store::set_default(&mut tx, environment(0), &change()).await?;
    policy_store::set_environment_state(
        &mut tx,
        environment(1),
        RuntimeEnvironmentState::Deprecated,
        &change(),
    )
    .await?;
    policy_store::set_environment_state(
        &mut tx,
        environment(2),
        RuntimeEnvironmentState::Blocked,
        &change(),
    )
    .await?;
    policy_store::set_legacy_unbound_allowed(&mut tx, false, &change()).await?;
    tx.commit().await?;
    Ok(())
}

/// A state this binary doesn't know, as a newer binary could write.
async fn insert_unreadable_state(pool: &sqlx::PgPool) -> TestResult {
    sqlx::query(sql!(
        "INSERT INTO runtime_environment_states (environment_ref, state, reason)
         VALUES ($1, 'retired', 'written by a newer binary')"
    ))
    .bind(environment(3).to_string())
    .execute(pool)
    .await?;
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn both_settings_off_serve_neither_and_never_read_the_policy() -> TestResult {
    let Some(fixture) = cluster_fixture().await? else {
        return Ok(());
    };
    let node_id = insert_worker(&fixture.service.pool).await?;
    write_policy(&fixture.service.pool).await?;
    write_catalog(&fixture.service.pool).await?;
    // A policy row the store can't read would fail an enabled call; disabled
    // must not touch the policy at all.
    insert_unreadable_state(&fixture.service.pool).await?;

    let response = fixture
        .service
        .get_node_runtime_desired_state(request(node_id))
        .await?
        .into_inner();
    assert_eq!(response.execution_policy, None);
    assert_eq!(response.recipe, None);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn enabled_serves_the_stored_policy() -> TestResult {
    let Some(mut fixture) = cluster_fixture().await? else {
        return Ok(());
    };
    fixture.service.runtime_desired_state = RuntimeDesiredState::Enabled;
    let node_id = insert_worker(&fixture.service.pool).await?;
    write_policy(&fixture.service.pool).await?;

    let response = fixture
        .service
        .get_node_runtime_desired_state(request(node_id))
        .await?
        .into_inner();
    assert_eq!(
        response.execution_policy,
        Some(RuntimeEnvironmentDesiredPolicy {
            generation: 4,
            default_environment_ref: Some(environment(0).to_string()),
            executing_environment_refs: vec![
                environment(0).to_string(),
                environment(1).to_string(),
            ],
            legacy_unbound_allowed: Some(false),
        })
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn enabled_bootstrap_policy_leaves_every_setting_to_the_node() -> TestResult {
    let Some(mut fixture) = cluster_fixture().await? else {
        return Ok(());
    };
    fixture.service.runtime_desired_state = RuntimeDesiredState::Enabled;
    let node_id = insert_worker(&fixture.service.pool).await?;

    let response = fixture
        .service
        .get_node_runtime_desired_state(request(node_id))
        .await?
        .into_inner();
    assert_eq!(
        response.execution_policy,
        Some(RuntimeEnvironmentDesiredPolicy {
            generation: 0,
            default_environment_ref: None,
            executing_environment_refs: Vec::new(),
            legacy_unbound_allowed: None,
        })
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn an_unregistered_node_is_refused() -> TestResult {
    let Some(mut fixture) = cluster_fixture().await? else {
        return Ok(());
    };
    fixture.service.runtime_desired_state = RuntimeDesiredState::Enabled;

    let status = fixture
        .service
        .get_node_runtime_desired_state(request(NodeId::new()))
        .await
        .expect_err("an unregistered node must be refused");
    assert_eq!(status.code(), Code::NotFound);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn an_unreadable_policy_fails_the_call() -> TestResult {
    let Some(mut fixture) = cluster_fixture().await? else {
        return Ok(());
    };
    fixture.service.runtime_desired_state = RuntimeDesiredState::Enabled;
    let node_id = insert_worker(&fixture.service.pool).await?;
    insert_unreadable_state(&fixture.service.pool).await?;

    let status = fixture
        .service
        .get_node_runtime_desired_state(request(node_id))
        .await
        .expect_err("an unknown stored state must fail the call");
    assert_eq!(status.code(), Code::Internal);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn an_admitted_node_with_an_empty_catalog_gets_an_empty_recipe() -> TestResult {
    let Some(mut fixture) = cluster_fixture().await? else {
        return Ok(());
    };
    fixture.service.runtime_desired_state = RuntimeDesiredState::Enabled;
    fixture.service.runtime_provisioning_cohort = admit_every_node();
    let node_id = insert_worker(&fixture.service.pool).await?;

    let response = fixture
        .service
        .get_node_runtime_desired_state(request(node_id))
        .await?
        .into_inner();
    // Present and empty: a recipe that requires nothing, not an absent one.
    assert_eq!(
        response.recipe,
        Some(RuntimeProvisioningRecipe {
            required_environments: Vec::new(),
        })
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn the_recipe_lists_every_unretired_environment_whatever_its_policy() -> TestResult {
    let Some(mut fixture) = cluster_fixture().await? else {
        return Ok(());
    };
    fixture.service.runtime_desired_state = RuntimeDesiredState::Enabled;
    fixture.service.runtime_provisioning_cohort = admit_every_node();
    let node_id = insert_worker(&fixture.service.pool).await?;
    write_policy(&fixture.service.pool).await?;
    write_catalog(&fixture.service.pool).await?;

    let response = fixture
        .service
        .get_node_runtime_desired_state(request(node_id))
        .await?
        .into_inner();
    assert_eq!(response.recipe, catalog_recipe());
    // The recipe doesn't change what executes.
    assert_eq!(
        response
            .execution_policy
            .map(|policy| policy.executing_environment_refs),
        Some(vec![environment(0).to_string(), environment(1).to_string()])
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn builders_get_the_worker_recipe() -> TestResult {
    let Some(mut fixture) = cluster_fixture().await? else {
        return Ok(());
    };
    fixture.service.runtime_desired_state = RuntimeDesiredState::Enabled;
    fixture.service.runtime_provisioning_cohort = admit_every_node();
    let node_id = insert_node(&fixture.service.pool, "builder").await?;
    write_catalog(&fixture.service.pool).await?;

    let response = fixture
        .service
        .get_node_runtime_desired_state(request_as(node_id, Some(ProtoNodeRole::Builder)))
        .await?
        .into_inner();
    assert_eq!(response.recipe, catalog_recipe());
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn cluster_nodes_get_no_recipe_but_the_policy() -> TestResult {
    let Some(mut fixture) = cluster_fixture().await? else {
        return Ok(());
    };
    fixture.service.runtime_desired_state = RuntimeDesiredState::Enabled;
    fixture.service.runtime_provisioning_cohort = admit_every_node();
    let node_id = insert_node(&fixture.service.pool, "cluster").await?;
    write_catalog(&fixture.service.pool).await?;
    // No hint: the registered role decides.
    let response = fixture
        .service
        .get_node_runtime_desired_state(request_as(node_id, None))
        .await?
        .into_inner();
    assert_eq!(response.recipe, None);
    assert!(response.execution_policy.is_some());
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn an_unreadable_catalog_omits_the_recipe_but_serves_the_policy() -> TestResult {
    let Some(mut fixture) = cluster_fixture().await? else {
        return Ok(());
    };
    fixture.service.runtime_desired_state = RuntimeDesiredState::Enabled;
    fixture.service.runtime_provisioning_cohort = admit_every_node();
    let node_id = insert_worker(&fixture.service.pool).await?;
    write_policy(&fixture.service.pool).await?;
    write_catalog(&fixture.service.pool).await?;
    insert_unreadable_catalog(&fixture.service.pool).await?;

    let response = fixture
        .service
        .get_node_runtime_desired_state(request(node_id))
        .await?
        .into_inner();
    assert_eq!(response.recipe, None);
    assert_eq!(
        response
            .execution_policy
            .map(|policy| policy.executing_environment_refs),
        Some(vec![environment(0).to_string(), environment(1).to_string()])
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn the_policy_setting_alone_serves_no_recipe() -> TestResult {
    let Some(mut fixture) = cluster_fixture().await? else {
        return Ok(());
    };
    fixture.service.runtime_desired_state = RuntimeDesiredState::Enabled;
    let node_id = insert_worker(&fixture.service.pool).await?;
    write_catalog(&fixture.service.pool).await?;

    let response = fixture
        .service
        .get_node_runtime_desired_state(request(node_id))
        .await?
        .into_inner();
    assert_eq!(response.recipe, None);
    assert!(response.execution_policy.is_some());
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn the_recipe_setting_alone_serves_no_policy_and_never_reads_it() -> TestResult {
    let Some(mut fixture) = cluster_fixture().await? else {
        return Ok(());
    };
    fixture.service.runtime_provisioning_cohort = admit_every_node();
    let node_id = insert_worker(&fixture.service.pool).await?;
    write_catalog(&fixture.service.pool).await?;
    insert_unreadable_state(&fixture.service.pool).await?;

    let response = fixture
        .service
        .get_node_runtime_desired_state(request(node_id))
        .await?
        .into_inner();
    assert_eq!(response.recipe, catalog_recipe());
    assert_eq!(response.execution_policy, None);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn a_node_outside_the_cohort_gets_no_recipe() -> TestResult {
    let Some(mut fixture) = cluster_fixture().await? else {
        return Ok(());
    };
    let node_id = insert_worker(&fixture.service.pool).await?;
    fixture.service.runtime_provisioning_cohort = cohort(&["another-node"], 0);
    write_catalog(&fixture.service.pool).await?;

    let response = fixture
        .service
        .get_node_runtime_desired_state(request(node_id))
        .await?
        .into_inner();
    assert_eq!(response.recipe, None);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn a_percentage_admits_nodes_by_their_hash() -> TestResult {
    let Some(mut fixture) = cluster_fixture().await? else {
        return Ok(());
    };
    write_catalog(&fixture.service.pool).await?;
    // One node in the lower half of the buckets and one in the upper half.
    // Bounded, so a hash that ignores the node ID fails here instead of
    // spinning: 1,000 draws all on one side has odds of 2^-999.
    let ids: Vec<NodeId> = (0..1_000).map(|_| NodeId::new()).collect();
    let low = *ids
        .iter()
        .find(|id| hash_bucket(**id) < 50)
        .expect("a node in the lower half");
    let high = *ids
        .iter()
        .find(|id| hash_bucket(**id) >= 50)
        .expect("a node in the upper half");
    let nodes = [
        (low, Ipv4Addr::new(10, 0, 0, 41)),
        (high, Ipv4Addr::new(10, 0, 0, 42)),
    ];
    for (id, address) in nodes {
        insert_data_plane_worker_node(&fixture.service.pool, id, &address.to_string()).await?;
    }
    let call = |service: &ClusterApiService, (id, address): (NodeId, Ipv4Addr)| {
        let request = request_from_node(
            GetNodeRuntimeDesiredStateRequest {
                node_id: id.to_string(),
                role: Some(ProtoNodeRole::Worker as i32),
            },
            address.into(),
        );
        let service = service.clone();
        async move {
            service
                .get_node_runtime_desired_state(request)
                .await
                .map(|response| response.into_inner().recipe)
        }
    };
    let [low, high] = nodes;

    fixture.service.runtime_provisioning_cohort = cohort(&[], 50);
    for _ in 0..3 {
        assert_eq!(call(&fixture.service, low).await?, catalog_recipe());
        assert_eq!(call(&fixture.service, high).await?, None);
    }
    fixture.service.runtime_provisioning_cohort = admit_every_node();
    assert_eq!(call(&fixture.service, low).await?, catalog_recipe());
    assert_eq!(call(&fixture.service, high).await?, catalog_recipe());
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn a_canary_matches_by_kubernetes_node_name_or_node_id() -> TestResult {
    let Some(mut fixture) = cluster_fixture().await? else {
        return Ok(());
    };
    let node_id = insert_worker(&fixture.service.pool).await?;
    sqlx::query(sql!(
        "INSERT INTO node_k8s (node_id, k8s_node_name, k8s_node_uid)
         VALUES ($1, 'canary-worker-a', 'canary-worker-a-uid')"
    ))
    .bind(node_id)
    .execute(&fixture.service.pool)
    .await?;
    write_catalog(&fixture.service.pool).await?;
    fixture.service.runtime_provisioning_cohort = cohort(&["canary-worker-a"], 0);

    let by_name = |name: &str| {
        request_from_node(
            GetNodeRuntimeDesiredStateRequest {
                node_id: name.to_owned(),
                role: Some(ProtoNodeRole::Worker as i32),
            },
            NODE_IP.into(),
        )
    };
    let response = fixture
        .service
        .get_node_runtime_desired_state(by_name("canary-worker-a"))
        .await?
        .into_inner();
    assert_eq!(response.recipe, catalog_recipe());

    // Listed by node ID, it matches while the node reports its name.
    fixture.service.runtime_provisioning_cohort = cohort(&[&node_id.to_string()], 0);
    let response = fixture
        .service
        .get_node_runtime_desired_state(by_name("canary-worker-a"))
        .await?
        .into_inner();
    assert_eq!(response.recipe, catalog_recipe());
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn a_catalog_over_the_recipe_limit_omits_the_recipe_but_serves_the_policy() -> TestResult {
    let Some(mut fixture) = cluster_fixture().await? else {
        return Ok(());
    };
    fixture.service.runtime_desired_state = RuntimeDesiredState::Enabled;
    fixture.service.runtime_provisioning_cohort = admit_every_node();
    let node_id = insert_worker(&fixture.service.pool).await?;
    let count = u8::try_from(MAX_RECIPE_ENVIRONMENTS + 1)?;
    for byte in 0..count {
        insert_catalog(&fixture.service.pool, &environment(byte).to_string(), false).await?;
    }

    let response = fixture
        .service
        .get_node_runtime_desired_state(request(node_id))
        .await?
        .into_inner();
    assert_eq!(response.recipe, None);
    assert!(response.execution_policy.is_some());

    // At the limit the recipe is served.
    sqlx::query(sql!(
        "UPDATE runtime_environments SET retired_at = now() WHERE environment_ref = $1"
    ))
    .bind(environment(0).to_string())
    .execute(&fixture.service.pool)
    .await?;
    let response = fixture
        .service
        .get_node_runtime_desired_state(request(node_id))
        .await?
        .into_inner();
    let recipe = response.recipe.expect("a recipe at the limit is served");
    assert_eq!(recipe.required_environments.len(), MAX_RECIPE_ENVIRONMENTS);
    Ok(())
}

/// The catalog read must use `runtime_catalog_pool`, not `pool`. Pinned
/// behaviourally: the injected pool points at a database whose catalog has
/// rows while the service's own pool points at one whose catalog is empty, so
/// only a read on the injected pool lists them.
#[tokio::test(flavor = "current_thread")]
async fn the_catalog_read_uses_the_runtime_catalog_pool() -> TestResult {
    let Some(main) = cluster_fixture().await? else {
        return Ok(());
    };
    let Some(sideline) = cluster_fixture().await? else {
        return Ok(());
    };
    write_catalog(&sideline.service.pool).await?;
    let node_id = insert_worker(&main.service.pool).await?;
    let mut service = main
        .service
        .clone()
        .with_runtime_catalog_pool(sideline.service.pool.clone());
    service.runtime_provisioning_cohort = admit_every_node();

    let response = service
        .get_node_runtime_desired_state(request(node_id))
        .await?
        .into_inner();
    assert_eq!(response.recipe, catalog_recipe());
    Ok(())
}
