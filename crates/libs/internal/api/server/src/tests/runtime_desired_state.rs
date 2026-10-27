//! `GetNodeRuntimeDesiredState` serves the stored execution policy only while
//! enabled, to registered nodes, and fails closed on a policy it can't read.

use super::*;
use crate::proto::RuntimeEnvironmentDesiredPolicy;
use crate::runtime_desired_state::RuntimeDesiredState;
use pplx_space_common_core::types::{Digest, EnvironmentReference};
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

fn request(node_id: NodeId) -> Request<GetNodeRuntimeDesiredStateRequest> {
    request_from_node(
        GetNodeRuntimeDesiredStateRequest {
            node_id: node_id.to_string(),
            role: Some(ProtoNodeRole::Worker as i32),
        },
        NODE_IP.into(),
    )
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
async fn disabled_serves_no_policy_and_never_reads_it() -> TestResult {
    let Some(fixture) = cluster_fixture().await? else {
        return Ok(());
    };
    let node_id = insert_worker(&fixture.service.pool).await?;
    write_policy(&fixture.service.pool).await?;
    // A row the store can't read would fail an enabled call; disabled must not
    // touch the policy at all.
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
