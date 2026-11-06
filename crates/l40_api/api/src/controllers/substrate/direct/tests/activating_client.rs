//! Direct writes the record (`sandboxes.activating_client_id`) from its own
//! receipts, because the worker, not the API, creates the runtime: a resident
//! start that's charged, a wake that started or restored the VM, and an owned
//! restore. A receipt that doesn't commit, or that answers for a runtime the
//! activation didn't create, leaves the record alone.

use pplx_space_common_core::types::ClientId;
use pplx_space_sandbox_store::attribute_runtime::{activating_client, set_activating_client};
use pplx_space_sbx_mgr_client::proto::{SandboxSubstrateKind, SpaceApiRouteIdentity};

use super::*;
use std::sync::Arc;

use crate::controllers::substrate::{
    ActivationAttribution, ActivationClientAttribution, CapPolicy, ChargeState, RecoveryCharge,
};

const PREVIOUS: &str = "previous-client";
const ACTIVATING: &str = "activating-client";

/// A Direct fixture with activation-client attribution on, as the receipts
/// below need to write the record at all.
async fn attributed_fixture() -> TestResult<Option<PostgresFixture>> {
    postgres_fixture_with(ActivationClientAttribution::Enabled).await
}

fn client(id: &str) -> ClientId {
    ClientId::parse(id).expect("test client ids are label values")
}

async fn seed_record(pool: &PgPool, sandbox_id: SandboxId, id: &str) -> TestResult {
    assert!(set_activating_client(pool, sandbox_id, Some(&client(id))).await?);
    Ok(())
}

async fn record(pool: &PgPool, sandbox_id: SandboxId) -> TestResult<Option<String>> {
    Ok(activating_client(pool, sandbox_id)
        .await?
        .expect("sandbox row")
        .map(|id| id.as_str().to_owned()))
}

fn identity(node_id: NodeId, generation: u64) -> SpaceApiRouteIdentity {
    SpaceApiRouteIdentity {
        route_generation: Some(generation),
        current_node_id: Some(node_id.to_string()),
        substrate_kind: SandboxSubstrateKind::Direct as i32,
        execution_generation: Some(generation as i64),
        activation_attempt_id: Some(Uuid::now_v7().to_string()),
        ..Default::default()
    }
}

/// Only an activation that resolved to a client id of its own writes. An
/// inheriting activation that found nothing recorded keeps the record `NULL`
/// rather than erasing a value written since, and one that never resolved the
/// record would only write back what's there.
#[test]
fn only_a_resolved_client_id_is_written() {
    let alpha = client("alpha");
    let native = ClientId::native();
    for (label, attribution, expected) in [
        (
            "client",
            ActivationAttribution::client(alpha.clone()),
            ReceiptRecord::Write(&alpha),
        ),
        (
            "native",
            ActivationAttribution::native(),
            ReceiptRecord::Write(&native),
        ),
        (
            "inherit a record",
            ActivationAttribution::inherit(Some(&alpha)),
            ReceiptRecord::Write(&alpha),
        ),
        (
            "inherit NULL",
            ActivationAttribution::inherit(None),
            ReceiptRecord::Unchanged,
        ),
        (
            "unresolved",
            ActivationAttribution::inherit_unresolved(),
            ReceiptRecord::Unchanged,
        ),
    ] {
        let charge = RecoveryCharge::paid(CapPolicy::Skip, attribution);
        assert_eq!(ReceiptRecord::for_runtime(&charge), expected, "{label}");
    }
}

/// Admits every activation.
struct AdmitAll;

impl crate::controllers::substrate::ActivationAdmission for AdmitAll {
    fn admit(
        &self,
        _class: crate::controllers::substrate::ActivationAdmissionClass,
    ) -> crate::controllers::substrate::ActivationAdmissionFuture<'_> {
        Box::pin(async { Ok(()) })
    }
}

fn start_charge(state: ChargeState) -> RecoveryCharge {
    let attribution = ActivationAttribution::client(client(ACTIVATING));
    match state {
        ChargeState::Enforce => {
            RecoveryCharge::enforce(Some(&Arc::new(AdmitAll)), CapPolicy::Check, attribution)
        }
        ChargeState::Paid => RecoveryCharge::paid(CapPolicy::Reserved, attribution),
        ChargeState::Exempt => RecoveryCharge::exempt(CapPolicy::Skip, attribution),
    }
}

/// A resident start writes the record only when it's charged, here or
/// upstream. An exempt start leaves the previous record.
#[rstest::rstest]
#[case::enforce(ChargeState::Enforce, ACTIVATING)]
#[case::paid(ChargeState::Paid, ACTIVATING)]
#[case::exempt(ChargeState::Exempt, PREVIOUS)]
#[tokio::test(flavor = "current_thread")]
async fn a_charged_start_records_its_client(
    #[case] state: ChargeState,
    #[case] expected: &str,
) -> TestResult {
    let charge = start_charge(state);
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let Some(fixture) = attributed_fixture().await? else {
        return Ok(());
    };
    let sandbox_id = SandboxId::new();
    let node_id = NodeId::new();
    let (server, worker) = start_sim_worker(node_id).await?;
    let mut resident = creating_record(sandbox_id);
    resident.status = "stopped".to_string();
    worker.insert_new(resident).await?;
    insert_sandbox(&fixture.pool, sandbox_id, Some(node_id)).await?;
    insert_direct_route_provenance(&fixture.pool, sandbox_id, 1).await?;
    insert_ready_worker(&fixture.pool, node_id, server.endpoint(), "9.0.0").await?;
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET active_node_id = $2, placement = 'on_node', lifecycle = 'stopped'
          WHERE id = $1"
    ))
    .bind(sandbox_id)
    .bind(node_id)
    .execute(&fixture.pool)
    .await?;
    seed_record(&fixture.pool, sandbox_id, PREVIOUS).await?;

    fixture
        .substrate
        .start_execution(
            pplx_space_metrics::sandbox::api::SandboxApiMetricIdentity::native("start"),
            sandbox_id,
            ActivationOptions {
                blocking: true,
                ready_timeout: Duration::from_secs(5),
            },
            charge,
        )
        .await?;

    assert_eq!(worker.get(&sandbox_id.to_string()).await?.status, "running");
    assert_eq!(
        record(&fixture.pool, sandbox_id).await?.as_deref(),
        Some(expected)
    );
    drop(server);
    Ok(())
}

/// A wake that restores the VM writes the record, through the implicit-access
/// entry that hands the wake its charge.
#[tokio::test(flavor = "current_thread")]
async fn a_wake_that_restores_records_its_client() -> TestResult {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let Some(fixture) = attributed_fixture().await? else {
        return Ok(());
    };
    let node_id = NodeId::new();
    let (server, state) = start_sim_worker(node_id).await?;
    insert_ready_worker(&fixture.pool, node_id, server.endpoint(), "9.0.0").await?;
    let policy = crate::e2b::auto_lifecycle_policy(300, true, true);
    let sandbox_id = SandboxId::new();
    insert_sandbox(&fixture.pool, sandbox_id, Some(node_id)).await?;
    place_suspended_sandbox(&fixture.pool, sandbox_id, node_id, &policy).await?;
    let mut resident = creating_record(sandbox_id);
    resident.auto_lifecycle_authority = PolicyAuthority::Local;
    state.insert_new(resident).await?;
    state.set_status(&sandbox_id.to_string(), "running").await?;
    state
        .apply_access_deadline(
            &sandbox_id.to_string(),
            None,
            Some(prost::Message::encode_to_vec(
                &crate::controllers::auto_lifecycle::policy_to_proto(&policy),
            )),
        )
        .await?;
    state.suspend_to_shared_dir(&sandbox_id.to_string()).await?;
    seed_record(&fixture.pool, sandbox_id, PREVIOUS).await?;
    let sandbox = crate::controllers::sandbox::fetch_sandbox(&fixture.pool, sandbox_id)
        .await?
        .expect("sandbox row");

    fixture
        .substrate
        .access_runtime_client(
            pplx_space_metrics::sandbox::api::SandboxApiMetricIdentity::native("ingress"),
            &sandbox,
            ExecutionActivityKind::Access,
            Duration::from_secs(5),
            RecoveryCharge::paid(
                CapPolicy::Check,
                ActivationAttribution::client(client(ACTIVATING)),
            ),
        )
        .await?;

    assert_eq!(state.get(&sandbox_id.to_string()).await?.status, "running");
    assert_eq!(
        record(&fixture.pool, sandbox_id).await?.as_deref(),
        Some(ACTIVATING)
    );
    drop(server);
    Ok(())
}

/// A wake receipt writes only for a transition that started or restored the
/// VM, or after an identity-free answer that did and was hydrated by a retry.
/// A reconnect to a running or paused VM, or a start through a worker that
/// returns no receipt, leaves the previous record.
#[rstest::rstest]
#[case::start(WakeTransition::Start, true, false, ACTIVATING)]
#[case::restore(WakeTransition::Restore, true, false, ACTIVATING)]
#[case::noop(WakeTransition::Noop, true, false, PREVIOUS)]
#[case::resume(WakeTransition::Resume, true, false, PREVIOUS)]
#[case::wait(WakeTransition::Wait, true, false, PREVIOUS)]
#[case::hydrated_after_start(WakeTransition::Noop, true, true, ACTIVATING)]
#[case::start_without_receipt(WakeTransition::Start, false, false, PREVIOUS)]
#[tokio::test(flavor = "current_thread")]
async fn a_wake_records_its_client_only_when_it_activated(
    #[case] transition: WakeTransition,
    #[case] with_receipt: bool,
    #[case] activated_earlier: bool,
    #[case] expected: &str,
) -> TestResult {
    let Some(fixture) = attributed_fixture().await? else {
        return Ok(());
    };
    let sandbox_id = SandboxId::new();
    let node_id = NodeId::new();
    insert_sandbox(&fixture.pool, sandbox_id, Some(node_id)).await?;
    seed_record(&fixture.pool, sandbox_id, PREVIOUS).await?;
    let activating = client(ACTIVATING);

    fixture
        .substrate
        .persist_wake_route_identity(
            sandbox_id,
            node_id,
            &WakeResponse {
                sandbox_id: sandbox_id.to_string(),
                transition: transition as i32,
                route_identity: with_receipt.then(|| identity(node_id, 2)),
            },
            ReceiptRecord::Write(&activating),
            activated_earlier,
        )
        .await?;

    assert_eq!(
        record(&fixture.pool, sandbox_id).await?.as_deref(),
        Some(expected)
    );
    Ok(())
}

/// The record rides with the route identity: a receipt the monotonic upsert
/// refuses commits neither.
#[tokio::test(flavor = "current_thread")]
async fn a_refused_receipt_leaves_the_record() -> TestResult {
    let Some(fixture) = attributed_fixture().await? else {
        return Ok(());
    };
    let sandbox_id = SandboxId::new();
    let node_id = NodeId::new();
    insert_sandbox(&fixture.pool, sandbox_id, Some(node_id)).await?;
    seed_record(&fixture.pool, sandbox_id, PREVIOUS).await?;
    let activating = client(ACTIVATING);
    let response_sandbox_id = sandbox_id.to_string();
    let persist = |generation| {
        fixture.substrate.persist_route_identity_receipt(
            sandbox_id,
            node_id,
            &response_sandbox_id,
            Some(identity(node_id, generation)),
            DirectRouteIdentityOperation::Start,
            ReceiptRecord::Write(&activating),
        )
    };
    persist(5).await?;
    set_activating_client(&fixture.pool, sandbox_id, Some(&client(PREVIOUS))).await?;

    let error = persist(4)
        .await
        .expect_err("an older receipt must be refused");

    assert!(error.to_string().contains("refused stale"), "{error}");
    assert_eq!(
        record(&fixture.pool, sandbox_id).await?.as_deref(),
        Some(PREVIOUS)
    );
    Ok(())
}

/// The owned restore receipt writes the record in the transaction that
/// commits the restore's placement, and not when the lease was lost.
#[tokio::test(flavor = "current_thread")]
async fn an_owned_restore_records_its_client_only_while_it_owns_the_lease() -> TestResult {
    let Some(fixture) = attributed_fixture().await? else {
        return Ok(());
    };
    let sandbox_id = SandboxId::new();
    let node_id = NodeId::new();
    insert_sandbox(&fixture.pool, sandbox_id, Some(NodeId::new())).await?;
    seed_record(&fixture.pool, sandbox_id, PREVIOUS).await?;
    let activating = client(ACTIVATING);
    let response = pplx_space_sbx_mgr_client::proto::StartResponse {
        sandbox_id: sandbox_id.to_string(),
        ..Default::default()
    };
    let lease = fixture
        .substrate
        .claim_direct_restore_lease(sandbox_id)
        .await?;
    fixture
        .substrate
        .release_direct_restore_lease(sandbox_id, lease)
        .await?;

    let error = fixture
        .substrate
        .persist_owned_restore_route_identity_receipt(
            sandbox_id,
            node_id,
            &response,
            identity(node_id, 2),
            lease,
            ReceiptRecord::Write(&activating),
        )
        .await
        .expect_err("a released lease no longer owns the restore");
    assert!(matches!(error, SubstrateError::RestoreInFlight { .. }));
    assert_eq!(
        record(&fixture.pool, sandbox_id).await?.as_deref(),
        Some(PREVIOUS)
    );

    let lease = fixture
        .substrate
        .claim_direct_restore_lease(sandbox_id)
        .await?;
    fixture
        .substrate
        .persist_owned_restore_route_identity_receipt(
            sandbox_id,
            node_id,
            &response,
            identity(node_id, 2),
            lease,
            ReceiptRecord::Write(&activating),
        )
        .await?;
    assert_eq!(
        record(&fixture.pool, sandbox_id).await?.as_deref(),
        Some(ACTIVATING)
    );
    Ok(())
}

/// With attribution off, neither the start and wake receipt nor the owned
/// restore receipt writes the record, though both still commit.
#[tokio::test(flavor = "current_thread")]
async fn no_receipt_records_a_client_with_attribution_off() -> TestResult {
    let Some(fixture) = postgres_fixture_with(ActivationClientAttribution::Disabled).await? else {
        return Ok(());
    };
    let sandbox_id = SandboxId::new();
    let node_id = NodeId::new();
    insert_sandbox(&fixture.pool, sandbox_id, Some(node_id)).await?;
    seed_record(&fixture.pool, sandbox_id, PREVIOUS).await?;
    let activating = client(ACTIVATING);

    fixture
        .substrate
        .persist_route_identity_receipt(
            sandbox_id,
            node_id,
            &sandbox_id.to_string(),
            Some(identity(node_id, 2)),
            DirectRouteIdentityOperation::Start,
            ReceiptRecord::Write(&activating),
        )
        .await?
        .expect("the receipt commits");
    assert_eq!(
        record(&fixture.pool, sandbox_id).await?.as_deref(),
        Some(PREVIOUS)
    );

    let lease = fixture
        .substrate
        .claim_direct_restore_lease(sandbox_id)
        .await?;
    fixture
        .substrate
        .persist_owned_restore_route_identity_receipt(
            sandbox_id,
            node_id,
            &pplx_space_sbx_mgr_client::proto::StartResponse {
                sandbox_id: sandbox_id.to_string(),
                ..Default::default()
            },
            identity(node_id, 3),
            lease,
            ReceiptRecord::Write(&activating),
        )
        .await?
        .expect("the restore receipt commits");
    assert_eq!(
        record(&fixture.pool, sandbox_id).await?.as_deref(),
        Some(PREVIOUS)
    );
    Ok(())
}
