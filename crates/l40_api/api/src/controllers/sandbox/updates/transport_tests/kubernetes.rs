//! The deadline-update transport tests in the parent module whose fresh
//! client depends on the substrate, run against the Kubernetes substrate
//! under the same names. `RuntimeClient::call` and the observer resolve that
//! client through the substrate: on direct from the `node_direct` endpoint,
//! which the direct tests rewrite to the restarted worker; here from the
//! active execution's pod, which points at the restarted worker from the
//! start. The client a test sends on is dialed directly, as on direct.

use super::*;
use crate::test_support::kubernetes::{
    RouteIdentity, insert_kubernetes_worker_node, insert_live_execution, kubernetes,
    substrate_over_pods,
};

struct KubernetesFixture {
    _db: TestDb,
    controller: SandboxController,
    sandbox: SandboxId,
}

impl KubernetesFixture {
    /// A started sandbox whose active execution's pod reaches `worker`.
    async fn new(
        worker_for: impl AsyncFnOnce(SandboxId) -> TestResult<SimWorkerHarness>,
    ) -> TestResult<(Self, SimWorkerHarness)> {
        let db = pg_test_db().await?;
        let node = insert_kubernetes_worker_node(&db.pool).await?;
        let sandbox = SandboxId::new();
        sqlx::query(sql!(
            "INSERT INTO sandboxes
                 (id, user_id, org_id, managed_by, lifecycle, placement,
                  active_node_id, network_policy_generation)
             VALUES ($1, $2, $3, 'k8s', 'started', 'on_node', $4, 1)"
        ))
        .bind(sandbox)
        .bind(SEED_USER_ID)
        .bind(SEED_ORG_ID)
        .bind(node)
        .execute(&db.pool)
        .await?;
        let pod = insert_live_execution(&db.pool, sandbox, RouteIdentity::Published)
            .await?
            .pod;
        let worker = worker_for(sandbox).await?;
        let (substrate, _) = substrate_over_pods(&db.pool, worker.port(), vec![pod])?;
        let fixture = Self {
            _db: db,
            controller: SandboxController::new(kubernetes(substrate)),
            sandbox,
        };
        Ok((fixture, worker))
    }

    async fn send(
        &self,
        client: RuntimeClient,
        operation: AccessIdleOperation,
    ) -> Result<(), ControllerError> {
        self.controller
            .send_access_idle_deadline(
                client,
                self.sandbox,
                GENERATION,
                timestamp(jiff::Timestamp::now().as_second()),
                tokio::time::Instant::now() + Duration::from_secs(10),
                operation,
            )
            .await
    }
}

/// Same outcome as on direct: the redial that fails before sending is
/// replayed once, on the fresh client resolved through the pod.
#[tokio::test(flavor = "current_thread")]
async fn untransmitted_update_is_replayed_once_on_a_fresh_client() -> TestResult {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    for (label, operation) in [
        (
            "extension",
            AccessIdleOperation::ExtendUntil(timestamp(jiff::Timestamp::now().as_second() + 900)),
        ),
        ("reset", reset_operation()),
    ] {
        let (fixture, worker) = KubernetesFixture::new(start_worker).await?;
        let backend: std::net::SocketAddr = worker.endpoint().parse()?;
        let front = RestartingFront::start(backend).await?;
        let client = crate::controllers::substrate::connect_worker(
            tonic::transport::Endpoint::from_shared(format!("http://{}", front.addr))?,
        )
        .await?;
        front.stop().await;
        // Let the client observe the closed connection, so the next request
        // takes the redial path rather than racing the close.
        tokio::time::sleep(Duration::from_millis(100)).await;

        let mut reader = client.clone();
        let state = reader
            .call(
                &fixture.controller.substrate,
                fixture.sandbox,
                tokio::time::Instant::now() + Duration::from_secs(10),
                tonic::Request::new(pplx_space_sbx_mgr_client::proto::StateRequest {
                    sandbox_ids: vec![fixture.sandbox.to_string()],
                }),
                |mut client, request| async move { client.sandbox_state(request).await },
            )
            .await?
            .into_inner();
        assert_eq!(state.sandboxes.len(), 1);
        assert_eq!(state.sandboxes[0].execution_generation, Some(GENERATION));

        fixture
            .send(client, operation.clone())
            .await
            .map_err(|error| format!("{label}: {error}"))?;

        let record = worker.state().get(&fixture.sandbox.to_string()).await?;
        match operation {
            AccessIdleOperation::ExtendUntil(until) => {
                assert_eq!(record.access_extended_until, Some(until.seconds), "{label}");
            }
            AccessIdleOperation::ResetPolicy(policy) => {
                use prost::Message;
                assert_eq!(record.access_idle_policy, policy.encode_to_vec(), "{label}");
            }
        }
    }
    Ok(())
}

/// Same outcome as on direct: the observer reaches the restarted worker
/// through the pod, cannot attribute a deadline to the interrupted update,
/// and the update is a paced 503 that is never resent.
#[tokio::test(flavor = "current_thread")]
async fn interrupted_update_is_observed_not_resent() -> TestResult {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    for (label, operation) in [
        (
            "extension",
            AccessIdleOperation::ExtendUntil(timestamp(jiff::Timestamp::now().as_second() + 900)),
        ),
        ("reset", reset_operation()),
    ] {
        let (dropping, requests) = start_dropping_worker().await?;
        let (fixture, worker) = KubernetesFixture::new(start_worker).await?;
        let interrupted = crate::controllers::substrate::connect_worker(
            tonic::transport::Endpoint::from_shared(format!("http://{dropping}"))?,
        )
        .await?;
        let before = worker.state().get(&fixture.sandbox.to_string()).await?;

        let error = fixture
            .send(interrupted, operation)
            .await
            .expect_err("an unobserved update cannot succeed");

        assert_eq!(error.status(), StatusCode::SERVICE_UNAVAILABLE, "{label}");
        assert!(error.retry_after_header().is_some(), "{label}");
        assert_eq!(requests.load(Ordering::SeqCst), 1, "{label}: sent once");
        let after = worker.state().get(&fixture.sandbox.to_string()).await?;
        assert_eq!(
            after.access_extended_until, before.access_extended_until,
            "{label}: no resend to the successor"
        );
        assert_eq!(
            after.access_idle_policy, before.access_idle_policy,
            "{label}: no resend to the successor"
        );
    }
    Ok(())
}
