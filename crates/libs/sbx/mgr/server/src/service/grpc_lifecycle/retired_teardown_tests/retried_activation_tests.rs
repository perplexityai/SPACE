//! A retried activation asks the control plane whether its execution is still
//! current before it starts the runtime: kubelet retries a pod whose first
//! launch failed with the identity the failure kept open, after the control
//! plane may have retired that execution.

use super::*;

use pplx_space_common_core::types::{NodeId, RouteGeneration, SandboxActivationAttemptId};
use pplx_space_node_api_client::proto::{
    ExecutionCurrency, GetExecutionCurrencyRequest, GetExecutionCurrencyResponse,
};

/// A cluster-api stub whose `GetExecutionCurrency` answers `currency` for
/// every execution.
#[derive(Clone, Copy)]
struct CurrencyClusterApi {
    currency: ExecutionCurrency,
}

impl tonic::server::NamedService for CurrencyClusterApi {
    const NAME: &'static str = "space.node.api.v1.ClusterApi";
}

impl tonic::server::UnaryService<GetExecutionCurrencyRequest> for CurrencyClusterApi {
    type Response = GetExecutionCurrencyResponse;
    type Future = std::future::Ready<Result<tonic::Response<Self::Response>, tonic::Status>>;

    fn call(&mut self, _: tonic::Request<GetExecutionCurrencyRequest>) -> Self::Future {
        std::future::ready(Ok(tonic::Response::new(GetExecutionCurrencyResponse {
            currency: self.currency.into(),
        })))
    }
}

impl<B> tower::Service<http::Request<B>> for CurrencyClusterApi
where
    B: http_body::Body + Send + 'static,
    B::Error: Into<tonic::codegen::StdError> + Send + 'static,
{
    type Response = http::Response<tonic::body::Body>;
    type Error = std::convert::Infallible;
    type Future = futures_util::future::BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(
        &mut self,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: http::Request<B>) -> Self::Future {
        let path = request.uri().path().to_owned();
        let stub = *self;
        Box::pin(async move {
            if path.ends_with("/GetExecutionCurrency") {
                let mut grpc = tonic::server::Grpc::new(tonic_prost::ProstCodec::default());
                Ok(grpc.unary(stub, request).await)
            } else {
                Ok(tonic::Status::unimplemented(path).into_http())
            }
        })
    }
}

/// A suspended sandbox whose restore of `GENERATION` began under a cluster-
/// fenced route identity, leaving the open claim a retry of that pod matches.
fn begun_restore() -> (
    SandboxManagerService,
    String,
    crate::db::route_activation_claims::OpenRouteActivationClaim,
) {
    let conn = crate::db::test_conn();
    let id = SandboxId::new().to_string();
    crate::db::fixture::insert_test_sandbox(&conn, &id, SandboxState::Suspended);
    let attempt = SandboxActivationAttemptId::new();
    let route = pplx_space_common_core::types::SpaceApiRouteIdentity {
        sandbox_id: Some(id.parse().unwrap()),
        route_generation: Some(RouteGeneration::new(GENERATION as u64).unwrap()),
        execution_generation: Some(generation(GENERATION)),
        current_node_id: Some(NodeId::new()),
        substrate_kind: Some(pplx_space_common_core::types::NodeManagedBy::Kubernetes),
        activation_attempt_id: Some(attempt),
        pod_uid: Some("retried-pod".parse().unwrap()),
    };
    let provenance = crate::auto_lifecycle_delete_receipts::RouteIdentityProvenance::ClusterFenced;
    let claim =
        crate::db::route_activation_claims::OpenRouteActivationClaim::for_cluster_fenced_identity(
            &route, provenance,
        )
        .unwrap();
    crate::db::begin_activation_with_route_evidence(
        &conn,
        &id,
        SandboxState::Suspended,
        crate::db::ActivationKind::Restore,
        Some(generation(GENERATION)),
        Some(attempt),
        crate::db::ActivationRouteAdmission {
            evidence: crate::auto_lifecycle_delete_receipts::RouteEvidence::from_identity(
                &route, provenance,
            ),
            open_claim: Some(&claim),
            ..Default::default()
        },
    )
    .unwrap()
    .expect("the restore claims the suspended row");
    (SandboxManagerService::for_test(conn), id, claim)
}

/// Asks whether a retry of [`begun_restore`] may start.
async fn retry_of_begun_restore() -> Result<(), tonic::Status> {
    let (service, id, claim) = begun_restore();
    service
        .refuse_retired_activation_retry(&id, Some(&claim))
        .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_retried_activation_of_a_retired_execution_does_not_start() {
    let retired = CurrencyClusterApi {
        currency: ExecutionCurrency::Retired,
    };
    with_cluster_api(retired, async || {
        let error = retry_of_begun_restore()
            .await
            .expect_err("the retired execution's runtime must not start");
        assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    })
    .await;
}

/// Only an answer that the execution was retired holds a retry back.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_retried_activation_of_a_current_execution_starts() {
    let current = CurrencyClusterApi {
        currency: ExecutionCurrency::Current,
    };
    with_cluster_api(current, async || {
        retry_of_begun_restore()
            .await
            .expect("a current execution's retry starts");
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_retried_activation_the_cluster_cannot_place_starts() {
    let unknown = CurrencyClusterApi {
        currency: ExecutionCurrency::Unspecified,
    };
    with_cluster_api(unknown, async || {
        retry_of_begun_restore()
            .await
            .expect("no answer starts the retry as before");
    })
    .await;
}

/// A control plane that predates the question answers `Unimplemented`, and
/// the retry starts as it did before.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_retried_activation_against_a_control_plane_without_the_question_starts() {
    with_cluster_api(RETIRING, async || {
        retry_of_begun_restore()
            .await
            .expect("an unanswered question starts the retry as before");
    })
    .await;
}

/// An activation with no open claim of its own is a first one, which the
/// control plane isn't asked about.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_first_activation_is_not_asked() {
    let retired = CurrencyClusterApi {
        currency: ExecutionCurrency::Retired,
    };
    with_cluster_api(retired, async || {
        let (service, _, claim) = begun_restore();
        let other = SandboxId::new().to_string();
        crate::db::fixture::insert_test_sandbox(
            &service.db.lock().unwrap(),
            &other,
            SandboxState::Suspended,
        );
        service
            .refuse_retired_activation_retry(&other, Some(&claim))
            .await
            .expect("a first activation starts unasked");
    })
    .await;
}
