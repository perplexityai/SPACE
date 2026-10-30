//! Internal activation RPC hosted beside the API's existing lifecycle owner.
use pplx_space_common_core::types::{SandboxId, parse_grpc_timeout};
use pplx_space_common_svc::{
    auth,
    ingress_admission::{IngressPreparationPermit, preparation_timeout},
};
use pplx_space_node_api_client::{
    activation::IngressActivation,
    proto::{
        ImplicitWakeActivity, PrepareIngressRequest, PrepareIngressResponse,
        QueueImplicitWakeRequest, QueueImplicitWakeResponse,
        ingress_activation_api_server::{IngressActivationApi, IngressActivationApiServer},
    },
};
use std::{future::Future, pin::Pin, sync::Arc};
use tokio_util::{sync::CancellationToken, task::TaskTracker};
use tonic::{Request, Response, Status};

#[derive(Clone)]
struct ActivationService {
    activation: Arc<dyn IngressActivation>,
    shutdown: CancellationToken,
}
#[tonic::async_trait]
impl IngressActivationApi for ActivationService {
    async fn prepare_ingress(
        &self,
        request: Request<PrepareIngressRequest>,
    ) -> Result<Response<PrepareIngressResponse>, Status> {
        if self.shutdown.is_cancelled() {
            return Err(Status::unavailable("activation API is draining"));
        }
        let budget = request
            .metadata()
            .get("grpc-timeout")
            .and_then(|v| v.to_str().ok())
            .and_then(parse_grpc_timeout)
            .unwrap_or(preparation_timeout())
            .min(preparation_timeout());
        if budget.is_zero() {
            return Err(Status::deadline_exceeded(
                "ingress preparation deadline exceeded",
            ));
        }
        let id = request
            .into_inner()
            .sandbox_id
            .parse::<SandboxId>()
            .map_err(|_| Status::invalid_argument("sandbox_id must be a UUID"))?;
        let _waiter = IngressPreparationPermit::acquire(Some(id))?;
        self.activation.prepare(id, budget).await?;
        Ok(Response::new(PrepareIngressResponse {}))
    }

    async fn queue_implicit_wake(
        &self,
        request: Request<QueueImplicitWakeRequest>,
    ) -> Result<Response<QueueImplicitWakeResponse>, Status> {
        if self.shutdown.is_cancelled() {
            return Err(Status::unavailable("activation API is draining"));
        }
        let request = request.into_inner();
        let id = request
            .sandbox_id
            .parse::<SandboxId>()
            .map_err(|_| Status::invalid_argument("sandbox_id must be a UUID"))?;
        // An activity this build does not know is unspecified: access.
        let activity = ImplicitWakeActivity::try_from(request.activity)
            .unwrap_or(ImplicitWakeActivity::Unspecified);
        let queued = self
            .activation
            .queue_implicit_wake(id, activity, preparation_timeout())
            .await?;
        Ok(Response::new(QueueImplicitWakeResponse { queued }))
    }
}

pub(crate) type ActivationServer = Pin<Box<dyn Future<Output = Result<(), Status>> + Send>>;

/// Only configured API deployments host the internal listener. Existing public
/// and admin deployments can upgrade before route resolvers are switched over.
pub(crate) async fn server(
    state: &crate::routes::AppState,
    tasks: TaskTracker,
    shutdown: CancellationToken,
) -> Result<ActivationServer, Status> {
    let address = match std::env::var("SPACE_INGRESS_ACTIVATION_LISTEN") {
        Ok(address) if !address.trim().is_empty() => address,
        Ok(_) | Err(std::env::VarError::NotPresent) => {
            return Ok(Box::pin(async move {
                shutdown.cancelled().await;
                Ok(())
            }));
        }
        Err(error) => return Err(Status::invalid_argument(error.to_string())),
    };
    let listener = tokio::net::TcpListener::bind(&address)
        .await
        .map_err(|error| Status::unavailable(format!("bind activation listener: {error}")))?;
    let token = auth::required_bearer_token(auth::SPACE_INTERNAL_TOKEN_ENV)?;
    let auth_config = auth::NodeApiAuthConfig::from_env(auth::NodeRole::Cluster, token, None)
        .map_err(|error| Status::failed_precondition(error.to_string()))?;
    let auth_layer = auth::NodeApiAuthLayer::from_config(auth_config)
        .await
        .map_err(|error| Status::failed_precondition(error.to_string()))?;
    let service = ActivationService {
        activation: crate::controllers::sandbox::ingress_activation(
            state.pool.clone(),
            state.sandbox_controller.clone(),
            tasks,
            shutdown.clone(),
        ),
        shutdown: shutdown.clone(),
    };
    tracing::info!(listen = %address, "ingress activation listener configured");
    serve(service, listener, auth_layer, shutdown)
}

fn serve(
    service: ActivationService,
    listener: tokio::net::TcpListener,
    auth_layer: auth::NodeApiAuthLayer,
    shutdown: CancellationToken,
) -> Result<ActivationServer, Status> {
    let server = pplx_space_common_grpc::internal_transport::server_builder()
        .map_err(|error| Status::failed_precondition(error.to_string()))?;
    Ok(Box::pin(async move {
        server
            .layer(pplx_space_common_svc::grpc_trace_layer())
            .layer(auth_layer)
            .add_service(
                IngressActivationApiServer::new(service)
                    .max_decoding_message_size(1024)
                    .max_encoding_message_size(1024),
            )
            .serve_with_incoming_shutdown(
                pplx_space_common_grpc::internal_transport::Incoming::new(
                    tonic::transport::server::TcpIncoming::from(listener).with_nodelay(Some(true)),
                    shutdown.clone(),
                ),
                shutdown.cancelled_owned(),
            )
            .await
            .map_err(|error| Status::unavailable(error.to_string()))
    }))
}

#[cfg(test)]
mod tests;
