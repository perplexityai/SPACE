//! Clients and channels to the node services serving a sandbox pod, and the
//! builder node client.

use super::*;

impl KubernetesSubstrate {
    pub(in crate::controllers::substrate::kubernetes) fn endpoint_for_pod_ip(
        pod_ip: PodIp,
        port: u16,
    ) -> Result<tonic::transport::Endpoint, SubstrateError> {
        let addr = pod_ip.socket_addr(port).to_string();
        node_api_endpoint(
            &addr,
            POD_WORKER_CONNECT_TIMEOUT,
            crate::controllers::substrate::NODE_RPC_TIMEOUT,
        )
    }

    pub(in crate::controllers::substrate::kubernetes) async fn pod_worker_client(
        pod_ip: PodIp,
        port: u16,
        connect_timeout: Duration,
    ) -> Option<crate::controllers::substrate::RuntimeClient> {
        Self::pod_worker_channel(pod_ip, port, connect_timeout)
            .await
            .map(crate::controllers::substrate::RuntimeClient::new)
    }

    pub(in crate::controllers::substrate::kubernetes) async fn pod_worker_channel(
        pod_ip: PodIp,
        port: u16,
        connect_timeout: Duration,
    ) -> Option<Channel> {
        let addr = pod_ip.socket_addr(port).to_string();
        let endpoint = node_api_endpoint(
            &addr,
            connect_timeout,
            crate::controllers::substrate::NODE_RPC_TIMEOUT,
        )
        .ok()?;
        crate::controllers::substrate::node_api_transport(endpoint)
            .await
            .ok()?
            .connect()
            .await
            .ok()
    }

    pub(in crate::controllers::substrate::kubernetes) async fn pod_proxy_accepts_tcp(
        pod_ip: PodIp,
        port: u16,
        connect_timeout: Duration,
    ) -> bool {
        let addr = pod_ip.socket_addr(port);
        matches!(
            timeout(connect_timeout, TcpStream::connect(addr)).await,
            Ok(Ok(_stream))
        )
    }

    pub(in crate::controllers::substrate::kubernetes) async fn pod_worker_internal_client(
        pod_ip: PodIp,
        port: u16,
        connect_timeout: Duration,
        require_split_receipt_capability: bool,
    ) -> Result<Option<crate::controllers::substrate::RuntimeInternalClient>, SubstrateError> {
        let Some(channel) = Self::pod_worker_channel(pod_ip, port, connect_timeout).await else {
            return Ok(None);
        };
        let policy_health_channel = crate::controllers::substrate::node_api_channel(
            channel.clone(),
            pplx_space_common_svc::bearer_token::SpaceBearerToken::NodeApiInternal,
        );
        crate::controllers::substrate::require_activation_policy_support(
            policy_health_channel,
            connect_timeout,
        )
        .await
        .map_err(|error| SubstrateError::Unavailable {
            message: error.to_string(),
        })?;
        if !require_split_receipt_capability {
            return Ok(Some(WorkerApiInternalClient::new(
                crate::controllers::substrate::node_api_channel(
                    channel,
                    pplx_space_common_svc::bearer_token::SpaceBearerToken::NodeApiInternal,
                ),
            )));
        }
        let health_channel = crate::controllers::substrate::node_api_channel(
            channel.clone(),
            pplx_space_common_svc::bearer_token::SpaceBearerToken::NodeApiInternal,
        );
        // This is the producer's proof that it reached a node-api which can
        // preserve the separate operation owner. Bound the local probe by the
        // same short budget as connection establishment; the surrounding Wake
        // phase has a much larger deadline and must not be consumed by a hung
        // health service before the actual lifecycle RPC is attempted.
        let response = match timeout(
            connect_timeout,
            tonic_health::pb::health_client::HealthClient::new(health_channel).check(
                tonic_health::pb::HealthCheckRequest {
                    service: pplx_space_sbx_mgr_client::OPERATION_ACTIVATION_RECEIPT_HEALTH_SERVICE
                        .to_string(),
                },
            ),
        )
        .await
        {
            Ok(Ok(response)) => response,
            Ok(Err(error)) => {
                return Err(SubstrateError::Unavailable {
                    message: format!(
                        "worker node-api operation-receipt capability check failed: {error}"
                    ),
                });
            }
            Err(_) => {
                return Err(SubstrateError::Unavailable {
                    message: format!(
                        "worker node-api operation-receipt capability check exceeded {connect_timeout:?}"
                    ),
                });
            }
        };
        if response.into_inner().status
            != tonic_health::pb::health_check_response::ServingStatus::Serving as i32
        {
            return Err(SubstrateError::Unavailable {
                message: "worker node-api does not advertise split operation-receipt support"
                    .to_string(),
            });
        }
        Ok(Some(WorkerApiInternalClient::new(
            crate::controllers::substrate::node_api_channel(
                channel,
                pplx_space_common_svc::bearer_token::SpaceBearerToken::NodeApiInternal,
            ),
        )))
    }

    pub(in crate::controllers::substrate::kubernetes) async fn builder_node_client(
        &self,
    ) -> Result<BuilderClient, ControllerError> {
        self.builder_node_client_at("space-builder-node-api:9800")
            .await
    }

    pub(in crate::controllers::substrate::kubernetes) async fn builder_node_client_at(
        &self,
        addr: &str,
    ) -> Result<BuilderClient, ControllerError> {
        let endpoint = node_api_endpoint(
            addr,
            POD_WORKER_CONNECT_TIMEOUT,
            crate::controllers::substrate::NODE_RPC_TIMEOUT,
        )
        .map_err(ControllerError::from)?;
        let channel = crate::controllers::substrate::node_api_connect_endpoint(endpoint)
            .await?
            .connect_with_retry(
                "builder-node-api",
                addr,
                pplx_space_common_grpc::DEFAULT_TCP_GRPC_CONNECT_RETRY_POLICY,
            )
            .await
            .map_err(|err| {
                ControllerError::service_unavailable(format!(
                    "builder node-api not available: {err}"
                ))
            })?;
        Ok(BuilderClient::new(
            crate::controllers::substrate::node_api_channel(
                channel,
                pplx_space_common_svc::bearer_token::SpaceBearerToken::NodeApi,
            ),
        ))
    }
}
