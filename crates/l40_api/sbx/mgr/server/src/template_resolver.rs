//! How sandbox-manager looks up template builds.
//!
//! Create, start, wake, pool cloning, restore staging and the template
//! network-policy cache ask template-manager's `GetTemplate` through one
//! [`TemplateResolver`], carried in the service options. Production uses
//! [`HostTemplateManager`], which reaches template-manager's fixed Unix socket
//! through the `pplx-space-template-mgr-client` constructors exactly as each
//! caller did directly. Callers keep their own error mapping. Tests substitute
//! a resolver that records requests and returns scripted answers.

use pplx_space_template_mgr_client::ConnectError;
use pplx_space_template_mgr_client::proto::{GetTemplateRequest, GetTemplateResponse};

/// Why a [`TemplateResolver::get_template`] call failed. The connect and RPC
/// failures stay distinct because callers report them differently.
#[derive(Debug, thiserror::Error)]
pub enum TemplateLookupError {
    #[error(transparent)]
    Connect(#[from] ConnectError),
    #[error(transparent)]
    Rpc(#[from] tonic::Status),
}

/// Requests and responses cross the resolver whole, so callers can attach and
/// read gRPC metadata as they could on the client directly.
#[async_trait::async_trait]
pub trait TemplateResolver: Send + Sync {
    /// `GetTemplate` on a client dialed for this call under the default
    /// connect-retry policy.
    async fn get_template(
        &self,
        request: tonic::Request<GetTemplateRequest>,
    ) -> Result<tonic::Response<GetTemplateResponse>, TemplateLookupError>;

    /// `GetTemplate` on the process-wide reconnecting channel, which dials on
    /// first use. Connection failures surface as the RPC status.
    async fn get_template_on_shared_channel(
        &self,
        request: tonic::Request<GetTemplateRequest>,
    ) -> Result<tonic::Response<GetTemplateResponse>, tonic::Status>;
}

/// Template-manager at [`pplx_space_template_mgr_client::SOCKET_PATH`].
pub struct HostTemplateManager;

#[async_trait::async_trait]
impl TemplateResolver for HostTemplateManager {
    async fn get_template(
        &self,
        request: tonic::Request<GetTemplateRequest>,
    ) -> Result<tonic::Response<GetTemplateResponse>, TemplateLookupError> {
        let mut client = pplx_space_template_mgr_client::connect().await?;
        Ok(client.get_template(request).await?)
    }

    async fn get_template_on_shared_channel(
        &self,
        request: tonic::Request<GetTemplateRequest>,
    ) -> Result<tonic::Response<GetTemplateResponse>, tonic::Status> {
        pplx_space_template_mgr_client::lazy()
            .get_template(request)
            .await
    }
}

#[cfg(test)]
pub(crate) mod fake;
