//! node-ctl's report that this node's template-manager reads a template
//! twin's own metadata (`NodeCtlStatus.reads_twin_metadata`). The report
//! comes from template-manager's own answer (`GetCapabilities`), never from
//! node-ctl's version: node-ctl and template-manager roll out separately.

use std::time::Duration;

use pplx_space_template_mgr_client::proto::{GetCapabilitiesRequest, GetCapabilitiesResponse};
use tonic::Status;

/// How long a status publication waits for template-manager's answer.
const CAPABILITIES_TIMEOUT: Duration = Duration::from_secs(5);

/// This node's template-manager's answer, as `reads_twin_metadata`. `None`
/// when it can't be read now; a template-manager that predates the RPC
/// answers that it doesn't.
pub(crate) async fn read() -> Option<bool> {
    let mut client = pplx_space_template_mgr_client::lazy();
    match tokio::time::timeout(
        CAPABILITIES_TIMEOUT,
        client.get_capabilities(GetCapabilitiesRequest {}),
    )
    .await
    {
        Ok(answer) => from_answer(answer.map(tonic::Response::into_inner)),
        Err(_elapsed) => None,
    }
}

/// `reads_twin_metadata` from template-manager's `GetCapabilities` answer:
/// its own value, `false` from a server without the RPC, and `None` when the
/// call failed otherwise. Only `Some(true)` lets a template campaign move.
fn from_answer(answer: Result<GetCapabilitiesResponse, Status>) -> Option<bool> {
    match answer {
        Ok(capabilities) => Some(capabilities.reads_twin_metadata),
        Err(status) if status.code() == tonic::Code::Unimplemented => Some(false),
        Err(_) => None,
    }
}

#[cfg(test)]
mod tests;
