use tonic::{Request, Status};

use super::NodeApiAuthenticatedCaller;
use crate::peer_auth::UdsPeerPrincipal;

const TRANSFER_AUTHORITY_HEADER: &str = "x-space-template-transfer-authority";

/// Cluster artifact transfer, including private releases. This grants neither
/// template mutation nor tenant identity assertion.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TemplateTransferAuthority {
    Denied,
    Granted,
}

pub fn require_template_transfer_authority<T>(request: &Request<T>) -> Result<(), Status> {
    if request
        .extensions()
        .get::<NodeApiAuthenticatedCaller>()
        .is_some_and(|caller| {
            caller.template_transfer_authority() == TemplateTransferAuthority::Granted
        })
    {
        Ok(())
    } else {
        Err(Status::permission_denied(
            "template transfer requires artifact-transfer authority",
        ))
    }
}

/// Replace caller-controlled metadata with the authenticated, method-scoped
/// grant. Only template-manager's authenticated node-api UDS peer may assert it.
pub fn prepare_template_export<T>(request: &mut Request<T>) -> Result<(), Status> {
    require_template_transfer_authority(request)?;
    request.metadata_mut().remove(TRANSFER_AUTHORITY_HEADER);
    request.metadata_mut().insert(
        TRANSFER_AUTHORITY_HEADER,
        tonic::metadata::MetadataValue::from_static("granted"),
    );
    Ok(())
}

pub fn authorize_forwarded_template_export<T>(
    request: &Request<T>,
    peer: UdsPeerPrincipal,
) -> Result<(), Status> {
    if peer
        == UdsPeerPrincipal::Service(
            crate::core::host::linux::identities::user_names::SPACE_NODEAPI,
        )
        && request
            .metadata()
            .get(TRANSFER_AUTHORITY_HEADER)
            .and_then(|value| value.to_str().ok())
            != Some("granted")
    {
        return Err(Status::permission_denied(
            "missing forwarded template-transfer authority",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests;
