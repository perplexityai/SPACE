use super::*;
use crate::auth::KubernetesAuthenticationMode;
use crate::core::host::linux::identities::user_names;

#[test]
fn missing_identity_and_forged_metadata_never_authorize_remote_calls() {
    let mut request = Request::new(());
    request
        .metadata_mut()
        .insert(TRANSFER_AUTHORITY_HEADER, "granted".parse().unwrap());

    assert_eq!(
        prepare_template_export(&mut request).unwrap_err().code(),
        tonic::Code::PermissionDenied
    );
}

#[test]
fn forwarding_preserves_only_authenticated_transfer_authority() {
    let peer = UdsPeerPrincipal::Service(user_names::SPACE_NODEAPI);
    let mut request = Request::new(());
    assert_eq!(
        authorize_forwarded_template_export(&request, peer)
            .unwrap_err()
            .code(),
        tonic::Code::PermissionDenied
    );
    request
        .extensions_mut()
        .insert(NodeApiAuthenticatedCaller::static_token(
            KubernetesAuthenticationMode::Disabled,
        ));
    request
        .metadata_mut()
        .insert(TRANSFER_AUTHORITY_HEADER, "denied".parse().unwrap());

    prepare_template_export(&mut request).unwrap();

    authorize_forwarded_template_export(&request, peer).unwrap();
    assert_eq!(
        request
            .metadata()
            .get_all(TRANSFER_AUTHORITY_HEADER)
            .iter()
            .count(),
        1
    );
    authorize_forwarded_template_export(&Request::new(()), UdsPeerPrincipal::Root).unwrap();
}
