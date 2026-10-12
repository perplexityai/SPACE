use super::*;

#[test]
fn missing_identity_never_authorizes_template_mutation() {
    let request = Request::new(());

    assert_eq!(
        require_template_mutation_authority(&request)
            .unwrap_err()
            .code(),
        tonic::Code::PermissionDenied
    );
}
