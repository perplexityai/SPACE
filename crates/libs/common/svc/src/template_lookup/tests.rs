use super::*;

#[test]
fn compatibility_receipts_cannot_authorize_a_lookup() {
    let requested = TemplateLookupScope::Cluster;
    for authorized in [
        None,
        Some(TemplateLookupScope::LocalExecution),
        Some(requested),
    ] {
        let mut response = Response::new(());
        requested.receipt(&mut response);
        if let Some(scope) = authorized {
            scope.acknowledge_authorization(&mut response);
        }

        let result = requested.require_authorization(&response);

        assert_eq!(result.is_ok(), authorized == Some(requested));
        if let Err(status) = result {
            assert_eq!(status.code(), tonic::Code::FailedPrecondition);
        }
    }
}
