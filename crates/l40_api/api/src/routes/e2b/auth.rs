use super::*;
pub(crate) use crate::e2b::{build_sandbox_response, ecw_token};

pub(super) async fn require_ecw_api_host(
    State(ecw_hosts): State<EcwHostConfig>,
    request: Request,
    next: Next,
) -> Response {
    if ecw_hosts.api_pair_for_headers(request.headers()).is_some() {
        return next.run(request).await;
    }

    E2bApiError::not_found("Endpoint").into_response()
}
