//! Origin dispatch returns native failures; the request coordinator owns replies
//! and terminal accounting. Header preparation checks current authorization.

use super::credentials::{credential_headers, inject};
use super::*;
use crate::certificates::NetworkCredentialAccessError;

#[derive(Debug, thiserror::Error)]
pub(super) enum DispatchError {
    #[error("origin setup failed")]
    Setup(#[source] Arc<upstream::Error>),
    #[error("origin sender failed before request submission")]
    Ready(#[source] hyper::Error),
    #[error("CONNECT requires an HTTP/1 origin")]
    InvalidConnect,
    #[error("request credentials are unavailable: {0:?}")]
    Credentials(NetworkCredentialAccessError),
    #[error("origin request failed")]
    Send(#[source] hyper::Error),
}

/// This check belongs after origin setup and sender readiness, immediately
/// before submission. Initial request admission is not permission to send later.
pub(super) fn prepare<B>(
    mut request: SanitizedRequest<B>,
    lease: &CertificateLease,
    protocol: HttpProtocol,
) -> Result<SanitizedRequest<B>, DispatchError> {
    if request.method() == http::Method::CONNECT {
        if protocol != HttpProtocol::Http1 {
            return Err(DispatchError::InvalidConnect);
        }
        if !lease.is_usable() {
            return Err(DispatchError::Credentials(
                NetworkCredentialAccessError::Stale,
            ));
        }
    } else {
        let headers = credential_headers(lease, &request).map_err(DispatchError::Credentials)?;
        inject(&mut request, headers);
        if protocol == HttpProtocol::Http1 {
            translate_to_http1(&mut request);
        }
    }
    Ok(request)
}

fn translate_to_http1<B>(request: &mut SanitizedRequest<B>) {
    // Preserve the exact wire authority when translating: default ports can
    // change signatures.
    if !request.headers().contains_key(header::HOST) {
        let authority = request
            .uri()
            .authority()
            .expect("validated HTTP/2 authority");
        let host = authority.as_str().parse().expect("validated authority");
        request.set_host(host);
    }
    *request.uri_mut() = request
        .uri()
        .path_and_query()
        .map_or("/", |path| path.as_str())
        .parse()
        .expect("Hyper parsed path");
    *request.version_mut() = Version::HTTP_11;
}
