//! One redacted audit record per issuance call, including refusals. A request
//! rejected before gRPC decoding has no key or certificate to record.

use sha2::{Digest, Sha256};
use std::sync::{Arc, Mutex};
use x509_parser::prelude::{FromDer, X509Certificate};

use crate::{auth::VerifiedAgentIdentity, intermediate_ca::csr::NodeCsr};

/// Install before rate limiting and authentication. The request extension lives
/// through gRPC decoding; its last owner also audits rejection or cancellation
/// before a handler can record an issuance decision.
pub(crate) fn track_request<B>(mut request: http::Request<B>) -> http::Request<B> {
    if request.uri().path() == crate::intermediate_ca::ISSUE_INTERMEDIATE_METHOD_PATH {
        request.extensions_mut().insert(RequestAudit::default());
    }
    request
}

#[derive(Clone, Default)]
pub(crate) struct RequestAudit(Arc<Mutex<AuditState>>);

#[derive(Default)]
struct AuditState {
    identity: Option<VerifiedAgentIdentity>,
    recorded: bool,
}

impl RequestAudit {
    pub(crate) fn set_identity(&self, identity: VerifiedAgentIdentity) {
        self.0.lock().expect("issuance audit lock").identity = Some(identity);
    }
}

impl Drop for AuditState {
    fn drop(&mut self) {
        if !self.recorded {
            emit(
                self.identity.as_ref(),
                None,
                None,
                "error",
                "request_incomplete",
            );
        }
    }
}

pub(crate) fn record(
    audit: Option<&RequestAudit>,
    identity: Option<&VerifiedAgentIdentity>,
    csr: Option<&[u8]>,
    certificate: Option<&[u8]>,
    decision: &str,
    reason: &str,
) {
    if let Some(audit) = audit {
        let mut state = audit.0.lock().expect("issuance audit lock");
        if state.recorded {
            return;
        }
        state.recorded = true;
    }
    emit(identity, csr, certificate, decision, reason);
}

fn emit(
    identity: Option<&VerifiedAgentIdentity>,
    csr: Option<&[u8]>,
    certificate: Option<&[u8]>,
    decision: &str,
    reason: &str,
) {
    let key_sha256 = csr
        .and_then(|csr| NodeCsr::parse(csr).ok())
        .map(|csr| hex::encode(Sha256::digest(csr.public_key_der())));
    let certificate =
        certificate.and_then(|der| X509Certificate::from_der(der).ok().map(|(_, cert)| cert));
    let serial = certificate.as_ref().map(|cert| cert.raw_serial_as_string());
    let not_after = certificate
        .as_ref()
        .map(|cert| cert.validity().not_after.timestamp());
    tracing::info!(
        event = "credential_ca_issuance_audit",
        pod_uid = identity.map(|id| id.pod_uid.as_str()),
        k8s_node_name = identity.map(|id| id.node_name.as_str()),
        k8s_node_uid = identity.map(|id| id.node_uid.as_str()),
        node_id = identity.map(|id| id.node_id.to_string()),
        peer_ip = identity.map(|id| id.peer_ip.to_string()),
        identity_verified = identity.is_some(),
        key_sha256,
        certificate_serial = serial,
        not_after,
        decision,
        reason,
        "node CA issuance audit"
    );
}
