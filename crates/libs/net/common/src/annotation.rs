//! Shim transport encoding, separate from the canonical policy JSON and hash.

use serde::Serialize;

use crate::{NetworkPolicyV1, TlsInterceptionExemptionV1};

/// Encoded pod policy. Only the encoder can construct this value, so pod
/// builders cannot accidentally forward canonical JSON containing scoped entries.
#[derive(Clone, Debug)]
pub struct NetworkPolicyAnnotation(String);

impl NetworkPolicyAnnotation {
    pub fn encode(policy: &NetworkPolicyV1) -> Result<Self, serde_json::Error> {
        #[derive(Serialize)]
        struct Annotation<'a> {
            #[serde(flatten)]
            legacy: &'a NetworkPolicyV1,
            #[serde(skip_serializing_if = "Option::is_none")]
            tls_interception_exemptions_with_ports: Option<&'a [TlsInterceptionExemptionV1]>,
        }

        let has_ports = policy
            .tls_interception_exemptions
            .iter()
            .any(|entry| entry.port.is_some());
        if !has_ports {
            return serde_json::to_string(policy).map(Self);
        }
        // Older lenient shims see only the all-port projection. The complete
        // ordered list is separate so ignoring the new key keeps interception on.
        let mut legacy = policy.clone();
        legacy
            .tls_interception_exemptions
            .retain(|entry| entry.port.is_none());
        serde_json::to_string(&Annotation {
            legacy: &legacy,
            tls_interception_exemptions_with_ports: Some(&policy.tls_interception_exemptions),
        })
        .map(Self)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}
