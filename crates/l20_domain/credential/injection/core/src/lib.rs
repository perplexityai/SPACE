//! Credential-reference validation and the secret-bearing network-credential
//! wire type shared by Credential Manager, Sandbox Manager, and Network Gateway.

pub mod secret_arn;
pub mod validation;

/// `space.credential.manager.v1.ResolvedNetworkCredential`. Crates that compile
/// protobufs importing it map the message here with `extern_path`.
pub mod proto {
    tonic::include_proto!("space.credential.manager.v1");

    // Generated code omits `debug_redact` until every production compiler
    // supports it. Keep a local `Debug` implementation as the current
    // type-level safeguard against exposing the secret-bearing field.
    impl std::fmt::Debug for ResolvedNetworkCredential {
        fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter
                .debug_struct("ResolvedNetworkCredential")
                .field("credential_reference_id", &self.credential_reference_id)
                .field("value", &"<redacted>")
                .finish()
        }
    }
}

#[cfg(test)]
mod tests;
