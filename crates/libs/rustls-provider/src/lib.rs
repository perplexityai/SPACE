//! Installs SPACE's process-wide rustls crypto provider, AWS-LC.
//!
//! When a build enables more than one rustls provider, rustls cannot choose
//! one and panics on first use unless the process has installed a default.
//! SPACE standardises on AWS-LC, so a process installs this provider once,
//! before telemetry or any client builds TLS state, and never a competing one.
//! It is its own crate so that a client can install the provider without
//! depending on `pplx-space-common-svc` and the service host behind it.

pub fn install_default_rustls_crypto_provider() {
    match rustls::crypto::aws_lc_rs::default_provider().install_default() {
        Ok(()) => {}
        Err(_already_installed) => {}
    }
}
