//! Guards the OCI error surface available to downstream crates.

use pplx_space_build_mgr::oci::{OciBuildError, OciCommandFailure, OciFileOperation, OciOperation};

#[test]
fn oci_error_payload_types_are_public() {
    fn assert_payloads(error: OciBuildError) {
        match error {
            OciBuildError::Command { operation, source } => {
                let _: OciOperation = operation;
                let _: OciCommandFailure = source;
            }
            OciBuildError::File { operation, .. } => {
                let _: OciFileOperation = operation;
            }
            _ => {}
        }
    }

    let _ = assert_payloads;
}
