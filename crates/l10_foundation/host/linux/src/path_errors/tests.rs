use super::*;
use std::io::{Error, ErrorKind};

#[test]
fn path_shape_and_access_failures_are_permanent() {
    for kind in [
        ErrorKind::NotFound,
        ErrorKind::PermissionDenied,
        ErrorKind::NotADirectory,
        ErrorKind::IsADirectory,
        ErrorKind::InvalidFilename,
        ErrorKind::InvalidInput,
        ErrorKind::Unsupported,
    ] {
        assert!(
            !host_path_io_error_is_transient(&Error::from(kind)),
            "{kind:?} describes the path, so a retry re-reads the same path"
        );
    }
}

#[test]
fn read_failures_are_transient() {
    for kind in [
        ErrorKind::Interrupted,
        ErrorKind::ResourceBusy,
        ErrorKind::TimedOut,
        ErrorKind::StaleNetworkFileHandle,
        ErrorKind::UnexpectedEof,
        ErrorKind::WouldBlock,
        ErrorKind::Other,
    ] {
        assert!(
            host_path_io_error_is_transient(&Error::from(kind)),
            "{kind:?} is a failed read, not a verdict about the path"
        );
    }
}

#[test]
fn a_malformed_path_is_permanent() {
    // An interior NUL never reaches the filesystem at all: `canonicalize`
    // rejects it with `InvalidInput` on every attempt.
    let error = std::fs::canonicalize("/var/cache/se\0ed")
        .expect_err("a path with an interior NUL cannot resolve");
    assert_eq!(error.kind(), ErrorKind::InvalidInput);
    assert!(!host_path_io_error_is_transient(&error));
}

#[test]
fn an_unrecognized_kind_defaults_to_transient() {
    // A bounded retry loop is the cheaper mistake; permanently failing an
    // operation that a retry would have satisfied is not.
    assert!(host_path_io_error_is_transient(&Error::other(
        "kind that io::ErrorKind does not name"
    )));
}
