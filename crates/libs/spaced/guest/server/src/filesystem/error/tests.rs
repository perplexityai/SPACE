use super::*;

#[test]
fn map_io_error_uses_errno_matrix() {
    let cases = [
        (libc::ENOENT, tonic::Code::NotFound),
        (libc::ENOSPC, tonic::Code::ResourceExhausted),
        (libc::EROFS, tonic::Code::PermissionDenied),
        (libc::EISDIR, tonic::Code::FailedPrecondition),
        (libc::ENOTDIR, tonic::Code::FailedPrecondition),
        (libc::ELOOP, tonic::Code::FailedPrecondition),
        (libc::EINVAL, tonic::Code::InvalidArgument),
        (libc::EXDEV, tonic::Code::FailedPrecondition),
        (libc::EEXIST, tonic::Code::AlreadyExists),
        (libc::EACCES, tonic::Code::PermissionDenied),
        (libc::ENAMETOOLONG, tonic::Code::InvalidArgument),
        (libc::E2BIG, tonic::Code::InvalidArgument),
        (libc::EBADMSG, tonic::Code::DataLoss),
    ];

    for (errno, code) in cases {
        let status = map_io_error(std::io::Error::from_raw_os_error(errno));
        assert_eq!(status.code(), code, "errno {errno}");
    }
}

#[test]
fn a_full_disk_or_quota_is_marked_apart_from_admission_limits() {
    use pplx_space_common_core::spaced::{ERROR_REASON_METADATA_KEY, GUEST_STORAGE_FULL};
    let reason = |status: &tonic::Status| {
        status
            .metadata()
            .get(ERROR_REASON_METADATA_KEY)
            .map(|reason| reason.to_str().unwrap().to_owned())
    };
    for errno in [libc::ENOSPC, libc::EDQUOT] {
        let status = map_io_error(std::io::Error::from_raw_os_error(errno));
        assert_eq!(status.code(), tonic::Code::ResourceExhausted);
        assert_eq!(
            reason(&status).as_deref(),
            Some(GUEST_STORAGE_FULL),
            "errno {errno}"
        );
        assert!(!status.message().is_empty());
        // The reason survives the hop from a filesystem worker to the router.
        let relayed = super::super::operations::checked_outcome(super::super::operations::outcome(
            Err(status),
        ))
        .unwrap_err();
        assert_eq!(reason(&relayed).as_deref(), Some(GUEST_STORAGE_FULL));
    }
    let other = map_io_error(std::io::Error::from_raw_os_error(libc::EACCES));
    assert_eq!(reason(&other), None);
}
