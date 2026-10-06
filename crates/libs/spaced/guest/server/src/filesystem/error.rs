use tonic::Status;

pub(crate) fn map_io_error(error: std::io::Error) -> Status {
    if let Some(raw) = error.raw_os_error() {
        match raw {
            libc::ENOENT => return Status::not_found(error.to_string()),
            libc::EEXIST => return Status::already_exists(error.to_string()),
            libc::EACCES | libc::EPERM | libc::EROFS => {
                return Status::permission_denied(error.to_string());
            }
            libc::ENOSPC | libc::EDQUOT => {
                let mut status = Status::resource_exhausted(error.to_string());
                status.metadata_mut().insert(
                    pplx_space_common_core::spaced::ERROR_REASON_METADATA_KEY,
                    tonic::metadata::MetadataValue::from_static(
                        pplx_space_common_core::spaced::GUEST_STORAGE_FULL,
                    ),
                );
                return status;
            }
            libc::EISDIR | libc::ENOTDIR | libc::EXDEV | libc::ELOOP | libc::ENOEXEC => {
                return Status::failed_precondition(error.to_string());
            }
            // Invalid request content (EINVAL), an overlong path or path
            // component (ENAMETOOLONG), or an exec argv/env exceeding the
            // kernel's limits (E2BIG) is caller input, not a server fault.
            libc::EINVAL | libc::ENAMETOOLONG | libc::E2BIG => {
                return Status::invalid_argument(error.to_string());
            }
            // EBADMSG from the kernel signals on-device corruption; DataLoss
            // distinguishes it from generic Internal in connect codes and
            // telemetry. Retry behavior is unchanged: no layer retries
            // Internal or DataLoss.
            libc::EBADMSG => return Status::data_loss(error.to_string()),
            _ => {}
        }
    }

    match error.kind() {
        std::io::ErrorKind::NotFound => Status::not_found(error.to_string()),
        std::io::ErrorKind::PermissionDenied => Status::permission_denied(error.to_string()),
        std::io::ErrorKind::InvalidInput => Status::invalid_argument(error.to_string()),
        std::io::ErrorKind::AlreadyExists => Status::already_exists(error.to_string()),
        _ => Status::internal(error.to_string()),
    }
}

#[cfg(test)]
mod tests;
