use std::path::Path;

use pplx_space_common_core::types::{
    GuestPath, GuestUserName, normalize_linux_guest_path, validate_linux_guest_path_input,
};
use pplx_space_spaced_guest_protocol::invalid_argument_status;
use tonic::Status;

use crate::identity::GuestAccount;

pub(crate) fn effective_user(
    requested_user: Option<String>,
    default_user: &GuestUserName,
) -> Result<GuestUserName, Status> {
    requested_user
        .map(GuestUserName::parse)
        .transpose()
        .map_err(invalid_argument_status)?
        .map_or_else(|| Ok(default_user.clone()), Ok)
}

pub(crate) fn normalize_for_account(
    path: &str,
    account: &GuestAccount,
) -> Result<GuestPath, Status> {
    normalize_for_home(path, account.home())
}

pub(crate) fn normalize_for_home(path: &str, home: &Path) -> Result<GuestPath, Status> {
    validate_linux_guest_path_input(path).map_err(invalid_argument_status)?;
    let home = home
        .to_str()
        .ok_or_else(|| Status::internal("guest account home path must be valid UTF-8"))?;
    let home = GuestPath::parse(home).map_err(invalid_argument_status)?;
    normalize_linux_guest_path(path, &home).map_err(invalid_argument_status)
}

#[cfg(test)]
mod tests;
