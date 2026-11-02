//! The sandbox identifier forms every admin sandbox route accepts.
//!
//! An operator holds a sandbox's identity in whatever form the tool in front
//! of them printed: the public `sbx_` ID from the API, the bare UUID from the
//! debug view and node routes, the undashed UUID from logs, or the pod name
//! from `kubectl`. The admin sandbox routes and the debug route resolve all of
//! them through [`parse_admin_sandbox_id`], so a form works on every route or
//! on none.

use pplx_space_common_core::types::SandboxId;
use pplx_space_public_api::PUBLIC_SANDBOX_ID_PREFIX;
use uuid::Uuid;

/// The prefix of the names `pplx_space_common_core::kubernetes::sandbox_pod_name`
/// gives sandbox pods: `space-sbx-<undashed uuid>-g<execution generation>`.
const SANDBOX_POD_NAME_PREFIX: &str = "space-sbx-";
const SANDBOX_POD_GENERATION_PREFIX: &str = "-g";
const DASHED_UUID_LEN: usize = 36;
const UNDASHED_UUID_LEN: usize = 32;

/// The identifier matched none of the forms the admin sandbox routes accept.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[error(
    "sandbox id must be sbx_<uuid>, a dashed or undashed UUID, or a sandbox pod name \
     (space-sbx-<undashed uuid>, optionally with its -g<generation> suffix)"
)]
pub(in crate::routes) struct AdminSandboxIdError;

/// Resolve an admin sandbox route's identifier to the sandbox it names.
///
/// Accepts `sbx_<uuid>`, a bare UUID, and either UUID with its dashes removed,
/// with hex digits in either case, and the sandbox's pod name with or without
/// its generation suffix. Nothing is trimmed, and other UUID spellings (braced,
/// `urn:uuid:`) are refused, so the route answers 400 rather than guessing.
pub(in crate::routes) fn parse_admin_sandbox_id(
    value: &str,
) -> Result<SandboxId, AdminSandboxIdError> {
    let uuid = if let Some(uuid) = value.strip_prefix(PUBLIC_SANDBOX_ID_PREFIX) {
        parse_uuid(uuid)?
    } else if let Some(name) = value.strip_prefix(SANDBOX_POD_NAME_PREFIX) {
        parse_pod_name_uuid(name)?
    } else {
        parse_uuid(value)?
    };
    Ok(SandboxId::from_uuid(uuid))
}

/// A dashed or undashed UUID and nothing else: `Uuid::try_parse` also takes
/// the braced and URN spellings, which no SPACE tool prints.
fn parse_uuid(value: &str) -> Result<Uuid, AdminSandboxIdError> {
    if !matches!(value.len(), DASHED_UUID_LEN | UNDASHED_UUID_LEN) {
        return Err(AdminSandboxIdError);
    }
    Uuid::try_parse(value).map_err(|_| AdminSandboxIdError)
}

/// The UUID of a pod name with its `space-sbx-` prefix removed.
fn parse_pod_name_uuid(name: &str) -> Result<Uuid, AdminSandboxIdError> {
    let (uuid, generation) = match name.split_at_checked(UNDASHED_UUID_LEN) {
        Some(parts) => parts,
        None => return Err(AdminSandboxIdError),
    };
    if !generation.is_empty() {
        let digits = generation
            .strip_prefix(SANDBOX_POD_GENERATION_PREFIX)
            .ok_or(AdminSandboxIdError)?;
        if digits.is_empty() || !digits.bytes().all(|byte| byte.is_ascii_digit()) {
            return Err(AdminSandboxIdError);
        }
    }
    parse_uuid(uuid)
}

#[cfg(test)]
mod tests;
