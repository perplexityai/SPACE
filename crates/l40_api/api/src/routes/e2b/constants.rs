pub(super) const SPACE_ECW_HOSTS_ENV: &str = "SPACE_ECW_HOSTS";

// `ECW_DEFAULT_TEMPLATE_ID` is used when the E2B request omits `templateID`.
// `ECW_DEFAULT_CREATE_TIMEOUT_SECS` follows E2B's SDK create-time sandbox
// timeout default.
// `ECW_RESPONSE_DEFAULT_*` values are only used when SPACE data is
// unavailable while shaping E2B responses.
pub(super) const ECW_DEFAULT_TEMPLATE_ID: &str = "seed";
pub(super) const ECW_DEFAULT_CREATE_TIMEOUT_SECS: u32 = 300;
pub(super) const ECW_RESPONSE_DEFAULT_CPU_COUNT: u32 = 2;
pub(super) const ECW_RESPONSE_DEFAULT_MEMORY_MB: u32 = 1024;
pub(super) const ECW_RESPONSE_DEFAULT_DISK_SIZE_MB: u32 = 0;
pub(super) const ECW_RESPONSE_DEFAULT_TIMEOUT_SECS: u32 = 15;

// E2B SDK gates filesystem upload behavior on semantic envd versions.
pub(super) use crate::e2b::ECW_RESPONSE_DEFAULT_ENVD_VERSION;
