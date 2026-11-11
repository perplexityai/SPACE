//! Authoritative environment limits and byte grammar shared with the Go shim.
//! Regenerate with `bash scripts/generate-env-contract.sh`.

/// Maximum number of environment variables accepted in one map.
pub const MAX_ENV_ENTRIES: usize = 256;
/// Maximum UTF-8 byte length for a single environment key.
pub const MAX_ENV_KEY_BYTES: usize = 128;
/// Maximum UTF-8 byte length for a single environment value.
pub const MAX_ENV_VALUE_BYTES: usize = 8192;
/// Maximum combined UTF-8 byte length of all keys and values.
pub const MAX_ENV_TOTAL_BYTES: usize = 64 * 1024;

/// Allowed bytes at the beginning of a shell-safe environment key.
pub const ENV_KEY_FIRST: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz_";
/// Allowed bytes after the first byte of a shell-safe environment key.
pub const ENV_KEY_REST: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz_0123456789";
