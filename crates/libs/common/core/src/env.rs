//! SPACE-specific environment settings and test-only overrides.
//!
//! The generic parsing (the shared flag vocabulary, the positive-`f64`
//! helpers and the [`Environment`] source) lives in `pplx-space-env-vars`,
//! which has no SPACE dependencies; it is re-exported here by name so the
//! `env::` paths keep resolving.

pub use pplx_space_env_vars::{
    Environment, ProcessEnvironment, env_destructive_gate, env_destructive_gate_var, env_flag,
    env_flag_var, parse_env_flag, parse_positive_f64_env, valid_positive_f64,
};

/// Admission gate for create-time sandbox node-pool placement.
pub const SANDBOX_ALLOW_PLACEMENT_ENV: &str = "SPACE_ALLOW_SANDBOX_PLACEMENT";
/// Placement is operator-only and remains disabled unless explicitly enabled.
pub const DEFAULT_SANDBOX_ALLOW_PLACEMENT: bool = false;

/// Read a test-only override.
///
/// A few settings exist only so kind, smoke scenarios and tests can shrink a
/// timer or flip a scenario switch. They are not part of the public
/// configuration: this returns `None` unless the `test-hooks` feature is
/// compiled in, which release binaries never do. Use it for a value that
/// no operator should set, never for a setting a deployment may need.
#[cfg(feature = "test-hooks")]
pub fn test_hook(env: &dyn Environment, name: &str) -> Option<String> {
    env.var(name).ok()
}

/// See the `test-hooks` variant. Without the feature there is no override.
#[cfg(not(feature = "test-hooks"))]
pub fn test_hook(_env: &dyn Environment, _name: &str) -> Option<String> {
    None
}

/// [`test_hook`] over the live process environment.
pub fn test_hook_var(name: &str) -> Option<String> {
    test_hook(&ProcessEnvironment, name)
}

#[cfg(test)]
mod tests;
