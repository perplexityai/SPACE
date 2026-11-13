//! Test-only environment overrides, compiled in only with the `test-hooks`
//! feature; see [`test_hook`].

use pplx_space_env_vars::{Environment, ProcessEnvironment};

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
