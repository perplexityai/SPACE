pub(super) const EPPO_SDK_KEY_ENV: &str = "EPPO_SDK_KEY";
pub(super) const EPPO_BASE_URL_ENV: &str = "EPPO_BASE_URL";

pub(super) fn eppo_sdk_key_from_env() -> Option<String> {
    non_empty_env(EPPO_SDK_KEY_ENV)
}

pub(super) fn non_empty_env(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|value| !value.is_empty())
}

#[cfg(test)]
mod tests;
