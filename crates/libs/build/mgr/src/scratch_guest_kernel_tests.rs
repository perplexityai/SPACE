use super::*;
use pplx_space_common_core::types::{ArtifactVersion, Digest};
use std::collections::HashMap;

const KERNEL_VERSION: &str = "vmlinux-6.1.200-custom";
const KERNEL_DIGEST: &str =
    "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

#[test]
fn scratch_kernel_env_is_only_set_for_an_exact_pin() {
    let mut ordinary_command = tokio::process::Command::new("true");
    ordinary_command.env("SPACE_SEED_KERNEL_VERSION", "inherited-version");
    ordinary_command.env("SPACE_SEED_KERNEL_DIGEST", "inherited-digest");
    configure_scratch_guest_kernel(&mut ordinary_command, None);
    let ordinary_env = command_env(&ordinary_command);
    assert_eq!(ordinary_env.get("SPACE_SEED_KERNEL_VERSION"), Some(&None));
    assert_eq!(ordinary_env.get("SPACE_SEED_KERNEL_DIGEST"), Some(&None));

    let pin = GuestKernelPin {
        version: ArtifactVersion::parse(KERNEL_VERSION).expect("valid kernel version"),
        digest: KERNEL_DIGEST
            .parse::<Digest>()
            .expect("valid kernel digest"),
    };
    let mut pinned_command = tokio::process::Command::new("true");
    configure_scratch_guest_kernel(&mut pinned_command, Some(&pin));
    let pinned_env = command_env(&pinned_command);

    assert_eq!(
        pinned_env
            .get("SPACE_SEED_KERNEL_VERSION")
            .and_then(Option::as_deref),
        Some(KERNEL_VERSION)
    );
    assert_eq!(
        pinned_env
            .get("SPACE_SEED_KERNEL_DIGEST")
            .and_then(Option::as_deref),
        Some(KERNEL_DIGEST)
    );
}

fn command_env(command: &tokio::process::Command) -> HashMap<String, Option<String>> {
    command
        .as_std()
        .get_envs()
        .map(|(name, value)| {
            (
                name.to_string_lossy().into_owned(),
                value.map(|value| value.to_string_lossy().into_owned()),
            )
        })
        .collect()
}
