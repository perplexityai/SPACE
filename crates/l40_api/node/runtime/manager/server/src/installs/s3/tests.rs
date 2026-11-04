use pplx_space_common_core::types::{Arch, RuntimeKind, StorageUri};

use super::*;
use crate::installs::source::FIRECRACKER_CI_KERNEL_SOURCE;
use crate::installs::test_support::fc_line;

#[test]
fn the_public_source_addresses_lines_and_objects_by_key() {
    let source = S3KernelSource::new(FIRECRACKER_CI_KERNEL_SOURCE.parse().unwrap());
    assert_eq!(source.runtime(), RuntimeKind::Firecracker);
    assert_eq!(
        source.line_prefix(fc_line("1.15"), Arch::X86_64),
        "firecracker-ci/v1.15/x86_64/vmlinux-"
    );
    for (line, snapshot) in [
        ("1.16.0", "20260603-e656faf58d6c-0"),
        ("1.16.1", "20260603-e656faf58d6c-0"),
        ("1.16.2", "20260603-e656faf58d6c-0"),
        ("1.17.0", "20260909-a8e1c3830545-0"),
    ] {
        for architecture in [Arch::X86_64, Arch::Aarch64] {
            assert_eq!(
                source.line_prefix(fc_line(line), architecture),
                format!("firecracker-ci/{snapshot}/{architecture}/vmlinux-")
            );
        }
    }
    let mirror = S3KernelSource::new("s3://mirror/kernels?region=us-east-1".parse().unwrap());
    assert_eq!(
        mirror.line_prefix(fc_line("1.17"), Arch::Aarch64),
        "kernels/v1.17/aarch64/vmlinux-"
    );
    assert_eq!(source.root().to_string(), FIRECRACKER_CI_KERNEL_SOURCE);
    let object = source.object("firecracker-ci/v1.15/x86_64/vmlinux-6.1.155");
    assert_eq!(
        object.to_string(),
        "s3://spec.ccfc.min/firecracker-ci/v1.15/x86_64/vmlinux-6.1.155?region=us-east-1&path_style=true"
    );
    // Recorded locations round-trip through their text form.
    assert_eq!(object.to_string().parse::<StorageUri>().unwrap(), object);
    assert!(source.holds(&object));
    let elsewhere = StorageUri {
        bucket: "other".to_string(),
        ..object
    };
    assert!(!source.holds(&elsewhere));
}

/// Lists the real public bucket. Run by hand: `cargo test -- --ignored`.
#[tokio::test]
#[ignore = "contacts Firecracker's public kernel bucket"]
async fn the_public_bucket_lists_the_kernels_for_a_line() {
    let source = S3KernelSource::new(FIRECRACKER_CI_KERNEL_SOURCE.parse().unwrap());
    let catalog = pplx_space_node_runtime_core::catalog::RuntimeCatalog::compiled().unwrap();
    for line in ["1.15", "1.16", "1.17"] {
        for architecture in [Arch::X86_64, Arch::Aarch64] {
            let kernels = source.list(fc_line(line), architecture).await.unwrap();
            for pin in catalog.kernels(
                RuntimeKind::Firecracker,
                &line.parse().unwrap(),
                architecture,
            ) {
                assert!(
                    kernels.iter().any(|kernel| {
                        kernel.version.artifact_version() == &pin.version && kernel.size == pin.size
                    }),
                    "{line} {architecture} {} is not published at its pinned size",
                    pin.version
                );
            }
        }
    }
}
