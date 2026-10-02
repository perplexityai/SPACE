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
    let kernels = source.list(fc_line("1.15"), Arch::X86_64).await.unwrap();
    let versions: Vec<_> = kernels
        .iter()
        .map(|kernel| kernel.version.to_string())
        .collect();
    assert!(versions.contains(&"6.1.155".to_string()), "{versions:?}");
    assert!(
        kernels.iter().all(|kernel| kernel.md5.is_some()),
        "{kernels:?}"
    );
}
