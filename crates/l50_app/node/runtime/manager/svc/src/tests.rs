use super::*;

#[test]
fn release_source_options_reject_credentials_without_echoing_them() {
    for flag in ["--release-source", "--cloud-hypervisor-release-source"] {
        for value in [
            "https://user:synthetic-private-value@example.test/releases",
            "https://example.test/releases?token=synthetic-private-value",
            "https://example.test/releases#synthetic-private-value",
        ] {
            let error = Cli::try_parse_from(["space-rtmgr-svc", flag, value])
                .err()
                .expect("credential-bearing release sources must be refused");
            assert!(!error.to_string().contains("synthetic-private-value"));
        }
        Cli::try_parse_from([
            "space-rtmgr-svc",
            flag,
            "https://example.test/releases/@stable",
        ])
        .unwrap();
    }
}

#[test]
fn the_space_kernel_source_is_opt_in() {
    let firecracker = |args: &[&str]| {
        let cli =
            Cli::try_parse_from(std::iter::once("space-rtmgr-svc").chain(args.iter().copied()))
                .unwrap();
        let mut config = RuntimeManagerRunConfig::try_from(cli).unwrap();
        config.kernel_sources.swap_remove(0)
    };
    let upstream: StorageUri = FIRECRACKER_CI_KERNEL_SOURCE.parse().unwrap();
    // Unset, the Firecracker source is the upstream one alone, as before.
    assert!(matches!(firecracker(&[]), KernelSourceConfig::S3(root) if root == upstream));
    let KernelSourceConfig::SpacePublished {
        root,
        upstream: wrapped,
    } = firecracker(&[
        "--space-kernel-source",
        "s3://kernels/guest-kernels?region=us-east-1",
    ])
    else {
        panic!("the flag configures the SPACE-published source");
    };
    assert_eq!(root.bucket, "kernels");
    assert_eq!(root.key_prefix, "guest-kernels");
    assert!(matches!(*wrapped, KernelSourceConfig::S3(root) if root == upstream));
}
