use super::*;

#[test]
fn local_feed_replaces_only_its_runtime_and_duplicate_assignments_are_refused() {
    let cli = Cli::try_parse_from([
        "space-rtmgr-svc",
        "--artifact-source",
        "cloud-hypervisor=/opt/artifacts",
    ])
    .unwrap();
    let config = RuntimeManagerRunConfig::try_from(cli).unwrap();
    assert!(matches!(
        config.kernel_sources.as_slice(),
        [KernelSourceConfig::S3(_), KernelSourceConfig::Directory(_)]
    ));
    assert!(matches!(
        config.releases[0].source,
        ReleaseSourceConfig::GitHub(_)
    ));
    assert!(matches!(
        config.releases[1].source,
        ReleaseSourceConfig::Directory(_)
    ));
    let duplicate = Cli::try_parse_from([
        "space-rtmgr-svc",
        "--artifact-source",
        "cloud-hypervisor=/opt/a",
        "--artifact-source",
        "cloud-hypervisor=/opt/b",
    ])
    .unwrap();
    RuntimeManagerRunConfig::try_from(duplicate).unwrap_err();
    for source in ["cloud-hypervisor=relative", "qemu=/opt/artifacts"] {
        assert!(
            Cli::try_parse_from(["space-rtmgr-svc", "--artifact-source", source]).is_err(),
            "{source}"
        );
    }
}
