use pplx_space_common_core::types::RuntimeVersion;

use super::*;
use crate::installs::release_source::FIRECRACKER_RELEASE_SOURCE;

fn source() -> GitHubReleaseSource {
    GitHubReleaseSource::new(FIRECRACKER_RELEASE_SOURCE.parse().unwrap()).unwrap()
}

fn release(version: &str) -> RuntimeIdentity {
    RuntimeIdentity {
        kind: RuntimeKind::Firecracker,
        version: RuntimeVersion::parse(version).unwrap(),
    }
}

#[test]
fn archives_are_addressed_as_firecracker_publishes_them() {
    let source = source();
    assert_eq!(source.runtime(), RuntimeKind::Firecracker);
    let archive = source.archive(&release("1.15.1"), Arch::X86_64);
    assert_eq!(
        archive.as_str(),
        "https://github.com/firecracker-microvm/firecracker/releases/download/v1.15.1/firecracker-v1.15.1-x86_64.tgz"
    );
    assert!(source.holds(&archive));
    assert_eq!(
        source.archive_member(&release("1.15.1"), Arch::X86_64, "jailer"),
        "release-v1.15.1-x86_64/jailer-v1.15.1-x86_64"
    );
    assert!(!source.holds(&"https://example.test/firecracker.tgz".parse().unwrap()));
    assert!(
        !source.holds(
            &"https://github.com/firecracker-microvm/firecracker/releases/downloadx/a.tgz"
                .parse()
                .unwrap()
        )
    );
}

/// Cross-checks the compiled pins against the checksums Firecracker
/// publishes beside each archive: a mistyped pin fails here rather than on a
/// node. Contacts GitHub; run by hand: `cargo test -- --ignored`.
#[tokio::test]
#[ignore = "contacts GitHub"]
async fn every_compiled_release_pin_matches_its_published_checksum() {
    let source = source();
    let catalog = pplx_space_node_runtime_core::catalog::RuntimeCatalog::compiled().unwrap();
    let mut checked = 0;
    for architecture in [Arch::X86_64, Arch::Aarch64] {
        for pinned in catalog.releases(RuntimeKind::Firecracker, architecture) {
            let archive = source.archive(&pinned.release, architecture);
            let checksum: ReleaseUrl = format!("{archive}.sha256.txt").parse().unwrap();
            let text = source.send(&checksum).await.unwrap().text().await.unwrap();
            let published = text.split_whitespace().next().unwrap_or_default();
            assert_eq!(
                format!("sha256:{published}"),
                pinned.archive_sha256.to_string(),
                "{} {architecture}",
                pinned.release
            );
            checked += 1;
        }
    }
    assert!(checked >= 8, "{checked}");
}
