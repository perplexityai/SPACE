use pplx_space_common_core::types::RuntimeVersion;

use super::*;
use crate::installs::release_source::FIRECRACKER_RELEASE_SOURCE;

fn source() -> GitHubReleaseSource {
    GitHubReleaseSource::new(
        GitHubRuntime::Firecracker,
        FIRECRACKER_RELEASE_SOURCE.parse().unwrap(),
    )
    .unwrap()
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
    let ReleasePayload::TarGz { members } = source.payload(&release("1.15.1"), Arch::X86_64) else {
        panic!("Firecracker uses tar.gz")
    };
    assert_eq!(members[1].binary, "bin/jailer");
    assert_eq!(
        members[1].member,
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
/// node. Contacts GitHub; CI runs it with the ignored tests.
///
/// A wrong checksum fails it, and so does a checksum file GitHub reports
/// missing. GitHub being unavailable skips that pin's comparison instead, so
/// an outage cannot fail a pull request: a 5xx or 429 answer, a connection or
/// body that fails, or no answer within `PATIENCE`, which keeps the test
/// inside its duration ceiling.
#[tokio::test]
#[ignore = "contacts GitHub"]
async fn every_compiled_release_pin_matches_its_published_checksum() {
    const PATIENCE: std::time::Duration = std::time::Duration::from_secs(4);
    let source = source();
    let catalog = pplx_space_node_runtime_core::catalog::RuntimeCatalog::compiled().unwrap();
    let pins: Vec<_> = [Arch::X86_64, Arch::Aarch64]
        .into_iter()
        .flat_map(|architecture| {
            catalog
                .releases(RuntimeKind::Firecracker, architecture)
                .map(move |pinned| (architecture, pinned))
        })
        .collect();
    assert!(pins.len() >= 8, "{}", pins.len());
    // All at once: one after another, the downloads alone took up to 7.7 s.
    let published = futures_util::future::join_all(pins.iter().map(|(architecture, pinned)| {
        let checksum: ReleaseUrl = format!(
            "{}.sha256.txt",
            source.archive(&pinned.release, *architecture)
        )
        .parse()
        .unwrap();
        let source = &source;
        async move {
            tokio::time::timeout(PATIENCE, published_checksum(source, &checksum))
                .await
                .unwrap_or_else(|_| Err(format!("no answer within {}s", PATIENCE.as_secs())))
                .map_err(|unavailable| format!("{checksum}: {unavailable}"))
        }
    }))
    .await;
    for ((architecture, pinned), text) in pins.iter().zip(&published) {
        let text = match text {
            Ok(text) => text,
            Err(unavailable) => {
                eprintln!("skipped, GitHub is unavailable: {unavailable}");
                continue;
            }
        };
        let published = text.split_whitespace().next().unwrap_or_default();
        assert_eq!(
            format!("sha256:{published}"),
            pinned.archive_sha256.to_string(),
            "{} {architecture}",
            pinned.release
        );
    }
}

/// The checksum file's text, or why GitHub could not serve it after two
/// retries. Panics on an answer that means the pin is wrong, such as a 404.
async fn published_checksum(
    source: &GitHubReleaseSource,
    checksum: &ReleaseUrl,
) -> Result<String, String> {
    let mut attempt = 0;
    loop {
        let unavailable = match source.send(checksum).await {
            Ok(response) => match response.text().await {
                Ok(text) => return Ok(text),
                Err(error) => error.to_string(),
            },
            Err(error) if github_unavailable(&error) => error.to_string(),
            Err(error) => panic!("{checksum}: {error:?}"),
        };
        if attempt == 2 {
            return Err(unavailable);
        }
        attempt += 1;
        tokio::time::sleep(std::time::Duration::from_millis(500 * attempt)).await;
    }
}

fn github_unavailable(error: &ReleaseSourceError) -> bool {
    match error {
        ReleaseSourceError::Fetch { source, .. } => source
            .downcast_ref::<reqwest::Error>()
            .is_some_and(|error| match error.status() {
                Some(status) => {
                    status.is_server_error() || status == reqwest::StatusCode::TOO_MANY_REQUESTS
                }
                None => error.is_connect() || error.is_timeout() || error.is_request(),
            }),
        ReleaseSourceError::TimedOut { .. } => true,
        _ => false,
    }
}

#[test]
fn cloud_hypervisor_uses_the_upstream_static_asset_without_a_jailer_or_archive() {
    use crate::installs::release_source::CLOUD_HYPERVISOR_RELEASE_SOURCE;
    let source = GitHubReleaseSource::new(
        GitHubRuntime::CloudHypervisor,
        CLOUD_HYPERVISOR_RELEASE_SOURCE.parse().unwrap(),
    )
    .unwrap();
    let release = RuntimeIdentity {
        kind: RuntimeKind::CloudHypervisor,
        version: "54.0".parse().unwrap(),
    };
    for (architecture, asset) in [
        (Arch::Aarch64, "cloud-hypervisor-static-aarch64"),
        (Arch::X86_64, "cloud-hypervisor-static"),
    ] {
        let location = source.archive(&release, architecture);
        assert_eq!(
            location.as_str(),
            format!(
                "https://github.com/cloud-hypervisor/cloud-hypervisor/releases/download/v54.0/{asset}"
            )
        );
        assert_eq!(
            source.payload(&release, architecture),
            ReleasePayload::Executable
        );
        assert!(source.holds(&location));
    }
    let foreign = FIRECRACKER_RELEASE_SOURCE
        .parse::<ReleaseUrl>()
        .unwrap()
        .join("v1.15.1")
        .join("firecracker-v1.15.1-aarch64.tgz");
    assert!(!source.holds(&foreign));
}
