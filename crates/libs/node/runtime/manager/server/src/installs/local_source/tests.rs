use super::*;
use crate::installs::test_support::{FakeSource, TestPins, fc_line};
use futures_util::TryStreamExt as _;

#[test]
fn durable_locations_preserve_existing_encodings_and_reject_local_escapes() {
    for uri in [
        "https://example.test/runtime",
        "file:///opt/artifacts/release",
    ] {
        let location: ReleaseLocation = uri.parse().unwrap();
        let json = serde_json::to_string(&location).unwrap();
        assert_eq!(json, format!("\"{uri}\""));
        assert_eq!(
            serde_json::from_str::<ReleaseLocation>(&json).unwrap(),
            location
        );
    }
    for uri in [
        "file://remote/path",
        "file:///safe/../secret",
        "file:///safe/%2e%2e/secret",
        "file:///safe?query",
        "file:///safe/./file",
    ] {
        assert!(uri.parse::<LocalSourcePath>().is_err(), "{uri}");
    }
}

#[tokio::test]
async fn kernel_feed_uses_catalog_candidates_and_refuses_symlink_escapes() {
    let dir = tempfile::tempdir().unwrap();
    let kernels = Arc::new(FakeSource::default());
    let offered = kernels.offer("1.15", "6.1.155", b"pinned kernel");
    let pins = TestPins::new(kernels);
    let source = LocalArtifactSource::new(
        format!("firecracker={}", dir.path().display())
            .parse()
            .unwrap(),
        pins,
    );
    assert!(
        source
            .list(fc_line("1.15"), offered.architecture)
            .await
            .unwrap()
            .is_empty()
    );
    let path = source.kernel_path(offered.line, offered.architecture, offered.version.as_str());
    std::fs::create_dir_all(path.as_path().parent().unwrap()).unwrap();
    std::fs::write(path.as_path(), b"pinned kernel").unwrap();
    let listed = source
        .list(offered.line, offered.architecture)
        .await
        .unwrap();
    assert_eq!(listed.len(), 1);
    let request =
        listed[0].download_request(pplx_space_common_core::types::Digest::sha256([1; 32]));
    let chunks: Vec<_> = KernelSource::open(&source, &request)
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    assert_eq!(chunks.concat(), b"pinned kernel");
    let mut foreign = request.clone();
    foreign.location = "file:///etc/passwd".parse().unwrap();
    assert!(matches!(
        KernelSource::open(&source, &foreign).await,
        Err(SourceError::Foreign { .. })
    ));
    std::fs::remove_file(path.as_path()).unwrap();
    let outside = tempfile::NamedTempFile::new().unwrap();
    std::os::unix::fs::symlink(outside.path(), path.as_path()).unwrap();
    assert!(KernelSource::open(&source, &request).await.is_err());
    source
        .list(offered.line, offered.architecture)
        .await
        .unwrap_err();
}

#[test]
fn feed_member_walk_refuses_symlink_directories_and_non_regular_files() {
    let dir = tempfile::tempdir().unwrap();
    let outside = tempfile::tempdir().unwrap();
    std::fs::write(outside.path().join("payload"), b"outside").unwrap();
    std::os::unix::fs::symlink(outside.path(), dir.path().join("linked")).unwrap();
    open_regular(dir.path(), &dir.path().join("linked/payload")).unwrap_err();
    open_regular(dir.path(), &dir.path().join("linked")).unwrap_err();
    std::fs::create_dir(dir.path().join("directory")).unwrap();
    open_regular(dir.path(), &dir.path().join("directory")).unwrap_err();
    let status = std::process::Command::new("mkfifo")
        .arg(dir.path().join("fifo"))
        .status()
        .unwrap();
    assert!(status.success());
    open_regular(dir.path(), &dir.path().join("fifo")).unwrap_err();
}
