use sha2::{Digest as _, Sha256};
use std::sync::Mutex;

use super::*;

#[test]
fn docker_hub_token_and_access_token_fields_are_compatible() {
    let response = serde_json::from_str::<client::TokenResponse>(
        r#"{"token":"signed-token","access_token":"signed-token"}"#,
    )
    .expect("Docker Hub token response should deserialize");

    assert_eq!(response.into_token().as_deref(), Some("signed-token"));
}

#[rstest::fixture]
fn manifest_body() -> Vec<u8> {
    br#"{"schemaVersion":2}"#.to_vec()
}

#[rstest::rstest]
fn manifest_without_digest_header_uses_computed_digest(manifest_body: Vec<u8>) {
    let expected = Digest::sha256(Sha256::digest(&manifest_body).into());

    let payload = client::verified_manifest_payload(None, manifest_body)
        .expect("registries may omit the manifest digest header");

    assert_eq!(payload.digest, expected);
}

#[rstest::rstest]
fn manifest_digest_header_must_match_body(manifest_body: Vec<u8>) {
    let digest_header = reqwest::header::HeaderValue::from_static(
        "sha256:0000000000000000000000000000000000000000000000000000000000000000",
    );

    let error = match client::verified_manifest_payload(Some(&digest_header), manifest_body) {
        Ok(_) => panic!("a mismatched manifest digest must be rejected"),
        Err(error) => error,
    };

    assert!(matches!(error, RegistryError::ManifestDigestMismatch));
}

#[cfg(feature = "aws-ecr")]
#[rstest::fixture]
fn ecr_repositories() -> Vec<ecr::EcrRepository> {
    ecr::repositories_from_json(
        r#"[{"reference":"111111111111.dkr.ecr.us-east-1.amazonaws.com/example/browser-runtime","region":"us-east-1"}]"#,
    )
    .expect("valid ECR repository configuration")
}

#[cfg(feature = "aws-ecr")]
#[rstest::rstest]
fn ecr_allowlist_does_not_authorize_other_repositories_or_registries(
    ecr_repositories: Vec<ecr::EcrRepository>,
) {
    let allowed = "111111111111.dkr.ecr.us-east-1.amazonaws.com/example/browser-runtime:test"
        .parse()
        .expect("valid allowed ECR reference");
    let sibling = "111111111111.dkr.ecr.us-east-1.amazonaws.com/example/other:latest"
        .parse()
        .expect("valid sibling ECR reference");
    let other_account =
        "222222222222.dkr.ecr.us-east-1.amazonaws.com/example/browser-runtime:latest"
            .parse()
            .expect("valid cross-account ECR reference");
    let arbitrary_registry = "127.0.0.1:5000/example/browser-runtime:latest"
        .parse()
        .expect("valid private registry reference");

    let repository = ecr::repository_for(&allowed, &ecr_repositories)
        .expect("configured repository should be allowed");
    assert_eq!(repository.region.as_ref(), "us-east-1");
    assert!(ecr::repository_for(&sibling, &ecr_repositories).is_none());
    assert!(ecr::repository_for(&other_account, &ecr_repositories).is_none());
    assert!(ecr::repository_for(&arbitrary_registry, &ecr_repositories).is_none());
}

#[cfg(feature = "aws-ecr")]
#[rstest::rstest]
#[case("not JSON")]
#[case(r#"[{"reference":"not a reference","region":"us-east-1"}]"#)]
#[case(r#"[{"reference":"docker.io/library/python","region":""}]"#)]
fn invalid_ecr_repository_configuration_is_rejected(#[case] raw: &str) {
    ecr::repositories_from_json(raw).unwrap_err();
}

struct FixtureSource {
    payload: Vec<u8>,
}

fn payload(body: Vec<u8>) -> ManifestPayload {
    let digest = Digest::sha256(Sha256::digest(&body).into());
    ManifestPayload { digest, body }
}

#[async_trait]
impl ManifestSource for FixtureSource {
    async fn fetch(
        &self,
        _repository: &str,
        _manifest: &str,
    ) -> Result<ManifestPayload, RegistryError> {
        Ok(payload(self.payload.clone()))
    }
}

struct GraphSource {
    root: Vec<u8>,
    child: Vec<u8>,
    child_digest: Digest,
    fetched: Mutex<Vec<String>>,
}

#[async_trait]
impl ManifestSource for GraphSource {
    async fn fetch(
        &self,
        _repository: &str,
        manifest: &str,
    ) -> Result<ManifestPayload, RegistryError> {
        self.fetched.lock().unwrap().push(manifest.to_string());
        if manifest == "3.12-slim" {
            return Ok(payload(self.root.clone()));
        }
        if manifest == self.child_digest.to_string() {
            return Ok(payload(self.child.clone()));
        }
        Err(RegistryError::ManifestRejected {
            status: reqwest::StatusCode::NOT_FOUND,
        })
    }
}

#[rstest::rstest]
#[case(Arch::X86_64, "amd64")]
#[case(Arch::Aarch64, "arm64")]
#[tokio::test]
async fn selected_manifest_alternate_urls_are_rejected(
    #[case] arch: Arch,
    #[case] image_arch: &str,
) {
    let source = FixtureSource {
        payload: format!(
            r#"{{
                "schemaVersion": 2,
                "mediaType": "application/vnd.oci.image.index.v1+json",
                "manifests": [{{
                    "mediaType": "application/vnd.oci.image.manifest.v1+json",
                    "digest": "sha256:1111111111111111111111111111111111111111111111111111111111111111",
                    "size": 1,
                    "urls": ["http://169.254.169.254/latest/meta-data"],
                    "platform": {{"architecture": "{image_arch}", "os": "linux"}}
                }}]
            }}"#
        )
        .into_bytes(),
    };
    let reference = "docker.io/library/python:3.12-slim"
        .parse()
        .expect("valid OCI reference");

    let error = resolve_with_source(&reference, arch, &source)
        .await
        .expect_err("alternate descriptor URL must be rejected");

    assert!(matches!(error, RegistryError::AlternateDownloadUrls));
}

#[tokio::test]
async fn layer_alternate_urls_are_rejected() {
    let source = FixtureSource {
        payload: br#"{
            "schemaVersion": 2,
            "mediaType": "application/vnd.oci.image.manifest.v1+json",
            "config": {
                "mediaType": "application/vnd.oci.image.config.v1+json",
                "digest": "sha256:1111111111111111111111111111111111111111111111111111111111111111",
                "size": 1
            },
            "layers": [{
                "mediaType": "application/vnd.oci.image.layer.v1.tar+gzip",
                "digest": "sha256:2222222222222222222222222222222222222222222222222222222222222222",
                "size": 1,
                "urls": ["http://169.254.169.254/latest/meta-data"]
            }]
        }"#
        .to_vec(),
    };
    let reference = "docker.io/library/python:3.12-slim"
        .parse()
        .expect("valid OCI reference");

    let error = resolve_with_source(&reference, Arch::X86_64, &source)
        .await
        .expect_err("alternate layer URL must be rejected");

    assert!(matches!(error, RegistryError::AlternateDownloadUrls));
}

#[tokio::test]
async fn verified_manifest_graph_is_pinned_to_its_root_digest() {
    let child = br#"{
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "config": {
            "mediaType": "application/vnd.oci.image.config.v1+json",
            "digest": "sha256:1111111111111111111111111111111111111111111111111111111111111111",
            "size": 1
        },
        "layers": [{
            "mediaType": "application/vnd.oci.image.layer.v1.tar+gzip",
            "digest": "sha256:2222222222222222222222222222222222222222222222222222222222222222",
            "size": 1
        }]
    }"#
    .to_vec();
    let child_digest = payload(child.clone()).digest;
    let root = format!(
        r#"{{
            "schemaVersion": 2,
            "mediaType": "application/vnd.oci.image.index.v1+json",
            "manifests": [{{
                "mediaType": "application/vnd.oci.image.manifest.v1+json",
                "digest": "{child_digest}",
                "size": {},
                "platform": {{"architecture": "amd64", "os": "linux"}}
            }}]
        }}"#,
        child.len()
    )
    .into_bytes();
    let root_digest = payload(root.clone()).digest;
    let source = GraphSource {
        root,
        child,
        child_digest,
        fetched: Mutex::new(Vec::new()),
    };
    let reference = "docker.io/library/python:3.12-slim"
        .parse::<OciReference>()
        .expect("valid OCI reference");

    let resolved = resolve_with_source(&reference, Arch::X86_64, &source)
        .await
        .expect("safe graph should resolve");

    assert_eq!(
        resolved.reference,
        reference.clone_with_digest(root_digest.to_string())
    );
    assert_eq!(
        resolved.config_digest.to_string(),
        "sha256:1111111111111111111111111111111111111111111111111111111111111111"
    );
    assert_eq!(
        *source.fetched.lock().unwrap(),
        ["3.12-slim".to_string(), child_digest.to_string()]
    );
}
