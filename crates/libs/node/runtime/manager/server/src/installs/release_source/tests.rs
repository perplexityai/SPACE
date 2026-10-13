use pplx_space_common_core::types::RuntimeVersion;

use super::*;

fn archive() -> ReleaseUrl {
    "https://example.test/v1.16.2/firecracker-v1.16.2-x86_64.tgz"
        .parse()
        .unwrap()
}

const HEX: &str = "d4a32ab2322d887ca1bc4a4e7afa9cc35393e6362dfc2b3becb389d362e4275a";

#[test]
fn a_release_archive_is_an_https_url() {
    archive();
    for value in [
        "",
        "http://example.test/a.tgz",
        "https://",
        "https:///a.tgz",
        "https://a b",
        "https://example.test/a.tgz?token=1",
    ] {
        value.parse::<ReleaseUrl>().expect_err(value);
    }
}

#[test]
fn a_verifiable_release_round_trips_as_a_job_payload() {
    let release = VerifiableRelease {
        release: RuntimeIdentity {
            kind: RuntimeKind::Firecracker,
            version: RuntimeVersion::parse("1.16.2").unwrap(),
        },
        architecture: Arch::X86_64,
        archive: archive(),
        sha256: format!("sha256:{HEX}").parse().unwrap(),
    };
    let json = serde_json::to_value(&release).unwrap();
    assert_eq!(json["release"]["kind"], "firecracker");
    assert_eq!(json["release"]["version"], "1.16.2");
    assert_eq!(
        serde_json::from_value::<VerifiableRelease>(json).unwrap(),
        release
    );
}
