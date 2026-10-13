use super::*;
use crate::installs::test_support::fc_line;

#[test]
fn a_line_is_named_by_major_and_minor_with_or_without_patch_or_v() {
    for value in ["1.15", "1.15.1", "v1.15", "v1.15.0"] {
        assert_eq!(
            value.parse::<ReleaseLine>(),
            Ok(ReleaseLine::new(1, 15)),
            "{value}"
        );
    }
    assert_eq!("1.15.3".parse::<ReleaseLine>().unwrap().to_string(), "1.15");
}

#[test]
fn a_runtime_line_names_its_runtime_and_is_a_runtime_identity() {
    let line = fc_line("1.15");
    assert_eq!(line.to_string(), "firecracker 1.15");
    assert_eq!(
        line.identity().unwrap(),
        RuntimeIdentity {
            kind: RuntimeKind::Firecracker,
            version: RuntimeVersion::parse("1.15").unwrap(),
        }
    );
}

#[test]
fn anything_else_is_not_a_line() {
    for value in [
        "",
        "1",
        "1.x",
        "1.15.1.2",
        "1.15-pcie",
        "1.15.1-rc1",
        "vv1.15",
        "+1.15",
        "1.+15",
    ] {
        value.parse::<ReleaseLine>().expect_err(value);
    }
}

#[test]
fn kernel_versions_are_the_names_the_source_uses() {
    for value in ["6.1.155", "5.10.245-no-acpi"] {
        assert_eq!(value.parse::<KernelVersion>().unwrap().to_string(), value);
    }
}

#[test]
fn configs_aliases_and_derived_identities_are_not_kernel_versions() {
    for value in [
        "",
        "5.10.245.config",
        "5.10.245-no-acpi.config",
        "default",
        "legacy-default",
        "legacy-e20e46d0c36c",
        "../6.1.155",
        "6.1.155/x",
        "6.1.155.",
        "v6.1.155",
        "6.1.155_debug",
        "6.1.155-NO-ACPI",
    ] {
        assert!(value.parse::<KernelVersion>().is_err(), "{value}");
    }
}

#[test]
fn only_a_single_part_entity_tag_is_an_md5() {
    assert_eq!(
        ContentMd5::from_etag("\"44d1aea84475e5bed0dd4f27ddfca550\"").map(|md5| md5.to_string()),
        Some("44d1aea84475e5bed0dd4f27ddfca550".to_string())
    );
    assert_eq!(
        ContentMd5::from_etag("\"44d1aea84475e5bed0dd4f27ddfca550-3\""),
        None
    );
    assert_eq!(
        ContentMd5::from_etag("\"44D1AEA84475E5BED0DD4F27DDFCA550\""),
        None
    );
    assert_eq!(ContentMd5::from_etag(""), None);
}

#[test]
fn a_verifiable_kernel_round_trips_as_a_job_payload() {
    let kernel = VerifiableKernel {
        digest: Digest::sha256([7; 32]),
        version: "6.1.155".parse().unwrap(),
        line: fc_line("1.15"),
        architecture: Arch::X86_64,
        location: "s3://spec.ccfc.min/firecracker-ci/v1.15/x86_64/vmlinux-6.1.155?region=us-east-1&path_style=true"
            .parse()
            .unwrap(),
        size: 44_279_576,
        md5: Some("44d1aea84475e5bed0dd4f27ddfca550".parse().unwrap()),
    };
    let json = serde_json::to_value(&kernel).unwrap();
    assert_eq!(json["version"], "6.1.155");
    assert_eq!(json["line"]["runtime"], "firecracker");
    assert_eq!(json["line"]["release"], "1.15");
    assert_eq!(json["architecture"], "x86_64");
    assert_eq!(json["md5"], "44d1aea84475e5bed0dd4f27ddfca550");
    assert_eq!(
        serde_json::from_value::<VerifiableKernel>(json).unwrap(),
        kernel
    );

    let mut tampered = serde_json::to_value(&kernel).unwrap();
    tampered["version"] = "../etc".into();
    serde_json::from_value::<VerifiableKernel>(tampered).unwrap_err();
}

#[test]
fn a_download_request_preserves_optional_transfer_evidence() {
    for md5 in [None, Some(ContentMd5::new([7; 16]))] {
        let listed = SourceKernel {
            version: "6.1.155".parse().unwrap(),
            line: fc_line("1.15"),
            architecture: Arch::X86_64,
            location: "s3://fake/k?region=us-east-1".parse().unwrap(),
            size: 1,
            md5,
        };
        let request = listed.download_request(Digest::sha256([7; 32]));
        let json = serde_json::to_value(&request).unwrap();
        assert_eq!(request.md5, md5);
        assert_eq!(
            serde_json::from_value::<VerifiableKernel>(json).unwrap(),
            request
        );
    }
}
