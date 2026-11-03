use super::*;

#[test]
fn historical_omission_and_explicit_platforms_have_distinct_admission() {
    for (version, os, admitted) in [
        ("0.1.0", None, true),
        ("0.8.0", None, true),
        ("0.8.1", None, false),
        ("0.8.1", Some(GuestOs::Linux), true),
        ("0.8.0", Some(GuestOs::Windows), false),
        ("0.8.0", Some(GuestOs::Unspecified), false),
    ] {
        let response = BridgeGuestWaitReadyResponse {
            version: version.into(),
            compatibility_family:
                pplx_space_spaced_common::spaced_compatibility_family_for_version(version).unwrap(),
            platform: os.map(|os| {
                pplx_space_spaced_common::proto::platform::GuestPlatformDescriptor {
                    os: os as i32,
                    operations: vec![],
                }
            }),
            ..Default::default()
        };
        let parsed = GuestPlatformObservation::parse(&response, QualifiedGuestAgent::LinuxV1);
        assert_eq!(parsed.is_ok(), admitted, "{version} {os:?}");
        if os.is_some() && admitted {
            assert!(
                parsed
                    .unwrap()
                    .require(GuestOperation::Process, None)
                    .is_err()
            );
        }
    }
}

#[test]
fn native_readiness_requires_the_trusted_os_and_cannot_grant_operations() {
    for (os, accepted) in [
        (Some(GuestOs::Windows), true),
        (Some(GuestOs::Linux), false),
        (None, false),
    ] {
        let response = BridgeGuestWaitReadyResponse {
            version: "0.8.0".into(),
            compatibility_family: "0.8".into(),
            platform: os.map(|os| {
                pplx_space_spaced_common::proto::platform::GuestPlatformDescriptor {
                    os: os as i32,
                    operations: vec![GuestOperation::Process as i32, GuestOperation::Flush as i32],
                }
            }),
            ..Default::default()
        };
        assert!(
            GuestPlatformObservation::parse(&response, QualifiedGuestAgent::default()).is_err(),
            "an omitted policy must not authorize guest claims"
        );
        let result = GuestPlatformObservation::parse(&response, QualifiedGuestAgent::WindowsV1);
        assert_eq!(result.is_ok(), accepted);
        if let Ok(platform) = result {
            assert!(platform.require(GuestOperation::Process).is_err());
            assert!(platform.require(GuestOperation::Flush, None).is_err());
        }
    }
}
