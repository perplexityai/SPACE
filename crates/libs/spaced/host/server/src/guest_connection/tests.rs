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
        let parsed = GuestPlatformObservation::parse(&response);
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
