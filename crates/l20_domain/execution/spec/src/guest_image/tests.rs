use super::*;
use serde_json::json;

#[test]
fn image_contract_codec_rejects_unknown_semantics_and_invalid_ports() {
    let image = GuestImageContract::new(
        GuestDiskLayout::OpaqueRawDisk,
        GuestReadiness::TcpService {
            port: NonZeroU16::new(3389).unwrap(),
        },
        GuestNetwork::SlotDhcpV1,
        crate::guest_image::GuestAgent::Absent,
    )
    .unwrap();
    let wire = serde_json::to_value(image).unwrap();
    assert_eq!(
        serde_json::from_value::<GuestImageContract>(wire.clone()).unwrap(),
        image
    );
    assert!(!image.expects_agent());
    assert!(GuestImageContract::default().expects_agent());
    for (pointer, value) in [
        ("/version", json!(2)),
        ("/disk", json!("unknown")),
        ("/agent", json!("unknown")),
        ("/network", json!("unknown")),
        ("/readiness/kind", json!("unknown")),
        ("/readiness/port", json!(0)),
        ("/readiness/port", json!(65536)),
    ] {
        let mut invalid = wire.clone();
        *invalid.pointer_mut(pointer).unwrap() = value;
        serde_json::from_value::<GuestImageContract>(invalid).unwrap_err();
    }
    let mut extra = wire;
    extra["unchecked"] = true.into();
    serde_json::from_value::<GuestImageContract>(extra).unwrap_err();
}

#[test]
fn agent_capability_is_independent_of_readiness() {
    for agent in [GuestAgent::Absent, GuestAgent::Spaced] {
        for readiness in [
            GuestReadiness::Agent,
            GuestReadiness::TcpService {
                port: NonZeroU16::new(22).unwrap(),
            },
        ] {
            let image = GuestImageContract::new(
                GuestDiskLayout::OpaqueRawDisk,
                readiness,
                GuestNetwork::SlotDhcpV1,
                agent,
            );
            if readiness == GuestReadiness::Agent && agent == GuestAgent::Absent {
                assert_eq!(image.unwrap_err(), GuestImageContractError::MissingAgent);
            } else {
                let image = image.unwrap();
                assert_eq!(image.expects_agent(), agent == GuestAgent::Spaced);
                let persisted = serde_json::to_value(image).unwrap();
                assert_eq!(
                    serde_json::from_value::<GuestImageContract>(persisted).unwrap(),
                    image
                );
            }
        }
    }
}
