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
    );
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
