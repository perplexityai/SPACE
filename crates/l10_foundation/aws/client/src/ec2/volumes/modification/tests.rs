use std::collections::HashMap;

use http::StatusCode;

use super::*;
use crate::query::QueryResponse;
use crate::query::test_support::{RecordingTransport, credentials_provider, parse_body};

const ID: &str = "vol-0123456789abcdef0";
const MODIFICATION: &str = r#"
<volumeId>vol-0123456789abcdef0</volumeId>
<modificationState>modifying</modificationState>
<originalSize>128</originalSize><targetSize>256</targetSize>
<originalVolumeType>gp3</originalVolumeType><targetVolumeType>gp3</targetVolumeType>
"#;

fn receipt(item: &str) -> String {
    format!(
        "<ModifyVolumeResponse><volumeModification>{item}</volumeModification></ModifyVolumeResponse>"
    )
}

fn lookup(items: &str) -> String {
    format!(
        "<DescribeVolumesModificationsResponse><volumeModificationSet>{items}</volumeModificationSet></DescribeVolumesModificationsResponse>"
    )
}

#[tokio::test]
async fn size_only_request_returns_pending_receipt_without_polling_or_performance_changes() {
    let transport = RecordingTransport::new(vec![QueryResponse::new(
        StatusCode::OK,
        receipt(MODIFICATION),
    )]);
    let client = Client::with_transport("us-west-2", credentials_provider(), transport.clone());
    let modification = client
        .modify_volume_size(ID, NonZeroU32::new(256).unwrap())
        .await
        .unwrap();
    assert_eq!(
        modification,
        VolumeModification {
            volume_id: ID.into(),
            state: VolumeModificationState::Modifying,
            original_size_gib: NonZeroU32::new(128).unwrap(),
            target_size_gib: NonZeroU32::new(256).unwrap(),
            original_volume_type: "gp3".into(),
            target_volume_type: "gp3".into(),
        }
    );
    let requests = transport.requests();
    assert_eq!(requests.len(), 1);
    assert_eq!(
        parse_body(&requests[0].body),
        HashMap::from([
            ("Action".into(), "ModifyVolume".into()),
            ("Version".into(), "2016-11-15".into()),
            ("VolumeId".into(), ID.into()),
            ("Size".into(), "256".into()),
        ])
    );
}

#[tokio::test]
async fn exact_lookup_distinguishes_absent_history_and_each_provider_state() {
    let states = [
        ("modifying", VolumeModificationState::Modifying),
        ("optimizing", VolumeModificationState::Optimizing),
        ("completed", VolumeModificationState::Completed),
        ("failed", VolumeModificationState::Failed),
    ];
    let responses = std::iter::once(QueryResponse::new(StatusCode::OK, lookup("")))
        .chain(states.iter().map(|(wire, _)| {
            QueryResponse::new(
                StatusCode::OK,
                lookup(&format!(
                    "<item>{}</item>",
                    MODIFICATION.replace("modifying", wire)
                )),
            )
        }))
        .collect();
    let transport = RecordingTransport::new(responses);
    let client = Client::with_transport("us-west-2", credentials_provider(), transport.clone());
    assert!(
        client
            .describe_volume_modification(ID)
            .await
            .unwrap()
            .is_none()
    );
    for (_, expected) in states {
        let observed = client
            .describe_volume_modification(ID)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(observed.volume_id, ID);
        assert_eq!(observed.state, expected);
        assert_eq!(observed.target_size_gib.get(), 256);
    }
    let requests = transport.requests();
    assert_eq!(requests.len(), 5);
    for request in requests {
        assert_eq!(
            parse_body(&request.body),
            HashMap::from([
                ("Action".into(), "DescribeVolumesModifications".into()),
                ("Version".into(), "2016-11-15".into()),
                ("VolumeId.1".into(), ID.into()),
            ])
        );
    }
}

#[tokio::test]
async fn malformed_or_mismatched_receipts_never_establish_a_successful_resize() {
    let cases = [
        MODIFICATION.replace(ID, "vol-11111111111111111"),
        MODIFICATION.replace(
            "<targetSize>256</targetSize>",
            "<targetSize>512</targetSize>",
        ),
        MODIFICATION.replace(
            "<targetSize>256</targetSize>",
            "<targetSize>64</targetSize>",
        ),
        MODIFICATION.replace("<targetSize>256</targetSize>", "<targetSize>0</targetSize>"),
        MODIFICATION.replace("<targetSize>256</targetSize>", ""),
        MODIFICATION.replace("modifying", "unknown-future-state"),
        format!("{MODIFICATION}<modificationState>completed</modificationState>"),
        format!("{MODIFICATION}<volumeId>{ID}</volumeId>"),
    ];
    for item in cases {
        let transport =
            RecordingTransport::new(vec![QueryResponse::new(StatusCode::OK, receipt(&item))]);
        let client = Client::with_transport("us-west-2", credentials_provider(), transport.clone());
        assert!(
            matches!(
                client
                    .modify_volume_size(ID, NonZeroU32::new(256).unwrap())
                    .await,
                Err(AwsClientError::Xml(_)),
            ),
            "{item}"
        );
        assert_eq!(
            transport.requests().len(),
            1,
            "invalid receipts must not cause blind retries"
        );
    }
}

#[tokio::test]
async fn ambiguous_inventory_is_not_absent_history() {
    let item = format!("<item>{MODIFICATION}</item>");
    let cases = [
        "<DescribeVolumesModificationsResponse/>".to_owned(),
        lookup(&format!("{item}{item}")),
        lookup(&item.replace(ID, "vol-11111111111111111")),
        lookup(&item).replace(
            "</DescribeVolumesModificationsResponse>",
            "<nextToken>more</nextToken></DescribeVolumesModificationsResponse>",
        ),
        lookup("").replace(
            "</DescribeVolumesModificationsResponse>",
            "<volumeModificationSet/></DescribeVolumesModificationsResponse>",
        ),
        receipt(MODIFICATION),
    ];
    for body in cases {
        let transport =
            RecordingTransport::new(vec![QueryResponse::new(StatusCode::OK, body.clone())]);
        let client = Client::with_transport("us-west-2", credentials_provider(), transport);
        assert!(
            matches!(
                client.describe_volume_modification(ID).await,
                Err(AwsClientError::Xml(_))
            ),
            "{body}"
        );
    }
}

#[tokio::test]
async fn aws_failure_keeps_its_structured_code_and_does_not_resubmit() {
    let transport = RecordingTransport::new(vec![QueryResponse::new(
        StatusCode::BAD_REQUEST,
        "<Response><Errors><Error><Code>IncorrectModificationState</Code><Message>previous change pending</Message></Error></Errors></Response>",
    )]);
    let client = Client::with_transport("us-west-2", credentials_provider(), transport.clone());
    assert!(matches!(
        client.modify_volume_size(ID, NonZeroU32::new(256).unwrap()).await,
        Err(AwsClientError::AwsService { code, .. }) if code == "IncorrectModificationState",
    ));
    assert_eq!(transport.requests().len(), 1);
}
