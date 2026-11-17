use http::StatusCode;
use std::collections::HashMap;

use super::*;
use crate::query::QueryResponse;
use crate::query::test_support::{RecordingTransport, credentials_provider, parse_body};

const VOLUME: &str = r#"
<volumeId>vol-0123456789abcdef0</volumeId><size>128</size>
<availabilityZone>us-west-2a</availabilityZone><status>available</status>
<volumeType>gp3</volumeType><encrypted>true</encrypted>
<attachmentSet/>
<tagSet><item><key>experiment</key><value>example-run</value></item></tagSet>
"#;
const ATTACHMENT: &str = r#"
<instanceId>i-0123456789abcdef0</instanceId><device>/dev/sdf</device><status>attaching</status>
"#;

#[tokio::test]
async fn create_sends_idempotency_encryption_and_atomic_tags_without_a_snapshot() {
    let transport = RecordingTransport::new(vec![QueryResponse::new(
        StatusCode::OK,
        // CreateVolume does not return attachmentSet; only DescribeVolumes
        // can establish the disk's current attachment inventory.
        format!(
            "<CreateVolumeResponse>{}</CreateVolumeResponse>",
            VOLUME
                .replace("<attachmentSet/>", "")
                .replace("<status>available</status>", "<status>creating</status>")
        ),
    )]);
    let client = Client::with_transport("us-west-2", credentials_provider(), transport.clone());
    let tags = BTreeMap::from([("experiment".to_string(), "example-run".to_string())]);
    let volume = client
        .create_encrypted_gp3_volume(&CreateEncryptedGp3Volume {
            availability_zone: "us-west-2a",
            size_gib: 128,
            client_token: "durable-create-token",
            tags: &tags,
        })
        .await
        .unwrap();
    assert_eq!(
        volume,
        CreatedVolume {
            volume_id: "vol-0123456789abcdef0".into(),
        }
    );
    let requests = transport.requests();
    assert_eq!(requests.len(), 1);
    assert_eq!(
        parse_body(&requests[0].body),
        HashMap::from([
            ("Action".into(), "CreateVolume".into()),
            ("Version".into(), "2016-11-15".into()),
            ("AvailabilityZone".into(), "us-west-2a".into()),
            ("Size".into(), "128".into()),
            ("VolumeType".into(), "gp3".into()),
            ("Encrypted".into(), "true".into()),
            ("ClientToken".into(), "durable-create-token".into()),
            ("TagSpecification.1.ResourceType".into(), "volume".into()),
            ("TagSpecification.1.Tag.1.Key".into(), "experiment".into()),
            (
                "TagSpecification.1.Tag.1.Value".into(),
                "example-run".into()
            ),
        ])
    );
}

#[test]
fn creation_receipts_require_one_nonempty_volume_identity() {
    for invalid in [
        "<CreateVolumeResponse/>",
        "<CreateVolumeResponse><volumeId/></CreateVolumeResponse>",
        "<CreateVolumeResponse><volumeId>vol-one</volumeId><volumeId>vol-two</volumeId></CreateVolumeResponse>",
        "<Unexpected><volumeId>vol-one</volumeId></Unexpected>",
    ] {
        parse_creation_receipt(invalid).unwrap_err();
    }
}

#[tokio::test]
async fn inventory_preserves_pagination_tags_and_all_attachment_references() {
    let item = VOLUME.replace(
        "<attachmentSet/>",
        &format!("<attachmentSet><item>{ATTACHMENT}</item></attachmentSet>"),
    );
    let transport = RecordingTransport::new(vec![QueryResponse::new(
        StatusCode::OK,
        format!(
            "<DescribeVolumesResponse><volumeSet><item>{item}</item></volumeSet><nextToken>page-two</nextToken></DescribeVolumesResponse>"
        ),
    )]);
    let client = Client::with_transport("us-west-2", credentials_provider(), transport.clone());
    let tags = BTreeMap::from([("experiment".to_string(), "example-run".to_string())]);
    let page = client
        .describe_volumes_page(&tags, Some("page-one"))
        .await
        .unwrap();
    assert_eq!(page.next_token.as_deref(), Some("page-two"));
    assert_eq!(page.volumes.len(), 1);
    assert_eq!(
        page.volumes[0].attachments,
        vec![VolumeAttachment {
            instance_id: "i-0123456789abcdef0".into(),
            device: "/dev/sdf".into(),
            state: "attaching".into(),
        }]
    );
    let requests = transport.requests();
    assert_eq!(requests.len(), 1);
    assert_eq!(
        parse_body(&requests[0].body),
        HashMap::from([
            ("Action".into(), "DescribeVolumes".into()),
            ("Version".into(), "2016-11-15".into()),
            ("MaxResults".into(), "1000".into()),
            ("Filter.1.Name".into(), "tag:experiment".into()),
            ("Filter.1.Value.1".into(), "example-run".into()),
            ("NextToken".into(), "page-one".into()),
        ])
    );
}

#[tokio::test]
async fn attachment_changes_identify_the_exact_target_and_never_force_detach() {
    let transport = RecordingTransport::new(vec![
        QueryResponse::new(
            StatusCode::OK,
            format!("<AttachVolumeResponse>{ATTACHMENT}</AttachVolumeResponse>"),
        ),
        QueryResponse::new(
            StatusCode::OK,
            format!(
                "<DetachVolumeResponse>{}</DetachVolumeResponse>",
                ATTACHMENT.replace("attaching", "detaching")
            ),
        ),
        QueryResponse::new(
            StatusCode::OK,
            "<DeleteVolumeResponse><return>true</return></DeleteVolumeResponse>",
        ),
    ]);
    let client = Client::with_transport("us-west-2", credentials_provider(), transport.clone());
    let attached = client
        .attach_volume("vol-0123456789abcdef0", "i-0123456789abcdef0", "/dev/sdf")
        .await
        .unwrap();
    assert_eq!(attached.state, "attaching");
    let detached = client
        .detach_volume("vol-0123456789abcdef0", "i-0123456789abcdef0", "/dev/sdf")
        .await
        .unwrap();
    assert_eq!(detached.state, "detaching");
    client.delete_volume("vol-0123456789abcdef0").await.unwrap();
    let requests = transport.requests();
    assert_eq!(requests.len(), 3);
    for (index, action) in ["AttachVolume", "DetachVolume", "DeleteVolume"]
        .into_iter()
        .enumerate()
    {
        let mut expected = HashMap::from([
            ("Action".into(), action.to_string()),
            ("Version".into(), "2016-11-15".into()),
            ("VolumeId".into(), "vol-0123456789abcdef0".into()),
        ]);
        if index < 2 {
            expected.insert("InstanceId".into(), "i-0123456789abcdef0".into());
            expected.insert("Device".into(), "/dev/sdf".into());
        }
        if index == 1 {
            expected.insert("Force".into(), "false".into());
        }
        assert_eq!(parse_body(&requests[index].body), expected);
    }
}

#[test]
fn incomplete_or_ambiguous_inventory_is_an_error_not_an_empty_or_safe_disk() {
    for invalid in [
        VOLUME.replace("<volumeId>vol-0123456789abcdef0</volumeId>", ""),
        VOLUME.replace("<encrypted>true</encrypted>", ""),
        VOLUME.replace("<encrypted>true</encrypted>", "<encrypted>unknown</encrypted>"),
        VOLUME.replace("<size>128</size>", "<size>bad</size>"),
        VOLUME.replace("<attachmentSet/>", ""),
        VOLUME.replace("<attachmentSet/>", "<attachmentSet><item><instanceId>i-0123456789abcdef0</instanceId></item></attachmentSet>"),
        VOLUME.replace("</tagSet>", "<item><key>experiment</key><value>other-run</value></item></tagSet>"),
    ] {
        let response = format!("<DescribeVolumesResponse><volumeSet><item>{invalid}</item></volumeSet></DescribeVolumesResponse>");
        assert!(parse_volume_page(&response).is_err(), "{response}");
    }
    for invalid in [
        "<DescribeVolumesResponse/>",
        "<Unexpected><volumeSet/></Unexpected>",
    ] {
        parse_volume_page(invalid).unwrap_err();
    }
    assert_eq!(
        parse_volume_page("<DescribeVolumesResponse><volumeSet/></DescribeVolumesResponse>")
            .unwrap(),
        VolumePage {
            volumes: vec![],
            next_token: None
        },
    );
}
