use super::*;
use pplx_space_accounts::{NodeId, OrgId};
use pplx_space_aws_client::ec2::VolumeAttachment;
use pplx_space_common_core::types::SandboxId;
use pplx_space_execution_spec::{
    BlockVolumeBackend, BlockVolumeMount, BlockVolumePermissions, BlockVolumeSizeGiB,
    GuestBlockMountPath,
};
use pplx_space_sandbox_store::block_volume::Initialization;
use pplx_space_test_env::TestEnv;
use sqlx::types::Json;

const POLICY: &str = r#"{
    "account_id":"123456789012","region":"us-west-2","availability_zone":"us-west-2a",
    "cluster":"example-cluster","experiment":"example-run",
    "instance_ids":["i-0123456789abcdef0"]
}"#;
const INSTANCE: &str = "i-0123456789abcdef0";
const DEVICE: &str = "/dev/sdf";
const VOLUME: &str = "vol-0123456789abcdef0";

#[tokio::test]
async fn independent_reconciler_recovers_lost_create_and_delete_receipts()
-> pplx_space_test_env::TestResult {
    use axum::{Router, extract::State, http::StatusCode, routing::post};
    use pplx_space_db_test_fixtures::{SEED_ORG_ID, pg_test_db};
    use std::sync::{Arc, Mutex};

    #[derive(Default)]
    struct Cloud {
        present: bool,
        create_calls: usize,
        delete_calls: usize,
        requests: Vec<BTreeMap<String, String>>,
        item: String,
    }
    async fn respond(State(state): State<Arc<Mutex<Cloud>>>, body: String) -> (StatusCode, String) {
        let request: BTreeMap<String, String> = form_urlencoded::parse(body.as_bytes())
            .into_owned()
            .collect();
        let mut cloud = state.lock().unwrap();
        let action = request["Action"].clone();
        let exact = request.contains_key("VolumeId.1");
        cloud.requests.push(request);
        match action.as_str() {
            "DescribeVolumes" if !cloud.present && exact => (
                StatusCode::BAD_REQUEST,
                "<Response><Errors><Error><Code>InvalidVolume.NotFound</Code><Message>absent</Message></Error></Errors></Response>".into(),
            ),
            "DescribeVolumes" => (
                StatusCode::OK,
                format!(
                    "<DescribeVolumesResponse><volumeSet>{}</volumeSet></DescribeVolumesResponse>",
                    if cloud.present { &cloud.item } else { "" },
                ),
            ),
            "CreateVolume" => {
                cloud.create_calls += 1;
                cloud.present = true;
                // AWS performed the mutation, but the caller has no receipt.
                (StatusCode::INTERNAL_SERVER_ERROR, "<Response><Errors><Error><Code>InternalError</Code><Message>lost receipt</Message></Error></Errors></Response>".into())
            }
            "DeleteVolume" => {
                cloud.delete_calls += 1;
                // First attempt fails before deletion; the second loses its
                // response after deletion. Neither may lose cleanup authority.
                if cloud.delete_calls == 2 {
                    cloud.present = false;
                }
                (StatusCode::INTERNAL_SERVER_ERROR, "<Response><Errors><Error><Code>InternalError</Code><Message>uncertain deletion</Message></Error></Errors></Response>".into())
            }
            _ => panic!("unexpected cloud action {action}"),
        }
    }

    let db = pg_test_db().await?;
    let policy = policy();
    let org = OrgId::from_uuid(SEED_ORG_ID);
    let id = Uuid::now_v7();
    let mut tx = db.pool.begin().await?;
    store::resource::reserve(
        &mut tx,
        store::resource::NewResource {
            id,
            org_id: org,
            managed_by: "test",
            availability_zone: &policy.availability_zone,
            resource_limit: 1,
        },
    )
    .await?;
    tx.commit().await?;
    let tags: String = policy
        .tags()
        .into_iter()
        .chain([
            (LOGICAL_ID_TAG.into(), id.to_string()),
            (RETAINED_TAG.into(), "true".into()),
        ])
        .map(|(key, value)| format!("<item><key>{key}</key><value>{value}</value></item>"))
        .collect();
    let state = Arc::new(Mutex::new(Cloud {
        item: format!(
            "<item><volumeId>{VOLUME}</volumeId><size>128</size><availabilityZone>us-west-2a</availabilityZone><status>available</status><volumeType>gp3</volumeType><encrypted>true</encrypted><attachmentSet/><tagSet>{tags}</tagSet></item>"
        ),
        ..Cloud::default()
    }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let endpoint = format!("http://{}", listener.local_addr()?);
    let router = Router::new()
        .route("/", post(respond))
        .with_state(state.clone());
    let server = tokio_util::task::AbortOnDropHandle::new(tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap()
    }));
    let config = aws_config::SdkConfig::builder()
        .region(aws_config::Region::new("us-west-2"))
        .credentials_provider(
            aws_credential_types::provider::SharedCredentialsProvider::new(
                aws_credential_types::Credentials::new("test", "test", None, None, "test"),
            ),
        )
        .endpoint_url(endpoint)
        .build();
    let client = Client::from_config(&config);
    assert!(
        independent::tick(&db.pool, &client, &policy, "test")
            .await
            .is_err()
    );
    let pending = store::resource::get(&db.pool, org, id).await?.unwrap();
    assert_eq!(pending.phase, VolumePhase::Requested);
    assert!(pending.aws_volume_id.is_none());
    independent::tick(&db.pool, &client, &policy, "test").await?;
    independent::tick(&db.pool, &client, &policy, "test").await?;
    let available = store::resource::get(&db.pool, org, id).await?.unwrap();
    assert_eq!(available.phase, VolumePhase::Available);
    assert_eq!(available.aws_volume_id.as_deref(), Some(VOLUME));
    let mut tx = db.pool.begin().await?;
    assert!(store::resource::request_delete(&mut tx, org, id, Uuid::now_v7()).await?);
    tx.commit().await?;
    for _ in 0..2 {
        assert!(
            independent::tick(&db.pool, &client, &policy, "test")
                .await
                .is_err()
        );
        let pending = store::resource::get(&db.pool, org, id).await?.unwrap();
        assert!(pending.delete_requested);
        assert_eq!(pending.aws_volume_id.as_deref(), Some(VOLUME));
    }
    independent::tick(&db.pool, &client, &policy, "test").await?;
    let deleted = store::resource::get(&db.pool, org, id).await?.unwrap();
    assert_eq!(deleted.phase, VolumePhase::Deleted);
    server.abort();
    let cloud = state.lock().unwrap();
    assert_eq!((cloud.create_calls, cloud.delete_calls), (1, 2));
    let create = cloud
        .requests
        .iter()
        .find(|request| request["Action"] == "CreateVolume")
        .unwrap();
    assert_eq!(create["ClientToken"], id.to_string());
    assert_eq!(create["Size"], "128");
    assert_eq!(create["VolumeType"], "gp3");
    assert_eq!(create["Encrypted"], "true");
    assert!(
        cloud.requests.iter().all(|request| {
            !matches!(request["Action"].as_str(), "AttachVolume" | "DetachVolume")
        })
    );
    Ok(())
}

#[tokio::test]
async fn inventory_exhausts_empty_pages_and_refuses_partial_or_ambiguous_results() {
    use axum::{Router, extract::State, http::StatusCode, routing::post};
    use std::{
        collections::VecDeque,
        sync::{Arc, Mutex},
    };

    type Requests = Arc<Mutex<Vec<BTreeMap<String, String>>>>;
    type Replies = Arc<Mutex<VecDeque<(StatusCode, String)>>>;
    async fn respond(
        State((requests, replies)): State<(Requests, Replies)>,
        body: String,
    ) -> (StatusCode, String) {
        requests.lock().unwrap().push(
            form_urlencoded::parse(body.as_bytes())
                .into_owned()
                .collect(),
        );
        replies
            .lock()
            .unwrap()
            .pop_front()
            .expect("unexpected cloud call")
    }
    const EMPTY_NEXT: &str = "<DescribeVolumesResponse><volumeSet/><nextToken>page-two</nextToken></DescribeVolumesResponse>";
    const ITEM: &str = "<item><volumeId>vol-0123456789abcdef0</volumeId><size>128</size><availabilityZone>us-west-2a</availabilityZone><status>available</status><volumeType>gp3</volumeType><encrypted>true</encrypted><attachmentSet/><tagSet/></item>";
    let terminal =
        format!("<DescribeVolumesResponse><volumeSet>{ITEM}</volumeSet></DescribeVolumesResponse>");
    let repeated = format!(
        "<DescribeVolumesResponse><volumeSet>{ITEM}</volumeSet><nextToken>page-two</nextToken></DescribeVolumesResponse>"
    );
    let denied = "<Response><Errors><Error><Code>UnauthorizedOperation</Code><Message>denied</Message></Error></Errors></Response>";
    let cases = [
        (
            vec![
                (StatusCode::OK, EMPTY_NEXT.to_owned()),
                (StatusCode::OK, terminal.clone()),
            ],
            true,
        ),
        (
            vec![
                (StatusCode::OK, EMPTY_NEXT.to_owned()),
                (StatusCode::OK, EMPTY_NEXT.to_owned()),
            ],
            false,
        ),
        (
            vec![
                (StatusCode::OK, EMPTY_NEXT.to_owned()),
                (StatusCode::FORBIDDEN, denied.to_owned()),
            ],
            false,
        ),
        (
            vec![(StatusCode::OK, repeated), (StatusCode::OK, terminal)],
            false,
        ),
    ];
    for (responses, succeeds) in cases {
        let requests: Requests = Arc::default();
        let replies: Replies = Arc::new(Mutex::new(responses.into()));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let router = Router::new()
            .route("/", post(respond))
            .with_state((requests.clone(), replies.clone()));
        let server = tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });
        let config = aws_config::SdkConfig::builder()
            .region(aws_config::Region::new("us-west-2"))
            .credentials_provider(
                aws_credential_types::provider::SharedCredentialsProvider::new(
                    aws_credential_types::Credentials::new("test", "test", None, None, "test"),
                ),
            )
            .endpoint_url(endpoint)
            .build();
        let result = inventory(&Client::from_config(&config), &policy().tags()).await;
        server.abort();
        if succeeds {
            assert_eq!(
                result
                    .unwrap()
                    .into_iter()
                    .map(|v| v.volume_id)
                    .collect::<Vec<_>>(),
                [VOLUME]
            );
        } else {
            result.unwrap_err();
        }
        assert!(replies.lock().unwrap().is_empty());
        let requests = requests.lock().unwrap();
        assert_eq!(requests.len(), 2);
        let mut expected = BTreeMap::from([
            ("Action".to_owned(), "DescribeVolumes".to_owned()),
            ("Version".to_owned(), "2016-11-15".to_owned()),
            ("MaxResults".to_owned(), "1000".to_owned()),
            ("Filter.1.Name".to_owned(), "tag:space:cluster".to_owned()),
            ("Filter.1.Value.1".to_owned(), "example-cluster".to_owned()),
            (
                "Filter.2.Name".to_owned(),
                "tag:space:experiment".to_owned(),
            ),
            ("Filter.2.Value.1".to_owned(), "example-run".to_owned()),
        ]);
        assert_eq!(requests[0], expected);
        expected.insert("NextToken".to_owned(), "page-two".to_owned());
        assert_eq!(requests[1], expected);
    }
}

fn policy() -> Policy {
    let mut env = TestEnv::default();
    env.set(POLICY_ENV, POLICY);
    Policy::from_environment(&env).unwrap().unwrap()
}

fn row() -> BlockVolume {
    BlockVolume {
        retained: false,
        id: Uuid::now_v7(),
        sandbox_id: SandboxId::new(),
        org_id: OrgId::from_uuid(Uuid::from_u128(1)),
        managed_by: "test".into(),
        mount_spec: Json(BlockVolumeMount {
            backend: BlockVolumeBackend::Ebs,
            size_gib: BlockVolumeSizeGiB::try_from(128).unwrap(),
            permissions: BlockVolumePermissions::ReadWrite,
            mount: GuestBlockMountPath::try_from("/data".to_owned()).unwrap(),
        }),
        availability_zone: "us-west-2a".into(),
        aws_volume_id: None,
        phase: VolumePhase::Requested,
        delete_requested: false,
        attachment_epoch: 0,
        node_id: None,
        instance_id: None,
        device_name: None,
        execution_id: None,
        released_epoch: None,
        filesystem_uuid: Uuid::now_v7(),
        initialization: Initialization::Fresh,
        initialization_claim: None,
        ever_exposed: false,
        disk_generation: 0,
        checkpoint_id: None,
        checkpoint_generation: None,
        activation_id: None,
        activated_epoch: None,
        revision: 0,
    }
}

fn cloud(policy: &Policy, row: &BlockVolume) -> Volume {
    let mut tags = policy.tags();
    tags.insert(LOGICAL_ID_TAG.into(), row.id.to_string());
    tags.insert(SANDBOX_ID_TAG.into(), row.sandbox_id.to_string());
    Volume {
        volume_id: VOLUME.into(),
        availability_zone: row.availability_zone.clone(),
        size_gib: 128,
        encrypted: true,
        volume_type: "gp3".into(),
        state: "available".into(),
        attachments: Vec::new(),
        tags,
    }
}

#[test]
fn policy_is_default_off_and_refuses_incomplete_or_widened_scope() {
    assert!(
        Policy::from_environment(&TestEnv::default())
            .unwrap()
            .is_none()
    );
    for invalid in [
        "{}".to_owned(),
        POLICY.replace("123456789012", "invalid"),
        POLICY.replace("us-west-2a", "us-west-3a"),
        POLICY.replace(r#""i-0123456789abcdef0""#, ""),
        POLICY.replace("i-0123456789abcdef0", "*"),
        POLICY.replace("\"example-run\"", "\"\""),
        POLICY.replace("\"account_id\"", "\"unknown_setting\""),
    ] {
        let mut env = TestEnv::default();
        env.set(POLICY_ENV, invalid);
        Policy::from_environment(&env).unwrap_err();
    }
    let policy = policy();
    assert!(policy.instance_ids.contains(INSTANCE));
}

#[test]
fn uncertain_creation_reconciles_the_original_resource_and_never_substitutes_a_disk() {
    let policy = policy();
    let mut row = row();
    assert_eq!(plan(&policy, &row, None).unwrap(), Action::Create);
    row.delete_requested = true;
    // Deletion cannot erase an unknown create outcome: settle its original
    // client token first, then dispose of the same disk normally.
    assert_eq!(plan(&policy, &row, None).unwrap(), Action::Create);
    let volume = cloud(&policy, &row);
    assert_eq!(
        plan(&policy, &row, Some(&volume)).unwrap(),
        Action::Observe(VolumePhase::Available)
    );
    row.phase = VolumePhase::Available;
    row.aws_volume_id = Some(VOLUME.into());
    plan(&policy, &row, None).unwrap_err();
    assert_eq!(plan(&policy, &row, Some(&volume)).unwrap(), Action::Delete);
    let mut wrong = volume.clone();
    wrong.volume_id = "vol-11111111111111111".into();
    plan(&policy, &row, Some(&wrong)).unwrap_err();
    let mutations: [fn(&mut Volume); 4] = [
        |volume| volume.encrypted = false,
        |volume| volume.size_gib = 256,
        |volume| volume.volume_type = "io2".into(),
        |volume| {
            volume
                .tags
                .insert("space:experiment".into(), "another-run".into());
        },
    ];
    for mutate in mutations {
        let mut wrong = volume.clone();
        mutate(&mut wrong);
        plan(&policy, &row, Some(&wrong)).unwrap_err();
    }
    let mut wrong = volume.clone();
    wrong.tags.remove(LOGICAL_ID_TAG);
    plan(&policy, &row, Some(&wrong)).unwrap_err();
    wrong = volume;
    wrong.availability_zone = "us-west-2b".into();
    plan(&policy, &row, Some(&wrong)).unwrap_err();
}

#[test]
fn independent_disks_reconcile_lost_receipts_but_refuse_foreign_identity_or_attachment() {
    let policy = policy();
    let bound = row();
    let mut volume = cloud(&policy, &bound);
    volume.tags.remove(SANDBOX_ID_TAG);
    volume.tags.insert(RETAINED_TAG.into(), "true".into());
    let mut resource = store::resource::Resource {
        id: bound.id,
        org_id: bound.org_id,
        sandbox_id: None,
        mount_spec: None,
        availability_zone: bound.availability_zone,
        aws_volume_id: None,
        size_gib: 128,
        phase: VolumePhase::Requested,
        delete_requested: false,
        retained: true,
        revision: 0,
    };
    assert_eq!(
        independent::plan(&policy, &resource, None).unwrap(),
        Action::Create
    );
    volume.state = "creating".into();
    assert_eq!(
        independent::plan(&policy, &resource, Some(&volume)).unwrap(),
        Action::Observe(VolumePhase::Provisioning)
    );
    volume.state = "available".into();
    assert_eq!(
        independent::plan(&policy, &resource, Some(&volume)).unwrap(),
        Action::Observe(VolumePhase::Available)
    );
    resource.aws_volume_id = Some(VOLUME.into());
    resource.phase = VolumePhase::Available;
    independent::plan(&policy, &resource, None).unwrap_err();
    let mutations: [fn(&mut Volume); 6] = [
        |disk| disk.encrypted = false,
        |disk| disk.size_gib = 256,
        |disk| disk.volume_id = "vol-11111111111111111".into(),
        |disk| disk.availability_zone = "us-west-2b".into(),
        |disk| {
            disk.tags.remove(RETAINED_TAG);
        },
        |disk| {
            disk.attachments.push(VolumeAttachment {
                instance_id: INSTANCE.into(),
                device: DEVICE.into(),
                state: "attached".into(),
            })
        },
    ];
    resource.delete_requested = true;
    for mutate in mutations {
        let mut wrong = volume.clone();
        mutate(&mut wrong);
        independent::plan(&policy, &resource, Some(&wrong)).unwrap_err();
    }
    assert_eq!(
        independent::plan(&policy, &resource, Some(&volume)).unwrap(),
        Action::Delete
    );
    resource.phase = VolumePhase::Deleting;
    volume.state = "deleting".into();
    assert_eq!(
        independent::plan(&policy, &resource, Some(&volume)).unwrap(),
        Action::Observe(VolumePhase::Deleting)
    );
    assert_eq!(
        independent::plan(&policy, &resource, None).unwrap(),
        Action::Observe(VolumePhase::Deleted)
    );
}

#[test]
fn detach_requires_exact_epoch_release_and_never_uses_lifecycle_or_heartbeat_inference() {
    let policy = policy();
    let mut row = row();
    row.phase = VolumePhase::Attaching;
    row.node_id = Some(NodeId::from_uuid(Uuid::from_u128(2)));
    row.execution_id = Some(Uuid::now_v7());
    row.instance_id = Some(INSTANCE.into());
    row.device_name = Some(DEVICE.into());
    row.attachment_epoch = 3;
    row.aws_volume_id = Some(VOLUME.into());
    let mut volume = cloud(&policy, &row);
    assert_eq!(plan(&policy, &row, Some(&volume)).unwrap(), Action::Attach);
    volume.state = "in-use".into();
    volume.attachments.push(VolumeAttachment {
        instance_id: INSTANCE.into(),
        device: DEVICE.into(),
        state: "attached".into(),
    });
    assert_eq!(
        plan(&policy, &row, Some(&volume)).unwrap(),
        Action::Observe(VolumePhase::Attached)
    );
    row.phase = VolumePhase::Releasing;
    row.delete_requested = true;
    assert_eq!(plan(&policy, &row, Some(&volume)).unwrap(), Action::Wait);
    row.phase = VolumePhase::Detaching;
    for released in [None, Some(2), Some(4)] {
        row.released_epoch = released;
        plan(&policy, &row, Some(&volume)).unwrap_err();
    }
    row.released_epoch = Some(3);
    assert_eq!(plan(&policy, &row, Some(&volume)).unwrap(), Action::Detach);
    volume.attachments[0].instance_id = "i-11111111111111111".into();
    plan(&policy, &row, Some(&volume)).unwrap_err();
    volume.attachments.clear();
    volume.state = "available".into();
    assert_eq!(
        plan(&policy, &row, Some(&volume)).unwrap(),
        Action::FinishDetach
    );
}
