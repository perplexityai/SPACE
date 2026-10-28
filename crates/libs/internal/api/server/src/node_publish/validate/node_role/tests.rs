use super::*;

#[test]
fn role_hints_preserve_omission_and_sandbox_defaults() {
    for role in [
        None,
        Some(ProtoNodeRole::Worker),
        Some(ProtoNodeRole::Builder),
    ] {
        for sandbox_inventory_complete in [false, true] {
            let request = PublishNodeStateRequest {
                role: role.map(i32::from),
                sandbox_inventory_complete,
                ..Default::default()
            };
            let intent = PublicationRoleIntent::try_from_proto(&request).unwrap();
            let expected = match role {
                Some(ProtoNodeRole::Worker) => Some(NodeRole::Worker),
                Some(ProtoNodeRole::Builder) => Some(NodeRole::Builder),
                None if sandbox_inventory_complete => Some(NodeRole::Worker),
                None => None,
                _ => unreachable!(),
            };
            assert_eq!(intent.role_hint(), expected);
        }
    }
    let cluster_status = PublishNodeStateRequest {
        role: Some(ProtoNodeRole::Cluster.into()),
        ..Default::default()
    };
    assert_eq!(
        PublicationRoleIntent::try_from_proto(&cluster_status)
            .unwrap()
            .role_hint(),
        Some(NodeRole::Cluster)
    );
}

#[test]
fn declared_role_rejections_preserve_codes_and_messages() {
    let unknown = PublishNodeStateRequest {
        role: Some(i32::MAX),
        ..Default::default()
    };
    let error = PublicationRoleIntent::try_from_proto(&unknown).unwrap_err();
    assert_eq!(error.code(), tonic::Code::InvalidArgument);
    assert_eq!(error.message(), format!("unknown node role {}", i32::MAX));
    for mut request in [
        PublishNodeStateRequest {
            sandbox_inventory_complete: true,
            ..Default::default()
        },
        PublishNodeStateRequest {
            sandboxes: vec![Default::default()],
            ..Default::default()
        },
        PublishNodeStateRequest {
            auto_lifecycle_delete_completions: vec![Default::default()],
            ..Default::default()
        },
    ] {
        request.role = Some(ProtoNodeRole::Cluster.into());
        let error = PublicationRoleIntent::try_from_proto(&request).unwrap_err();
        assert_eq!(error.code(), tonic::Code::InvalidArgument);
        assert_eq!(
            error.message(),
            "sandbox inventory state can only be published by worker or builder nodes"
        );
    }
}

#[test]
fn delete_receipts_require_the_authenticated_worker_role() {
    for declared in [
        None,
        Some(ProtoNodeRole::Worker),
        Some(ProtoNodeRole::Builder),
    ] {
        let request = PublishNodeStateRequest {
            role: declared.map(i32::from),
            auto_lifecycle_delete_completions: vec![Default::default()],
            ..Default::default()
        };
        let intent = PublicationRoleIntent::try_from_proto(&request).unwrap();
        intent
            .validate_authenticated_role(NodeRole::Worker)
            .unwrap();
        for role in [NodeRole::Builder, NodeRole::Cluster] {
            let error = intent.validate_authenticated_role(role).unwrap_err();
            assert_eq!(error.code(), tonic::Code::InvalidArgument);
            assert_eq!(
                error.message(),
                "auto-lifecycle delete completions can only be published by worker nodes"
            );
        }
    }
}
