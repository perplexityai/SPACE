use pplx_space_cluster_api_client::proto as new_proto;
use prost::Message;
use prost_types::{FileDescriptorProto, FileDescriptorSet};

fn round_trip<O, N>(old: O) -> O
where
    O: Message + Default,
    N: Message + Default,
{
    let via_new: N =
        N::decode(old.encode_to_vec().as_slice()).expect("round-trip encode to new failed");
    O::decode(via_new.encode_to_vec().as_slice()).expect("round-trip encode back to old failed")
}

#[test]
fn list_nodes_request_round_trips() {
    let old = crate::proto::ListNodesRequest {
        role: Some(1),
        include_deleted: true,
    };
    let rt = round_trip::<_, new_proto::ListNodesRequest>(old);
    assert_eq!(old.encode_to_vec(), rt.encode_to_vec());
}

#[test]
fn begin_artifact_backup_request_round_trips() {
    let old = crate::proto::BeginArtifactBackupRequest {
        sandbox_id: "sandbox-123".to_string(),
        local_artifact_id: "art-abc".to_string(),
        artifact_kind: 1,
        backup_attempt_id: "019e6370-cfc9-7a43-9337-399c7b51a1d1".to_string(),
        storage_uri: "s3://bucket/key?region=us-east-1".to_string(),
        artifact_origin: 1,
        snapshot_name: Some("snap-456".to_string()),
        execution: None,
        ..Default::default()
    };
    let rt = round_trip::<_, new_proto::BeginArtifactBackupRequest>(old.clone());
    assert_eq!(old.encode_to_vec(), rt.encode_to_vec());
}

#[test]
fn register_template_release_request_round_trips() {
    let old = crate::proto::RegisterTemplateReleaseRequest {
        template_name: "base".to_string(),
        runtime: "fc".to_string(),
        hash: "sha256:aabbcc".to_string(),
        tags: vec!["v1".to_string(), "latest".to_string()],
        storage_connection_strings: vec!["s3://bucket/key?region=us-east-1".to_string()],
        org_id: Some("org-uuid".to_string()),
        execution: None,
        ..Default::default()
    };
    let rt = round_trip::<_, new_proto::RegisterTemplateReleaseRequest>(old.clone());
    assert_eq!(old.encode_to_vec(), rt.encode_to_vec());
}

#[test]
fn claim_node_reconcile_refs_response_round_trips() {
    use pplx_space_node_api_client::proto::{FacetRef, ReconcileFacet, facet_ref};
    let old = crate::proto::ClaimNodeReconcileRefsResponse {
        refs: vec![FacetRef {
            facet: ReconcileFacet::SandboxDesiredState as i32,
            element: Some(facet_ref::Element::SandboxId(
                "019e6370-cfc9-7a43-9337-399c7b51a1d1".to_string(),
            )),
        }],
        claim_token: "token-xyz".to_string(),
        ..Default::default()
    };
    let rt = round_trip::<_, new_proto::ClaimNodeReconcileRefsResponse>(old.clone());
    assert_eq!(old.encode_to_vec(), rt.encode_to_vec());
}

fn cluster_file(descriptor_set: &[u8], name: &str) -> FileDescriptorProto {
    FileDescriptorSet::decode(descriptor_set)
        .expect("descriptor set decodes")
        .file
        .into_iter()
        .find(|file| file.name.as_deref() == Some(name))
        .unwrap_or_else(|| panic!("{name} is in the descriptor set"))
}

/// The compat layer re-encodes every message across the two packages, and
/// prost drops fields it doesn't know. A field, RPC, or enum value added to
/// one package and not the other would vanish in transit without an error,
/// so the two files must describe the same schema apart from the package.
#[test]
fn cluster_packages_describe_the_same_schema() {
    let old = cluster_file(
        crate::proto::CLUSTER_FILE_DESCRIPTOR_SET,
        "node/api/v1/cluster.proto",
    );
    let new = cluster_file(
        new_proto::CLUSTER_FILE_DESCRIPTOR_SET,
        "cluster/api/v1/cluster.proto",
    );
    let mut new = rename_package(new, ".space.cluster.api.v1.", ".space.node.api.v1.");
    new.name.clone_from(&old.name);
    new.package.clone_from(&old.package);

    const RESYNC: &str = "proto/cluster/api/v1/cluster.proto must be proto/node/api/v1/cluster.proto \
        with the package renamed and common.proto types qualified as space.node.api.v1";
    assert_eq!(old.service, new.service, "{RESYNC}");
    assert_eq!(old.enum_type, new.enum_type, "{RESYNC}");
    // Compare message by message first so a failure names the message.
    for (old_message, new_message) in old.message_type.iter().zip(&new.message_type) {
        assert_eq!(old_message, new_message, "{:?}: {RESYNC}", old_message.name);
    }
    assert_eq!(old.message_type.len(), new.message_type.len(), "{RESYNC}");
    assert_eq!(old.dependency, new.dependency, "{RESYNC}");
    assert_eq!(old.options, new.options, "{RESYNC}");
    // proto2 and proto3 describe some fields identically but differ in presence.
    assert_eq!(old.syntax, new.syntax, "{RESYNC}");
}

fn rename_package(mut file: FileDescriptorProto, from: &str, to: &str) -> FileDescriptorProto {
    fn rename(type_name: &mut Option<String>, from: &str, to: &str) {
        if let Some(name) = type_name.as_mut().filter(|name| name.starts_with(from)) {
            name.replace_range(..from.len(), to);
        }
    }
    fn rename_message(message: &mut prost_types::DescriptorProto, from: &str, to: &str) {
        for field in &mut message.field {
            rename(&mut field.type_name, from, to);
        }
        for nested in &mut message.nested_type {
            rename_message(nested, from, to);
        }
    }
    for message in &mut file.message_type {
        rename_message(message, from, to);
    }
    for method in file
        .service
        .iter_mut()
        .flat_map(|service| &mut service.method)
    {
        rename(&mut method.input_type, from, to);
        rename(&mut method.output_type, from, to);
    }
    file
}
