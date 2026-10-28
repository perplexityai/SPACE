use super::*;
use crate::proto::{ExecutionInventoryObservation, PublishedNodeStatus, PublishedTemplateState};
use pplx_space_common_core::types::{NodeId, NodeRole};
use pplx_space_execution_common::proto::{NodeExecutionInventory, RuntimeInstallation};

fn origin() -> OriginNode {
    OriginNode {
        id: NodeId::from_uuid(uuid::Uuid::from_u128(1)),
        display_id: "synthetic-node".to_owned(),
        address: None,
        role: NodeRole::Worker,
    }
}

#[test]
fn publication_rejection_precedence_is_independent_of_persistence() {
    let mut request = PublishNodeStateRequest {
        node_status: Some(PublishedNodeStatus {
            network_slots_ready: u32::MAX,
            ..Default::default()
        }),
        execution_observation: Some(ExecutionInventoryObservation::default()),
        templates: vec![PublishedTemplateState::default()],
        execution_inventory: Some(NodeExecutionInventory {
            runtimes: vec![RuntimeInstallation::default()],
            ..Default::default()
        }),
        ..Default::default()
    };
    for (step, message) in [
        "network_slots_ready exceeds i32::MAX: number too large to fit in target type",
        "execution observation is missing a supported outcome",
        "template must not be empty",
        "runtime installation is missing identity",
    ]
    .into_iter()
    .enumerate()
    {
        let error = ValidatedPublish::try_from_proto(origin(), request.clone()).unwrap_err();
        assert_eq!(error.code(), tonic::Code::InvalidArgument);
        assert_eq!(error.message(), message);
        match step {
            0 => request.node_status = None,
            1 => request.execution_observation = None,
            2 => request.templates.clear(),
            3 => request.execution_inventory = None,
            _ => unreachable!(),
        }
    }
    ValidatedPublish::try_from_proto(origin(), request).unwrap();
}
