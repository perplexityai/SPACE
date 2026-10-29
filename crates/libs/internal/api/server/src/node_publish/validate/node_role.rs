use pplx_space_common_core::types::NodeRole;
use tonic::Status;

use crate::proto::{NodeRole as ProtoNodeRole, PublishNodeStateRequest};

pub(crate) fn db_role_from_proto(role: i32) -> Result<NodeRole, Status> {
    if role == ProtoNodeRole::Worker as i32 {
        Ok(NodeRole::Worker)
    } else if role == ProtoNodeRole::Builder as i32 {
        Ok(NodeRole::Builder)
    } else if role == ProtoNodeRole::Cluster as i32 {
        Ok(NodeRole::Cluster)
    } else {
        Err(Status::invalid_argument(format!(
            "unknown node role {role}"
        )))
    }
}

pub(crate) fn optional_db_role_from_proto(role: Option<i32>) -> Result<Option<NodeRole>, Status> {
    role.map(db_role_from_proto).transpose()
}

pub(crate) fn db_role_from_proto_or_default(
    role: Option<i32>,
    default: NodeRole,
) -> Result<NodeRole, Status> {
    optional_db_role_from_proto(role).map(|role| role.unwrap_or(default))
}

/// Role constraints carried by the request, not authentication evidence.
#[derive(Debug)]
pub(crate) struct PublicationRoleIntent {
    role_hint: Option<NodeRole>,
    publishes_delete_completions: bool,
}

impl PublicationRoleIntent {
    pub(crate) fn try_from_proto(request: &PublishNodeStateRequest) -> Result<Self, Status> {
        let requested_role = optional_db_role_from_proto(request.role)?;
        let publishes_delete_completions = !request.auto_lifecycle_delete_completions.is_empty();
        let publishes_sandboxes = request.sandbox_inventory_complete
            || !request.sandboxes.is_empty()
            || publishes_delete_completions;
        if publishes_sandboxes
            && let Some(role) = requested_role
            && role != NodeRole::Worker
            && role != NodeRole::Builder
        {
            return Err(Status::invalid_argument(
                "sandbox inventory state can only be published by worker or builder nodes",
            ));
        }
        let role_hint = match (requested_role, publishes_sandboxes) {
            (Some(role), _) => Some(role),
            (None, true) => Some(NodeRole::Worker),
            (None, false) => None,
        };
        Ok(Self {
            role_hint,
            publishes_delete_completions,
        })
    }

    pub(crate) fn role_hint(&self) -> Option<NodeRole> {
        self.role_hint
    }

    /// Only workers execute the cluster-owned deletes these receipts prove.
    /// The optional self-declared role cannot authorize receipt consumption.
    pub(crate) fn validate_authenticated_role(&self, role: NodeRole) -> Result<(), Status> {
        if self.publishes_delete_completions && role != NodeRole::Worker {
            return Err(Status::invalid_argument(
                "auto-lifecycle delete completions can only be published by worker nodes",
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
