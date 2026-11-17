//! Node-bound retained-disk authority. Cloud provisioning stays in the
//! controller; this service only records fenced intent and progress.

use std::collections::BTreeMap;

use pplx_space_accounts::NodeId;
use pplx_space_common_core::{
    sql,
    types::{ExecutionGeneration, NodeRole, SandboxId},
};
use pplx_space_execution_common::proto::{
    BlockVolumeBinding, BlockVolumeExecution, BlockVolumeFence,
};
use pplx_space_execution_spec::GuestDataDrive;
use pplx_space_sandbox_store::{
    self as sandboxes,
    block_volume::{self as store, BlockVolume, Initialization, VolumePhase},
};
use serde::Deserialize;
use sqlx::{Postgres, Transaction};
use tonic::{Request, Response, Status};
use uuid::Uuid;

use crate::{ClusterApiService, proto::*};

const HOSTS_ENV: &str = "SPACE_EBS_NODE_BINDINGS";

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Host {
    instance_id: String,
    availability_zone: String,
}

/// Administrator-verified node -> EC2 identity, kept out of node assertions.
/// Experimental deployments pin at most two hosts, both in one AZ.
#[derive(Clone, Debug, Default)]
pub(crate) struct Hosts(BTreeMap<NodeId, Host>);

impl Hosts {
    fn require(&self, node: NodeId) -> Result<&Host, Status> {
        self.0.get(&node).ok_or_else(|| {
            Status::permission_denied("node has no administrator-verified block-volume identity")
        })
    }

    pub(crate) fn from_environment(
        env: &dyn pplx_space_env_vars::Environment,
    ) -> Result<Self, Status> {
        let Some(raw) = env.var_os(HOSTS_ENV) else {
            return Ok(Self::default());
        };
        let raw = raw.to_str().ok_or_else(|| {
            Status::failed_precondition("invalid block-volume host configuration")
        })?;
        let hosts: BTreeMap<NodeId, Host> = serde_json::from_str(raw)
            .map_err(|_| Status::failed_precondition("invalid block-volume host configuration"))?;
        let mut zones = std::collections::BTreeSet::new();
        let mut instances = std::collections::BTreeSet::new();
        if hosts.is_empty()
            || hosts.len() > 2
            || hosts.values().any(|host| {
                zones.insert(&host.availability_zone);
                !instances.insert(&host.instance_id)
                    || !host.instance_id.strip_prefix("i-").is_some_and(|id| {
                        id.len() == 17 && id.bytes().all(|b| b.is_ascii_hexdigit())
                    })
                    || host.availability_zone.is_empty()
                    || !host
                        .availability_zone
                        .bytes()
                        .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
            })
            || zones.len() != 1
        {
            return Err(Status::failed_precondition(
                "block-volume hosts must be distinct, explicit, and in one AZ",
            ));
        }
        Ok(Self(hosts))
    }
}

struct Execution {
    node: NodeId,
    sandbox: SandboxId,
    generation: ExecutionGeneration,
    attempt: Uuid,
}

impl Execution {
    fn parse(wire: &BlockVolumeExecution) -> Result<Self, Status> {
        Ok(Self {
            node: wire.node_id.parse().map_err(invalid)?,
            sandbox: wire.sandbox_id.parse().map_err(invalid)?,
            generation: ExecutionGeneration::new(
                i64::try_from(wire.execution_generation).map_err(invalid)?,
            )
            .map_err(invalid)?,
            attempt: wire.activation_attempt_id.parse().map_err(invalid)?,
        })
    }
}

fn invalid(error: impl std::fmt::Display) -> Status {
    Status::invalid_argument(format!("invalid block-volume identity: {error}"))
}

fn database(error: impl std::fmt::Display) -> Status {
    Status::internal(format!("block-volume authority storage: {error}"))
}

fn refused() -> Status {
    Status::failed_precondition("block-volume execution, epoch, or checkpoint is no longer current")
}

struct Fence {
    execution: Execution,
    volume: Uuid,
    epoch: i64,
}

impl Fence {
    fn parse(wire: &BlockVolumeFence) -> Result<Self, Status> {
        let epoch = i64::try_from(wire.attachment_epoch).map_err(invalid)?;
        if epoch <= 0 {
            return Err(refused());
        }
        Ok(Self {
            execution: Execution::parse(wire.execution.as_ref().ok_or_else(refused)?)?,
            volume: wire.volume_id.parse().map_err(invalid)?,
            epoch,
        })
    }

    fn owns(&self, row: &BlockVolume) -> bool {
        row.id == self.volume
            && row.node_id == Some(self.execution.node)
            && row.execution_id == Some(self.execution.attempt)
            && row.attachment_epoch == self.epoch
    }
}

impl ClusterApiService {
    async fn authenticate_block_execution<T>(
        &self,
        request: &Request<T>,
        execution: &Execution,
    ) -> Result<(), Status> {
        self.authenticated_origin_node_id(
            request,
            &execution.node.to_string(),
            Some(NodeRole::Worker),
        )
        .await?;
        self.block_volume_hosts.require(execution.node)?;
        Ok(())
    }

    /// Serialize with shared placement/lifecycle writers, then read the
    /// substrate-independent active execution projection under that lock.
    async fn lock_current_block_execution(
        &self,
        execution: &Execution,
    ) -> Result<Transaction<'static, Postgres>, Status> {
        let mut tx = self.pool.begin().await.map_err(database)?;
        let exists: Option<Uuid> =
            sqlx::query_scalar(sql!("SELECT id FROM sandboxes WHERE id = $1 FOR UPDATE"))
                .bind(execution.sandbox)
                .fetch_optional(&mut *tx)
                .await
                .map_err(database)?;
        if exists.is_none() {
            return Err(refused());
        }
        let current = sandboxes::auto_lifecycle_execution(&mut *tx, execution.sandbox)
            .await
            .map_err(database)?
            .ok_or_else(refused)?;
        if current.execution_generation != Some(execution.generation.get())
            || current.activation_attempt_id != Some(execution.attempt)
            || ![current.active_node_id, current.target_node_id].contains(&Some(execution.node))
        {
            return Err(refused());
        }
        Ok(tx)
    }

    async fn lock_block_fence<T>(
        &self,
        request: &Request<T>,
        fence: &Fence,
    ) -> Result<(Transaction<'static, Postgres>, BlockVolume), Status> {
        self.authenticate_block_execution(request, &fence.execution)
            .await?;
        let mut tx = self.lock_current_block_execution(&fence.execution).await?;
        let row = store::get(&mut *tx, fence.execution.sandbox)
            .await
            .map_err(database)?
            .ok_or_else(refused)?;
        if !fence.owns(&row) {
            return Err(refused());
        }
        Ok((tx, row))
    }

    pub(crate) async fn resolve_block_volume_binding(
        &self,
        request: Request<ResolveBlockVolumeBindingRequest>,
    ) -> Result<Response<ResolveBlockVolumeBindingResponse>, Status> {
        let wire = request.get_ref().execution.as_ref().ok_or_else(refused)?;
        let execution = Execution::parse(wire)?;
        // Every worker resolves this dependency, including workers outside the
        // disk experiment. Authenticate its current execution before revealing
        // absence; require an EBS host grant only when a disk actually exists.
        self.authenticated_origin_node_id(
            &request,
            &execution.node.to_string(),
            Some(NodeRole::Worker),
        )
        .await?;
        let mut tx = self.lock_current_block_execution(&execution).await?;
        let Some(mut row) = store::get(&mut *tx, execution.sandbox)
            .await
            .map_err(database)?
        else {
            tx.commit().await.map_err(database)?;
            return Ok(Response::new(ResolveBlockVolumeBindingResponse {
                binding: None,
            }));
        };
        let host = self.block_volume_hosts.require(execution.node)?;
        if row.delete_requested {
            return Err(refused());
        }
        if row.phase == VolumePhase::Available {
            store::reserve_attachment(
                &mut tx,
                execution.sandbox,
                store::AttachmentTarget {
                    node_id: execution.node,
                    instance_id: &host.instance_id,
                    device_name: "/dev/sdf",
                    execution_id: execution.attempt,
                    availability_zone: &host.availability_zone,
                    // One experimental attachment per host avoids device-name
                    // collisions and leaves existing non-experiment slots alone.
                    host_attachment_limit: 1,
                },
            )
            .await
            .map_err(database)?;
            row = store::get(&mut *tx, execution.sandbox)
                .await
                .map_err(database)?
                .ok_or_else(refused)?;
        }
        tx.commit().await.map_err(database)?;
        if row.phase != VolumePhase::Attached {
            return Err(Status::unavailable(
                "block volume is provisioning or awaiting a proven detach",
            ));
        }
        if row.node_id != Some(execution.node) || row.execution_id != Some(execution.attempt) {
            return Err(refused());
        }
        Ok(Response::new(ResolveBlockVolumeBindingResponse {
            binding: Some(BlockVolumeBinding {
                fence: Some(BlockVolumeFence {
                    execution: Some(wire.clone()),
                    volume_id: row.id.to_string(),
                    attachment_epoch: u64::try_from(row.attachment_epoch).map_err(database)?,
                }),
                aws_volume_id: row.aws_volume_id.ok_or_else(refused)?,
                filesystem_uuid: row.filesystem_uuid.to_string(),
                mount: row.mount_spec.mount.as_str().to_owned(),
                read_only: row.mount_spec.permissions.is_read_only(),
                disk_generation: u64::try_from(row.disk_generation).map_err(database)?,
                initialization_ready: row.initialization == Initialization::Ready,
                initialization_claim: row.initialization_claim.map(|id| id.to_string()),
                topology_version: GuestDataDrive::TOPOLOGY_VERSION,
            }),
        }))
    }

    pub(crate) async fn claim_block_volume_initialization(
        &self,
        request: Request<ClaimBlockVolumeInitializationRequest>,
    ) -> Result<Response<ClaimBlockVolumeInitializationResponse>, Status> {
        let req = request.get_ref();
        let fence = Fence::parse(req.fence.as_ref().ok_or_else(refused)?)?;
        let claim = req.claim_id.parse().map_err(invalid)?;
        let (mut tx, _) = self.lock_block_fence(&request, &fence).await?;
        let granted = store::claim_initialization(
            &mut *tx,
            fence.volume,
            fence.execution.attempt,
            fence.epoch,
            claim,
        )
        .await
        .map_err(database)?;
        tx.commit().await.map_err(database)?;
        Ok(Response::new(ClaimBlockVolumeInitializationResponse {
            granted,
        }))
    }

    pub(crate) async fn complete_block_volume_initialization(
        &self,
        request: Request<CompleteBlockVolumeInitializationRequest>,
    ) -> Result<Response<BlockVolumeProgressResponse>, Status> {
        let req = request.get_ref();
        let fence = Fence::parse(req.fence.as_ref().ok_or_else(refused)?)?;
        let (mut tx, row) = self.lock_block_fence(&request, &fence).await?;
        if !store::complete_initialization(
            &mut *tx,
            fence.volume,
            fence.execution.attempt,
            fence.epoch,
            req.claim_id.parse().map_err(invalid)?,
            req.filesystem_uuid.parse().map_err(invalid)?,
        )
        .await
        .map_err(database)?
        {
            return Err(refused());
        }
        tx.commit().await.map_err(database)?;
        progress(row.disk_generation)
    }

    pub(crate) async fn activate_block_volume(
        &self,
        request: Request<ActivateBlockVolumeRequest>,
    ) -> Result<Response<BlockVolumeProgressResponse>, Status> {
        let req = request.get_ref();
        let fence = Fence::parse(req.fence.as_ref().ok_or_else(refused)?)?;
        let (mut tx, _) = self.lock_block_fence(&request, &fence).await?;
        let checkpoint = req
            .checkpoint_id
            .as_deref()
            .map(str::parse)
            .transpose()
            .map_err(invalid)?;
        if !store::activate(
            &mut *tx,
            fence.volume,
            fence.execution.attempt,
            fence.epoch,
            req.activation_id.parse().map_err(invalid)?,
            checkpoint,
        )
        .await
        .map_err(database)?
        {
            return Err(refused());
        }
        let row = store::get(&mut *tx, fence.execution.sandbox)
            .await
            .map_err(database)?
            .ok_or_else(refused)?;
        tx.commit().await.map_err(database)?;
        progress(row.disk_generation)
    }

    pub(crate) async fn publish_block_volume_checkpoint(
        &self,
        request: Request<PublishBlockVolumeCheckpointRequest>,
    ) -> Result<Response<BlockVolumeProgressResponse>, Status> {
        let req = request.get_ref();
        let fence = Fence::parse(req.fence.as_ref().ok_or_else(refused)?)?;
        let (mut tx, _) = self.lock_block_fence(&request, &fence).await?;
        let generation = i64::try_from(req.disk_generation).map_err(invalid)?;
        if !store::publish_checkpoint(
            &mut *tx,
            fence.volume,
            fence.execution.attempt,
            fence.epoch,
            generation,
            req.checkpoint_id.parse().map_err(invalid)?,
        )
        .await
        .map_err(database)?
        {
            return Err(refused());
        }
        tx.commit().await.map_err(database)?;
        progress(generation)
    }

    pub(crate) async fn release_block_volume_binding(
        &self,
        request: Request<ReleaseBlockVolumeBindingRequest>,
    ) -> Result<Response<BlockVolumeProgressResponse>, Status> {
        let fence = Fence::parse(request.get_ref().fence.as_ref().ok_or_else(refused)?)?;
        self.authenticate_block_execution(&request, &fence.execution)
            .await?;
        // Cleanup authority survives execution retirement and sandbox deletion.
        // The retained row still requires the exact old node/attempt/epoch.
        let mut tx = self.pool.begin().await.map_err(database)?;
        let row = store::get(&mut *tx, fence.execution.sandbox)
            .await
            .map_err(database)?
            .ok_or_else(refused)?;
        if !fence.owns(&row)
            || !store::acknowledge_release(
                &mut *tx,
                fence.volume,
                fence.execution.attempt,
                fence.epoch,
            )
            .await
            .map_err(database)?
        {
            return Err(refused());
        }
        tx.commit().await.map_err(database)?;
        progress(row.disk_generation)
    }
}

fn progress(generation: i64) -> Result<Response<BlockVolumeProgressResponse>, Status> {
    Ok(Response::new(BlockVolumeProgressResponse {
        disk_generation: u64::try_from(generation).map_err(database)?,
    }))
}
