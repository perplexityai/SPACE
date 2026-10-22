//! Backup authority is historical evidence plus a freshly authenticated publisher.
use super::ClusterApiService;
use pplx_space_common_core::types::{ExecutionGeneration, K8sNodeUid, SubstrateKind};
use pplx_space_common_svc::auth::{
    KubernetesPodIdentity, NodeApiAuthenticatedCaller, NodeIdentityVerification,
};
use pplx_space_snapshot_store::{
    self as store, InsertOutcome, SnapshotRecord, backup_authority as authority,
};
use tonic::{Request, Status};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum IdentityMode {
    Shadow,
    Compatibility,
    Strict,
}
impl IdentityMode {
    pub(super) fn from_env() -> Result<Self, Status> {
        match std::env::var("SPACE_BACKUP_IDENTITY_MODE").as_deref() {
            Err(std::env::VarError::NotPresent) | Ok("shadow") => Ok(Self::Shadow),
            Ok("compatibility") => Ok(Self::Compatibility),
            Ok("strict") => Ok(Self::Strict),
            _ => Err(Status::failed_precondition(
                "SPACE_BACKUP_IDENTITY_MODE must be shadow, compatibility, or strict",
            )),
        }
    }
}

pub(super) struct Caller {
    pub remote_addr: Option<std::net::SocketAddr>,
    identity: Option<Result<KubernetesPodIdentity, Status>>,
}
impl Caller {
    pub(super) fn verified(&self) -> Result<&KubernetesPodIdentity, Status> {
        self.identity
            .as_ref()
            .ok_or_else(|| {
                Status::permission_denied("backup requires a pod-bound worker identity")
            })?
            .as_ref()
            .map_err(Clone::clone)
    }
}
pub(super) fn database_error(error: sqlx::Error) -> Status {
    Status::internal(format!("backup authority: {error}"))
}

impl ClusterApiService {
    pub(super) async fn backup_caller<T>(&self, request: &Request<T>) -> Caller {
        // Verification is bounded and completed before any database lock. Static
        // authentication carries no historical Kubernetes incarnation evidence.
        let bound = request
            .extensions()
            .get::<NodeApiAuthenticatedCaller>()
            .and_then(|caller| caller.kubernetes_pod())
            .is_some();
        Caller {
            remote_addr: request.remote_addr(),
            identity: if bound {
                Some(
                    self.backup_identity
                        .verify(request, NodeIdentityVerification::Required)
                        .await,
                )
            } else {
                None
            },
        }
    }

    async fn check_incarnation(
        &self,
        conn: &mut sqlx::PgConnection,
        attempt: &SnapshotRecord,
        uid: Option<&K8sNodeUid>,
        caller: &Caller,
    ) -> Result<Option<KubernetesPodIdentity>, Status> {
        // Shadow deploys readers before issuing new authority. Legacy rows keep
        // the existing peer/generation policy even when inventory or live lookup
        // is unavailable. Persisted proof still takes the required path below.
        if uid.is_none() && self.backup_identity_mode == IdentityMode::Shadow {
            return Ok(None);
        }
        let source = attempt
            .source_node_id()
            .ok_or_else(|| Status::permission_denied("backup has no source node"))?;
        let node = authority::node_incarnation(conn, source)
            .await
            .map_err(database_error)?
            .ok_or_else(|| Status::permission_denied("backup source node no longer registered"))?;
        match node.managed_by {
            SubstrateKind::Space if uid.is_none() => return Ok(None),
            SubstrateKind::Space => {
                return Err(Status::permission_denied(
                    "verified source changed substrate",
                ));
            }
            SubstrateKind::Kubernetes => (),
        }
        if uid.is_none()
            && caller.identity.is_none()
            && self.backup_identity_mode != IdentityMode::Strict
        {
            return Ok(None);
        }
        let identity = caller.verified()?;
        if node.node_name.as_ref() != Some(identity.node_name())
            || node.node_uid.as_ref() != Some(identity.node_uid())
            || uid.is_some_and(|uid| uid != identity.node_uid())
        {
            return Err(Status::permission_denied(
                "backup publisher incarnation does not match its recorded source",
            ));
        }
        Ok(Some(identity.clone()))
    }

    pub(super) async fn authorize_backup_incarnation(
        &self,
        conn: &mut sqlx::PgConnection,
        attempt: &SnapshotRecord,
        caller: &Caller,
    ) -> Result<(), Status> {
        self.check_incarnation(conn, attempt, attempt.source_node_uid(), caller)
            .await?;
        if attempt.source_node_uid().is_none() && self.backup_identity_mode == IdentityMode::Strict
        {
            let node = authority::node_incarnation(
                conn,
                attempt.source_node_id().expect("source checked"),
            )
            .await
            .map_err(database_error)?;
            if node.is_some_and(|node| node.managed_by == SubstrateKind::Kubernetes) {
                return Err(Status::failed_precondition(
                    "legacy backup has no recorded incarnation; explicit recovery required",
                ));
            }
        }
        Ok(())
    }

    pub(super) async fn authorize_backup_registration(
        &self,
        conn: &mut sqlx::PgConnection,
        attempt: &SnapshotRecord,
        outcome: InsertOutcome,
        prior: Option<&authority::Registration>,
        generation: Option<ExecutionGeneration>,
        caller: &Caller,
    ) -> Result<(), Status> {
        if outcome != InsertOutcome::Created || attempt.source_node_uid().is_some() {
            return self
                .authorize_backup_incarnation(conn, attempt, caller)
                .await;
        }
        let expected_uid = prior.map(|prior| &prior.source_node_uid);
        let Some(identity) = self
            .check_incarnation(conn, attempt, expected_uid, caller)
            .await?
        else {
            return Ok(());
        };
        let proven = if prior.is_some() {
            true
        } else if let Some(generation) = generation {
            // An existing unverified registration cannot acquire historical proof
            // from today's registry or from a later execution's supplied generation.
            let legacy = store::artifact_registered_by_another_attempt(
                &mut *conn,
                attempt.sandbox_id(),
                attempt
                    .artifact()
                    .and_then(|artifact| artifact.local_artifact_id())
                    .expect("registration artifact"),
                attempt.id(),
            )
            .await
            .map_err(|error| Status::internal(format!("backup registration lookup: {error}")))?;
            let execution =
                authority::execution_incarnation(conn, attempt.sandbox_id(), generation)
                    .await
                    .map_err(database_error)?;
            if execution.as_ref().is_some_and(|execution| {
                execution
                    .node_name
                    .as_ref()
                    .is_some_and(|name| name != identity.node_name())
                    || execution
                        .node_uid
                        .as_ref()
                        .is_some_and(|uid| uid != identity.node_uid())
            }) {
                return Err(Status::permission_denied(
                    "capture execution belongs to another worker incarnation",
                ));
            }
            !legacy
                && execution.is_some_and(|execution| {
                    execution.node_name.as_ref() == Some(identity.node_name())
                        && execution.node_uid.as_ref() == Some(identity.node_uid())
                })
        } else {
            false
        };
        if !proven {
            return if self.backup_identity_mode == IdentityMode::Strict {
                Err(Status::failed_precondition(
                    "backup execution incarnation is unknown; explicit recovery required",
                ))
            } else {
                Ok(())
            };
        }
        // A verified retry preserves its existing authority even after rollback
        // to shadow; creating an unverified successor would downgrade enforcement.
        if !authority::certify(
            conn,
            attempt.id(),
            attempt.source_node_id().expect("source checked"),
            identity.node_uid(),
            prior.map(|prior| prior.id),
        )
        .await
        .map_err(database_error)?
        {
            return Err(Status::aborted(
                "backup registration changed during identity verification",
            ));
        }
        Ok(())
    }
}

pub(super) fn recovery_error(error: store::recovery::RecoveryError) -> Status {
    match error {
        store::recovery::RecoveryError::Refused(message) => Status::failed_precondition(message),
        _ => Status::internal(format!("backup recovery: {error}")),
    }
}
