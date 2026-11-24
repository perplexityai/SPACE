use std::str::FromStr;

use pplx_space_accounts::{NodeId, OrgId, SystemPublicationGrantId, TenantPublicationGrantId};
use pplx_space_template_model::{BuildRunId, BuildRunIdParseError, TemplateName};
use pplx_space_template_store::{build_base_grants, build_jobs, publication_grants, templates};
use sqlx::{PgExecutor, PgPool};
use time::OffsetDateTime;

#[derive(Clone)]
pub struct TemplateBuildJobsStore {
    pool: PgPool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct BuildJob {
    pub daemon_job_id: BuildRunId,
}

/// A build a builder accepted, as the control plane recorded it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RecordedBuild {
    pub daemon_job_id: BuildRunId,
    /// The builder that accepted the job; `None` for a job routed without a
    /// pinned builder, or whose builder's node row has since been deleted.
    pub builder_node_id: Option<NodeId>,
    /// When the row was first written: at the builder's acceptance for an
    /// admin build, at the ECW template request for an ECW build.
    pub created_at: OffsetDateTime,
}

/// One build submission's pre-dispatch base grant
/// (`template_build_base_grants.id`).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct BuildBaseGrantId(uuid::Uuid);

impl BuildBaseGrantId {
    pub(crate) const fn as_uuid(self) -> uuid::Uuid {
        self.0
    }
}

/// The pre-dispatch grants of one build submission that are pinned to the
/// builder that accepts it.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct BuilderPinnedGrants {
    pub base: Option<BuildBaseGrantId>,
    pub tenant_publication: Option<TenantPublicationGrantId>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct BuildSpec {
    pub tags: Vec<String>,
    pub cpu_count: Option<u32>,
    pub memory_mb: Option<u32>,
}

#[derive(Debug, thiserror::Error)]
pub enum TemplateBuildJobsStoreError {
    #[error("template build job storage failed: {0}")]
    Storage(#[from] sqlx::Error),
    #[error("stored template build job has an invalid daemon job ID: {0}")]
    InvalidDaemonJobId(#[from] BuildRunIdParseError),
    #[error("template build inputs belong to another organization")]
    OwnershipConflict,
    #[error("tenant publication requires the accepting builder identity")]
    BuilderIdentityRequired,
}

impl TemplateBuildJobsStoreError {
    /// The wrapped `sqlx::Error`, when this is a storage failure rather than a
    /// malformed stored row.
    ///
    /// Lives here rather than in each surface's error mapper so the variant
    /// list is written once. Two surfaces classify this enum for contention
    /// (the controller boundary and the ECW template-build handlers) and a
    /// third variant that wrapped a `sqlx::Error` would otherwise have to be
    /// remembered in both — a miss there fails open, reporting a lost lock
    /// race as permanent.
    pub(crate) fn as_sql(&self) -> Option<&sqlx::Error> {
        match self {
            Self::Storage(error) => Some(error),
            Self::InvalidDaemonJobId(_)
            | Self::OwnershipConflict
            | Self::BuilderIdentityRequired => None,
        }
    }
}

impl TemplateBuildJobsStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub(crate) async fn template_owner_matches(
        &self,
        template_name: &TemplateName,
        org_id: OrgId,
    ) -> Result<bool, sqlx::Error> {
        template_build_owner_matches(&self.pool, template_name, org_id).await
    }

    pub(crate) async fn record_accepted_build(
        &self,
        org_id: OrgId,
        template_name: &TemplateName,
        client_build_id: Option<&str>,
        daemon_job_id: BuildRunId,
        builder_node_id: Option<NodeId>,
        grants: BuilderPinnedGrants,
    ) -> Result<(), TemplateBuildJobsStoreError> {
        if grants.tenant_publication.is_some() && builder_node_id.is_none() {
            return Err(TemplateBuildJobsStoreError::BuilderIdentityRequired);
        }
        if !template_build_owner_matches(&self.pool, template_name, org_id).await? {
            return Err(TemplateBuildJobsStoreError::OwnershipConflict);
        }
        let daemon_job_id_text = daemon_job_id.to_string();
        let build_id = client_build_id.unwrap_or(&daemon_job_id_text);

        let mut tx = self.pool.begin().await?;
        let recorded = build_jobs::record_accepted(
            &mut *tx,
            build_jobs::AcceptedBuild {
                template: template_name,
                build_id,
                daemon_job_id,
                builder_node_id,
                org_id,
            },
        )
        .await?;

        if !recorded {
            return Err(TemplateBuildJobsStoreError::OwnershipConflict);
        }
        if let Some(builder_node_id) = builder_node_id {
            if let Some(base_grant) = grants.base {
                Self::pin_base_grant(&mut *tx, base_grant, builder_node_id).await?;
            }
            if let Some(publication_grant) = grants.tenant_publication {
                Self::pin_tenant_publication_grant(&mut *tx, publication_grant, builder_node_id)
                    .await?;
            }
        }
        tx.commit().await?;
        Ok(())
    }

    /// Record, before `StartBuild` is dispatched, that this submission of
    /// `org_id`'s build of `template_name` is authorized to read `base`.
    ///
    /// ClusterApi scopes release lookups by caller node and lets a registered
    /// builder resolve the base while this grant is fresh. The grant has to
    /// exist before dispatch because the API learns which builder takes the
    /// job only from the response; [`Self::pin_base_grant`] then binds it to
    /// that builder. Every submission gets its own row, so accepting one build
    /// never alters another build's authorization. Rows older than the longest
    /// validity ClusterApi honours are swept here, since nothing else observes
    /// a build's end.
    pub(crate) async fn record_base_grant(
        &self,
        org_id: OrgId,
        template_name: &TemplateName,
        base: &TemplateName,
    ) -> Result<BuildBaseGrantId, sqlx::Error> {
        build_base_grants::delete_expired(&self.pool).await?;
        let grant = BuildBaseGrantId(uuid::Uuid::now_v7());
        build_base_grants::insert(&self.pool, grant.as_uuid(), template_name, base, org_id).await?;
        Ok(grant)
    }

    /// Record, before `StartBuild` is dispatched, that this submission may
    /// publish `template_name` as a system template.
    ///
    /// Build-manager also publishes tenant builds, so ClusterApi accepts its
    /// system publication only under such a grant, and the release
    /// registration consumes it. The grant has to exist before dispatch
    /// because the build may publish before the API records the response.
    /// Rows older than the freshness ClusterApi honours are swept here, since
    /// nothing else observes a build's end.
    pub(crate) async fn record_system_publication_grant(
        &self,
        template_name: &TemplateName,
    ) -> Result<SystemPublicationGrantId, TemplateBuildJobsStoreError> {
        publication_grants::delete_expired_system(&self.pool).await?;
        let grant = SystemPublicationGrantId::new();
        publication_grants::insert_system(&self.pool, grant, template_name).await?;
        Ok(grant)
    }

    /// Withdraw the grant of a submission that was never dispatched. Other
    /// submissions' grants are untouched.
    pub(crate) async fn revoke_system_publication_grant(
        &self,
        grant: SystemPublicationGrantId,
    ) -> Result<(), TemplateBuildJobsStoreError> {
        publication_grants::delete_system(&self.pool, grant).await?;
        Ok(())
    }

    /// Record, before `StartBuild` is dispatched, that this submission may
    /// publish `template_name` for `org_id`.
    ///
    /// Build-manager's publisher identity can assert any organization, so
    /// ClusterApi accepts its tenant publication only under such a grant, from
    /// the builder the grant is bound to, and the release registration
    /// consumes it. The grant has to exist before dispatch because the build
    /// may publish before the API records the response;
    /// [`Self::record_accepted_build`] then pins it to the accepting builder.
    /// Rows older than the longest validity ClusterApi honours are swept
    /// here, since nothing else observes a build's end.
    pub(crate) async fn record_tenant_publication_grant(
        &self,
        org_id: OrgId,
        template_name: &TemplateName,
    ) -> Result<TenantPublicationGrantId, TemplateBuildJobsStoreError> {
        publication_grants::delete_expired_tenant(&self.pool).await?;
        let grant = TenantPublicationGrantId::new();
        publication_grants::insert_tenant(&self.pool, grant, template_name, org_id).await?;
        Ok(grant)
    }

    /// Withdraw publication authority when dispatch or acceptance fails.
    /// Other submissions' grants are untouched.
    pub(crate) async fn revoke_tenant_publication_grant(
        &self,
        grant: TenantPublicationGrantId,
    ) -> Result<(), TemplateBuildJobsStoreError> {
        publication_grants::delete_tenant(&self.pool, grant).await?;
        Ok(())
    }

    /// Pin one submission's publication grant to the builder that accepted
    /// it, with the same lifetime as its base grant's pin. Other submissions'
    /// grants are untouched.
    async fn pin_tenant_publication_grant(
        executor: impl PgExecutor<'_>,
        grant: TenantPublicationGrantId,
        builder_node_id: NodeId,
    ) -> Result<(), sqlx::Error> {
        publication_grants::pin_tenant_to_builder(executor, grant, builder_node_id).await
    }

    /// Pin one submission's base grant to the builder that accepted it. From
    /// here on only that builder may use the grant, for as long as its queued
    /// job can still be running, so a build that waits behind long jobs keeps
    /// its base authorization. Other submissions' grants are untouched.
    async fn pin_base_grant(
        executor: impl PgExecutor<'_>,
        base_grant: BuildBaseGrantId,
        builder_node_id: NodeId,
    ) -> Result<(), sqlx::Error> {
        build_base_grants::pin_to_builder(executor, base_grant.as_uuid(), builder_node_id).await
    }

    pub(crate) async fn lookup_build(
        &self,
        org_id: OrgId,
        template_name: &TemplateName,
        build_id: &str,
    ) -> Result<Option<BuildJob>, TemplateBuildJobsStoreError> {
        if !template_build_owner_matches(&self.pool, template_name, org_id).await? {
            return Ok(None);
        }
        let daemon_job_id =
            build_jobs::daemon_job_id(&self.pool, template_name, build_id, org_id).await?;

        daemon_job_id
            .map(|job_id| {
                BuildRunId::from_str(&job_id).map(|daemon_job_id| BuildJob { daemon_job_id })
            })
            .transpose()
            .map_err(Into::into)
    }

    pub(crate) async fn find_builder_node(
        &self,
        daemon_job_id: BuildRunId,
    ) -> Result<Option<NodeId>, TemplateBuildJobsStoreError> {
        build_jobs::builder_node(&self.pool, daemon_job_id)
            .await
            .map_err(Into::into)
    }

    /// The newest `limit` builds of `template_name` that a builder accepted,
    /// newest first. Rows the ECW flow recorded before submitting carry no
    /// job ID yet and are left out.
    pub(crate) async fn recent_builds(
        &self,
        template_name: &TemplateName,
        limit: u32,
    ) -> Result<Vec<RecordedBuild>, TemplateBuildJobsStoreError> {
        let rows = build_jobs::recent_accepted(&self.pool, template_name, limit).await?;
        rows.into_iter()
            .map(|row| {
                Ok(RecordedBuild {
                    daemon_job_id: BuildRunId::from_str(&row.daemon_job_id)?,
                    builder_node_id: row.builder_node_id.map(NodeId::from_uuid),
                    created_at: row.created_at,
                })
            })
            .collect()
    }

    pub(crate) async fn record_build_spec(
        &self,
        org_id: OrgId,
        template_name: &TemplateName,
        build_id: &str,
        spec: BuildSpec,
    ) -> Result<(), TemplateBuildJobsStoreError> {
        if !template_build_owner_matches(&self.pool, template_name, org_id).await? {
            return Err(TemplateBuildJobsStoreError::OwnershipConflict);
        }
        let recorded = build_jobs::record_spec(
            &self.pool,
            template_name,
            build_id,
            org_id,
            build_jobs::BuildSpec {
                tags: &spec.tags,
                cpu_count: spec.cpu_count.map(|value| value as i32),
                memory_mb: spec.memory_mb.map(|value| value as i32),
            },
        )
        .await?;

        if !recorded {
            return Err(TemplateBuildJobsStoreError::OwnershipConflict);
        }
        Ok(())
    }

    pub(crate) async fn lookup_build_spec(
        &self,
        org_id: OrgId,
        template_name: &TemplateName,
        build_id: &str,
    ) -> Result<Option<BuildSpec>, TemplateBuildJobsStoreError> {
        if !template_build_owner_matches(&self.pool, template_name, org_id).await? {
            return Ok(None);
        }
        let row = build_jobs::spec(&self.pool, template_name, build_id, org_id).await?;

        row.map(|row| {
            Ok(BuildSpec {
                tags: row.tags,
                cpu_count: optional_i32_to_u32(row.cpu_count)?,
                memory_mb: optional_i32_to_u32(row.memory_mb)?,
            })
        })
        .transpose()
    }
}

pub(crate) async fn template_build_owner_matches(
    pool: &PgPool,
    template_name: &TemplateName,
    org_id: OrgId,
) -> Result<bool, sqlx::Error> {
    templates::not_owned_by_another_org(pool, template_name, org_id).await
}

fn optional_i32_to_u32(value: Option<i32>) -> Result<Option<u32>, TemplateBuildJobsStoreError> {
    value
        .map(u32::try_from)
        .transpose()
        .map_err(|error| sqlx::Error::Decode(Box::new(error)).into())
}

#[cfg(test)]
mod tests;
