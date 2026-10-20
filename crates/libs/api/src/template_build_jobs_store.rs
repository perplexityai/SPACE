use std::str::FromStr;

use pplx_space_common_core::{
    sql,
    types::{
        BuildRunId, BuildRunIdParseError, NodeId, OrgId, SystemPublicationGrantId, TemplateName,
        TenantPublicationGrantId,
    },
};
use sqlx::{PgExecutor, PgPool};

#[derive(Clone)]
pub struct TemplateBuildJobsStore {
    pool: PgPool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct BuildJob {
    pub daemon_job_id: BuildRunId,
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
        let result = sqlx::query(sql!(
            "INSERT INTO template_build_jobs (
                 template_id,
                 build_id,
                 daemon_job_id,
                 builder_node_id,
                 org_id
             )
             VALUES ($1, $2, $3, $4, $5)
             ON CONFLICT (template_id, build_id) DO UPDATE
             SET daemon_job_id = EXCLUDED.daemon_job_id,
                 builder_node_id = EXCLUDED.builder_node_id
             WHERE template_build_jobs.org_id IS NULL OR template_build_jobs.org_id = EXCLUDED.org_id"
        ))
        .bind(template_name.as_str())
        .bind(build_id)
        .bind(&daemon_job_id_text)
        .bind(builder_node_id.map(NodeId::as_uuid))
        .bind(org_id.as_uuid())
        .execute(&mut *tx)
        .await?;

        if result.rows_affected() == 0 {
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
        sqlx::query(sql!(
            "DELETE FROM template_build_base_grants
             WHERE granted_at < NOW() - INTERVAL '7 days'"
        ))
        .execute(&self.pool)
        .await?;
        let grant = BuildBaseGrantId(uuid::Uuid::now_v7());
        sqlx::query(sql!(
            "INSERT INTO template_build_base_grants (id, template_id, base_template_name, org_id)
             VALUES ($1, $2, $3, $4)"
        ))
        .bind(grant.as_uuid())
        .bind(template_name.as_str())
        .bind(base.as_str())
        .bind(org_id.as_uuid())
        .execute(&self.pool)
        .await?;
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
        sqlx::query(sql!(
            "DELETE FROM template_system_publication_grants
             WHERE granted_at < NOW() - INTERVAL '7 days'"
        ))
        .execute(&self.pool)
        .await?;
        let grant = SystemPublicationGrantId::new();
        sqlx::query(sql!(
            "INSERT INTO template_system_publication_grants (id, template_name)
             VALUES ($1, $2)"
        ))
        .bind(grant.as_uuid())
        .bind(template_name.as_str())
        .execute(&self.pool)
        .await?;
        Ok(grant)
    }

    /// Withdraw the grant of a submission that was never dispatched. Other
    /// submissions' grants are untouched.
    pub(crate) async fn revoke_system_publication_grant(
        &self,
        grant: SystemPublicationGrantId,
    ) -> Result<(), TemplateBuildJobsStoreError> {
        sqlx::query(sql!(
            "DELETE FROM template_system_publication_grants WHERE id = $1"
        ))
        .bind(grant.as_uuid())
        .execute(&self.pool)
        .await?;
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
        sqlx::query(sql!(
            "DELETE FROM template_tenant_publication_grants
             WHERE granted_at < NOW() - INTERVAL '7 days'"
        ))
        .execute(&self.pool)
        .await?;
        let grant = TenantPublicationGrantId::new();
        sqlx::query(sql!(
            "INSERT INTO template_tenant_publication_grants (id, template_name, org_id)
             VALUES ($1, $2, $3)"
        ))
        .bind(grant.as_uuid())
        .bind(template_name.as_str())
        .bind(org_id.as_uuid())
        .execute(&self.pool)
        .await?;
        Ok(grant)
    }

    /// Withdraw publication authority when dispatch or acceptance fails.
    /// Other submissions' grants are untouched.
    pub(crate) async fn revoke_tenant_publication_grant(
        &self,
        grant: TenantPublicationGrantId,
    ) -> Result<(), TemplateBuildJobsStoreError> {
        sqlx::query(sql!(
            "DELETE FROM template_tenant_publication_grants WHERE id = $1"
        ))
        .bind(grant.as_uuid())
        .execute(&self.pool)
        .await?;
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
        sqlx::query(sql!(
            "UPDATE template_tenant_publication_grants
             SET builder_node_id = $2
             WHERE id = $1"
        ))
        .bind(grant.as_uuid())
        .bind(builder_node_id.as_uuid())
        .execute(executor)
        .await?;
        Ok(())
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
        sqlx::query(sql!(
            "UPDATE template_build_base_grants
             SET builder_node_id = $2
             WHERE id = $1"
        ))
        .bind(base_grant.as_uuid())
        .bind(builder_node_id.as_uuid())
        .execute(executor)
        .await?;
        Ok(())
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
        let daemon_job_id = sqlx::query_scalar::<_, String>(sql!(
            "SELECT daemon_job_id
             FROM template_build_jobs
             WHERE template_id = $1
               AND build_id = $2
               AND (org_id = $3 OR org_id IS NULL)
               AND daemon_job_id IS NOT NULL"
        ))
        .bind(template_name.as_str())
        .bind(build_id)
        .bind(org_id.as_uuid())
        .fetch_optional(&self.pool)
        .await?;

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
        sqlx::query_as::<_, (Option<uuid::Uuid>,)>(sql!(
            "SELECT builder_node_id
             FROM template_build_jobs
             WHERE daemon_job_id = $1"
        ))
        .bind(daemon_job_id.to_string())
        .fetch_optional(&self.pool)
        .await
        .map(|row| row.and_then(|(builder_node_id,)| builder_node_id.map(NodeId::from_uuid)))
        .map_err(Into::into)
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
        let result = sqlx::query(sql!(
            "INSERT INTO template_build_jobs (template_id, build_id, cpu_count, memory_mb, tags, org_id)
             VALUES ($1, $2, $3, $4, $5, $6)
             ON CONFLICT (template_id, build_id) DO UPDATE
             SET cpu_count = EXCLUDED.cpu_count,
                 memory_mb = EXCLUDED.memory_mb,
                 tags = EXCLUDED.tags
             WHERE template_build_jobs.org_id IS NULL OR template_build_jobs.org_id = EXCLUDED.org_id"
        ))
        .bind(template_name.as_str())
        .bind(build_id)
        .bind(spec.cpu_count.map(|value| value as i32))
        .bind(spec.memory_mb.map(|value| value as i32))
        .bind(spec.tags)
        .bind(org_id.as_uuid())
        .execute(&self.pool)
        .await?;

        if result.rows_affected() == 0 {
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
        let row = sqlx::query_as::<_, (Vec<String>, Option<i32>, Option<i32>)>(sql!(
            "SELECT tags, cpu_count, memory_mb
             FROM template_build_jobs
             WHERE template_id = $1
               AND build_id = $2
               AND (org_id = $3 OR org_id IS NULL)"
        ))
        .bind(template_name.as_str())
        .bind(build_id)
        .bind(org_id.as_uuid())
        .fetch_optional(&self.pool)
        .await?;

        row.map(|(tags, cpu_count, memory_mb)| {
            Ok(BuildSpec {
                tags,
                cpu_count: optional_i32_to_u32(cpu_count)?,
                memory_mb: optional_i32_to_u32(memory_mb)?,
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
    sqlx::query_scalar(sql!(
        "SELECT NOT EXISTS (SELECT 1 FROM templates WHERE name = $1 AND org_id <> $2)"
    ))
    .bind(template_name.as_str())
    .bind(org_id.as_uuid())
    .fetch_one(pool)
    .await
}

fn optional_i32_to_u32(value: Option<i32>) -> Result<Option<u32>, TemplateBuildJobsStoreError> {
    value
        .map(u32::try_from)
        .transpose()
        .map_err(|error| sqlx::Error::Decode(Box::new(error)).into())
}

#[cfg(test)]
mod tests;
