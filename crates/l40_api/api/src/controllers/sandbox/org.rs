use pplx_space_accounts::OrgId;
use pplx_space_sandbox_model::SandboxId;

use pplx_space_accounts::SandboxOwnerSource;
use pplx_space_sandbox_store::direct::sandbox_listing::{self, FederatedSubject, PageWalk};

use crate::federated_identity::FederatedOwner;

use super::list_page::{ListPage, SANDBOX_LIST_ROW_OVERHEAD_BYTES};

use super::*;

#[derive(Debug, Clone, Copy)]
pub struct SandboxPageCursor {
    pub created_at: time::OffsetDateTime,
    pub id: SandboxId,
}

/// Owner scope of an authenticated ECW principal. Sandbox reads and actions
/// resolve through this scope so a principal only ever sees its own rows:
/// local principals see local org rows; federated principals see the rows
/// owned by their upstream subject plus the local (pre-federation) rows of the
/// API key's org. Every pre-federation sandbox of a cluster lives in that one
/// org and was already reachable with that key, so this crosses no org
/// boundary. Federated rows are never visible to a local scope.
#[derive(Debug, Clone)]
pub enum SandboxAccessScope {
    LocalOrg {
        org_id: OrgId,
    },
    FederatedSubject {
        owner: FederatedOwner,
        /// The API key's local org whose legacy local-owned rows stay reachable.
        legacy_org_id: OrgId,
    },
}

fn sandbox_in_scope(sandbox: &Sandbox, scope: &SandboxAccessScope) -> bool {
    match (scope, sandbox.owner_source) {
        (SandboxAccessScope::LocalOrg { org_id }, SandboxOwnerSource::Local)
        | (
            SandboxAccessScope::FederatedSubject {
                legacy_org_id: org_id,
                ..
            },
            SandboxOwnerSource::Local,
        ) => sandbox.org_id == *org_id,
        (
            SandboxAccessScope::FederatedSubject { owner, .. },
            SandboxOwnerSource::FederatedIdentity,
        ) => sandbox_matches_federated_owner(sandbox, owner),
        (SandboxAccessScope::LocalOrg { .. }, SandboxOwnerSource::FederatedIdentity) => false,
    }
}

/// Strict native identity matching intentionally excludes the ECW legacy bridge.
pub(crate) fn sandbox_matches_federated_owner(sandbox: &Sandbox, owner: &FederatedOwner) -> bool {
    sandbox.owner_source == SandboxOwnerSource::FederatedIdentity
        && sandbox.upstream_issuer.as_deref() == Some(owner.issuer.as_str())
        && sandbox.upstream_org_id.as_deref() == Some(owner.org_id.as_str())
        && sandbox.upstream_subject_id.as_deref() == Some(owner.subject_id.as_str())
}

impl SandboxController {
    pub async fn sandbox(
        &self,
        pool: &PgPool,
        sandbox_id: SandboxId,
    ) -> Result<Option<Sandbox>, ControllerError> {
        fetch_sandbox(pool, sandbox_id)
            .await
            .map_err(|err| crate::controllers::db_contention_error("failed to fetch sandbox", &err))
    }

    pub async fn scoped_sandbox(
        &self,
        pool: &PgPool,
        scope: &SandboxAccessScope,
        sandbox_id: SandboxId,
    ) -> Result<Option<Sandbox>, ControllerError> {
        let sandbox = self.sandbox(pool, sandbox_id).await?;
        let sandbox = sandbox.filter(|sandbox| sandbox_in_scope(sandbox, scope));
        if let Some(sandbox) = &sandbox
            && matches!(scope, SandboxAccessScope::FederatedSubject { .. })
            && matches!(sandbox.owner_source, SandboxOwnerSource::Local)
        {
            // Drain signal for the legacy bridge; drop the bridge once this stays at zero.
            tracing::info!(
                sandbox_id = %sandbox.id,
                "federated principal resolved legacy local sandbox"
            );
        }
        Ok(sandbox)
    }

    /// One ECW list page of the sandboxes `scope` sees, newest first, within
    /// the list byte budget: at most `limit` rows, and whether more follow.
    pub(crate) async fn scoped_sandbox_page(
        &self,
        pool: &PgPool,
        scope: &SandboxAccessScope,
        cursor: Option<SandboxPageCursor>,
        limit: u32,
        metadata_filter: Option<serde_json::Value>,
    ) -> Result<ListPage<Sandbox>, ControllerError> {
        let walk = PageWalk {
            after: cursor.map(|cursor| (cursor.created_at, cursor.id)),
            max_rows: i64::from(limit.saturating_add(1)),
            max_bytes: self.list_page_max_bytes,
            row_overhead_bytes: SANDBOX_LIST_ROW_OVERHEAD_BYTES,
        };
        let rows = match scope {
            SandboxAccessScope::LocalOrg { org_id } => {
                sandbox_listing::local_org_page(pool, *org_id, metadata_filter, &walk).await
            }
            SandboxAccessScope::FederatedSubject {
                owner,
                legacy_org_id,
            } => {
                sandbox_listing::federated_subject_page(
                    pool,
                    FederatedSubject {
                        issuer: &owner.issuer,
                        upstream_org_id: &owner.org_id,
                        subject_id: &owner.subject_id,
                    },
                    *legacy_org_id,
                    metadata_filter,
                    &walk,
                )
                .await
            }
        }
        .map_err(|err| {
            crate::controllers::db_contention_error("failed to fetch sandbox page", &err)
        })?;
        let page = ListPage::from_rows(rows, limit as usize, |row| row.has_next);
        let page = ListPage {
            rows: page.rows.into_iter().map(|row| row.sandbox).collect(),
            has_more: page.has_more,
        };
        if matches!(scope, SandboxAccessScope::FederatedSubject { .. }) {
            let legacy_rows = page
                .rows
                .iter()
                .filter(|sandbox| matches!(sandbox.owner_source, SandboxOwnerSource::Local))
                .count();
            if legacy_rows > 0 {
                // Same drain signal as the point read, for list traffic.
                tracing::info!(
                    legacy_rows,
                    "federated principal listed legacy local sandboxes"
                );
            }
        }
        Ok(page)
    }

    pub fn cached_sandbox_response(&self, sandbox: &Sandbox) -> SandboxResponse {
        sandbox_to_response(sandbox, None)
    }
}

#[cfg(test)]
mod owner_scope_tests;
