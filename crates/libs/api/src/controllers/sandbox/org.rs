use pplx_space_common_core::types::{OrgId, SandboxId};

use pplx_space_common_core::sql;
use pplx_space_common_core::types::SandboxOwnerSource;

use std::sync::LazyLock;

use crate::federated_identity::FederatedOwner;

use super::list_page::{
    ListPage, MAX_SANDBOX_LIST_PAGE_BYTES, PageWalkParams, SANDBOX_LIST_ROW_OVERHEAD_BYTES,
    page_rows_cte,
};

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

/// A sandbox on an ECW list page and whether another sandbox follows it.
#[derive(sqlx::FromRow)]
struct ScopedPageRow {
    #[sqlx(flatten)]
    sandbox: Sandbox,
    has_next: bool,
}

/// The local org's sandboxes ($1) after the walk's current row, matching
/// the metadata filter $5.
const LOCAL_ORG_NEXT_ROW: &str = "SELECT s.id, s.created_at
    FROM sandboxes s
    JOIN users owner_user ON owner_user.id = s.user_id
    WHERE owner_user.owner_source = 'local'
      AND s.org_id = $1
      AND (s.created_at, s.id) < (walk.created_at, walk.id)
      AND ($5::jsonb IS NULL OR s.metadata @> $5::jsonb)
    ORDER BY s.created_at DESC, s.id DESC
    LIMIT 1";

static LOCAL_ORG_PAGE_SQL: LazyLock<String> = LazyLock::new(|| {
    let page_rows = page_rows_cte(
        LOCAL_ORG_NEXT_ROW,
        &PageWalkParams {
            cursor_created_at: "$2",
            cursor_id: "$3",
            max_rows: "$4",
            max_bytes: "$6",
            row_overhead_bytes: "$7",
        },
    );
    sandbox_query(&format!(
        "{page_rows}
         SELECT {{sandbox_columns}}, page.has_next
         FROM {{sandbox_from}}
         JOIN page_rows page ON page.id = s.id
         ORDER BY s.created_at DESC, s.id DESC"
    ))
});

/// The federated subject's sandboxes ($1-$3) and the key org's legacy local
/// sandboxes ($8) after the walk's current row, matching the metadata filter
/// $7. Each arm keeps its own index-backed `ORDER BY` / `LIMIT`; the outer
/// sort picks the newer of the two candidates.
const FEDERATED_SUBJECT_NEXT_ROW: &str = "SELECT candidate.id, candidate.created_at
    FROM (
        (SELECT s.id, s.created_at
         FROM sandboxes s
         JOIN users owner_user ON owner_user.id = s.user_id
         JOIN organizations owner_org ON owner_org.id = s.org_id
         WHERE owner_user.owner_source = 'federated_identity'
           AND owner_user.upstream_issuer = $1
           AND owner_org.upstream_issuer = $1
           AND owner_org.upstream_org_id = $2
           AND owner_user.upstream_subject_id = $3
           AND (s.created_at, s.id) < (walk.created_at, walk.id)
           AND ($7::jsonb IS NULL OR s.metadata @> $7::jsonb)
         ORDER BY s.created_at DESC, s.id DESC
         LIMIT 1)
        UNION ALL
        (SELECT s.id, s.created_at
         FROM sandboxes s
         JOIN users owner_user ON owner_user.id = s.user_id
         WHERE owner_user.owner_source = 'local'
           AND s.org_id = $8
           AND (s.created_at, s.id) < (walk.created_at, walk.id)
           AND ($7::jsonb IS NULL OR s.metadata @> $7::jsonb)
         ORDER BY s.created_at DESC, s.id DESC
         LIMIT 1)
    ) candidate
    ORDER BY candidate.created_at DESC, candidate.id DESC
    LIMIT 1";

static FEDERATED_SUBJECT_PAGE_SQL: LazyLock<String> = LazyLock::new(|| {
    let page_rows = page_rows_cte(
        FEDERATED_SUBJECT_NEXT_ROW,
        &PageWalkParams {
            cursor_created_at: "$4",
            cursor_id: "$5",
            max_rows: "$6",
            max_bytes: "$9",
            row_overhead_bytes: "$10",
        },
    );
    sandbox_query(&format!(
        "{page_rows}
         SELECT {{sandbox_columns}}, page.has_next
         FROM {{sandbox_from}}
         JOIN page_rows page ON page.id = s.id
         ORDER BY s.created_at DESC, s.id DESC"
    ))
});

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
        let max_rows = i64::from(limit.saturating_add(1));
        let cursor_created_at = cursor.map(|cursor| cursor.created_at);
        let cursor_id = cursor.map(|cursor| cursor.id);
        let rows = match scope {
            SandboxAccessScope::LocalOrg { org_id } => {
                sqlx::query_as::<_, ScopedPageRow>(sqlx::AssertSqlSafe(sql!(
                    LOCAL_ORG_PAGE_SQL.as_str()
                )))
                .bind(*org_id)
                .bind(cursor_created_at)
                .bind(cursor_id)
                .bind(max_rows)
                .bind(metadata_filter)
                .bind(MAX_SANDBOX_LIST_PAGE_BYTES)
                .bind(SANDBOX_LIST_ROW_OVERHEAD_BYTES)
                .fetch_all(pool)
                .await
            }
            SandboxAccessScope::FederatedSubject {
                owner,
                legacy_org_id,
            } => {
                sqlx::query_as::<_, ScopedPageRow>(sqlx::AssertSqlSafe(sql!(
                    FEDERATED_SUBJECT_PAGE_SQL.as_str()
                )))
                .bind(owner.issuer.as_str())
                .bind(owner.org_id.as_str())
                .bind(owner.subject_id.as_str())
                .bind(cursor_created_at)
                .bind(cursor_id)
                .bind(max_rows)
                .bind(metadata_filter)
                .bind(*legacy_org_id)
                .bind(MAX_SANDBOX_LIST_PAGE_BYTES)
                .bind(SANDBOX_LIST_ROW_OVERHEAD_BYTES)
                .fetch_all(pool)
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
