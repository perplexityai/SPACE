//! The placement catalog read: one template's compatibility columns with
//! every node group and its allowlist entry for that template.
//!
//! One statement, not two, so the template row and the group rows are read
//! at one instant; the api's node-group placement module says why that
//! matters and what it builds from the rows.

use pplx_space_accounts::OrgId;
use pplx_space_node_model::{NodeGroupName, NodeGroupTemplatePolicy};
use pplx_space_postgres_pool::{DbError, Executor};
use pplx_space_sql_locator::sql;
use pplx_space_template_model::TemplateVisibility;
use sqlx::FromRow;
use uuid::Uuid;

/// One row per node group, in group-name order. Every `group_*` column is
/// nullable because the `LEFT JOIN ... ON TRUE` yields one all-NULL group row
/// when `node_groups` is empty; no row at all means no `templates` row has
/// that name.
#[derive(Debug, Clone, FromRow)]
pub struct TemplateCatalogRow {
    pub template_org_id: Option<OrgId>,
    pub template_visibility: TemplateVisibility,
    pub group_id: Option<Uuid>,
    /// Decoded through [`NodeGroupName`]'s own `sqlx::Decode`, which parses via
    /// the same smart constructor used at the wire boundary — so an
    /// out-of-grammar value in the column fails the decode instead of
    /// materializing a name that could reach a node label.
    pub group_name: Option<NodeGroupName>,
    pub group_org_id: Option<OrgId>,
    pub group_template_policy: Option<NodeGroupTemplatePolicy>,
    pub group_is_default: Option<bool>,
    /// **Narrowed to the queried template.** The join filters
    /// `template_name = $1`, so this is `[]` or `[$1]` rather than the group's
    /// whole allowlist — see the statement below for why.
    pub group_template_allowlist: Vec<String>,
}

/// Read the catalog rows for one template name.
pub async fn template_catalog(
    executor: impl Executor<'_>,
    template_name: &str,
) -> Result<Vec<TemplateCatalogRow>, DbError> {
    // `desired_state` is deliberately not filtered. It records whether a
    // template is *publishable*, and a suspended sandbox whose template was
    // since deleted still has to resolve a candidate set to restore into; the
    // create path's own authorization gate (`gate_template_access`) is what
    // rejects an unpublished template, and it runs before this.
    // The compat join is filtered to `$1`, not aggregated whole. The allowlist
    // has exactly one consumer — `NodeGroup::compatible`'s
    // `template_allowlist.contains(&template.name)`, where that name *is* `$1`
    // — so every other allowlisted name would be decoded, parsed and inserted
    // into a `BTreeSet` per pod build to answer one boolean. Filtering in the
    // join makes it a probe on the `(node_group_id, template_name)` primary key
    // and leaves `contains` answering identically.
    //
    // `t.name` is deliberately not selected: it is tautologically `$1`, and the
    // caller's own string is the one that has to survive into the placement
    // anyway — the pod builder cross-checks the placement's template against
    // the template the pod renders.
    Ok(sqlx::query_as::<_, TemplateCatalogRow>(sql!(
        "SELECT t.org_id AS template_org_id,
                t.visibility AS template_visibility,
                g.id AS group_id,
                g.name AS group_name,
                g.org_id AS group_org_id,
                g.template_policy AS group_template_policy,
                g.is_default AS group_is_default,
                COALESCE(
                    ARRAY_AGG(c.template_name) FILTER (WHERE c.template_name IS NOT NULL),
                    ARRAY[]::TEXT[]
                ) AS group_template_allowlist
           FROM templates t
           LEFT JOIN node_groups g ON TRUE
           LEFT JOIN node_group_template_compat c
                  ON c.node_group_id = g.id AND c.template_name = t.name
          WHERE t.name = $1
          GROUP BY t.org_id,
                   t.visibility,
                   g.id,
                   g.name,
                   g.org_id,
                   g.template_policy,
                   g.is_default
          ORDER BY g.name ASC"
    ))
    .bind(template_name)
    .fetch_all(executor.into_sqlx())
    .await?)
}
