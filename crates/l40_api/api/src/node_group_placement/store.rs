//! The one read behind a node-group candidate set: a template's compatibility
//! slice plus every `node_groups` row and its allowlist.
//!
//! One statement, not two, because the pair has to be read as a unit: the
//! candidate set is `{ g : compatible(caller_org, template, g) }`, and a
//! template row read at a different instant than the group rows can produce a
//! set for a visibility the row no longer has. It is also on the create and
//! restore hot paths, where a second round trip is a second chance to queue on
//! the pooled connection.
//!
//! On the **create** path this is not the first read of that `templates` row.
//! A cold create reads it twice: `gate_template_access`
//! (`crates/l40_api/api/src/controllers/sandbox/mod.rs`) authorizes the template,
//! and the pod build resolves through here. A create that *claims* a warm slot
//! and is then refused adds a third, because the claim path's adoption check
//! resolves through here too — it is lazy, so a pool miss never pays it. A
//! refusal on the *dedicated-only* arm specifically pays a fourth, because that
//! arm re-asks the same question for the pool's org-less subject to decide
//! whether the slot may be recycled. Only the claim-path reads repeat on a
//! prepare retry: the claim sits inside that loop, while `gate_template_access`
//! precedes it and the pod build runs after it returns.
//!
//! Threading a parsed spec down instead was considered and declined: the
//! marginal cost is the round trip, not the row, and the trip is needed for the
//! group rows regardless, while a threaded spec would give up the single-instant
//! read of template and groups that is this statement's whole point. Memoizing
//! the claim path's answer for the pod build in the same request would remove
//! the third and is the obvious next step if this shows up in create latency.
//! Recorded so the next reader takes the duplication for a trade rather than an
//! oversight.
//!
//! The shape that makes one statement work is the `LEFT JOIN node_groups ON
//! TRUE`: it keeps the template row when `node_groups` is empty. Without it,
//! "this template has no row" and "this cluster has no node groups" both come
//! back as zero rows, and the resolver has to fail closed on both — turning
//! a flag flip on an unprovisioned
//! cluster into a total create outage. See
//! [`super::ResolvedPlacement::Unprovisioned`].

use pplx_space_common_core::sql;
use pplx_space_common_core::types::{
    NodeGroup, NodeGroupId, NodeGroupName, NodeGroupPolicy, NodeGroupTemplatePolicy, OrgId,
    TemplateCompatSpec, TemplateName, TemplateVisibility,
};
use uuid::Uuid;

/// A template's compatibility slice together with the cluster's node groups —
/// everything [`NodeGroup::compatible`] needs, read at one instant.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NodeGroupCatalog {
    /// The `templates` row's placement-relevant columns. Read from the row
    /// rather than assembled from request fields, because
    /// `NodeGroupPlacement::for_caller` validates candidates against whatever
    /// spec it is handed and only the template *name* survives into the
    /// placement — a spec that mislabels an org-specific template as generic
    /// is unrecoverable downstream.
    pub template: TemplateCompatSpec,
    /// Every node group in the cluster, in name order. Unfiltered on purpose:
    /// the compatibility predicate is the definition of the candidate set, so
    /// filtering in SQL would fork it. Empty means the cluster has no node
    /// groups at all.
    pub groups: Vec<NodeGroup>,
}

/// Failure reading the catalog.
#[derive(Debug, thiserror::Error)]
pub enum NodeGroupCatalogError {
    #[error(transparent)]
    Database(#[from] sqlx::Error),
    /// A template name is outside [`TemplateName`]'s grammar — schema/code
    /// drift rather than a runtime condition. Fails closed. Node-group names
    /// never reach this variant: their `sqlx::Decode` impl rejects an
    /// out-of-grammar value as a `ColumnDecode` error under [`Self::Database`],
    /// which is what keeps such a name out of a node label.
    #[error("stored {column} {value:?} is not a valid name: {source}")]
    InvalidStoredName {
        column: &'static str,
        value: String,
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

/// Raw column shape. Every `group_*` column is nullable because the
/// `LEFT JOIN ... ON TRUE` yields one all-NULL group row when `node_groups`
/// is empty.
#[derive(sqlx::FromRow)]
struct CatalogRow {
    template_org_id: Option<OrgId>,
    template_visibility: TemplateVisibility,
    group_id: Option<Uuid>,
    /// Decoded through [`NodeGroupName`]'s own `sqlx::Decode`, which parses via
    /// the same smart constructor used at the wire boundary — so an
    /// out-of-grammar value in the column fails the decode instead of
    /// materializing a name that could reach a node label.
    group_name: Option<NodeGroupName>,
    group_org_id: Option<OrgId>,
    group_template_policy: Option<NodeGroupTemplatePolicy>,
    group_is_default: Option<bool>,
    /// **Narrowed to the queried template.** The join filters
    /// `template_name = $1`, so this is `[]` or `[$1]` rather than the group's
    /// whole allowlist — see the statement below for why.
    group_template_allowlist: Vec<String>,
}

/// Read the catalog for one template name.
///
/// `Ok(None)` means no `templates` row exists under that name — distinct from
/// a row with no compatible group, and from a cluster with no groups. Accepts
/// a pool or a transaction connection so a caller already inside a transaction
/// reads the same snapshot.
pub async fn catalog_for_template(
    executor: impl sqlx::PgExecutor<'_>,
    template_name: &str,
) -> Result<Option<NodeGroupCatalog>, NodeGroupCatalogError> {
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
    let rows: Vec<CatalogRow> = sqlx::query_as(sql!(
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
    .fetch_all(executor)
    .await?;

    let Some(first) = rows.first() else {
        return Ok(None);
    };
    let template = TemplateCompatSpec {
        name: stored_template_name(template_name, "templates.name")?,
        org_id: first.template_org_id,
        visibility: first.template_visibility,
    };
    let mut groups = Vec::with_capacity(rows.len());
    for row in &rows {
        // The all-NULL group row: a template that exists on a cluster with no
        // node groups. One row in, zero groups out.
        let (Some(id), Some(name), Some(is_default), Some(template_policy)) = (
            row.group_id,
            row.group_name.as_ref(),
            row.group_is_default,
            row.group_template_policy,
        ) else {
            continue;
        };
        // `NodeGroupPolicy` makes the states the table's CHECKs forbid
        // unrepresentable, so the mapping drops what each kind cannot carry:
        // a generic group's `template_policy` (`node_groups_restricted_requires_org`
        // pins it to `default`, and `compatible` never consults an allowlist
        // for a generic group) and a dedicated group's `is_default`
        // (`node_groups_default_is_generic` pins it to false).
        let policy = match row.group_org_id {
            None => NodeGroupPolicy::Generic { is_default },
            Some(org_id) => NodeGroupPolicy::Dedicated {
                org_id,
                template_policy,
            },
        };
        let mut template_allowlist = std::collections::BTreeSet::new();
        for allowed in &row.group_template_allowlist {
            template_allowlist.insert(stored_template_name(
                allowed,
                "node_group_template_compat.template_name",
            )?);
        }
        groups.push(NodeGroup {
            id: NodeGroupId::from_uuid(id),
            name: name.clone(),
            policy,
            template_allowlist,
        });
    }
    Ok(Some(NodeGroupCatalog { template, groups }))
}

/// Parses a template name, attaching the column it came from to a failure.
///
/// Only [`TemplateName`] needs this. [`NodeGroupName`] and
/// [`NodeGroupTemplatePolicy`] carry `sqlx::Decode` impls that parse through
/// the same constructor used at the wire boundary, so sqlx surfaces their drift
/// as a `ColumnDecode` error with the column already named; `TemplateName` has
/// no such impl.
fn stored_template_name(
    value: &str,
    column: &'static str,
) -> Result<TemplateName, NodeGroupCatalogError> {
    TemplateName::parse(value).map_err(|source| NodeGroupCatalogError::InvalidStoredName {
        column,
        value: value.to_string(),
        source: Box::new(source),
    })
}
