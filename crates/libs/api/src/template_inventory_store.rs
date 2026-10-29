//! Template releases as worker nodes report them in their inventory.

use pplx_space_common_core::{
    sql,
    types::{MemorySize, RuntimeKind, TemplateName, VcpuCount},
};
use sqlx::PgExecutor;

/// The VM shape a template release was built for, from its `metadata.json`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TemplateBuildShape {
    pub(crate) vcpu: VcpuCount,
    pub(crate) memory: MemorySize,
}

impl TemplateBuildShape {
    /// A shape needs both values, each valid; template metadata and pool
    /// shapes set them together. A partial report is not a shape.
    fn from_inventory(vcpu: Option<i32>, memory_mib: Option<i32>) -> Option<Self> {
        let vcpu = VcpuCount::try_from_u32(u32::try_from(vcpu?).ok()?).ok()?;
        let memory = MemorySize::mib(u64::try_from(memory_mib?).ok()?).ok()?;
        Some(Self { vcpu, memory })
    }
}

/// Published defaults survive node cache eviction. Legacy releases without
/// recorded defaults retain the existing inventory lookup until rebuilt.
pub(crate) async fn release_build_shape(
    executor: impl PgExecutor<'_>,
    template: &TemplateName,
    content_hash: &str,
    runtime: RuntimeKind,
) -> Result<Option<TemplateBuildShape>, sqlx::Error> {
    let row = sqlx::query_as::<_, (Option<i32>, Option<i32>)>(sql!(
        "SELECT vcpu, memory_mib FROM (
             SELECT vcpu, memory_mib, 0 AS priority, created_at AS observed_at
             FROM template_releases
             WHERE template_name = $1 AND hash = $2 AND type = $3
               AND vcpu > 0 AND memory_mib > 0
             UNION ALL
             SELECT nti.vcpu, nti.memory_mib,
                    CASE WHEN n.role = 'builder' THEN 1 ELSE 2 END, nti.observed_at
             FROM node_template_inventory nti
             JOIN nodes n ON n.id = nti.node_id
             WHERE nti.template_name = $1 AND nti.content_hash = $2
               AND (nti.runtime IS NULL OR nti.runtime = '' OR nti.runtime = $3)
               AND nti.vcpu > 0 AND nti.memory_mib > 0
         ) shapes ORDER BY priority, observed_at DESC LIMIT 1"
    ))
    .bind(template.as_str())
    .bind(content_hash)
    .bind(runtime.alias())
    .fetch_optional(executor)
    .await?;
    Ok(row.and_then(|(vcpu, memory_mib)| TemplateBuildShape::from_inventory(vcpu, memory_mib)))
}

#[cfg(test)]
mod tests;
