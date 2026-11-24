//! Template releases as worker nodes report them in their inventory, read
//! through `pplx-space-node-template-inventory-store`.

use pplx_space_byte_size::MemorySize;
use pplx_space_execution_spec::{RuntimeKind, VcpuCount};
use pplx_space_template_model::TemplateName;
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

/// The shape `content_hash` of `template` was built for, or `None` when no
/// node reports one. Every node that holds the release reports the same
/// metadata; the builder's row is preferred, as for the lifecycle default,
/// but the shape survives the builder evicting the release.
pub(crate) async fn release_build_shape(
    executor: impl PgExecutor<'_>,
    template: &TemplateName,
    content_hash: &str,
    runtime: RuntimeKind,
) -> Result<Option<TemplateBuildShape>, sqlx::Error> {
    let row = pplx_space_node_template_inventory_store::release_build_shape(
        executor,
        template,
        content_hash,
        runtime,
    )
    .await?;
    Ok(row.and_then(|row| TemplateBuildShape::from_inventory(row.vcpu, row.memory_mib)))
}

/// The execution environment every node holding `content_hash` of
/// `template` with a binding reports it bound to, unparsed. `None` when no
/// node reports one, or when nodes report different ones: one hash can be
/// cached bound to several environments, and then no single binding is the
/// build's. A row without a binding doesn't count either way. Either spelling
/// of the digest matches the other, as the template release lookups match
/// it: a tag on a legacy unprefixed release reports that spelling while the
/// digest's holders record the `sha256:` one.
///
/// A row bound to one of `excluded` doesn't count either way: the caller
/// passes the bindings of the hash's other releases, its twins, whose
/// builds share the template and hash but can't stand in for this release's
/// binding.
pub(crate) async fn agreed_build_environment(
    executor: impl PgExecutor<'_>,
    template: &str,
    content_hash: &str,
    excluded: &[String],
) -> Result<Option<String>, sqlx::Error> {
    pplx_space_node_template_inventory_store::agreed_build_environment(
        executor,
        template,
        content_hash,
        excluded,
    )
    .await
}

#[cfg(test)]
mod tests;
