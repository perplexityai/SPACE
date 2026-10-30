//! Which environments a node's build certifies and which it can run. This is
//! evidence for the control plane, never approval: usable is not executable.

use std::collections::{BTreeMap, BTreeSet};

use pplx_space_common_core::types::{
    DigestParseError, EnvironmentReference, VersionSortKey, VersionSortKeyError,
};

use super::{
    EnvironmentDecodeError, EnvironmentDefinition, RUNTIME_NEUTRAL_ENVIRONMENT_SCHEMA_VERSION,
};
use crate::proto;

pub const ENVIRONMENT_OBSERVATION_SCHEMA_VERSION: u32 = 1;

/// One compiled catalog entry with the ordering and edges its build certifies.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct KnownEnvironment {
    definition: EnvironmentDefinition,
    runtime_version_sort_key: Option<VersionSortKey>,
    upgrade_edges: BTreeSet<EnvironmentReference>,
    memory_edges: BTreeSet<EnvironmentReference>,
}

impl KnownEnvironment {
    /// The runtime driver may define no order for a release; edges are
    /// checked against the whole observation in [`EnvironmentObservation::new`].
    pub fn new(
        definition: EnvironmentDefinition,
        runtime_version_sort_key: Option<VersionSortKey>,
        upgrade_edges: impl IntoIterator<Item = EnvironmentReference>,
        memory_edges: impl IntoIterator<Item = EnvironmentReference>,
    ) -> Result<Self, EnvironmentObservationError> {
        let source = definition.reference();
        // Observations carry the generic representation only. A historical
        // definition is normalized once at the catalog boundary, never here.
        if definition.schema_version() != RUNTIME_NEUTRAL_ENVIRONMENT_SCHEMA_VERSION {
            return Err(EnvironmentObservationError::LegacyDefinition(source));
        }
        let upgrade_edges = unique_edges(source, upgrade_edges)?;
        let memory_edges = unique_edges(source, memory_edges)?;
        // A memory edge additionally certifies an upgrade edge's target as a
        // loader; on its own it would be a move no upgrade rule allows.
        if let Some(target) = memory_edges.difference(&upgrade_edges).next() {
            return Err(EnvironmentObservationError::MemoryEdgeWithoutUpgrade {
                from: source,
                to: *target,
            });
        }
        Ok(Self {
            definition,
            runtime_version_sort_key,
            upgrade_edges,
            memory_edges,
        })
    }

    pub fn definition(&self) -> &EnvironmentDefinition {
        &self.definition
    }
    pub fn reference(&self) -> EnvironmentReference {
        self.definition.reference()
    }
    pub fn runtime_version_sort_key(&self) -> Option<&VersionSortKey> {
        self.runtime_version_sort_key.as_ref()
    }
    pub fn upgrade_edges(&self) -> &BTreeSet<EnvironmentReference> {
        &self.upgrade_edges
    }
    pub fn memory_edges(&self) -> &BTreeSet<EnvironmentReference> {
        &self.memory_edges
    }
}

fn unique_edges(
    source: EnvironmentReference,
    edges: impl IntoIterator<Item = EnvironmentReference>,
) -> Result<BTreeSet<EnvironmentReference>, EnvironmentObservationError> {
    let mut unique = BTreeSet::new();
    for target in edges {
        if target == source {
            return Err(EnvironmentObservationError::SelfEdge(source));
        }
        if !unique.insert(target) {
            return Err(EnvironmentObservationError::DuplicateEdge {
                from: source,
                to: target,
            });
        }
    }
    Ok(unique)
}

/// A node's certified catalog and the subset it has verified locally.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EnvironmentObservation {
    known: BTreeMap<EnvironmentReference, KnownEnvironment>,
    usable: BTreeSet<EnvironmentReference>,
}

#[derive(Debug, thiserror::Error)]
pub enum EnvironmentObservationError {
    #[error("unsupported environment observation schema {0}")]
    Schema(u32),
    #[error("known environment is missing its definition")]
    MissingDefinition,
    #[error("invalid known environment definition: {0}")]
    Definition(#[from] EnvironmentDecodeError),
    #[error("known environment {0} is not a generic schema-2 definition")]
    LegacyDefinition(EnvironmentReference),
    #[error("invalid environment reference: {0}")]
    Reference(#[from] DigestParseError),
    #[error("invalid runtime version sort key: {0}")]
    SortKey(#[from] VersionSortKeyError),
    #[error("environment observation repeats known environment {0}")]
    DuplicateKnown(EnvironmentReference),
    #[error("environment observation repeats usable environment {0}")]
    DuplicateUsable(EnvironmentReference),
    #[error("usable environment {0} is not known to the node")]
    UsableNotKnown(EnvironmentReference),
    #[error("environment {0} has an edge to itself")]
    SelfEdge(EnvironmentReference),
    #[error("environment {from} repeats edge to {to}")]
    DuplicateEdge {
        from: EnvironmentReference,
        to: EnvironmentReference,
    },
    #[error("environment {from} has an edge to {to}, which is not known")]
    EdgeNotKnown {
        from: EnvironmentReference,
        to: EnvironmentReference,
    },
    #[error("environment {from} has a memory edge to {to} without an upgrade edge")]
    MemoryEdgeWithoutUpgrade {
        from: EnvironmentReference,
        to: EnvironmentReference,
    },
}

impl EnvironmentObservation {
    pub fn new(
        known: impl IntoIterator<Item = KnownEnvironment>,
        usable: impl IntoIterator<Item = EnvironmentReference>,
    ) -> Result<Self, EnvironmentObservationError> {
        let mut entries = BTreeMap::new();
        for entry in known {
            let reference = entry.reference();
            if entries.insert(reference, entry).is_some() {
                return Err(EnvironmentObservationError::DuplicateKnown(reference));
            }
        }
        for entry in entries.values() {
            // Memory edges are a subset of upgrade edges, checked per entry.
            if let Some(target) = entry
                .upgrade_edges
                .iter()
                .find(|target| !entries.contains_key(target))
            {
                return Err(EnvironmentObservationError::EdgeNotKnown {
                    from: entry.reference(),
                    to: *target,
                });
            }
        }
        let mut unique = BTreeSet::new();
        for reference in usable {
            if !entries.contains_key(&reference) {
                return Err(EnvironmentObservationError::UsableNotKnown(reference));
            }
            if !unique.insert(reference) {
                return Err(EnvironmentObservationError::DuplicateUsable(reference));
            }
        }
        Ok(Self {
            known: entries,
            usable: unique,
        })
    }

    /// Absence of the field is a node that predates environment observations.
    /// Callers must treat `Ok(None)` as unknown, never as an empty catalog.
    pub fn from_inventory(
        inventory: &proto::NodeExecutionInventory,
    ) -> Result<Option<Self>, EnvironmentObservationError> {
        inventory
            .environments
            .as_ref()
            .map(Self::try_from)
            .transpose()
    }

    pub fn known(&self) -> impl ExactSizeIterator<Item = &KnownEnvironment> {
        self.known.values()
    }
    pub fn get(&self, reference: EnvironmentReference) -> Option<&KnownEnvironment> {
        self.known.get(&reference)
    }
    pub fn usable(&self) -> &BTreeSet<EnvironmentReference> {
        &self.usable
    }
}

fn references(values: &[String]) -> Result<Vec<EnvironmentReference>, DigestParseError> {
    values.iter().map(|value| value.parse()).collect()
}

impl TryFrom<&proto::KnownEnvironment> for KnownEnvironment {
    type Error = EnvironmentObservationError;
    fn try_from(value: &proto::KnownEnvironment) -> Result<Self, Self::Error> {
        let definition = value
            .definition
            .as_ref()
            .ok_or(EnvironmentObservationError::MissingDefinition)?;
        // Name the rule an observation breaks, rather than the codec's
        // generic unsupported-schema error.
        if definition.schema_version != RUNTIME_NEUTRAL_ENVIRONMENT_SCHEMA_VERSION {
            return Err(EnvironmentObservationError::LegacyDefinition(
                definition.reference.parse()?,
            ));
        }
        let sort_key = VersionSortKey::from_wire(&value.runtime_version_sort_key)?;
        Self::new(
            EnvironmentDefinition::try_from(definition)?,
            sort_key,
            references(&value.upgrade_edges)?,
            references(&value.memory_edges)?,
        )
    }
}

impl TryFrom<&proto::EnvironmentObservation> for EnvironmentObservation {
    type Error = EnvironmentObservationError;
    fn try_from(value: &proto::EnvironmentObservation) -> Result<Self, Self::Error> {
        if value.schema_version != ENVIRONMENT_OBSERVATION_SCHEMA_VERSION {
            return Err(EnvironmentObservationError::Schema(value.schema_version));
        }
        let known = value
            .known
            .iter()
            .map(KnownEnvironment::try_from)
            .collect::<Result<Vec<_>, _>>()?;
        Self::new(known, references(&value.usable_environments)?)
    }
}

impl From<&KnownEnvironment> for proto::KnownEnvironment {
    fn from(value: &KnownEnvironment) -> Self {
        let strings = |edges: &BTreeSet<EnvironmentReference>| {
            edges.iter().map(ToString::to_string).collect()
        };
        Self {
            definition: Some((&value.definition).into()),
            runtime_version_sort_key: VersionSortKey::to_wire(
                value.runtime_version_sort_key.as_ref(),
            ),
            upgrade_edges: strings(&value.upgrade_edges),
            memory_edges: strings(&value.memory_edges),
        }
    }
}

impl From<&EnvironmentObservation> for proto::EnvironmentObservation {
    fn from(value: &EnvironmentObservation) -> Self {
        Self {
            schema_version: ENVIRONMENT_OBSERVATION_SCHEMA_VERSION,
            known: value.known().map(Into::into).collect(),
            usable_environments: value.usable.iter().map(ToString::to_string).collect(),
        }
    }
}

#[cfg(test)]
mod tests;
