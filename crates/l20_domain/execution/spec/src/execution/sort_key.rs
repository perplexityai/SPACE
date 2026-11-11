//! Portable form of a runtime driver's version ordering. The driver assigns the
//! key; readers only compare keys, never parse versions themselves.

/// Element-wise ordering key for one runtime release. Keys order
/// lexicographically, with a proper prefix first, as SQL integer arrays do.
/// Keys are comparable only within one runtime kind.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct VersionSortKey(Vec<u32>);

#[derive(Debug, Clone, Eq, PartialEq, thiserror::Error)]
pub enum VersionSortKeyError {
    #[error("version sort key is empty")]
    Empty,
    #[error(
        "version sort key has {0} components; at most {VERSION_SORT_KEY_MAX_COMPONENTS} are allowed"
    )]
    TooLong(usize),
    #[error("version sort key component {0} exceeds {VERSION_SORT_KEY_MAX_COMPONENT}")]
    ComponentTooLarge(u32),
}

/// The component cap keeps every key storable as a Postgres `INT[]` with its
/// order intact; the length cap bounds what one publication carries.
const VERSION_SORT_KEY_MAX_COMPONENTS: usize = 8;
const VERSION_SORT_KEY_MAX_COMPONENT: u32 = i32::MAX as u32;

impl VersionSortKey {
    pub fn new(components: Vec<u32>) -> Result<Self, VersionSortKeyError> {
        if components.is_empty() {
            return Err(VersionSortKeyError::Empty);
        }
        if components.len() > VERSION_SORT_KEY_MAX_COMPONENTS {
            return Err(VersionSortKeyError::TooLong(components.len()));
        }
        if let Some(component) = components
            .iter()
            .find(|value| **value > VERSION_SORT_KEY_MAX_COMPONENT)
        {
            return Err(VersionSortKeyError::ComponentTooLarge(*component));
        }
        Ok(Self(components))
    }

    pub fn components(&self) -> &[u32] {
        &self.0
    }

    /// Wire form: an empty repeated field means the driver defined no order.
    pub fn from_wire(components: &[u32]) -> Result<Option<Self>, VersionSortKeyError> {
        match components {
            [] => Ok(None),
            components => Self::new(components.to_vec()).map(Some),
        }
    }

    pub fn to_wire(key: Option<&Self>) -> Vec<u32> {
        key.map(|key| key.0.clone()).unwrap_or_default()
    }
}

#[cfg(test)]
mod tests;
