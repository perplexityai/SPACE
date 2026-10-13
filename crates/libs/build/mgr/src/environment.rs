use std::collections::BTreeMap;

use pplx_space_common_core::types::{
    EnvKey, EnvMap, EnvParseError, EnvValue, MAX_ENV_ENTRIES, MAX_ENV_TOTAL_BYTES,
};

use crate::recipe::{Recipe, Step};

#[derive(Debug, thiserror::Error)]
pub enum BuildEnvironmentError {
    #[error(
        "recipe step {index} env_refs: {name} is not exported; the operator must set SPACE_BUILD_EXPORT_{name}"
    )]
    NotExported { index: usize, name: EnvKey },
    #[error("recipe step {index} env_refs: export {name} must be UTF-8")]
    NotUnicode { index: usize, name: EnvKey },
    #[error("recipe step {index} env_refs: export {name}: {source}")]
    InvalidValue {
        index: usize,
        name: EnvKey,
        #[source]
        source: EnvParseError,
    },
    #[error("recipe step {index} {field}: {reason}")]
    Limit {
        index: usize,
        field: &'static str,
        reason: &'static str,
    },
}

/// A recipe and its bounded, unique operator exports, retained for one execution.
/// The recipe cannot change after preflight or be paired with another job's exports.
#[derive(Debug)]
pub struct PreparedRecipe {
    recipe: Recipe,
    exports: EnvMap,
}

impl PreparedRecipe {
    pub fn new(recipe: Recipe) -> Result<Self, BuildEnvironmentError> {
        Self::resolve_with(recipe, |name| std::env::var(name))
    }

    pub fn recipe(&self) -> &Recipe {
        &self.recipe
    }

    fn resolve_with(
        recipe: Recipe,
        mut lookup: impl FnMut(&str) -> Result<String, std::env::VarError>,
    ) -> Result<Self, BuildEnvironmentError> {
        let mut exports = BTreeMap::new();
        let mut bytes = 0;
        for (offset, step) in recipe.steps.iter().enumerate() {
            let index = offset + 1;
            for name in &step.env_refs {
                if exports.contains_key(name) {
                    continue;
                }
                check_size(
                    index,
                    "env_refs",
                    exports.len() + 1,
                    bytes + name.as_str().len(),
                )?;
                let value =
                    lookup(&format!("SPACE_BUILD_EXPORT_{name}")).map_err(|error| match error {
                        std::env::VarError::NotPresent => BuildEnvironmentError::NotExported {
                            index,
                            name: name.clone(),
                        },
                        std::env::VarError::NotUnicode(_) => BuildEnvironmentError::NotUnicode {
                            index,
                            name: name.clone(),
                        },
                    })?;
                let value = EnvValue::try_from(value).map_err(|source| {
                    BuildEnvironmentError::InvalidValue {
                        index,
                        name: name.clone(),
                        source,
                    }
                })?;
                bytes += name.as_str().len() + value.expose().len();
                check_size(index, "env_refs", exports.len() + 1, bytes)?;
                exports.insert(name.clone(), value);
            }
        }
        let exports = EnvMap::new(exports).map_err(|source| BuildEnvironmentError::Limit {
            index: recipe.steps.len(),
            field: "env_refs",
            reason: source.as_str(),
        })?;
        let prepared = Self { recipe, exports };
        for (offset, env) in prepared.environments().enumerate() {
            let bytes = env
                .iter()
                .map(|(key, value)| key.as_str().len() + value.expose().len())
                .sum();
            check_size(offset + 1, "effective env", env.0.len(), bytes)?;
        }
        Ok(prepared)
    }

    pub(crate) fn environments(&self) -> impl Iterator<Item = StepEnvironment<'_>> {
        self.recipe
            .steps
            .iter()
            .scan(BTreeMap::new(), |literals, step| {
                if let Step::Env { vars } = &step.step {
                    literals.extend(vars.as_map());
                }
                let mut env = literals.clone();
                for name in &step.env_refs {
                    env.insert(
                        name,
                        self.exports
                            .as_map()
                            .get(name)
                            .expect("preflight resolved export"),
                    );
                }
                Some(StepEnvironment(env))
            })
    }
}

fn check_size(
    index: usize,
    field: &'static str,
    count: usize,
    bytes: usize,
) -> Result<(), BuildEnvironmentError> {
    let reason = if count > MAX_ENV_ENTRIES {
        "env has too many entries"
    } else if bytes > MAX_ENV_TOTAL_BYTES {
        "env has too many bytes"
    } else {
        return Ok(());
    };
    Err(BuildEnvironmentError::Limit {
        index,
        field,
        reason,
    })
}

/// Borrows a preflighted step's values; exports never accumulate across steps.
#[derive(Default)]
pub(crate) struct StepEnvironment<'a>(BTreeMap<&'a EnvKey, &'a EnvValue>);

impl<'a> StepEnvironment<'a> {
    pub(crate) fn iter(&self) -> impl Iterator<Item = (&'a EnvKey, &'a EnvValue)> + '_ {
        self.0.iter().map(|(key, value)| (*key, *value))
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub(crate) fn get(&self, name: &str) -> Option<&'a EnvValue> {
        self.iter()
            .find(|(key, _)| key.as_str() == name)
            .map(|(_, value)| value)
    }

    #[cfg(test)]
    pub(crate) fn from_env(env: &'a EnvMap) -> Self {
        Self(env.as_map().iter().collect())
    }
}

#[cfg(test)]
mod tests;
