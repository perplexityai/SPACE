use std::collections::HashMap;

#[derive(Debug, thiserror::Error)]
pub enum BuildEnvironmentError {
    #[error("build environment names must match [A-Za-z_][A-Za-z0-9_]*")]
    InvalidName,
    #[error(
        "build environment reference {name} is not exported; the operator must set SPACE_BUILD_EXPORT_{name}"
    )]
    NotExported { name: String },
    #[error("build environment value for {name} must be UTF-8 without NUL bytes")]
    InvalidValue { name: String },
}

pub(crate) fn validate_name(name: &str) -> Result<(), BuildEnvironmentError> {
    let mut bytes = name.bytes();
    if bytes
        .next()
        .is_some_and(|b| b.is_ascii_alphabetic() || b == b'_')
        && bytes.all(|b| b.is_ascii_alphanumeric() || b == b'_')
    {
        Ok(())
    } else {
        Err(BuildEnvironmentError::InvalidName)
    }
}

pub(crate) fn resolve_refs(
    refs: &[String],
) -> Result<HashMap<String, String>, BuildEnvironmentError> {
    resolve_refs_with(refs, |name| std::env::var(name))
}

fn resolve_refs_with(
    refs: &[String],
    mut lookup: impl FnMut(&str) -> Result<String, std::env::VarError>,
) -> Result<HashMap<String, String>, BuildEnvironmentError> {
    refs.iter()
        .map(|name| {
            validate_name(name)?;
            let value =
                lookup(&format!("SPACE_BUILD_EXPORT_{name}")).map_err(|error| match error {
                    std::env::VarError::NotPresent => {
                        BuildEnvironmentError::NotExported { name: name.clone() }
                    }
                    std::env::VarError::NotUnicode(_) => {
                        BuildEnvironmentError::InvalidValue { name: name.clone() }
                    }
                })?;
            validate_value(name, &value)?;
            Ok((name.clone(), value))
        })
        .collect()
}

pub(crate) fn validate_value(name: &str, value: &str) -> Result<(), BuildEnvironmentError> {
    if value.contains('\0') {
        Err(BuildEnvironmentError::InvalidValue { name: name.into() })
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests;
