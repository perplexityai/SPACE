//! Stored versioned environment documents. Database migrations remain owned by the first
//! policy store; this codec does not reinterpret legacy execution JSON.
use super::*;

#[cfg(test)]
mod tests;

// Schema dispatch must not depend on a descriptor shape this reader knows.
// Typed decoding also rejects duplicate schema_version fields.
#[derive(serde::Deserialize)]
struct SchemaEnvelope {
    schema_version: u32,
}

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Document {
    schema_version: u32,
    reference: EnvironmentReference,
    descriptor: EnvironmentSpec,
}

#[derive(Debug, thiserror::Error)]
pub enum EnvironmentDocumentError {
    #[error("invalid environment JSON: {0}")]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    Definition(#[from] EnvironmentDecodeError),
    #[error("environment document contains unknown fields or noncanonical scalar values")]
    UnrecognizedRepresentation,
}

impl EnvironmentDefinition {
    pub fn to_document(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string(&Document {
            schema_version: self.identity_schema_version(),
            reference: self.reference,
            descriptor: self
                .legacy_descriptor
                .as_ref()
                .unwrap_or(&self.descriptor)
                .spec()
                .clone(),
        })
    }

    pub fn from_document(text: &str) -> Result<Self, EnvironmentDocumentError> {
        let envelope: SchemaEnvelope = serde_json::from_str(text)?;
        if !matches!(
            envelope.schema_version,
            ENVIRONMENT_SCHEMA_VERSION | RUNTIME_NEUTRAL_ENVIRONMENT_SCHEMA_VERSION
        ) {
            return Err(EnvironmentDecodeError::UnsupportedSchema(envelope.schema_version).into());
        }
        // Decode the supported schema directly to reject duplicate keys, including
        // inside reused legacy types. Those types retain permissive JSON readers;
        // compare parsed trees here to reject any fields they discarded.
        let document: Document = serde_json::from_str(text)?;
        let supplied: serde_json::Value = serde_json::from_str(text)?;
        if supplied != serde_json::to_value(&document)? {
            return Err(EnvironmentDocumentError::UnrecognizedRepresentation);
        }
        let descriptor = EnvironmentDescriptor::try_from(document.descriptor)
            .map_err(EnvironmentDecodeError::from)?;
        Ok(Self::new(descriptor)
            .check_schema(document.schema_version)?
            .check_reference(document.reference)?
            .into_runtime_neutral()?)
    }
}
