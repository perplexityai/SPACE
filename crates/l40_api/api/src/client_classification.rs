use std::collections::BTreeMap;
use std::sync::Arc;

use pplx_space_metrics::sandbox::api::ClientClass;
use serde::de::{Error, MapAccess, Visitor};
use serde::{Deserialize, Deserializer};

const CLIENT_CLASSES_ENV: &str = "SPACE_API_CLIENT_CLASSES";

#[derive(Clone, Debug, Default)]
pub struct ClientClassification(Arc<BTreeMap<String, ClientClass>>);

#[derive(Debug, thiserror::Error)]
pub enum ClientClassificationError {
    #[error("SPACE_API_CLIENT_CLASSES is not valid Unicode")]
    NonUnicode,
    #[error("invalid SPACE_API_CLIENT_CLASSES: {0}")]
    Json(#[source] serde_json::Error),
}

impl ClientClassification {
    pub fn from_env() -> Result<Self, ClientClassificationError> {
        match std::env::var(CLIENT_CLASSES_ENV) {
            Ok(raw) => Self::parse(&raw),
            Err(std::env::VarError::NotPresent) => Ok(Self::default()),
            Err(std::env::VarError::NotUnicode(_)) => Err(ClientClassificationError::NonUnicode),
        }
    }

    fn parse(raw: &str) -> Result<Self, ClientClassificationError> {
        if raw.trim().is_empty() {
            return Ok(Self::default());
        }
        serde_json::from_str(raw).map_err(ClientClassificationError::Json)
    }

    pub fn classify(&self, client_id: Option<&str>) -> ClientClass {
        client_id
            .and_then(|id| self.0.get(id))
            .copied()
            .unwrap_or(ClientClass::Other)
    }
}

impl<'de> Deserialize<'de> for ClientClassification {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct ClassesVisitor;

        impl<'de> Visitor<'de> for ClassesVisitor {
            type Value = ClientClassification;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("a JSON object mapping client IDs to classes")
            }

            fn visit_map<M: MapAccess<'de>>(self, mut map: M) -> Result<Self::Value, M::Error> {
                let mut classes = BTreeMap::new();
                while let Some((client, class)) = map.next_entry::<String, String>()? {
                    let client = client.trim();
                    if client.is_empty() {
                        return Err(M::Error::custom("empty client ID"));
                    }
                    if classes.contains_key(client) {
                        return Err(M::Error::custom(format!("duplicate client ID {client:?}")));
                    }
                    let parsed = class.parse().map_err(|error| {
                        M::Error::custom(format!("client {client:?}, class {class:?}: {error}"))
                    })?;
                    classes.insert(client.to_string(), parsed);
                }
                Ok(ClientClassification(Arc::new(classes)))
            }
        }

        deserializer.deserialize_map(ClassesVisitor)
    }
}

#[cfg(test)]
mod tests;
