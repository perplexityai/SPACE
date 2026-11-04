use std::sync::{Arc, RwLock};

use pplx_space_common_svc::core::types::ClusterId;
use pplx_space_common_svc::space_api_route_ed25519::{
    SpaceApiRouteEd25519Error, SpaceApiRouteEd25519Issuer, SpaceApiRouteEd25519KeyId,
    SpaceApiRouteEd25519Token, SpaceApiRouteEd25519VerificationBundle,
    SpaceApiRouteEd25519VerificationBundleUpdate,
    SpaceApiRouteEd25519VerificationBundleUpdateError, SpaceApiRouteEd25519VerificationGeneration,
    SpaceApiRouteEd25519VerificationKey,
};

#[derive(Clone, Debug, Default)]
pub struct RouteVerificationState {
    inner: Arc<RwLock<RouteVerificationSnapshot>>,
}

#[derive(Clone, Debug, Default)]
struct RouteVerificationSnapshot {
    bundle: Option<SpaceApiRouteEd25519VerificationBundle>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RouteVerificationUpdate {
    Updated {
        generation: SpaceApiRouteEd25519VerificationGeneration,
    },
    Unchanged {
        generation: SpaceApiRouteEd25519VerificationGeneration,
    },
}

impl RouteVerificationUpdate {
    pub const fn generation(self) -> SpaceApiRouteEd25519VerificationGeneration {
        match self {
            Self::Updated { generation } | Self::Unchanged { generation } => generation,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum RouteVerificationUpdateError {
    #[error("route verification state lock is poisoned")]
    StatePoisoned,
    #[error("stale route verification key generation: current={current}, incoming={incoming}")]
    StaleGeneration {
        current: SpaceApiRouteEd25519VerificationGeneration,
        incoming: SpaceApiRouteEd25519VerificationGeneration,
    },
    #[error("conflicting route verification keys for generation {generation}")]
    GenerationConflict {
        generation: SpaceApiRouteEd25519VerificationGeneration,
    },
    #[error("{0}")]
    InvalidVerifier(#[from] SpaceApiRouteEd25519Error),
}

impl RouteVerificationState {
    pub fn update(
        &self,
        generation: SpaceApiRouteEd25519VerificationGeneration,
        issuer: SpaceApiRouteEd25519Issuer,
        cluster_id: ClusterId,
        keys: Vec<SpaceApiRouteEd25519VerificationKey>,
    ) -> Result<RouteVerificationUpdate, RouteVerificationUpdateError> {
        let incoming =
            SpaceApiRouteEd25519VerificationBundle::new(generation, issuer, cluster_id, keys)?;

        let mut current = self
            .inner
            .write()
            .map_err(|_| RouteVerificationUpdateError::StatePoisoned)?;
        if let Some(installed) = current.bundle.as_ref() {
            match installed.classify_update(&incoming) {
                Ok(SpaceApiRouteEd25519VerificationBundleUpdate::Unchanged) => {
                    return Ok(RouteVerificationUpdate::Unchanged { generation });
                }
                Ok(SpaceApiRouteEd25519VerificationBundleUpdate::Updated) => {}
                Err(SpaceApiRouteEd25519VerificationBundleUpdateError::StaleGeneration {
                    current,
                    incoming,
                }) => {
                    return Err(RouteVerificationUpdateError::StaleGeneration {
                        current,
                        incoming,
                    });
                }
                Err(SpaceApiRouteEd25519VerificationBundleUpdateError::GenerationConflict {
                    generation,
                }) => {
                    return Err(RouteVerificationUpdateError::GenerationConflict { generation });
                }
            }
        }
        current.bundle = Some(incoming);
        Ok(RouteVerificationUpdate::Updated { generation })
    }

    pub fn generation(&self) -> u64 {
        self.inner
            .read()
            .ok()
            .and_then(|snapshot| snapshot.bundle.as_ref().map(|bundle| bundle.generation))
            .map(SpaceApiRouteEd25519VerificationGeneration::get)
            .unwrap_or_default()
    }

    pub fn verify(
        &self,
        token: &SpaceApiRouteEd25519Token,
    ) -> Result<
        pplx_space_common_svc::space_api_route_ed25519::SpaceApiRouteEd25519Claims,
        SpaceApiRouteEd25519Error,
    > {
        let verifier = self
            .inner
            .read()
            .map_err(|_| SpaceApiRouteEd25519Error::MissingVerificationKeys)?
            .bundle
            .as_ref()
            .map(|bundle| bundle.verifier().clone())
            .ok_or(SpaceApiRouteEd25519Error::MissingVerificationKeys)?;
        verifier.verify(token)
    }
}

pub fn verification_key_from_wire(
    key_id: String,
    public_key: Vec<u8>,
) -> Result<SpaceApiRouteEd25519VerificationKey, SpaceApiRouteEd25519Error> {
    Ok(SpaceApiRouteEd25519VerificationKey {
        key_id: SpaceApiRouteEd25519KeyId::parse(key_id)?,
        public_key,
    })
}

#[cfg(test)]
mod tests;
