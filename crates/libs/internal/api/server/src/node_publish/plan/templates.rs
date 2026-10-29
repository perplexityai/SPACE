use pplx_space_common_core::types::ArtifactComponentKind;

use crate::node_publish::validate::templates::{
    PublishedTemplateName, TemplateComponentSet, ValidatedTemplateInventory,
};

/// A reported release remains in scope even when it reports no components.
#[derive(Clone, Debug)]
pub(crate) struct TemplateReleaseClaim {
    pub(crate) template: PublishedTemplateName,
    pub(crate) content_hash: String,
}

#[derive(Clone, Debug)]
pub(crate) struct TemplateComponentKeyClaim {
    pub(crate) release: TemplateReleaseClaim,
    pub(crate) kind: ArtifactComponentKind,
    pub(crate) role_key: String,
}

#[derive(Debug)]
pub(crate) struct TemplateComponentClaim {
    pub(crate) key: TemplateComponentKeyClaim,
    pub(crate) version: Option<String>,
    pub(crate) digest: Option<String>,
    pub(crate) format: Option<String>,
}

/// Component values move out of the validated execution projection, while its
/// opaque execution metadata remains available to the inventory observation.
#[derive(Debug)]
pub(crate) struct TemplatePlan {
    pub(crate) inventory: ValidatedTemplateInventory,
    pub(crate) component_scopes: Vec<TemplateReleaseClaim>,
    pub(crate) component_keys: Vec<TemplateComponentKeyClaim>,
    pub(crate) components: Vec<TemplateComponentClaim>,
}

impl From<ValidatedTemplateInventory> for TemplatePlan {
    fn from(mut inventory: ValidatedTemplateInventory) -> Self {
        let mut component_scopes = Vec::new();
        let mut component_keys = Vec::new();
        let mut components = Vec::new();
        for TemplateComponentSet {
            name,
            content_hash,
            components: reported,
        } in inventory.take_components()
        {
            let release = TemplateReleaseClaim {
                template: name,
                content_hash,
            };
            for component in reported {
                let key = TemplateComponentKeyClaim {
                    release: release.clone(),
                    kind: component.kind,
                    role_key: component.role_key,
                };
                component_keys.push(key.clone());
                components.push(TemplateComponentClaim {
                    key,
                    version: component.version,
                    digest: component.digest,
                    format: component.format,
                });
            }
            component_scopes.push(release);
        }
        Self {
            inventory,
            component_scopes,
            component_keys,
            components,
        }
    }
}
