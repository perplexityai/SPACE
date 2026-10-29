use pplx_space_common_core::types::ArtifactComponentKind;

use crate::node_publish::validate::templates::{
    PublishedTemplateName, TemplateComponentSet, ValidatedTemplateInventory,
};
pub(crate) use crate::node_publish::validate::templates::{
    TemplateInventoryClaim, TemplatePolicyClaim,
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

#[derive(Debug)]
pub(crate) struct TemplatePolicyAppliedClaim {
    pub(crate) template: PublishedTemplateName,
}

/// Component values move out of the validated execution projection, while its
/// opaque execution metadata remains available to the inventory observation.
#[derive(Debug)]
pub(crate) struct TemplatePlan {
    pub(crate) inventory: Vec<TemplateInventoryClaim>,
    pub(crate) absent: Vec<PublishedTemplateName>,
    pub(crate) component_scopes: Vec<TemplateReleaseClaim>,
    pub(crate) component_keys: Vec<TemplateComponentKeyClaim>,
    pub(crate) components: Vec<TemplateComponentClaim>,
    pub(crate) policies: Vec<TemplatePolicyClaim>,
    pub(crate) applied_policies: Vec<TemplatePolicyAppliedClaim>,
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
        let policies: Vec<_> = inventory.take_policies().collect();
        let applied_policies = policies
            .iter()
            .map(|claim| TemplatePolicyAppliedClaim {
                template: claim.template.clone(),
            })
            .collect();
        let (absent, inventory) = inventory.into_inventory();
        Self {
            inventory,
            absent,
            component_scopes,
            component_keys,
            components,
            policies,
            applied_policies,
        }
    }
}
