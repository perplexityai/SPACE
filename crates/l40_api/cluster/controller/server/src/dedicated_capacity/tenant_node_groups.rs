//! A node group per encrypted tenant, owned by the cluster controller.
//!
//! A tenant that brings a cache key (`node_groups.cache_kms_key_arn`) doesn't
//! draw from a shared pool: its group is its own pool (`pool = name`), an EKS
//! managed node group of the same name that this module creates and deletes.
//! Everything that makes the group the tenant's is fixed at creation, so a
//! node is the tenant's from its first boot:
//!
//! - **Role.** An IAM node role per tenant, under the Terraform-owned
//!   permissions boundary, with the shared node policies plus an inline grant
//!   to use the tenant's key, only under the node-cache encryption context.
//!   The tenant's key policy names this role (or the boundary's path).
//! - **Launch template.** A version of the Terraform-owned base template (the
//!   BYOK AMI, worker bootstrap, IMDS tags on) whose root volume is encrypted
//!   under the tenant's key and whose instances carry
//!   `SpaceCacheEncryptionKmsKeyArn=<key>`: the AMI's cache unit builds
//!   `/space/cache` on LUKS under a data key from that key before kubelet
//!   starts, and powers the node off if the key stops decrypting.
//! - **Node group.** Created at size 0, its nodes registered with the taints
//!   `space.pplx.net/dedicated-org=<org>` and `space.pplx.net/unassigned`, so
//!   nothing else schedules on a node before it is assigned. The boundary
//!   label `node-restriction.kubernetes.io/space-node-group` can't be set at
//!   registration (kubelet refuses the prefix), so the pool plan assigns the
//!   node exactly as for a shared pool: label, then lift `unassigned`. It
//!   grows the group with `UpdateNodegroupConfig` and releases nodes by drain
//!   and terminate.
//!
//! When the group's `desired_nodes` is 0 and none of its nodes are left in
//! the cluster, the node group is deleted, then its access entry and role.
//!
//! PoC scope: no controller handling of key revocation (the AMI's watchdog
//! powers the node off; replacements fail to launch until the key is
//! re-enabled), no quota check, and no update of an existing node group when
//! the base template moves.

use std::collections::{BTreeMap, HashMap, HashSet};

use pplx_space_aws_client::ec2::LaunchTemplateVersionRequest;
use pplx_space_aws_client::eks::NewNodegroup;
use pplx_space_aws_client::iam::NewRole;
use pplx_space_sandbox_pod::DEDICATED_NODE_TAINT_KEY;
use pplx_space_sql_locator::sql;
use serde_json::json;
use sqlx::PgPool;
use tokio::sync::Mutex;

use super::UNASSIGNED_TAINT_KEY;
use crate::env_config;

/// Instance, volume, node group and role tag naming the owner. Tag keys have
/// no `/`: the instances expose their tags through IMDS.
pub(crate) const MANAGED_BY_TAG: &str = "SpaceManagedBy";
pub(crate) const MANAGED_BY_VALUE: &str = "cluster-controller";
pub(crate) const GROUP_TAG: &str = "SpaceTenantNodeGroup";
/// The tag the AMI's cache unit reads to encrypt `/space/cache`.
pub(crate) const CACHE_KEY_TAG: &str = "SpaceCacheEncryptionKmsKeyArn";
const INLINE_POLICY_NAME: &str = "tenant-cache-key";
const IAM_ROLE_NAME_MAX: usize = 64;
/// `ClientToken` of `CreateLaunchTemplateVersion` is at most 64 characters.
const CLIENT_TOKEN_MAX: usize = 64;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Config {
    pub(crate) launch_template_id: String,
    pub(crate) launch_template_source_version: String,
    pub(crate) subnet_ids: Vec<String>,
    pub(crate) instance_types: Vec<String>,
    pub(crate) root_device_name: String,
    pub(crate) root_volume_gib: u32,
    pub(crate) max_nodes: i64,
    pub(crate) role_name_prefix: String,
    pub(crate) role_path: String,
    pub(crate) role_boundary_arn: Option<String>,
    pub(crate) role_policy_arns: Vec<String>,
    pub(crate) labels: BTreeMap<String, String>,
    pub(crate) tags: BTreeMap<String, String>,
}

fn var(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn list(name: &str) -> Vec<String> {
    var(name)
        .map(|value| {
            value
                .split(',')
                .map(|item| item.trim().to_string())
                .filter(|item| !item.is_empty())
                .collect()
        })
        .unwrap_or_default()
}

fn map(name: &str) -> Result<BTreeMap<String, String>, String> {
    match var(name) {
        None => Ok(BTreeMap::new()),
        Some(value) => {
            serde_json::from_str(&value).map_err(|err| format!("{name} is not a JSON map: {err}"))
        }
    }
}

/// The worker label set the space-worker DaemonSets select on.
fn default_labels() -> BTreeMap<String, String> {
    [
        ("node.kubernetes.io/space-enabled", "true"),
        ("space.pplx.net/node-pool", "workers"),
        ("space.pplx.net/subnet-tier", "secondary"),
        ("workload", "workers"),
    ]
    .into_iter()
    .map(|(key, value)| (key.to_string(), value.to_string()))
    .collect()
}

impl Config {
    /// `Ok(None)` when per-tenant node groups are off.
    pub(crate) fn from_env() -> Result<Option<Self>, String> {
        if !env_config::truthy_default("SPACE_TENANT_NODE_GROUPS_ENABLED", false) {
            return Ok(None);
        }
        let required = |name: &str| var(name).ok_or_else(|| format!("{name} is unset"));
        let non_empty = |name: &str| {
            let values = list(name);
            if values.is_empty() {
                Err(format!("{name} is unset"))
            } else {
                Ok(values)
            }
        };
        let labels = match var("SPACE_TENANT_NODE_GROUPS_LABELS") {
            None => default_labels(),
            Some(_) => map("SPACE_TENANT_NODE_GROUPS_LABELS")?,
        };
        Ok(Some(Self {
            launch_template_id: required("SPACE_TENANT_NODE_GROUPS_LAUNCH_TEMPLATE_ID")?,
            launch_template_source_version: required(
                "SPACE_TENANT_NODE_GROUPS_LAUNCH_TEMPLATE_VERSION",
            )?,
            subnet_ids: non_empty("SPACE_TENANT_NODE_GROUPS_SUBNET_IDS")?,
            instance_types: non_empty("SPACE_TENANT_NODE_GROUPS_INSTANCE_TYPES")?,
            root_device_name: var("SPACE_TENANT_NODE_GROUPS_ROOT_DEVICE_NAME")
                .unwrap_or_else(|| "/dev/xvda".to_string()),
            root_volume_gib: env_config::positive_u32(
                "SPACE_TENANT_NODE_GROUPS_ROOT_VOLUME_GIB",
                150,
            ),
            max_nodes: i64::from(env_config::positive_u32(
                "SPACE_TENANT_NODE_GROUPS_MAX_NODES",
                4,
            )),
            role_name_prefix: required("SPACE_TENANT_NODE_GROUPS_ROLE_NAME_PREFIX")?,
            role_path: var("SPACE_TENANT_NODE_GROUPS_ROLE_PATH").unwrap_or_else(|| "/".to_string()),
            role_boundary_arn: var("SPACE_TENANT_NODE_GROUPS_ROLE_BOUNDARY_ARN"),
            role_policy_arns: non_empty("SPACE_TENANT_NODE_GROUPS_ROLE_POLICY_ARNS")?,
            labels,
            tags: map("SPACE_TENANT_NODE_GROUPS_TAGS")?,
        }))
    }

    pub(crate) fn role_name(&self, group: &str) -> Result<String, String> {
        let name = format!("{}{group}", self.role_name_prefix);
        if name.len() > IAM_ROLE_NAME_MAX {
            return Err(format!(
                "node role name {name} is longer than {IAM_ROLE_NAME_MAX} characters"
            ));
        }
        Ok(name)
    }

    /// Tags on the role, node group, instances and volumes.
    pub(crate) fn tags_for(&self, group: &str) -> Vec<(String, String)> {
        let mut tags = self.tags.clone();
        tags.insert("Name".to_string(), group.to_string());
        tags.insert(MANAGED_BY_TAG.to_string(), MANAGED_BY_VALUE.to_string());
        tags.insert(GROUP_TAG.to_string(), group.to_string());
        tags.into_iter().collect()
    }

    pub(crate) fn node_group_for(
        &self,
        group: &KeyedGroup,
        node_role_arn: &str,
        launch_template_version: i64,
    ) -> NewNodegroup {
        NewNodegroup {
            name: group.name.clone(),
            node_role_arn: node_role_arn.to_string(),
            subnet_ids: self.subnet_ids.clone(),
            instance_types: self.instance_types.clone(),
            launch_template_id: self.launch_template_id.clone(),
            launch_template_version: launch_template_version.to_string(),
            max_size: self.max_nodes,
            labels: self.labels.clone().into_iter().collect(),
            taints: vec![
                (
                    DEDICATED_NODE_TAINT_KEY.to_string(),
                    group.org_id.to_string(),
                ),
                (UNASSIGNED_TAINT_KEY.to_string(), "true".to_string()),
            ],
            tags: self.tags_for(&group.name),
        }
    }

    pub(crate) fn launch_template_version_for(
        &self,
        group: &KeyedGroup,
    ) -> LaunchTemplateVersionRequest {
        let mut tags = self.tags_for(&group.name);
        tags.push((CACHE_KEY_TAG.to_string(), group.cache_kms_key_arn.clone()));
        let mut client_token = format!("{}-v{}", group.name, self.launch_template_source_version);
        client_token.truncate(CLIENT_TOKEN_MAX);
        LaunchTemplateVersionRequest {
            launch_template_id: self.launch_template_id.clone(),
            source_version: self.launch_template_source_version.clone(),
            client_token,
            description: format!("tenant node group {}", group.name),
            root_device_name: self.root_device_name.clone(),
            root_volume_gib: self.root_volume_gib,
            root_volume_kms_key_id: group.cache_kms_key_arn.clone(),
            tags,
        }
    }
}

/// The node role may use the tenant's key only to wrap and unwrap the cache's
/// data key.
pub(crate) fn cache_key_policy(key_arn: &str) -> String {
    json!({
        "Version": "2012-10-17",
        "Statement": [{
            "Sid": "CacheDataKey",
            "Effect": "Allow",
            "Action": ["kms:GenerateDataKey", "kms:Decrypt"],
            "Resource": key_arn,
            "Condition": {
                "StringEquals": { "kms:EncryptionContext:space:purpose": "node-cache" }
            }
        }]
    })
    .to_string()
}

fn ec2_trust_policy() -> String {
    json!({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": { "Service": "ec2.amazonaws.com" },
            "Action": "sts:AssumeRole"
        }]
    })
    .to_string()
}

/// A `node_groups` row that is its own encrypted pool.
#[derive(Debug, Clone, PartialEq, Eq, sqlx::FromRow)]
pub(crate) struct KeyedGroup {
    pub(crate) name: String,
    pub(crate) org_id: pplx_space_accounts::OrgId,
    pub(crate) desired_nodes: i32,
    pub(crate) cache_kms_key_arn: String,
}

/// What to do with one group's node group this tick.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Step {
    Create,
    Delete,
    Cleanup,
    Wait(&'static str),
}

/// `status` is the node group's EKS status (`None`: it doesn't exist);
/// `nodes` counts its kube Nodes.
pub(crate) fn step(desired_nodes: i32, status: Option<&str>, nodes: usize, cleaned: bool) -> Step {
    match (desired_nodes > 0, status) {
        (true, None) => Step::Create,
        (true, Some(_)) => Step::Wait("exists"),
        (false, Some("DELETING")) => Step::Wait("deleting"),
        (false, Some(_)) if nodes > 0 => Step::Wait("nodes still draining"),
        (false, Some(_)) => Step::Delete,
        (false, None) if cleaned => Step::Wait("gone"),
        (false, None) => Step::Cleanup,
    }
}

pub(crate) struct TenantNodeGroups {
    config: Config,
    eks_cluster: String,
    eks: pplx_space_aws_client::eks::Client,
    ec2: pplx_space_aws_client::ec2::Client,
    iam: pplx_space_aws_client::iam::Client,
    /// Groups whose node group and role this process has seen deleted, so a
    /// released tenant costs no AWS calls per tick.
    cleaned: Mutex<HashSet<String>>,
}

impl TenantNodeGroups {
    pub(crate) fn new(config: Config, eks_cluster: String, aws: &aws_config::SdkConfig) -> Self {
        Self {
            config,
            eks_cluster,
            eks: pplx_space_aws_client::eks::Client::from_config(aws),
            ec2: pplx_space_aws_client::ec2::Client::from_config(aws),
            iam: pplx_space_aws_client::iam::Client::from_config(aws),
            cleaned: Mutex::new(HashSet::new()),
        }
    }

    /// `nodes_by_group` counts kube Nodes per EKS node group name.
    pub(crate) async fn reconcile(
        &self,
        pool: &PgPool,
        nodes_by_group: &HashMap<String, usize>,
    ) -> Result<(), String> {
        let groups: Vec<KeyedGroup> = sqlx::query_as(sql!(
            "SELECT name, org_id, desired_nodes, cache_kms_key_arn
               FROM node_groups
              WHERE cache_kms_key_arn IS NOT NULL AND pool = name
                AND org_id IS NOT NULL AND desired_nodes IS NOT NULL"
        ))
        .fetch_all(pool)
        .await
        .map_err(|err| err.to_string())?;
        for group in groups {
            if let Err(err) = self.reconcile_one(&group, nodes_by_group).await {
                tracing::warn!(group = %group.name, error = %err, "tenant_node_groups: failed");
            }
        }
        Ok(())
    }

    async fn reconcile_one(
        &self,
        group: &KeyedGroup,
        nodes_by_group: &HashMap<String, usize>,
    ) -> Result<(), String> {
        let mut cleaned = self.cleaned.lock().await;
        if group.desired_nodes > 0 {
            cleaned.remove(&group.name);
        } else if cleaned.contains(&group.name) {
            return Ok(());
        }
        let status = self
            .eks
            .describe_nodegroup(&self.eks_cluster, &group.name)
            .await
            .map_err(|err| err.to_string())?;
        if let Some(status) = &status
            && !status.issues.is_empty()
        {
            tracing::warn!(
                group = %group.name,
                status = %status.status,
                issues = ?status.issues,
                "tenant_node_groups: node group reports health issues"
            );
        }
        let nodes = nodes_by_group.get(&group.name).copied().unwrap_or(0);
        let step = step(
            group.desired_nodes,
            status.as_ref().map(|status| status.status.as_str()),
            nodes,
            cleaned.contains(&group.name),
        );
        match step {
            Step::Wait(_) => Ok(()),
            Step::Create => self.create(group).await,
            Step::Delete => {
                tracing::info!(group = %group.name, "tenant_node_groups: deleting node group");
                self.eks
                    .delete_nodegroup(&self.eks_cluster, &group.name)
                    .await
                    .map_err(|err| err.to_string())
            }
            Step::Cleanup => {
                self.cleanup(&group.name).await?;
                cleaned.insert(group.name.clone());
                Ok(())
            }
        }
    }

    async fn create(&self, group: &KeyedGroup) -> Result<(), String> {
        let role_name = self.config.role_name(&group.name)?;
        let role_arn = self
            .iam
            .create_role(&NewRole {
                name: role_name.clone(),
                path: self.config.role_path.clone(),
                assume_role_policy: ec2_trust_policy(),
                permissions_boundary_arn: self.config.role_boundary_arn.clone(),
                tags: self.config.tags_for(&group.name),
            })
            .await
            .map_err(|err| format!("create role {role_name}: {err}"))?;
        for policy_arn in &self.config.role_policy_arns {
            self.iam
                .attach_role_policy(&role_name, policy_arn)
                .await
                .map_err(|err| format!("attach {policy_arn}: {err}"))?;
        }
        self.iam
            .put_role_policy(
                &role_name,
                INLINE_POLICY_NAME,
                &cache_key_policy(&group.cache_kms_key_arn),
            )
            .await
            .map_err(|err| format!("put cache key policy: {err}"))?;
        self.eks
            .create_node_access_entry(&self.eks_cluster, &role_arn)
            .await
            .map_err(|err| format!("create access entry: {err}"))?;
        let version = self
            .ec2
            .create_launch_template_version(&self.config.launch_template_version_for(group))
            .await
            .map_err(|err| format!("create launch template version: {err}"))?;
        // A role created a moment ago may not be assumable yet; the node group
        // call then fails and the next tick retries it.
        self.eks
            .create_nodegroup(
                &self.eks_cluster,
                &self.config.node_group_for(group, &role_arn, version),
            )
            .await
            .map_err(|err| format!("create node group: {err}"))?;
        tracing::info!(
            group = %group.name,
            role = %role_arn,
            launch_template_version = version,
            "tenant_node_groups: created node group"
        );
        Ok(())
    }

    async fn cleanup(&self, group: &str) -> Result<(), String> {
        let role_name = self.config.role_name(group)?;
        if let Some(role_arn) = self
            .iam
            .role_arn(&role_name)
            .await
            .map_err(|err| err.to_string())?
        {
            self.eks
                .delete_access_entry(&self.eks_cluster, &role_arn)
                .await
                .map_err(|err| format!("delete access entry: {err}"))?;
            self.iam
                .delete_role(
                    &role_name,
                    &self.config.role_policy_arns,
                    INLINE_POLICY_NAME,
                )
                .await
                .map_err(|err| format!("delete role {role_name}: {err}"))?;
            tracing::info!(group, role = %role_arn, "tenant_node_groups: deleted node role");
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn steps() {
        assert_eq!(step(1, None, 0, false), Step::Create);
        assert_eq!(step(1, Some("ACTIVE"), 1, false), Step::Wait("exists"));
        assert_eq!(
            step(0, Some("ACTIVE"), 1, false),
            Step::Wait("nodes still draining")
        );
        assert_eq!(step(0, Some("ACTIVE"), 0, false), Step::Delete);
        assert_eq!(step(0, Some("CREATE_FAILED"), 0, false), Step::Delete);
        assert_eq!(step(0, Some("DELETING"), 0, false), Step::Wait("deleting"));
        assert_eq!(step(0, None, 0, false), Step::Cleanup);
        assert_eq!(step(0, None, 0, true), Step::Wait("gone"));
    }

    #[test]
    fn cache_key_policy_is_scoped_to_the_node_cache_context() {
        let policy: serde_json::Value =
            serde_json::from_str(&cache_key_policy("arn:aws:kms:us-east-1:1:key/k")).unwrap();
        let statement = &policy["Statement"][0];
        assert_eq!(statement["Resource"], "arn:aws:kms:us-east-1:1:key/k");
        assert_eq!(
            statement["Condition"]["StringEquals"]["kms:EncryptionContext:space:purpose"],
            "node-cache"
        );
    }
}
