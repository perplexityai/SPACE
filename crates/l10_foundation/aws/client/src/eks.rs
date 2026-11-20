//! The EKS managed-node-group calls SPACE makes: read a node group's scaling
//! config and set its desired size, and create, describe and delete the
//! per-tenant node groups the cluster controller owns.

use aws_types::SdkConfig;
use serde_json::{Value, json};

use crate::query::{AwsClientError, AwsQueryClient, AwsQueryService, ReqwestTransport};

/// `scalingConfig` of a managed node group.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NodegroupScaling {
    pub min_size: i64,
    pub max_size: i64,
    pub desired_size: i64,
}

#[derive(Debug, Clone)]
pub struct Client {
    query: AwsQueryClient<ReqwestTransport>,
}

impl Client {
    pub fn from_config(config: &SdkConfig) -> Self {
        Self {
            query: AwsQueryClient::from_config(AwsQueryService::Eks, config),
        }
    }

    /// `DescribeNodegroup`, reduced to its scaling config.
    pub async fn describe_nodegroup_scaling(
        &self,
        cluster: &str,
        nodegroup: &str,
    ) -> Result<NodegroupScaling, AwsClientError> {
        let body = self
            .query
            .send_rest_json(
                "GET",
                &format!("clusters/{cluster}/node-groups/{nodegroup}"),
                String::new(),
            )
            .await?;
        parse_scaling(&body)
    }

    /// `UpdateNodegroupConfig` with only `scalingConfig.desiredSize` set.
    pub async fn set_nodegroup_desired_size(
        &self,
        cluster: &str,
        nodegroup: &str,
        desired_size: i64,
    ) -> Result<(), AwsClientError> {
        self.query
            .send_rest_json(
                "POST",
                &format!("clusters/{cluster}/node-groups/{nodegroup}/update-config"),
                json!({ "scalingConfig": { "desiredSize": desired_size } }).to_string(),
            )
            .await?;
        Ok(())
    }
}

/// What a per-tenant managed node group is created with.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewNodegroup {
    pub name: String,
    pub node_role_arn: String,
    pub subnet_ids: Vec<String>,
    pub instance_types: Vec<String>,
    pub launch_template_id: String,
    pub launch_template_version: String,
    pub max_size: i64,
    pub labels: Vec<(String, String)>,
    /// `(key, value)`, all `NO_SCHEDULE`.
    pub taints: Vec<(String, String)>,
    pub tags: Vec<(String, String)>,
}

/// `DescribeNodegroup`, reduced to what the controller reads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodegroupStatus {
    /// `CREATING`, `ACTIVE`, `DELETING`, `CREATE_FAILED`, `DEGRADED`, ...
    pub status: String,
    pub scaling: Option<NodegroupScaling>,
    /// `code: message` of each health issue.
    pub issues: Vec<String>,
}

impl Client {
    /// `DescribeNodegroup`. `Ok(None)` when the node group doesn't exist.
    pub async fn describe_nodegroup(
        &self,
        cluster: &str,
        nodegroup: &str,
    ) -> Result<Option<NodegroupStatus>, AwsClientError> {
        let result = self
            .query
            .send_rest_json(
                "GET",
                &format!("clusters/{cluster}/node-groups/{nodegroup}"),
                String::new(),
            )
            .await;
        match result {
            Ok(body) => parse_status(&body).map(Some),
            Err(err) if err.service_status() == Some(http::StatusCode::NOT_FOUND) => Ok(None),
            Err(err) => Err(err),
        }
    }

    /// `CreateNodegroup` with a launch template, starting empty (min 0,
    /// desired 0): the dedicated-capacity reconciler sizes it.
    pub async fn create_nodegroup(
        &self,
        cluster: &str,
        nodegroup: &NewNodegroup,
    ) -> Result<(), AwsClientError> {
        let map = |pairs: &[(String, String)]| {
            Value::Object(
                pairs
                    .iter()
                    .map(|(key, value)| (key.clone(), Value::String(value.clone())))
                    .collect(),
            )
        };
        let taints: Vec<Value> = nodegroup
            .taints
            .iter()
            .map(|(key, value)| json!({ "key": key, "value": value, "effect": "NO_SCHEDULE" }))
            .collect();
        let body = json!({
            "nodegroupName": nodegroup.name,
            "nodeRole": nodegroup.node_role_arn,
            "subnets": nodegroup.subnet_ids,
            "instanceTypes": nodegroup.instance_types,
            "capacityType": "ON_DEMAND",
            "launchTemplate": {
                "id": nodegroup.launch_template_id,
                "version": nodegroup.launch_template_version,
            },
            "scalingConfig": { "minSize": 0, "maxSize": nodegroup.max_size, "desiredSize": 0 },
            "updateConfig": { "maxUnavailable": 1 },
            "labels": map(&nodegroup.labels),
            "taints": taints,
            "tags": map(&nodegroup.tags),
            "clientRequestToken": format!("{}-{}", nodegroup.name, nodegroup.launch_template_version),
        });
        self.query
            .send_rest_json(
                "POST",
                &format!("clusters/{cluster}/node-groups"),
                body.to_string(),
            )
            .await?;
        Ok(())
    }

    /// `DeleteNodegroup`. A node group that's already gone is not an error.
    pub async fn delete_nodegroup(
        &self,
        cluster: &str,
        nodegroup: &str,
    ) -> Result<(), AwsClientError> {
        let result = self
            .query
            .send_rest_json(
                "DELETE",
                &format!("clusters/{cluster}/node-groups/{nodegroup}"),
                String::new(),
            )
            .await;
        match result {
            Ok(_) => Ok(()),
            Err(err) if err.service_status() == Some(http::StatusCode::NOT_FOUND) => Ok(()),
            Err(err) => Err(err),
        }
    }
}

impl Client {
    /// `CreateAccessEntry` of type `EC2_LINUX` for a node role. EKS creates
    /// this itself for managed node groups; an existing entry is not an error.
    pub async fn create_node_access_entry(
        &self,
        cluster: &str,
        principal_arn: &str,
    ) -> Result<(), AwsClientError> {
        let result = self
            .query
            .send_rest_json(
                "POST",
                &format!("clusters/{cluster}/access-entries"),
                json!({ "principalArn": principal_arn, "type": "EC2_LINUX" }).to_string(),
            )
            .await;
        match result {
            Ok(_) => Ok(()),
            Err(err) if err.service_status() == Some(http::StatusCode::CONFLICT) => Ok(()),
            Err(err) => Err(err),
        }
    }

    /// `DeleteAccessEntry`. An entry that's already gone is not an error.
    pub async fn delete_access_entry(
        &self,
        cluster: &str,
        principal_arn: &str,
    ) -> Result<(), AwsClientError> {
        let result = self
            .query
            .send_rest_json(
                "DELETE",
                &format!(
                    "clusters/{cluster}/access-entries/{}",
                    percent_encode_segment(principal_arn)
                ),
                String::new(),
            )
            .await;
        match result {
            Ok(_) => Ok(()),
            Err(err) if err.service_status() == Some(http::StatusCode::NOT_FOUND) => Ok(()),
            Err(err) => Err(err),
        }
    }
}

/// Percent-encodes everything outside RFC 3986's unreserved set, so an ARN
/// fits in one path segment.
fn percent_encode_segment(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~') {
            out.push(byte as char);
        } else {
            out.push_str(&format!("%{byte:02X}"));
        }
    }
    out
}

fn parse_status(body: &str) -> Result<NodegroupStatus, AwsClientError> {
    let value: Value =
        serde_json::from_str(body).map_err(|err| AwsClientError::Xml(err.to_string()))?;
    let nodegroup = value.get("nodegroup");
    let status = nodegroup
        .and_then(|nodegroup| nodegroup.get("status"))
        .and_then(Value::as_str)
        .ok_or_else(|| AwsClientError::Xml("nodegroup.status missing".to_string()))?
        .to_string();
    let issues = nodegroup
        .and_then(|nodegroup| nodegroup.get("health"))
        .and_then(|health| health.get("issues"))
        .and_then(Value::as_array)
        .map(|issues| {
            issues
                .iter()
                .map(|issue| {
                    format!(
                        "{}: {}",
                        issue.get("code").and_then(Value::as_str).unwrap_or("?"),
                        issue.get("message").and_then(Value::as_str).unwrap_or("")
                    )
                })
                .collect()
        })
        .unwrap_or_default();
    Ok(NodegroupStatus {
        status,
        scaling: parse_scaling(body).ok(),
        issues,
    })
}

fn parse_scaling(body: &str) -> Result<NodegroupScaling, AwsClientError> {
    let value: Value =
        serde_json::from_str(body).map_err(|err| AwsClientError::Xml(err.to_string()))?;
    let scaling = value
        .get("nodegroup")
        .and_then(|nodegroup| nodegroup.get("scalingConfig"));
    let field = |name: &str| {
        scaling
            .and_then(|scaling| scaling.get(name))
            .and_then(Value::as_i64)
            .ok_or_else(|| AwsClientError::Xml(format!("nodegroup.scalingConfig.{name} missing")))
    };
    Ok(NodegroupScaling {
        min_size: field("minSize")?,
        max_size: field("maxSize")?,
        desired_size: field("desiredSize")?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_the_scaling_config() {
        let body = r#"{"nodegroup":{"nodegroupName":"workers-12",
            "scalingConfig":{"minSize":0,"maxSize":99,"desiredSize":2}}}"#;
        assert_eq!(
            parse_scaling(body).unwrap(),
            NodegroupScaling {
                min_size: 0,
                max_size: 99,
                desired_size: 2
            }
        );
    }

    #[test]
    fn an_arn_is_one_path_segment() {
        assert_eq!(
            percent_encode_segment("arn:aws:iam::1:role/a-b"),
            "arn%3Aaws%3Aiam%3A%3A1%3Arole%2Fa-b"
        );
    }

    #[test]
    fn parses_the_status_and_health_issues() {
        let body = r#"{"nodegroup":{"status":"DEGRADED",
            "scalingConfig":{"minSize":0,"maxSize":4,"desiredSize":1},
            "health":{"issues":[{"code":"AsgInstanceLaunchFailures","message":"kms"}]}}}"#;
        let status = parse_status(body).unwrap();
        assert_eq!(status.status, "DEGRADED");
        assert_eq!(status.scaling.unwrap().desired_size, 1);
        assert_eq!(
            status.issues,
            vec!["AsgInstanceLaunchFailures: kms".to_string()]
        );
    }

    #[test]
    fn a_missing_field_is_an_error() {
        parse_scaling(r#"{"nodegroup":{}}"#).unwrap_err();
    }
}
