//! The two EKS managed-node-group calls SPACE makes: read a node group's
//! scaling config, and set its desired size.

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
    fn a_missing_field_is_an_error() {
        parse_scaling(r#"{"nodegroup":{}}"#).unwrap_err();
    }
}
