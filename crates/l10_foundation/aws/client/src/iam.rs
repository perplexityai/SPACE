//! The IAM calls the cluster controller makes to own a per-tenant node role:
//! create it under a permissions boundary, attach its policies, and delete it.

use aws_smithy_xml::decode::{Document, try_data};
use aws_types::SdkConfig;

use crate::query::{AwsClientError, AwsQueryClient, AwsQueryService, ReqwestTransport};

const IAM_API_VERSION: &str = "2010-05-08";

/// What a node role is created with.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewRole {
    pub name: String,
    pub path: String,
    pub assume_role_policy: String,
    pub permissions_boundary_arn: Option<String>,
    pub tags: Vec<(String, String)>,
}

#[derive(Debug, Clone)]
pub struct Client {
    query: AwsQueryClient<ReqwestTransport>,
}

fn is_code(err: &AwsClientError, code: &str) -> bool {
    err.service_code() == Some(code)
}

impl Client {
    pub fn from_config(config: &SdkConfig) -> Self {
        Self {
            query: AwsQueryClient::from_config(AwsQueryService::Iam, config),
        }
    }

    /// `GetRole`: the role's ARN, or `None` when it doesn't exist.
    pub async fn role_arn(&self, name: &str) -> Result<Option<String>, AwsClientError> {
        match self
            .query
            .send_action(
                "GetRole",
                IAM_API_VERSION,
                vec![("RoleName", name.to_string())],
            )
            .await
        {
            Ok(body) => parse_role_arn(&body).map(Some),
            Err(err) if is_code(&err, "NoSuchEntity") => Ok(None),
            Err(err) => Err(err),
        }
    }

    /// `CreateRole`. Returns the role's ARN; an existing role is not an error.
    pub async fn create_role(&self, role: &NewRole) -> Result<String, AwsClientError> {
        let mut params = vec![
            ("RoleName".to_string(), role.name.clone()),
            ("Path".to_string(), role.path.clone()),
            (
                "AssumeRolePolicyDocument".to_string(),
                role.assume_role_policy.clone(),
            ),
        ];
        if let Some(boundary) = &role.permissions_boundary_arn {
            params.push(("PermissionsBoundary".to_string(), boundary.clone()));
        }
        for (index, (key, value)) in role.tags.iter().enumerate() {
            params.push((format!("Tags.member.{}.Key", index + 1), key.clone()));
            params.push((format!("Tags.member.{}.Value", index + 1), value.clone()));
        }
        match self
            .query
            .send_action("CreateRole", IAM_API_VERSION, params)
            .await
        {
            Ok(body) => parse_role_arn(&body),
            Err(err) if is_code(&err, "EntityAlreadyExists") => self
                .role_arn(&role.name)
                .await?
                .ok_or_else(|| AwsClientError::Xml(format!("role {} vanished", role.name))),
            Err(err) => Err(err),
        }
    }

    /// `AttachRolePolicy` (idempotent).
    pub async fn attach_role_policy(
        &self,
        role: &str,
        policy_arn: &str,
    ) -> Result<(), AwsClientError> {
        self.query
            .send_action(
                "AttachRolePolicy",
                IAM_API_VERSION,
                vec![
                    ("RoleName", role.to_string()),
                    ("PolicyArn", policy_arn.to_string()),
                ],
            )
            .await?;
        Ok(())
    }

    /// `PutRolePolicy` (idempotent: replaces a policy of the same name).
    pub async fn put_role_policy(
        &self,
        role: &str,
        policy_name: &str,
        document: &str,
    ) -> Result<(), AwsClientError> {
        self.query
            .send_action(
                "PutRolePolicy",
                IAM_API_VERSION,
                vec![
                    ("RoleName", role.to_string()),
                    ("PolicyName", policy_name.to_string()),
                    ("PolicyDocument", document.to_string()),
                ],
            )
            .await?;
        Ok(())
    }

    /// Detach the given managed policies, delete the inline policy and the
    /// role. Anything already gone is skipped.
    pub async fn delete_role(
        &self,
        role: &str,
        managed_policy_arns: &[String],
        inline_policy_name: &str,
    ) -> Result<(), AwsClientError> {
        let gone = |err: &AwsClientError| is_code(err, "NoSuchEntity");
        for policy_arn in managed_policy_arns {
            if let Err(err) = self
                .query
                .send_action(
                    "DetachRolePolicy",
                    IAM_API_VERSION,
                    vec![
                        ("RoleName", role.to_string()),
                        ("PolicyArn", policy_arn.clone()),
                    ],
                )
                .await
                && !gone(&err)
            {
                return Err(err);
            }
        }
        if let Err(err) = self
            .query
            .send_action(
                "DeleteRolePolicy",
                IAM_API_VERSION,
                vec![
                    ("RoleName", role.to_string()),
                    ("PolicyName", inline_policy_name.to_string()),
                ],
            )
            .await
            && !gone(&err)
        {
            return Err(err);
        }
        match self
            .query
            .send_action(
                "DeleteRole",
                IAM_API_VERSION,
                vec![("RoleName", role.to_string())],
            )
            .await
        {
            Ok(_) => Ok(()),
            Err(err) if gone(&err) => Ok(()),
            Err(err) => Err(err),
        }
    }
}

/// `<…Result><Role><Arn>…</Arn>` of `GetRole` and `CreateRole`.
fn parse_role_arn(body: &str) -> Result<String, AwsClientError> {
    let mut document = Document::new(body);
    let mut root = document.root_element()?;
    while let Some(mut result) = root.next_tag() {
        if !result.start_el().local().ends_with("Result") {
            continue;
        }
        while let Some(mut role) = result.next_tag() {
            if !role.start_el().matches("Role") {
                continue;
            }
            while let Some(mut field) = role.next_tag() {
                if field.start_el().matches("Arn") {
                    return Ok(try_data(&mut field)?.into_owned());
                }
            }
        }
    }
    Err(AwsClientError::Xml("Role.Arn missing".to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_the_role_arn() {
        let body = r#"<GetRoleResponse xmlns="https://iam.amazonaws.com/doc/2010-05-08/">
            <GetRoleResult><Role><Path>/</Path><RoleName>r</RoleName>
            <Arn>arn:aws:iam::1:role/r</Arn></Role></GetRoleResult>
            <ResponseMetadata><RequestId>x</RequestId></ResponseMetadata>
            </GetRoleResponse>"#;
        assert_eq!(parse_role_arn(body).unwrap(), "arn:aws:iam::1:role/r");
    }
}
