//! Caller identity for account-fenced resource controllers.

use aws_smithy_xml::decode::{Document, try_data};
use aws_types::SdkConfig;

use crate::query::{AwsClientError, AwsQueryClient, AwsQueryService};

pub struct Client {
    query: AwsQueryClient,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CallerIdentity {
    pub account: String,
    pub arn: String,
}

impl Client {
    pub fn from_config(config: &SdkConfig) -> Self {
        Self {
            query: AwsQueryClient::from_config(AwsQueryService::Sts, config),
        }
    }

    pub async fn get_caller_identity(&self) -> Result<CallerIdentity, AwsClientError> {
        let body = self
            .query
            .send_action("GetCallerIdentity", "2011-06-15", vec![])
            .await?;
        parse_identity(&body)
    }
}

fn parse_identity(body: &str) -> Result<CallerIdentity, AwsClientError> {
    let mut document = Document::new(body);
    let mut root = document.root_element()?;
    if root.start_el().local() != "GetCallerIdentityResponse" {
        return Err(AwsClientError::Xml(
            "unexpected STS response document".into(),
        ));
    }
    let mut account = None;
    let mut arn = None;
    while let Some(mut result) = root.next_tag() {
        if result.start_el().local() != "GetCallerIdentityResult" {
            continue;
        }
        while let Some(mut field) = result.next_tag() {
            match field.start_el().local() {
                "Account" if account.is_none() => {
                    account = Some(try_data(&mut field)?.into_owned())
                }
                "Arn" if arn.is_none() => arn = Some(try_data(&mut field)?.into_owned()),
                "Account" | "Arn" => {
                    return Err(AwsClientError::Xml("duplicate STS identity field".into()));
                }
                _ => {}
            }
        }
    }
    let account = account
        .filter(|value| value.len() == 12 && value.bytes().all(|byte| byte.is_ascii_digit()))
        .ok_or_else(|| AwsClientError::Xml("missing or invalid STS account".into()))?;
    let arn = arn
        .filter(|value| {
            value.starts_with("arn:") && value.split(':').nth(4) == Some(account.as_str())
        })
        .ok_or_else(|| AwsClientError::Xml("missing or inconsistent STS caller ARN".into()))?;
    Ok(CallerIdentity { account, arn })
}

#[cfg(test)]
mod tests {
    use super::*;

    const RESPONSE: &str = r#"<GetCallerIdentityResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/">
      <GetCallerIdentityResult><Arn>arn:aws:sts::123456789012:assumed-role/test/run</Arn>
      <UserId>example</UserId><Account>123456789012</Account></GetCallerIdentityResult>
      <ResponseMetadata><RequestId>example</RequestId></ResponseMetadata></GetCallerIdentityResponse>"#;

    #[test]
    fn identity_requires_a_complete_consistent_account_and_caller() {
        let identity = parse_identity(RESPONSE).unwrap();
        assert_eq!(identity.account, "123456789012");
        assert_eq!(
            identity.arn,
            "arn:aws:sts::123456789012:assumed-role/test/run"
        );
        parse_identity(&RESPONSE.replace("<Account>123456789012</Account>", "")).unwrap_err();
        parse_identity(&RESPONSE.replace(
            "<Account>123456789012</Account>",
            "<Account>234567890123</Account>",
        ))
        .unwrap_err();
        parse_identity(
            &RESPONSE.replace("</Account>", "</Account><Account>123456789012</Account>"),
        )
        .unwrap_err();
        parse_identity("<UnexpectedResponse/>").unwrap_err();
    }
}
