use std::sync::{Arc, Mutex};

use aws_smithy_runtime_api::client::http::{
    HttpClient, HttpConnector, HttpConnectorFuture, HttpConnectorSettings, SharedHttpConnector,
};
use aws_smithy_runtime_api::client::orchestrator::{HttpRequest, HttpResponse};
use aws_smithy_runtime_api::client::runtime_components::RuntimeComponents;
use aws_smithy_runtime_api::http::StatusCode;
use aws_smithy_types::body::SdkBody;
use uuid::Uuid;

use super::*;
use crate::credential_provider::DestinationMatcher;

const READER_ROLE_ARN: &str = "arn:aws:iam::123456789012:role/space-secrets-reader";
const BASE_ACCESS_KEY: &str = "workload-identity";

fn secret_arn(region: &str) -> SecretArn {
    SecretArn::parse(format!(
        "arn:aws:secretsmanager:{region}:123456789012:secret:agents/openai-AbCdEf"
    ))
    .unwrap()
}

/// A request the fake AWS endpoint received.
#[derive(Clone, Debug)]
struct Seen {
    uri: String,
    form: Vec<(String, String)>,
    authorization: String,
}

impl Seen {
    fn is_sts(&self) -> bool {
        self.uri.contains("sts.")
    }

    fn param(&self, key: &str) -> Option<&str> {
        self.form
            .iter()
            .find(|(name, _)| name == key)
            .map(|(_, value)| value.as_str())
    }

    /// The access key in the SigV4 `Credential=<key>/<date>/<region>/...` scope.
    fn signing_key_and_region(&self) -> (String, String) {
        let scope = self
            .authorization
            .split("Credential=")
            .nth(1)
            .expect("signed request");
        let mut parts = scope.split('/');
        let key = parts.next().unwrap().to_owned();
        let region = parts.nth(1).unwrap().to_owned();
        (key, region)
    }
}

/// Answers STS AssumeRole with credentials named after the session and Secrets
/// Manager GetSecretValue with a fixed value, recording every request.
#[derive(Clone, Debug, Default)]
struct FakeAws(Arc<Mutex<Vec<Seen>>>);

impl FakeAws {
    fn seen(&self) -> Vec<Seen> {
        self.0.lock().unwrap().clone()
    }
}

fn decode_form(body: &str) -> Vec<(String, String)> {
    fn decode(component: &str) -> String {
        let bytes = component.as_bytes();
        let mut out = Vec::with_capacity(bytes.len());
        let mut index = 0;
        while index < bytes.len() {
            match bytes[index] {
                b'+' => out.push(b' '),
                b'%' => {
                    let hex = std::str::from_utf8(&bytes[index + 1..index + 3]).unwrap();
                    out.push(u8::from_str_radix(hex, 16).unwrap());
                    index += 2;
                }
                byte => out.push(byte),
            }
            index += 1;
        }
        String::from_utf8(out).unwrap()
    }
    body.split('&')
        .filter(|pair| !pair.is_empty())
        .map(|pair| {
            let (name, value) = pair.split_once('=').unwrap_or((pair, ""));
            (decode(name), decode(value))
        })
        .collect()
}

fn respond(content_type: &'static str, body: String) -> HttpResponse {
    let mut response = HttpResponse::new(StatusCode::try_from(200).unwrap(), SdkBody::from(body));
    response.headers_mut().insert("content-type", content_type);
    response
}

impl HttpConnector for FakeAws {
    fn call(&self, request: HttpRequest) -> HttpConnectorFuture {
        let body = request
            .body()
            .bytes()
            .map(|bytes| String::from_utf8_lossy(bytes).into_owned())
            .unwrap_or_default();
        let seen = Seen {
            uri: request.uri().to_owned(),
            form: decode_form(&body),
            authorization: request
                .headers()
                .get("authorization")
                .unwrap_or_default()
                .to_owned(),
        };
        let response = if seen.is_sts() {
            let session = seen.param("RoleSessionName").unwrap_or("none").to_owned();
            respond(
                "text/xml",
                format!(
                    "<AssumeRoleResponse xmlns=\"https://sts.amazonaws.com/doc/2011-06-15/\">\
                     <AssumeRoleResult><Credentials>\
                     <AccessKeyId>key-{session}</AccessKeyId>\
                     <SecretAccessKey>secret-{session}</SecretAccessKey>\
                     <SessionToken>token-{session}</SessionToken>\
                     <Expiration>2099-01-01T00:00:00Z</Expiration>\
                     </Credentials><AssumedRoleUser>\
                     <AssumedRoleId>AROAEXAMPLE:{session}</AssumedRoleId>\
                     <Arn>arn:aws:sts::123456789012:assumed-role/space-secrets-reader/{session}</Arn>\
                     </AssumedRoleUser></AssumeRoleResult>\
                     <ResponseMetadata><RequestId>request</RequestId></ResponseMetadata>\
                     </AssumeRoleResponse>"
                ),
            )
        } else {
            respond(
                "application/x-amz-json-1.1",
                r#"{"Name":"agents/openai","SecretString":"Bearer test-value","VersionId":"v1"}"#
                    .to_owned(),
            )
        };
        self.0.lock().unwrap().push(seen);
        HttpConnectorFuture::ready(Ok(response))
    }
}

impl HttpClient for FakeAws {
    fn http_connector(
        &self,
        _settings: &HttpConnectorSettings,
        _components: &RuntimeComponents,
    ) -> SharedHttpConnector {
        SharedHttpConnector::new(self.clone())
    }
}

async fn base_config(aws: FakeAws) -> aws_config::SdkConfig {
    aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region(aws_config::Region::new("us-east-1"))
        .credentials_provider(
            aws_credential_types::provider::SharedCredentialsProvider::new(
                aws_credential_types::Credentials::new(
                    BASE_ACCESS_KEY,
                    "workload-secret",
                    None,
                    None,
                    "aws-secret-resolver-test",
                ),
            ),
        )
        .http_client(aws)
        .retry_config(aws_config::retry::RetryConfig::disabled())
        .load()
        .await
}

async fn resolver(aws: &FakeAws) -> AwsSecretsManagerResolver {
    AwsSecretsManagerResolver::new(
        &base_config(aws.clone()).await,
        ReaderRoleArn::parse(READER_ROLE_ARN.to_owned()).unwrap(),
    )
}

async fn read(resolver: &AwsSecretsManagerResolver, organization_id: Uuid, region: &str) -> String {
    let destinations = [DestinationMatcher {
        exact_host: "api.example.com".to_string(),
        port: 443,
    }];
    let secret = resolver
        .prepare(ResolutionContext { organization_id }, &secret_arn(region))
        .await
        .unwrap()
        .resolve_for_destinations(&destinations)
        .await
        .unwrap();
    secret.into_value().to_string()
}

fn session_key(organization_id: Uuid) -> String {
    format!("key-space-org-{organization_id}")
}

#[tokio::test]
async fn reads_sign_with_a_session_scoped_to_the_requesting_organization() {
    let aws = FakeAws::default();
    let resolver = resolver(&aws).await;
    let organization = Uuid::now_v7();

    assert_eq!(
        read(&resolver, organization, "us-west-2").await,
        "Bearer test-value"
    );

    let seen = aws.seen();
    let (sts, reads): (Vec<_>, Vec<_>) = seen.iter().partition(|request| request.is_sts());
    let [assume] = sts.as_slice() else {
        panic!("expected one AssumeRole, saw {sts:?}");
    };
    assert_eq!(assume.param("Action"), Some("AssumeRole"));
    assert_eq!(assume.param("RoleArn"), Some(READER_ROLE_ARN));
    assert_eq!(
        assume.param("RoleSessionName"),
        Some(format!("space-org-{organization}").as_str())
    );
    assert_eq!(assume.param("Tags.member.1.Key"), Some("space-org-id"));
    assert_eq!(
        assume.param("Tags.member.1.Value"),
        Some(organization.to_string().as_str())
    );
    assert_eq!(assume.param("Tags.member.2.Key"), None);
    let policy: serde_json::Value =
        serde_json::from_str(assume.param("Policy").expect("session policy")).unwrap();
    assert_eq!(
        policy,
        serde_json::json!({
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": "secretsmanager:GetSecretValue",
                    "Resource": "*",
                    "Condition": {
                        "StringEquals": {
                            "secretsmanager:ResourceTag/space-org-id": organization.to_string()
                        }
                    }
                },
                {
                    // Customer-managed keys: decrypt only inside the Secrets
                    // Manager call the tag condition authorized.
                    "Effect": "Allow",
                    "Action": "kms:Decrypt",
                    "Resource": "*",
                    "Condition": {
                        "StringLike": { "kms:ViaService": "secretsmanager.*.amazonaws.com" }
                    }
                }
            ]
        })
    );
    // The workload identity signs only the AssumeRole; the secret read is
    // signed by the organization's session.
    assert_eq!(assume.signing_key_and_region().0, BASE_ACCESS_KEY);
    let [secret_read] = reads.as_slice() else {
        panic!("expected one GetSecretValue, saw {reads:?}");
    };
    assert_eq!(
        secret_read.signing_key_and_region(),
        (session_key(organization), "us-west-2".to_owned())
    );
}

#[tokio::test]
async fn sessions_are_per_organization_and_reused_across_regions() {
    let aws = FakeAws::default();
    let resolver = resolver(&aws).await;
    let (first, second) = (Uuid::now_v7(), Uuid::now_v7());
    let reads = [
        (first, "us-west-2"),
        (second, "us-west-2"),
        (first, "eu-west-1"),
        (second, "eu-west-1"),
        (first, "us-west-2"),
    ];

    for (organization, region) in reads {
        read(&resolver, organization, region).await;
    }

    let seen = aws.seen();
    let sessions: Vec<_> = seen
        .iter()
        .filter(|request| request.is_sts())
        .map(|request| request.param("RoleSessionName").unwrap().to_owned())
        .collect();
    assert_eq!(
        sessions,
        [format!("space-org-{first}"), format!("space-org-{second}")],
        "each organization assumes the role once"
    );
    let signers: Vec<_> = seen
        .iter()
        .filter(|request| !request.is_sts())
        .map(Seen::signing_key_and_region)
        .collect();
    let expected: Vec<_> = reads
        .iter()
        .map(|(organization, region)| (session_key(*organization), (*region).to_owned()))
        .collect();
    assert_eq!(signers, expected);
}

#[test]
fn reader_role_must_be_an_iam_role_arn() {
    assert_eq!(parse_reader_role_arn(None), Ok(None));
    assert_eq!(parse_reader_role_arn(Some(String::new())), Ok(None));
    assert_eq!(
        parse_reader_role_arn(Some(READER_ROLE_ARN.into())).map(|arn| arn.map(|arn| arn.0)),
        Ok(Some(READER_ROLE_ARN.to_owned()))
    );
    for invalid in [
        "space-secrets-reader",
        "arn:aws:iam::123456789012:role/",
        "arn:aws:iam::not-an-account:role/space-secrets-reader",
        "arn:aws:iam:us-east-1:123456789012:role/space-secrets-reader",
        "arn:aws:iam::123456789012:user/space",
        "arn:aws:secretsmanager:us-east-1:123456789012:secret:x",
    ] {
        assert_eq!(
            parse_reader_role_arn(Some(invalid.into())),
            Err(ReaderRoleArnError),
            "{invalid}"
        );
    }
}
