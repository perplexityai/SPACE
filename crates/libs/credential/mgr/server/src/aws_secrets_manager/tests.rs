use std::sync::{Arc, Mutex};

use aws_smithy_runtime_api::client::http::{
    HttpClient, HttpConnector, HttpConnectorFuture, HttpConnectorSettings, SharedHttpConnector,
};
use aws_smithy_runtime_api::client::orchestrator::HttpRequest;
use aws_smithy_runtime_api::client::result::ConnectorError;
use aws_smithy_runtime_api::client::runtime_components::RuntimeComponents;
use uuid::Uuid;

use super::*;
use crate::credential_provider::DestinationMatcher;

const SECRET_ARN: &str =
    "arn:aws:secretsmanager:us-west-2:123456789012:secret:agents/openai-AbCdEf";
const READER_ROLE_ARN: &str = "arn:aws:iam::123456789012:role/space-secrets-reader";
const ENCODED_READER_ROLE_ARN: &str =
    "arn%3Aaws%3Aiam%3A%3A123456789012%3Arole%2Fspace-secrets-reader";

/// Records every outgoing request as `(uri, body)` and fails it, so nothing
/// leaves the process.
#[derive(Clone, Debug, Default)]
struct CapturingHttp(Arc<Mutex<Vec<(String, String)>>>);

impl CapturingHttp {
    fn requests(&self) -> Vec<(String, String)> {
        self.0.lock().unwrap().clone()
    }
}

impl HttpConnector for CapturingHttp {
    fn call(&self, request: HttpRequest) -> HttpConnectorFuture {
        let body = request
            .body()
            .bytes()
            .map(|bytes| String::from_utf8_lossy(bytes).into_owned())
            .unwrap_or_default();
        self.0
            .lock()
            .unwrap()
            .push((request.uri().to_owned(), body));
        HttpConnectorFuture::ready(Err(ConnectorError::other(
            "request captured by the test".into(),
            None,
        )))
    }
}

impl HttpClient for CapturingHttp {
    fn http_connector(
        &self,
        _settings: &HttpConnectorSettings,
        _components: &RuntimeComponents,
    ) -> SharedHttpConnector {
        SharedHttpConnector::new(self.clone())
    }
}

async fn base_config(http: CapturingHttp) -> aws_config::SdkConfig {
    aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region(aws_config::Region::new("us-east-1"))
        .credentials_provider(
            aws_credential_types::provider::SharedCredentialsProvider::new(
                aws_credential_types::Credentials::new(
                    "test",
                    "test",
                    None,
                    None,
                    "aws-secret-resolver-test",
                ),
            ),
        )
        .http_client(http)
        .retry_config(aws_config::retry::RetryConfig::disabled())
        .load()
        .await
}

fn destinations() -> [DestinationMatcher; 1] {
    [DestinationMatcher {
        exact_host: "api.example.com".to_string(),
        port: 443,
    }]
}

async fn read_as(resolver: &AwsSecretsManagerResolver, organization_id: Uuid) -> ResolutionError {
    resolver
        .prepare(
            ResolutionContext { organization_id },
            &SecretArn::parse(SECRET_ARN).unwrap(),
        )
        .await
        .unwrap()
        .resolve_for_destinations(&destinations())
        .await
        .expect_err("the capturing transport fails every request")
}

#[tokio::test]
async fn reads_assume_the_reader_role_tagged_with_the_requesting_organization() {
    let http = CapturingHttp::default();
    let resolver =
        AwsSecretsManagerResolver::new(&base_config(http.clone()).await, READER_ROLE_ARN.into());
    let organization = Uuid::now_v7();

    assert_eq!(
        read_as(&resolver, organization).await,
        ResolutionError::ProviderUnavailable
    );

    let requests = http.requests();
    assert!(!requests.is_empty());
    for (uri, body) in &requests {
        // The secret read waits on the organization session; the workload
        // identity never calls Secrets Manager itself.
        assert!(uri.contains("sts."), "unexpected request to {uri}");
        assert!(body.contains("Action=AssumeRole"), "{body}");
        assert!(
            body.contains(&format!("RoleArn={ENCODED_READER_ROLE_ARN}")),
            "{body}"
        );
        assert!(
            body.contains(&format!("RoleSessionName=space-org-{organization}")),
            "{body}"
        );
        assert!(body.contains("Tags.member.1.Key=space-org-id"), "{body}");
        assert!(
            body.contains(&format!("Tags.member.1.Value={organization}")),
            "{body}"
        );
        assert!(!body.contains("Tags.member.2"), "{body}");
    }
}

#[tokio::test]
async fn each_organization_reads_through_its_own_session() {
    let http = CapturingHttp::default();
    let resolver =
        AwsSecretsManagerResolver::new(&base_config(http.clone()).await, READER_ROLE_ARN.into());
    let (first, second) = (Uuid::now_v7(), Uuid::now_v7());

    read_as(&resolver, first).await;
    read_as(&resolver, second).await;
    read_as(&resolver, first).await;

    let tagged_with = |organization: Uuid| {
        let tag = format!("Tags.member.1.Value={organization}");
        http.requests()
            .iter()
            .filter(|(_, body)| body.contains(&tag))
            .count()
    };
    assert!(tagged_with(first) > 0 && tagged_with(second) > 0);
    assert_eq!(
        tagged_with(first) + tagged_with(second),
        http.requests().len()
    );
    assert_eq!(resolver.cached_organizations(), 2);
}

#[tokio::test]
async fn reads_use_the_secret_arn_region() {
    let config = base_config(CapturingHttp::default()).await;
    assert_eq!(
        build_client(&config, "us-west-2")
            .config()
            .region()
            .expect("regional client")
            .as_ref(),
        "us-west-2",
    );
}

#[test]
fn reader_role_must_be_an_iam_role_arn() {
    assert_eq!(parse_reader_role_arn(None), Ok(None));
    assert_eq!(parse_reader_role_arn(Some(String::new())), Ok(None));
    assert_eq!(
        parse_reader_role_arn(Some(READER_ROLE_ARN.into())),
        Ok(Some(READER_ROLE_ARN.to_string()))
    );
    for invalid in [
        "space-secrets-reader",
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
