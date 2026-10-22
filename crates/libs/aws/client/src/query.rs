use std::future::Future;
use std::pin::Pin;
use std::time::{Duration, SystemTime};

use aws_credential_types::provider::{ProvideCredentials, SharedCredentialsProvider};
use aws_sigv4::http_request::{
    SignableBody, SignableRequest, SigningSettings, sign as sign_http_request,
};
use aws_sigv4::sign::v4;
use aws_smithy_runtime_api::client::identity::Identity;
use aws_smithy_xml::decode::{Document, ScopedDecoder, XmlDecodeError, try_data};
use aws_types::SdkConfig;
use aws_types::service_config::ServiceConfigKey;
use http::{Request, StatusCode, header};

#[derive(Debug, thiserror::Error)]
pub enum AwsClientError {
    #[error("AWS region is not configured")]
    MissingRegion,
    #[error("AWS credentials provider is not configured")]
    MissingCredentialsProvider,
    #[error("AWS credentials unavailable: {0}")]
    Credentials(String),
    #[error("failed to sign AWS request: {0}")]
    Signing(#[from] aws_sigv4::http_request::SigningError),
    #[error("failed to build AWS HTTP request: {0}")]
    HttpBuild(#[from] http::Error),
    #[error("AWS HTTP request failed: {0}")]
    Http(#[from] reqwest::Error),
    #[error("failed to convert AWS HTTP header to UTF-8: {0}")]
    Header(String),
    #[error("AWS {service} returned {code}: {message} (HTTP {status})")]
    AwsService {
        service: &'static str,
        status: StatusCode,
        code: String,
        message: String,
    },
    #[error("failed to parse AWS XML response: {0}")]
    Xml(String),
}

impl From<XmlDecodeError> for AwsClientError {
    fn from(value: XmlDecodeError) -> Self {
        Self::Xml(value.to_string())
    }
}

#[derive(Debug, Clone)]
pub struct QueryResponse {
    pub status: StatusCode,
    pub body: String,
}

impl QueryResponse {
    pub fn new(status: StatusCode, body: impl Into<String>) -> Self {
        Self {
            status,
            body: body.into(),
        }
    }
}

pub trait QueryTransport: Clone + Send + Sync + 'static {
    fn send(
        &self,
        request: Request<Vec<u8>>,
    ) -> Pin<Box<dyn Future<Output = Result<QueryResponse, AwsClientError>> + Send + '_>>;
}

#[derive(Debug, Clone)]
pub struct ReqwestTransport {
    client: reqwest::Client,
}

const AWS_CONNECT_TIMEOUT: Duration = Duration::from_millis(3100);
const AWS_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

impl ReqwestTransport {
    fn with_timeouts(connect_timeout: Duration, request_timeout: Duration) -> Self {
        let client = reqwest::Client::builder()
            .connect_timeout(connect_timeout)
            .timeout(request_timeout)
            .build()
            .expect("AWS reqwest client timeout configuration is valid");
        Self { client }
    }
}

impl Default for ReqwestTransport {
    fn default() -> Self {
        Self::with_timeouts(AWS_CONNECT_TIMEOUT, AWS_REQUEST_TIMEOUT)
    }
}

impl QueryTransport for ReqwestTransport {
    fn send(
        &self,
        request: Request<Vec<u8>>,
    ) -> Pin<Box<dyn Future<Output = Result<QueryResponse, AwsClientError>> + Send + '_>> {
        Box::pin(async move {
            let (parts, body) = request.into_parts();
            let mut builder = self
                .client
                .request(reqwest::Method::POST, parts.uri.to_string());
            for (name, value) in &parts.headers {
                builder = builder.header(name.as_str(), value.as_bytes());
            }
            let response = builder.body(body).send().await?;
            let status = StatusCode::from_u16(response.status().as_u16())
                .unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
            let body = response.text().await?;
            Ok(QueryResponse::new(status, body))
        })
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum AwsQueryService {
    Ec2,
    AutoScaling,
}

impl AwsQueryService {
    fn signing_name(self) -> &'static str {
        match self {
            Self::Ec2 => "ec2",
            Self::AutoScaling => "autoscaling",
        }
    }

    fn endpoint_prefix(self) -> &'static str {
        self.signing_name()
    }

    /// The SDK service ID, which names the service's own endpoint setting
    /// (`AWS_ENDPOINT_URL_EC2`, `AWS_ENDPOINT_URL_AUTO_SCALING`).
    fn service_id(self) -> &'static str {
        match self {
            Self::Ec2 => "EC2",
            Self::AutoScaling => "Auto Scaling",
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct AwsQueryClient<T = ReqwestTransport> {
    service: AwsQueryService,
    region: Option<String>,
    endpoint_url: Option<String>,
    credentials_provider: Option<SharedCredentialsProvider>,
    transport: T,
}

impl AwsQueryClient<ReqwestTransport> {
    pub(crate) fn from_config(service: AwsQueryService, config: &SdkConfig) -> Self {
        Self {
            service,
            region: config.region().map(|region| region.as_ref().to_string()),
            endpoint_url: configured_endpoint_url(service, config),
            credentials_provider: config.credentials_provider(),
            transport: ReqwestTransport::default(),
        }
    }
}

/// The endpoint override, resolved as the generated AWS SDK clients resolve
/// theirs: an endpoint set in code on the `SdkConfig` wins. Otherwise
/// aws-config's service config answers, in its order: the service's
/// `AWS_ENDPOINT_URL_<SERVICE>`, the global `AWS_ENDPOINT_URL`, `endpoint_url`
/// in the profile's `services` section for the service, then the profile's own
/// `endpoint_url`. The `SdkConfig`'s endpoint is the fallback when that yields
/// nothing.
fn configured_endpoint_url(service: AwsQueryService, config: &SdkConfig) -> Option<String> {
    if config.get_origin("endpoint_url").is_client_config() {
        return config.endpoint_url().map(str::to_string);
    }
    config
        .service_config()
        .and_then(|service_config| {
            service_config.load_config(
                ServiceConfigKey::builder()
                    .service_id(service.service_id())
                    .env("AWS_ENDPOINT_URL")
                    .profile("endpoint_url")
                    .build()
                    .expect("every service config key field is set"),
            )
        })
        .or_else(|| config.endpoint_url().map(str::to_string))
}

impl<T: QueryTransport> AwsQueryClient<T> {
    #[cfg(test)]
    pub(crate) fn with_transport(
        service: AwsQueryService,
        region: impl Into<String>,
        credentials_provider: SharedCredentialsProvider,
        transport: T,
    ) -> Self {
        Self {
            service,
            region: Some(region.into()),
            endpoint_url: None,
            credentials_provider: Some(credentials_provider),
            transport,
        }
    }

    pub(crate) async fn send_action(
        &self,
        action: &'static str,
        version: &'static str,
        params: Vec<(&'static str, String)>,
    ) -> Result<String, AwsClientError> {
        let mut all_params = Vec::with_capacity(params.len() + 2);
        all_params.push(("Action", action.to_string()));
        all_params.push(("Version", version.to_string()));
        all_params.extend(params);

        let body = encode_form(&all_params);
        let request = self.sign_request(body).await?;
        let response = self.transport.send(request).await?;
        if !response.status.is_success() {
            return Err(service_error(self.service, response.status, &response.body));
        }
        Ok(response.body)
    }

    async fn sign_request(&self, body: String) -> Result<Request<Vec<u8>>, AwsClientError> {
        let endpoint = self.endpoint()?;
        let bytes = body.into_bytes();
        let mut request = Request::builder()
            .method("POST")
            .uri(endpoint.as_str())
            .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
            .body(bytes)?;

        let credentials_provider = self
            .credentials_provider
            .as_ref()
            .ok_or(AwsClientError::MissingCredentialsProvider)?;
        let credentials = credentials_provider
            .provide_credentials()
            .await
            .map_err(|err| AwsClientError::Credentials(err.to_string()))?;
        let identity: Identity = credentials.into();
        let region = self
            .region
            .as_deref()
            .ok_or(AwsClientError::MissingRegion)?;
        let settings = SigningSettings::default();
        let signing_params = v4::SigningParams::builder()
            .identity(&identity)
            .region(region)
            .name(self.service.signing_name())
            .time(SystemTime::now())
            .settings(settings)
            .build()
            .expect("all required signing parameters are set")
            .into();
        let header_pairs = request
            .headers()
            .iter()
            .map(|(name, value)| {
                Ok::<_, AwsClientError>((
                    name.as_str(),
                    value
                        .to_str()
                        .map_err(|err| AwsClientError::Header(err.to_string()))?,
                ))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let signable_request = SignableRequest::new(
            "POST",
            endpoint,
            header_pairs.into_iter(),
            SignableBody::Bytes(request.body()),
        )?;
        let (instructions, _signature) =
            sign_http_request(signable_request, &signing_params)?.into_parts();
        instructions.apply_to_request_http1x(&mut request);
        Ok(request)
    }

    fn endpoint(&self) -> Result<String, AwsClientError> {
        if let Some(endpoint_url) = &self.endpoint_url {
            return Ok(format!("{}/", endpoint_url.trim_end_matches('/')));
        }

        let region = self
            .region
            .as_deref()
            .ok_or(AwsClientError::MissingRegion)?;
        Ok(format!(
            "https://{}.{}.amazonaws.com/",
            self.service.endpoint_prefix(),
            region
        ))
    }
}

fn encode_form(params: &[(&str, String)]) -> String {
    let mut serializer = form_urlencoded::Serializer::new(String::new());
    for (name, value) in params {
        serializer.append_pair(name, value);
    }
    serializer.finish()
}

fn service_error(service: AwsQueryService, status: StatusCode, body: &str) -> AwsClientError {
    let (code, message) = parse_error_body(body).unwrap_or_else(|| {
        let excerpt: String = body.chars().take(512).collect();
        ("Unknown".to_string(), excerpt)
    });
    AwsClientError::AwsService {
        service: service.signing_name(),
        status,
        code,
        message,
    }
}

fn parse_error_body(body: &str) -> Option<(String, String)> {
    let mut document = Document::new(body);
    let mut root = document.root_element().ok()?;
    let mut code = None;
    let mut message = None;
    collect_error_fields(&mut root, &mut code, &mut message).ok()?;
    Some((
        code.unwrap_or_else(|| "Unknown".to_string()),
        message.unwrap_or_else(|| "AWS returned an error without a message".to_string()),
    ))
}

fn collect_error_fields(
    decoder: &mut ScopedDecoder<'_, '_>,
    code: &mut Option<String>,
    message: &mut Option<String>,
) -> Result<(), XmlDecodeError> {
    while let Some(mut tag) = decoder.next_tag() {
        match tag.start_el().local() {
            "Code" if code.is_none() => *code = Some(try_data(&mut tag)?.into_owned()),
            "Message" if message.is_none() => *message = Some(try_data(&mut tag)?.into_owned()),
            _ => collect_error_fields(&mut tag, code, message)?,
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests;

#[cfg(test)]
pub(crate) mod test_support {
    use std::collections::{HashMap, VecDeque};
    use std::sync::{Arc, Mutex};

    use aws_credential_types::Credentials;
    use aws_credential_types::provider::SharedCredentialsProvider;

    use super::*;

    #[derive(Debug, Clone, PartialEq, Eq)]
    pub(crate) struct RecordedRequest {
        pub(crate) uri: String,
        pub(crate) headers: Vec<(String, String)>,
        pub(crate) body: String,
    }

    #[derive(Debug, Clone)]
    pub(crate) struct RecordingTransport {
        requests: Arc<Mutex<Vec<RecordedRequest>>>,
        responses: Arc<Mutex<VecDeque<QueryResponse>>>,
    }

    impl RecordingTransport {
        pub(crate) fn new(responses: Vec<QueryResponse>) -> Self {
            Self {
                requests: Arc::new(Mutex::new(Vec::new())),
                responses: Arc::new(Mutex::new(VecDeque::from(responses))),
            }
        }

        pub(crate) fn requests(&self) -> Vec<RecordedRequest> {
            self.requests.lock().unwrap().clone()
        }
    }

    impl QueryTransport for RecordingTransport {
        fn send(
            &self,
            request: Request<Vec<u8>>,
        ) -> Pin<Box<dyn Future<Output = Result<QueryResponse, AwsClientError>> + Send + '_>>
        {
            let recorded = RecordedRequest {
                uri: request.uri().to_string(),
                headers: request
                    .headers()
                    .iter()
                    .map(|(name, value)| {
                        (
                            name.as_str().to_string(),
                            value.to_str().unwrap_or("<non-utf8>").to_string(),
                        )
                    })
                    .collect(),
                body: String::from_utf8(request.body().clone()).unwrap(),
            };
            self.requests.lock().unwrap().push(recorded);
            let response = self
                .responses
                .lock()
                .unwrap()
                .pop_front()
                .expect("fake AWS response queue is empty");
            Box::pin(async move { Ok(response) })
        }
    }

    pub(crate) fn credentials_provider() -> SharedCredentialsProvider {
        SharedCredentialsProvider::new(Credentials::new(
            "AKIDEXAMPLE",
            "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
            None,
            None,
            "test",
        ))
    }

    pub(crate) fn parse_body(body: &str) -> HashMap<String, String> {
        form_urlencoded::parse(body.as_bytes())
            .map(|(key, value)| (key.into_owned(), value.into_owned()))
            .collect()
    }
}
