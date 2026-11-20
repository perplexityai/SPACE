use aws_smithy_xml::decode::{Document, ScopedDecoder, XmlDecodeError, try_data};
use aws_types::SdkConfig;

use crate::query::{
    AwsClientError, AwsQueryClient, AwsQueryService, QueryTransport, ReqwestTransport,
};

const DESCRIBE_INSTANCES_ACTION: &str = "DescribeInstances";
const EC2_API_VERSION: &str = "2016-11-15";
const DESCRIBE_PAGE_SIZE: &str = "1000";
const CREATE_LAUNCH_TEMPLATE_VERSION_ACTION: &str = "CreateLaunchTemplateVersion";

/// A launch template version derived from an existing one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LaunchTemplateVersionRequest {
    pub launch_template_id: String,
    pub source_version: String,
    /// Idempotency token (≤ 64 chars): the same token with the same request
    /// returns the version it created before.
    pub client_token: String,
    pub description: String,
    /// Root volume, encrypted under this key.
    pub root_device_name: String,
    pub root_volume_gib: u32,
    pub root_volume_kms_key_id: String,
    /// Replace the source's tag specifications: these go on the instance and
    /// its volumes.
    pub tags: Vec<(String, String)>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Instance {
    pub instance_id: String,
    pub state: String,
}

#[derive(Debug, Clone)]
pub struct Client<T = ReqwestTransport> {
    query: AwsQueryClient<T>,
}

impl Client<ReqwestTransport> {
    pub fn from_config(config: &SdkConfig) -> Self {
        Self {
            query: AwsQueryClient::from_config(AwsQueryService::Ec2, config),
        }
    }
}

impl<T: QueryTransport> Client<T> {
    #[cfg(test)]
    pub(crate) fn with_transport(
        region: impl Into<String>,
        credentials_provider: aws_credential_types::provider::SharedCredentialsProvider,
        transport: T,
    ) -> Self {
        Self {
            query: AwsQueryClient::with_transport(
                AwsQueryService::Ec2,
                region,
                credentials_provider,
                transport,
            ),
        }
    }

    pub async fn describe_instances_by_private_dns(
        &self,
        private_dns_name: &str,
    ) -> Result<Vec<Instance>, AwsClientError> {
        let mut instances = Vec::new();
        let mut next_token = None;

        loop {
            let page = self
                .describe_instances_page(private_dns_name, next_token.as_deref())
                .await?;
            instances.extend(page.instances);
            let Some(token) = page.next_token else {
                break;
            };
            next_token = Some(token);
        }

        Ok(instances)
    }

    async fn describe_instances_page(
        &self,
        private_dns_name: &str,
        next_token: Option<&str>,
    ) -> Result<DescribeInstancesPage, AwsClientError> {
        let mut params = vec![
            ("Filter.1.Name", "private-dns-name".to_string()),
            ("Filter.1.Value.1", private_dns_name.to_string()),
            ("MaxResults", DESCRIBE_PAGE_SIZE.to_string()),
        ];
        if let Some(next_token) = next_token {
            params.push(("NextToken", next_token.to_string()));
        }

        let body = self
            .query
            .send_action(DESCRIBE_INSTANCES_ACTION, EC2_API_VERSION, params)
            .await?;
        parse_describe_instances_response(&body).map_err(AwsClientError::from)
    }
}

impl<T: QueryTransport> Client<T> {
    /// `CreateLaunchTemplateVersion` from `source_version`. Returns the new
    /// version number.
    pub async fn create_launch_template_version(
        &self,
        request: &LaunchTemplateVersionRequest,
    ) -> Result<i64, AwsClientError> {
        let body = self
            .query
            .send_action(
                CREATE_LAUNCH_TEMPLATE_VERSION_ACTION,
                EC2_API_VERSION,
                launch_template_version_params(request),
            )
            .await?;
        parse_version_number(&body)
    }
}

fn launch_template_version_params(request: &LaunchTemplateVersionRequest) -> Vec<(String, String)> {
    let mut params = vec![
        (
            "LaunchTemplateId".to_string(),
            request.launch_template_id.clone(),
        ),
        ("SourceVersion".to_string(), request.source_version.clone()),
        ("ClientToken".to_string(), request.client_token.clone()),
        (
            "VersionDescription".to_string(),
            request.description.clone(),
        ),
    ];
    let bdm = "LaunchTemplateData.BlockDeviceMapping.1";
    params.extend([
        (
            format!("{bdm}.DeviceName"),
            request.root_device_name.clone(),
        ),
        (format!("{bdm}.Ebs.VolumeType"), "gp3".to_string()),
        (
            format!("{bdm}.Ebs.VolumeSize"),
            request.root_volume_gib.to_string(),
        ),
        (format!("{bdm}.Ebs.Encrypted"), "true".to_string()),
        (
            format!("{bdm}.Ebs.KmsKeyId"),
            request.root_volume_kms_key_id.clone(),
        ),
        (format!("{bdm}.Ebs.DeleteOnTermination"), "true".to_string()),
    ]);
    for (spec_index, resource_type) in ["instance", "volume"].into_iter().enumerate() {
        let spec = format!("LaunchTemplateData.TagSpecification.{}", spec_index + 1);
        params.push((format!("{spec}.ResourceType"), resource_type.to_string()));
        for (tag_index, (key, value)) in request.tags.iter().enumerate() {
            params.push((format!("{spec}.Tag.{}.Key", tag_index + 1), key.clone()));
            params.push((format!("{spec}.Tag.{}.Value", tag_index + 1), value.clone()));
        }
    }
    params
}

fn parse_version_number(body: &str) -> Result<i64, AwsClientError> {
    let mut document = Document::new(body);
    let mut root = document.root_element()?;
    while let Some(mut tag) = root.next_tag() {
        if !tag.start_el().matches("launchTemplateVersion") {
            continue;
        }
        while let Some(mut field) = tag.next_tag() {
            if field.start_el().matches("versionNumber") {
                let text = try_data(&mut field)?;
                return text
                    .trim()
                    .parse()
                    .map_err(|_| AwsClientError::Xml(format!("bad versionNumber {text:?}")));
            }
        }
    }
    Err(AwsClientError::Xml(
        "launchTemplateVersion.versionNumber missing".to_string(),
    ))
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct DescribeInstancesPage {
    instances: Vec<Instance>,
    next_token: Option<String>,
}

fn parse_describe_instances_response(body: &str) -> Result<DescribeInstancesPage, XmlDecodeError> {
    let mut document = Document::new(body);
    let mut root = document.root_element()?;
    let mut page = DescribeInstancesPage::default();

    while let Some(mut tag) = root.next_tag() {
        match tag.start_el().local() {
            "reservationSet" => parse_reservation_set(&mut tag, &mut page.instances)?,
            "nextToken" => {
                let token = try_data(&mut tag)?.into_owned();
                if !token.is_empty() {
                    page.next_token = Some(token);
                }
            }
            _ => {}
        }
    }

    Ok(page)
}

fn parse_reservation_set(
    decoder: &mut ScopedDecoder<'_, '_>,
    instances: &mut Vec<Instance>,
) -> Result<(), XmlDecodeError> {
    while let Some(mut reservation) = decoder.next_tag() {
        if reservation.start_el().matches("item") {
            parse_reservation(&mut reservation, instances)?;
        }
    }
    Ok(())
}

fn parse_reservation(
    decoder: &mut ScopedDecoder<'_, '_>,
    instances: &mut Vec<Instance>,
) -> Result<(), XmlDecodeError> {
    while let Some(mut tag) = decoder.next_tag() {
        if tag.start_el().matches("instancesSet") {
            parse_instances_set(&mut tag, instances)?;
        }
    }
    Ok(())
}

fn parse_instances_set(
    decoder: &mut ScopedDecoder<'_, '_>,
    instances: &mut Vec<Instance>,
) -> Result<(), XmlDecodeError> {
    while let Some(mut tag) = decoder.next_tag() {
        if tag.start_el().matches("item")
            && let Some(instance) = parse_instance(&mut tag)?
        {
            instances.push(instance);
        }
    }
    Ok(())
}

fn parse_instance(decoder: &mut ScopedDecoder<'_, '_>) -> Result<Option<Instance>, XmlDecodeError> {
    let mut instance_id = None;
    let mut state = None;

    while let Some(mut tag) = decoder.next_tag() {
        match tag.start_el().local() {
            "instanceId" => instance_id = Some(try_data(&mut tag)?.into_owned()),
            "instanceState" => state = parse_instance_state(&mut tag)?,
            _ => {}
        }
    }

    Ok(instance_id.map(|instance_id| Instance {
        instance_id,
        state: state.unwrap_or_else(|| "unknown".to_string()),
    }))
}

fn parse_instance_state(
    decoder: &mut ScopedDecoder<'_, '_>,
) -> Result<Option<String>, XmlDecodeError> {
    while let Some(mut tag) = decoder.next_tag() {
        if tag.start_el().matches("name") {
            return Ok(Some(try_data(&mut tag)?.into_owned()));
        }
    }
    Ok(None)
}

#[cfg(test)]
mod tests;
