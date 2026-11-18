use aws_smithy_xml::decode::{Document, ScopedDecoder, XmlDecodeError, try_data};
use aws_types::SdkConfig;

use crate::query::{
    AwsClientError, AwsQueryClient, AwsQueryService, QueryTransport, ReqwestTransport,
};

const DESCRIBE_INSTANCES_ACTION: &str = "DescribeInstances";
const EC2_API_VERSION: &str = "2016-11-15";
const DESCRIBE_PAGE_SIZE: &str = "1000";
/// EC2's limit on the values of one `DescribeInstances` filter.
pub const MAX_FILTER_VALUES: usize = 200;

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
        self.describe_instances_filtered("private-dns-name", &[private_dns_name])
            .await
    }

    /// Every instance EC2 lists with one of `instance_ids`, across pages, in
    /// calls of at most [`MAX_FILTER_VALUES`] ids each.
    ///
    /// Asks with an `instance-id` filter rather than `InstanceIds`: with an
    /// explicit id list, one id EC2 doesn't know fails the whole call
    /// (`InvalidInstanceID.NotFound`), while a filter leaves it out of the
    /// answer. An instance EC2 has forgotten, about an hour after it
    /// terminated, is therefore absent from the result, and so is an id in
    /// the wrong region or account: the caller needs a control that an
    /// absent id is not such a mistake.
    pub async fn describe_instances_by_ids(
        &self,
        instance_ids: &[&str],
    ) -> Result<Vec<Instance>, AwsClientError> {
        let mut instances = Vec::new();
        for chunk in instance_ids.chunks(MAX_FILTER_VALUES) {
            instances.extend(
                self.describe_instances_filtered("instance-id", chunk)
                    .await?,
            );
        }
        Ok(instances)
    }

    async fn describe_instances_filtered(
        &self,
        filter: &str,
        values: &[&str],
    ) -> Result<Vec<Instance>, AwsClientError> {
        let mut instances = Vec::new();
        let mut next_token = None;

        loop {
            let page = self
                .describe_instances_page(filter, values, next_token.as_deref())
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
        filter: &str,
        values: &[&str],
        next_token: Option<&str>,
    ) -> Result<DescribeInstancesPage, AwsClientError> {
        let mut params: Vec<(String, String)> = vec![("Filter.1.Name".into(), filter.to_string())];
        params.extend(
            values
                .iter()
                .enumerate()
                .map(|(n, value)| (format!("Filter.1.Value.{}", n + 1), (*value).to_string())),
        );
        params.push(("MaxResults".into(), DESCRIBE_PAGE_SIZE.to_string()));
        if let Some(next_token) = next_token {
            params.push(("NextToken".into(), next_token.to_string()));
        }

        let body = self
            .query
            .send_action(DESCRIBE_INSTANCES_ACTION, EC2_API_VERSION, params)
            .await?;
        parse_describe_instances_response(&body).map_err(AwsClientError::from)
    }
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
