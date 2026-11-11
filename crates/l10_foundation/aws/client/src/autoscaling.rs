use aws_types::SdkConfig;

use crate::query::{
    AwsClientError, AwsQueryClient, AwsQueryService, QueryTransport, ReqwestTransport,
};

const TERMINATE_INSTANCE_ACTION: &str = "TerminateInstanceInAutoScalingGroup";
const AUTOSCALING_API_VERSION: &str = "2011-01-01";

#[derive(Debug, Clone)]
pub struct Client<T = ReqwestTransport> {
    query: AwsQueryClient<T>,
}

impl Client<ReqwestTransport> {
    pub fn from_config(config: &SdkConfig) -> Self {
        Self {
            query: AwsQueryClient::from_config(AwsQueryService::AutoScaling, config),
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
                AwsQueryService::AutoScaling,
                region,
                credentials_provider,
                transport,
            ),
        }
    }

    pub async fn terminate_instance_in_auto_scaling_group(
        &self,
        instance_id: &str,
        should_decrement_desired_capacity: bool,
    ) -> Result<(), AwsClientError> {
        self.query
            .send_action(
                TERMINATE_INSTANCE_ACTION,
                AUTOSCALING_API_VERSION,
                vec![
                    ("InstanceId", instance_id.to_string()),
                    (
                        "ShouldDecrementDesiredCapacity",
                        should_decrement_desired_capacity.to_string(),
                    ),
                ],
            )
            .await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests;
