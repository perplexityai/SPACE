use std::time::Duration;

use pplx_space_common_core::types::{
    BuildRunId, ContentHash, SystemTemplate, TemplateBuildJobState as BuildState,
};
use serde::{Deserialize, Serialize};

use crate::{
    admin_client::{ADMIN_API_PREFIX, AdminClient, CliError},
    cli::TemplateCommands,
};

#[derive(Debug, Deserialize, Serialize)]
struct BuildJob {
    job_id: BuildRunId,
    template_name: SystemTemplate,
}

#[derive(Debug, Deserialize, Serialize)]
struct BuildStatus {
    #[serde(flatten)]
    job: BuildJob,
    state: BuildState,
    content_hash: Option<ContentHash>,
    error: Option<String>,
}

impl AdminClient {
    pub(crate) async fn template_command(
        &self,
        command: TemplateCommands,
    ) -> Result<String, CliError> {
        match command {
            TemplateCommands::Build {
                template,
                base,
                publish_tags,
                async_mode,
            } => {
                if let Some(base) = &base
                    && (template != SystemTemplate::CodeRuntime || base.name().as_str() != "seed")
                {
                    return Err(CliError::Argument(
                        "--base is only supported for code-runtime and must reference seed".into(),
                    ));
                }
                let url = format!(
                    "{}{ADMIN_API_PREFIX}/system-templates/{template}/builds",
                    self.base_url
                );
                let request = self.http.post(url).json(&serde_json::json!({
                    "base": base,
                    "publish_tags": publish_tags,
                }));
                let body = self.template_request(request).await?;
                let job: BuildJob =
                    serde_json::from_str(&body).map_err(|err| CliError::Decode(err.to_string()))?;
                if job.template_name != template {
                    return Err(CliError::Decode(
                        "build response names a different template".into(),
                    ));
                }
                if async_mode {
                    return Ok(body);
                }
                eprintln!(
                    "Build submitted. Inspect with: space-admin-cli template status {template} {}",
                    job.job_id
                );
                let status = tokio::time::timeout(Duration::from_secs(7200), self.wait_for_build(&job))
                    .await
                    .map_err(|_| {
                        CliError::Unsupported(format!(
                            "Stopped waiting after two hours; the build continues. Run: space-admin-cli template status {template} {}",
                            job.job_id
                        ))
                    })??;
                render_status(status)
            }
            TemplateCommands::Status { template, job_id } => render_status(
                self.build_status(&BuildJob {
                    template_name: template,
                    job_id,
                })
                .await?,
            ),
        }
    }

    async fn template_request(&self, request: reqwest::RequestBuilder) -> Result<String, CliError> {
        let response = request
            .timeout(Duration::from_secs(30))
            .send()
            .await
            .map_err(|err| CliError::Transport(err.to_string()))?;
        let status = response.status();
        let body = response
            .text()
            .await
            .map_err(|err| CliError::Transport(err.to_string()))?;
        if !status.is_success() {
            return Err(CliError::Api {
                status: status.as_u16(),
                body,
            });
        }
        Ok(body)
    }

    async fn build_status(&self, job: &BuildJob) -> Result<BuildStatus, CliError> {
        let body = self
            .template_request(self.http.get(format!(
                "{}{ADMIN_API_PREFIX}/templates/{}/builds/{}",
                self.base_url, job.template_name, job.job_id
            )))
            .await?;
        let status: BuildStatus =
            serde_json::from_str(&body).map_err(|err| CliError::Decode(err.to_string()))?;
        if status.job.job_id != job.job_id || status.job.template_name != job.template_name {
            return Err(CliError::Decode(
                "status response names a different build".into(),
            ));
        }
        Ok(status)
    }

    async fn wait_for_build(&self, job: &BuildJob) -> Result<BuildStatus, CliError> {
        loop {
            match self.build_status(job).await {
                Ok(status) => match status.state {
                    BuildState::Pending | BuildState::Running => {}
                    BuildState::Succeeded
                    | BuildState::Failed
                    | BuildState::PublishFailed
                    | BuildState::Unspecified => return Ok(status),
                },
                Err(error @ CliError::Transport(_))
                | Err(
                    error @ CliError::Api {
                        status: 500..=599, ..
                    },
                ) => {
                    eprintln!("Status temporarily unavailable; retrying: {error}");
                }
                Err(error) => return Err(error),
            }
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
    }
}

fn render_status(status: BuildStatus) -> Result<String, CliError> {
    match status.state {
        BuildState::Failed | BuildState::PublishFailed => Err(CliError::Build {
            job_id: status.job.job_id.to_string(),
            state: status.state,
            content_hash: status.content_hash,
            error: status
                .error
                .unwrap_or_else(|| "no error detail returned".into()),
        }),
        BuildState::Unspecified => Err(CliError::Decode(
            "builder returned an unspecified state".into(),
        )),
        BuildState::Succeeded if status.content_hash.is_none() => Err(CliError::Decode(
            "successful build omitted content_hash".into(),
        )),
        BuildState::Pending | BuildState::Running | BuildState::Succeeded => {
            serde_json::to_string_pretty(&status).map_err(|err| CliError::Decode(err.to_string()))
        }
    }
}

#[cfg(test)]
mod tests;
