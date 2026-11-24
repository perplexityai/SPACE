//! `space-admin-cli sandbox ...`: per-sandbox admin actions.

use crate::{
    admin_client::{ADMIN_API_PREFIX, AdminClient, CliError, interpret_response},
    cli::SandboxCommands,
};

impl AdminClient {
    pub(crate) async fn sandbox_command(
        &self,
        command: SandboxCommands,
    ) -> Result<String, CliError> {
        match command {
            SandboxCommands::DropMemory {
                sandbox,
                reason,
                idempotency_key,
            } => self.drop_memory(&sandbox, &reason, idempotency_key).await,
        }
    }

    async fn drop_memory(
        &self,
        sandbox: &str,
        reason: &str,
        idempotency_key: Option<String>,
    ) -> Result<String, CliError> {
        let sandbox = sandbox.trim();
        // The id lands in the path, so refuse anything that would change it.
        if sandbox.is_empty() || sandbox.contains(['/', '?', '#', ':', '%']) {
            return Err(CliError::Argument(format!(
                "invalid sandbox id: {sandbox:?}"
            )));
        }
        if reason.trim().is_empty() {
            return Err(CliError::Argument("--reason must not be empty".into()));
        }
        let idempotency_key = idempotency_key.unwrap_or_else(|| uuid::Uuid::now_v7().to_string());
        eprintln!("Idempotency-Key: {idempotency_key}");
        let url = format!(
            "{}{ADMIN_API_PREFIX}/sandboxes/{sandbox}:drop-memory",
            self.base_url
        );
        let response = self
            .http
            .post(url)
            .header("Idempotency-Key", &idempotency_key)
            .json(&serde_json::json!({ "reason": reason }))
            .send()
            .await
            .map_err(|err| CliError::Transport(err.to_string()))?;
        let status = response.status();
        let body = response
            .text()
            .await
            .map_err(|err| CliError::Transport(err.to_string()))?;
        interpret_response(status, &body)
    }
}

#[cfg(test)]
mod tests;
