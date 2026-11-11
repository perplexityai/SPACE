use super::*;
use std::collections::HashMap;
use std::io::Write;

pub(super) async fn handle(
    sandbox_id: SandboxSelector,
    user: Option<GuestUserName>,
    command: Vec<String>,
    client: &mut SandboxClient,
) -> ! {
    let status = run_exec_stream(sandbox_id, user, command, client).await;
    std::process::exit(status);
}

async fn run_exec_stream(
    sandbox_id: SandboxSelector,
    user: Option<GuestUserName>,
    command: Vec<String>,
    client: &mut SandboxClient,
) -> i32 {
    let mut stream = client
        .spaced_start_process(SpacedStartProcessRequest {
            sandbox_id: sandbox_id.to_string(),
            operation: Some(pplx_space_sbx_mgr_client::operations::StartRequest {
                command,
                cwd: None,
                env: HashMap::default(),
                user: user.map(String::from),
                timeout: None,
                background: false,
                idempotency_key: None,
                tag: None,
                stdin: false,
                pty: None,
                clean_env: false,
                process_id: None,
            }),
        })
        .await
        .unwrap_or_else(|error| exit_error(error))
        .into_inner();
    let mut status = 1;
    while let Some(event) = stream
        .message()
        .await
        .unwrap_or_else(|error| exit_error(error))
    {
        use pplx_space_sbx_mgr_client::proto::spaced_process_event::Event;
        match event.event {
            Some(Event::Stdout(output)) => {
                std::io::stdout()
                    .write_all(&output.data)
                    .unwrap_or_else(|e| exit_error(format!("cannot write stdout: {e}")));
            }
            Some(Event::Stderr(output)) => {
                std::io::stderr()
                    .write_all(&output.data)
                    .unwrap_or_else(|e| exit_error(format!("cannot write stderr: {e}")));
            }
            Some(Event::Exit(exit)) => {
                status = process_exit_status(exit);
            }
            _ => {}
        }
    }
    status
}

fn process_exit_status(exit: pplx_space_sbx_mgr_client::proto::SpacedProcessExit) -> i32 {
    use pplx_space_sbx_mgr_client::proto::spaced_process_exit::Outcome;
    match exit.outcome {
        Some(Outcome::Exited(exited)) => exited.exit_code,
        Some(Outcome::Signaled(signaled)) => signaled.signal.map_or(1, |signal| 128 + signal),
        Some(Outcome::TimedOut(timed_out)) => timed_out.signal.map_or(1, |signal| 128 + signal),
        Some(Outcome::Unknown(_)) | None => 1,
    }
}

#[cfg(test)]
mod tests;
