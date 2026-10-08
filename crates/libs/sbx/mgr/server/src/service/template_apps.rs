//! Snapshot-only app observation. This never starts, stops, or resets an app.
use super::*;
use pplx_space_common_core::types::{
    MAX_TEMPLATE_RUNTIME_BYTES, ProcessCommand, RuntimeStartupPolicy, TemplateRuntime,
};
use pplx_space_spaced_host_client::{SpacedHostClient, StartProcessOptions};

// One short-lived observer, rather than a process record for every poll. Both
// the guest process timeout and host deadline bound work after cancellation.
const OBSERVE: &str = include_str!("template_apps/observe.sh");
const CHECK_LIMIT: Duration = Duration::from_secs(70);

impl SandboxManagerService {
    pub(super) async fn wait_template_apps(
        &self,
        request: Request<crate::proto::WaitTemplateAppsRequest>,
    ) -> Result<Response<crate::proto::WaitTemplateAppsResponse>, Status> {
        self.authorize_peer(&request, "WaitTemplateApps", ALLOW_SANDBOX_ADMIN_OR_TMPLMGR)?;
        let id = self.lookup_sandbox(&request.get_ref().sandbox_id)?.id;
        let lock = self.auto_lifecycle.wake_lock(&id);
        let _guard = lock.lock().await;
        let sandbox = self.lookup_sandbox(&id)?;
        if !Self::sandbox_owned_by_template_manager(&sandbox) {
            return Err(Status::permission_denied(
                "app snapshot checks require a template-owned sandbox",
            ));
        }
        if sandbox.pool_template.is_some() || sandbox.state != SandboxState::Started {
            return Err(Status::failed_precondition(
                "app snapshot checks require a running, non-pool template sandbox",
            ));
        }
        let mut runtime = None;
        let observed = tokio::time::timeout(CHECK_LIMIT, async {
            let client = self.connect_spaced_host(&sandbox).await?;
            runtime = read_runtime(&client).await?;
            let Some(declaration) = runtime.as_ref() else {
                return Ok(Vec::new());
            };
            observe(&client, declaration).await
        })
        .await
        .map_err(|_| Status::deadline_exceeded("template app inspection exceeded its budget"))
        .and_then(|result| result);
        // A declaration read failure cannot prove that all apps are optional.
        let warnings = match runtime.as_ref() {
            Some(runtime) => apply_startup_policy(runtime, observed)?,
            None => observed?,
        };
        Ok(Response::new(crate::proto::WaitTemplateAppsResponse {
            warnings,
        }))
    }
}

fn apply_startup_policy(
    runtime: &TemplateRuntime,
    observed: Result<Vec<String>, Status>,
) -> Result<Vec<String>, Status> {
    match observed {
        Err(error)
            if runtime
                .apps()
                .iter()
                .all(|app| app.startup_policy() == RuntimeStartupPolicy::BestEffort) =>
        {
            Ok(runtime
                .apps()
                .iter()
                .map(|app| {
                    format!(
                        "{}: app inspection failed ({})",
                        app.name().as_str(),
                        error.code()
                    )
                })
                .collect())
        }
        other => other,
    }
}

async fn read_runtime(client: &SpacedHostClient) -> Result<Option<TemplateRuntime>, Status> {
    let mut stream = match client
        .read_stream_as_user(
            "/etc/space/apps/runtime.toml".into(),
            0,
            Some((MAX_TEMPLATE_RUNTIME_BYTES + 1) as u64),
            Some(
                GuestUserName::parse("root")
                    .map_err(|_| Status::internal("invalid inspector user"))?,
            ),
        )
        .await
    {
        Ok(stream) => stream,
        Err(error) if error.code() == tonic::Code::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };
    let mut bytes = Vec::new();
    loop {
        match stream.message().await {
            Ok(Some(chunk)) => {
                bytes.extend_from_slice(&chunk.data);
                if bytes.len() > MAX_TEMPLATE_RUNTIME_BYTES {
                    return Err(Status::failed_precondition(
                        "template app declaration exceeds size limit",
                    ));
                }
            }
            Ok(None) => break,
            Err(error) if bytes.is_empty() && error.code() == tonic::Code::NotFound => {
                return Ok(None);
            }
            Err(error) => return Err(error),
        }
    }
    TemplateRuntime::from_bytes(&bytes)
        .map(Some)
        .map_err(|_| Status::failed_precondition("invalid template app declaration"))
}

async fn observe(
    client: &SpacedHostClient,
    runtime: &TemplateRuntime,
) -> Result<Vec<String>, Status> {
    let mut argv = vec![
        "/bin/sh".into(),
        "-c".into(),
        OBSERVE.into(),
        "space-snapshot-app-check".into(),
    ];
    argv.extend(runtime.apps().iter().map(|app| app.name().unit()));
    let mut options = StartProcessOptions::new(
        ProcessCommand::new(argv)
            .map_err(|_| Status::internal("invalid app inspection command"))?,
    );
    options.user =
        Some(GuestUserName::parse("root").map_err(|_| Status::internal("invalid inspector user"))?);
    options.clean_env = true;
    options.timeout = Some(Duration::from_secs(65));
    options.tag = Some("space-snapshot-app-check".into());
    let mut events = client.start_process_stream_with_options(options).await?;
    let started = Instant::now();
    let mut process_id = None;
    let mut buffer = String::new();
    let result = async {
        loop {
            let event = events
                .message()
                .await?
                .ok_or_else(|| Status::unavailable("app inspection stream ended"))?;
            process_id = Some(event.process_id);
            match event.event {
                Some(host_proto::process_event::Event::Stdout(output)) => {
                    buffer.push_str(&String::from_utf8_lossy(&output.data));
                    if buffer.len() > 64 * 1024 {
                        return Err(Status::resource_exhausted(
                            "app inspection output exceeds limit",
                        ));
                    }
                    while let Some(end) = buffer.find("\n.\n") {
                        let batch: String = buffer.drain(..end + 3).collect();
                        if let Some(warnings) = evaluate(runtime, &batch, started.elapsed())? {
                            return Ok(warnings);
                        }
                    }
                }
                Some(host_proto::process_event::Event::Exit(_)) => {
                    return Err(Status::unavailable(
                        "app inspector exited before completing",
                    ));
                }
                _ => {}
            }
        }
    }
    .await;
    // Stop before capture whenever the guest connection permits it. If cleanup
    // fails, best-effort policy may still capture; the guest-side 65s process
    // timeout bounds the observer's remaining life. Completed records use the
    // guest's normal retention policy.
    if let Some(id) = process_id {
        client.kill_process(id).await?;
        while let Some(event) = events.message().await? {
            if matches!(event.event, Some(host_proto::process_event::Event::Exit(_))) {
                break;
            }
        }
    }
    result
}

#[derive(Default)]
struct UnitState<'a> {
    id: &'a str,
    load: &'a str,
    active: &'a str,
    sub: &'a str,
    started: bool,
}

fn evaluate(
    runtime: &TemplateRuntime,
    batch: &str,
    elapsed: Duration,
) -> Result<Option<Vec<String>>, Status> {
    let states: BTreeMap<_, _> = batch
        .split("\n\n")
        .filter_map(|block| {
            let mut state = UnitState::default();
            for line in block.lines() {
                let Some((key, value)) = line.split_once('=') else {
                    continue;
                };
                match key {
                    "Id" => state.id = value,
                    "LoadState" => state.load = value,
                    "ActiveState" => state.active = value,
                    "SubState" => state.sub = value,
                    "InactiveExitTimestampMonotonic" => {
                        state.started = value.parse::<u64>().is_ok_and(|v| v > 0)
                    }
                    _ => {}
                }
            }
            (!state.id.is_empty()).then_some((state.id, state))
        })
        .collect();
    let mut pending = false;
    let mut warnings = Vec::new();
    for app in runtime.apps() {
        let state = states.get(app.name().unit().as_str());
        let failure = match state {
            Some(s) if s.load == "loaded" && s.active == "active" && s.sub == "running" => continue,
            Some(s) if s.load == "not-found" || s.load == "error" || s.load == "masked" => {
                Some("unit unavailable")
            }
            Some(s) if s.active == "failed" => Some("failed"),
            Some(s) if s.active == "inactive" && s.started => Some("exited"),
            // Retry backend errors, queued starts, and automatic restarts within
            // one budget. Never restart the unit or replenish its start limit.
            _ if elapsed >= app.startup_timeout() + Duration::from_secs(2) => {
                Some("startup budget exhausted")
            }
            _ => None,
        };
        if let Some(reason) = failure {
            let message = format!("{}: {reason}", app.name().as_str());
            if app.startup_policy() == RuntimeStartupPolicy::Required {
                return Err(Status::failed_precondition(format!(
                    "required template app {message}"
                )));
            }
            warnings.push(message);
        } else {
            pending = true;
        }
    }
    Ok((!pending).then_some(warnings))
}

#[cfg(test)]
mod tests;
