//! Bounded session-0 processes with Job Object ownership and replayable output.
use super::{identity::Identity, launch, path::LocalPath};
use pplx_space_guest_process::{IdempotencyKey, ProcessCommand, ProcessId, status::*};
use pplx_space_spaced_guest_protocol::{proto::bridge_guest::*, validate_optional_duration};
use std::{
    collections::{HashMap, VecDeque},
    fs::File,
    io::{Read, Write},
    sync::{Arc, Condvar, Mutex},
    time::{Duration, Instant},
};
use tokio::sync::{Notify, Semaphore};
use tokio_util::{sync::CancellationToken, task::TaskTracker};
use tonic::{Request, Response, Status};
const PROCESS_RECORD_LIMIT: usize = 512;
const OUTPUT_CHUNK_BYTES: usize = 8 * 1024;

type Stream =
    std::pin::Pin<Box<dyn futures_util::Stream<Item = Result<ProcessEvent, Status>> + Send>>;
#[derive(Clone)]
pub(super) struct Processes {
    identity: Arc<Identity>,
    records: Arc<Mutex<Records>>,
    running: Arc<Semaphore>,
    output_limit: usize,
    tasks: TaskTracker,
    shutdown: CancellationToken,
}
#[derive(Default)]
struct Records {
    processes: HashMap<ProcessId, Arc<Record>>,
    keys: HashMap<IdempotencyKey, ProcessId>,
}
struct Record {
    id: ProcessId,
    request: StartRequest,
    state: Mutex<State>,
    notify: Arc<Notify>,
    consumed: Condvar,
    output_limit: usize,
    job: Mutex<Option<Arc<std::os::windows::io::OwnedHandle>>>,
    stdin: Mutex<Option<File>>,
    input: Arc<Semaphore>,
    readers: Arc<Semaphore>,
}
#[derive(Default)]
struct State {
    info: Option<ProcessInfo>,
    error: Option<Status>,
    output: VecDeque<process_event::Event>,
    first: u64,
    bytes: usize,
    exit: Option<ProcessExit>,
    ended: Option<Instant>,
    primary: Option<u64>,
    primary_claimed: bool,
    drain_deadline: Option<Instant>,
}
// Own cleanup even when the supervising worker unwinds or cannot start readers.
struct ProcessCleanup(Arc<Record>);
impl Drop for ProcessCleanup {
    fn drop(&mut self) {
        let mut job = self.0.job.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(job) = job.take() {
            let _ = launch::kill(&job);
        }
        self.0
            .stdin
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take();
        let mut state = self.0.state.lock().unwrap_or_else(|e| e.into_inner());
        if state.exit.is_none() && state.error.is_none() {
            state.error = Some(Status::internal(
                "Windows process supervisor ended unexpectedly",
            ));
        }
        if let Some(info) = &mut state.info
            && info.exit.is_none()
        {
            info.exit = Some(ProcessExit {
                outcome: Some(process_exit::Outcome::Unknown(ProcessUnknown {})),
            });
            info.ended_at = Some(std::time::SystemTime::now().into());
        }
        state.primary = None;
        self.0.consumed.notify_all();
        state.ended = Some(Instant::now());
        drop(state);
        self.0.notify.notify_waiters();
    }
}
fn output_lagged(first: u64) -> Status {
    let mut status = Status::resource_exhausted(PROCESS_OUTPUT_LAGGED_MESSAGE);
    status.metadata_mut().insert(
        ERROR_REASON_METADATA_KEY,
        tonic::metadata::MetadataValue::from_static(PROCESS_OUTPUT_LAGGED),
    );
    status.metadata_mut().insert(
        PROCESS_OUTPUT_FIRST_SEQUENCE_METADATA_KEY,
        tonic::metadata::MetadataValue::from(first),
    );
    status
}
fn output_ahead() -> Status {
    let mut status = Status::out_of_range("process output cursor ahead");
    status.metadata_mut().insert(
        ERROR_REASON_METADATA_KEY,
        tonic::metadata::MetadataValue::from_static(PROCESS_OUTPUT_CURSOR_AHEAD),
    );
    status
}
struct PrimaryReader {
    record: Arc<Record>,
    owns: bool,
}
impl Drop for PrimaryReader {
    fn drop(&mut self) {
        if self.owns {
            let mut state = self.record.state.lock().expect("process journal");
            state.primary = None;
            state.primary_claimed = false;
            drop(state);
            self.record.consumed.notify_all();
        }
    }
}

impl Record {
    fn output(&self, event: process_event::Event) {
        let size = match &event {
            process_event::Event::Stdout(o) | process_event::Event::Stderr(o) => o.data.len(),
            _ => 0,
        } + std::mem::size_of::<process_event::Event>();
        let mut s = self.state.lock().expect("process journal");
        if size > self.output_limit {
            s.error = Some(Status::resource_exhausted(
                "process output record exceeds retention budget",
            ));
            drop(s);
            self.notify.notify_waiters();
            return;
        }
        while s.bytes + size > self.output_limit || s.output.len() >= 1024 {
            if s.primary.is_some_and(|position| position <= s.first)
                && s.drain_deadline
                    .is_none_or(|deadline| Instant::now() < deadline)
            {
                s = self
                    .consumed
                    .wait_timeout(s, Duration::from_millis(100))
                    .expect("process journal")
                    .0;
                continue;
            }
            if let Some(process_event::Event::Stdout(o) | process_event::Event::Stderr(o)) =
                s.output.pop_front()
            {
                s.bytes -= o.data.len() + std::mem::size_of::<process_event::Event>();
            }
            s.first += 1;
        }
        s.output.push_back(event);
        s.bytes += size;
        drop(s);
        self.notify.notify_waiters();
    }
    fn event(&self, event: process_event::Event, pid: u32) -> ProcessEvent {
        ProcessEvent {
            process_id: self.id.as_str().into(),
            pid: Some(pid),
            event: Some(event),
        }
    }
    async fn stream(
        self: Arc<Self>,
        cursor: Option<u64>,
        oldest: bool,
        started: bool,
    ) -> Result<Stream, Status> {
        let permit = self
            .readers
            .clone()
            .try_acquire_owned()
            .map_err(|_| Status::resource_exhausted("too many process subscribers"))?;
        let mut primary = PrimaryReader {
            record: self.clone(),
            owns: false,
        };
        let (pid, position) = loop {
            let notified = self.notify.clone().notified_owned();
            tokio::pin!(notified);
            notified.as_mut().enable();
            {
                let mut s = self.state.lock().expect("process journal");
                if let Some(e) = &s.error {
                    return Err(e.clone());
                }
                if let Some(info) = &s.info {
                    let mut position = cursor.unwrap_or(s.first + s.output.len() as u64);
                    if position < s.first && oldest {
                        position = s.first;
                    }
                    if position < s.first {
                        return Err(output_lagged(s.first));
                    }
                    if position > s.first + s.output.len() as u64 {
                        return Err(output_ahead());
                    }
                    let pid = info.pid;
                    if primary.owns || !s.primary_claimed {
                        primary.owns = true;
                        s.primary_claimed = true;
                        s.primary = Some(position);
                    }
                    break (pid, position);
                }
                if started && !s.primary_claimed {
                    primary.owns = true;
                    s.primary_claimed = true;
                }
            }
            notified.await;
        };
        let prefix = if started {
            process_event::Event::Started(ProcessStarted {
                start_time: Some(std::time::SystemTime::now().into()),
            })
        } else {
            process_event::Event::Connected(ProcessConnected {
                output_cursor: cursor.map(|_| position),
            })
        };
        struct Reader {
            record: Arc<Record>,
            position: u64,
            pid: u32,
            prefix: Option<process_event::Event>,
            _permit: tokio::sync::OwnedSemaphorePermit,
            done: bool,
            primary: PrimaryReader,
        }
        Ok(Box::pin(futures_util::stream::unfold(
            Reader {
                record: self,
                position,
                pid,
                prefix: Some(prefix),
                _permit: permit,
                done: false,
                primary,
            },
            |mut r| async move {
                if r.done {
                    return None;
                }
                if let Some(event) = r.prefix.take() {
                    return Some((Ok(r.record.event(event, r.pid)), r));
                }
                loop {
                    let notified = r.record.notify.clone().notified_owned();
                    tokio::pin!(notified);
                    notified.as_mut().enable();
                    let next = {
                        let mut s = r.record.state.lock().expect("process journal");
                        if let Some(error) = &s.error {
                            r.done = true;
                            Some(Err(error.clone()))
                        } else if r.position < s.first {
                            r.done = true;
                            Some(Err(output_lagged(s.first)))
                        } else if let Some(event) = s.output.get((r.position - s.first) as usize) {
                            let event = event.clone();
                            r.position += 1;
                            if r.primary.owns {
                                s.primary = Some(r.position);
                                r.record.consumed.notify_all();
                            }
                            Some(Ok(r.record.event(event, r.pid)))
                        } else if let Some(exit) = &s.exit {
                            r.done = true;
                            Some(Ok(r.record.event(process_event::Event::Exit(*exit), r.pid)))
                        } else {
                            None
                        }
                    };
                    if let Some(item) = next {
                        return Some((item, r));
                    }
                    notified.await;
                }
            },
        )))
    }
}
fn process_io_status(error: std::io::Error) -> Status {
    match error.kind() {
        std::io::ErrorKind::NotFound => Status::not_found("guest executable not found"),
        std::io::ErrorKind::PermissionDenied => {
            Status::permission_denied("guest process access denied")
        }
        std::io::ErrorKind::InvalidInput => {
            Status::invalid_argument("invalid guest process request")
        }
        _ => Status::internal(format!("Windows process operation failed: {error}")),
    }
}

fn require_supported_signal(signal: i32) -> Result<(), Status> {
    let signal = pplx_space_guest_process::ProcessSignal::try_from_i32(signal)
        .map_err(pplx_space_spaced_guest_protocol::invalid_argument_status)?;
    if signal.as_i32() != 9 {
        return Err(super::unsupported("Windows supports SIGKILL only"));
    }
    Ok(())
}

fn output_limit() -> usize {
    use windows_sys::Win32::System::SystemInformation::{GlobalMemoryStatusEx, MEMORYSTATUSEX};
    let mut memory = MEMORYSTATUSEX {
        dwLength: std::mem::size_of::<MEMORYSTATUSEX>() as u32,
        ..Default::default()
    };
    // SAFETY: memory points to the initialized, correctly sized output structure.
    let total = if unsafe { GlobalMemoryStatusEx(&mut memory) } != 0 {
        memory.ullTotalPhys / 64
    } else {
        16 * 1024 * 1024
    };
    let budget = total.clamp(16 * 1024 * 1024, 64 * 1024 * 1024) as usize;
    (budget / 2 / PROCESS_RECORD_LIMIT)
        .max(OUTPUT_CHUNK_BYTES + std::mem::size_of::<process_event::Event>())
}

impl Processes {
    pub(super) fn new(
        identity: Arc<Identity>,
        shutdown: CancellationToken,
        tasks: TaskTracker,
    ) -> Self {
        Self {
            identity,
            records: Arc::default(),
            running: Arc::new(Semaphore::new(32)),
            output_limit: output_limit(),
            tasks,
            shutdown,
        }
    }
    fn get(&self, id: &str) -> Result<Arc<Record>, Status> {
        let id = ProcessId::parse(id).map_err(|e| Status::invalid_argument(e.to_string()))?;
        self.records
            .lock()
            .map_err(|_| Status::internal("process records poisoned"))?
            .processes
            .get(&id)
            .cloned()
            .ok_or_else(|| Status::not_found("process not found"))
    }
}
#[tonic::async_trait]
impl process_server::Process for Processes {
    type StartStream = Stream;
    type ConnectProcessStream = Stream;
    type ListProcessesStream =
        std::pin::Pin<Box<dyn futures_util::Stream<Item = Result<ProcessInfo, Status>> + Send>>;
    async fn start(&self, r: Request<StartRequest>) -> Result<Response<Stream>, Status> {
        let r = r.into_inner();
        self.identity.check_user(r.user.as_deref())?;
        ProcessCommand::new(r.command.clone())
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        if r.pty.is_some() {
            return Err(super::unsupported("Windows PTY is not qualified"));
        }
        if r.env.len() > 256
            || r.env.iter().map(|(k, v)| k.len() + v.len()).sum::<usize>() > 128 * 1024
        {
            return Err(Status::resource_exhausted(
                "process environment exceeds bound",
            ));
        }
        let mut keys = std::collections::HashSet::new();
        for (k, v) in &r.env {
            if pplx_space_guest_process::EnvKey::parse(k).is_err()
                || k.contains(['=', '\0'])
                || v.contains('\0')
                || !keys.insert(k.to_ascii_uppercase())
            {
                return Err(Status::invalid_argument(
                    "invalid or case-duplicate Windows environment key",
                ));
            }
        }
        let cwd = LocalPath::parse(r.cwd.as_deref().unwrap_or("."), &self.identity.home)?;
        let timeout = validate_optional_duration("timeout", r.timeout)?;
        let key = r
            .idempotency_key
            .as_ref()
            .map(IdempotencyKey::parse)
            .transpose()
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        let id = r
            .process_id
            .as_ref()
            .map(ProcessId::parse)
            .transpose()
            .map_err(|e| Status::invalid_argument(e.to_string()))?
            .unwrap_or_default();
        let (existing, record, permit) = {
            let mut records = self
                .records
                .lock()
                .map_err(|_| Status::internal("process records poisoned"))?;
            if let Some(previous) = key
                .as_ref()
                .and_then(|k| records.keys.get(k))
                .and_then(|id| records.processes.get(id))
            {
                if previous.request != r {
                    return Err(Status::already_exists("process idempotency key reused"));
                }
                (true, previous.clone(), None)
            } else {
                if records.processes.contains_key(&id) {
                    return Err(Status::already_exists("process identity already used"));
                }
                let mut ended = records
                    .processes
                    .iter()
                    .filter_map(|(id, record)| {
                        record
                            .state
                            .lock()
                            .ok()?
                            .ended
                            .map(|time| (id.clone(), time))
                    })
                    .collect::<Vec<_>>();
                ended.sort_by_key(|(_, time)| *time);
                for (id, time) in ended {
                    if records.processes.len() < PROCESS_RECORD_LIMIT
                        && time.elapsed() < Duration::from_secs(900)
                    {
                        break;
                    }
                    records.processes.remove(&id);
                    records.keys.retain(|_, process| process != &id);
                }
                if records.processes.len() >= PROCESS_RECORD_LIMIT {
                    return Err(Status::resource_exhausted("process history full"));
                }
                let permit = self
                    .running
                    .clone()
                    .try_acquire_owned()
                    .map_err(|_| Status::resource_exhausted("too many running processes"))?;
                let record = Arc::new(Record {
                    id: id.clone(),
                    request: r.clone(),
                    state: Mutex::new(State {
                        primary: Some(0),
                        ..State::default()
                    }),
                    consumed: Condvar::new(),
                    output_limit: self.output_limit,
                    notify: Arc::default(),
                    job: Mutex::default(),
                    stdin: Mutex::default(),
                    input: Arc::new(Semaphore::new(1)),
                    readers: Arc::new(Semaphore::new(4)),
                });
                records.processes.insert(id.clone(), record.clone());
                if let Some(key) = key {
                    records.keys.insert(key, id);
                }
                (false, record, Some(permit))
            }
        };
        if !existing {
            let record = record.clone();
            let identity = self.identity.clone();
            let shutdown = self.shutdown.clone();
            self.tasks.spawn_blocking(move || {
                let _permit = permit;
                let _cleanup = ProcessCleanup(record.clone());
                let child = match launch::spawn(&identity, &r.command, cwd.as_path(), &r.env) {
                    Ok(c) => c,
                    Err(e) => {
                        record.state.lock().expect("process journal").error =
                            Some(process_io_status(e));
                        record.notify.notify_waiters();
                        return;
                    }
                };
                let launch::Child {
                    process,
                    job,
                    pid,
                    stdin,
                    stdout,
                    stderr,
                } = child;
                *record.job.lock().expect("process job") = Some(job.clone());
                if r.stdin {
                    *record.stdin.lock().expect("process stdin") = Some(stdin);
                } else {
                    drop(stdin);
                }
                record.state.lock().expect("process journal").info = Some(ProcessInfo {
                    process_id: record.id.as_str().into(),
                    pid,
                    command: r.command,
                    cwd: Some(cwd.as_path().to_string_lossy().into_owned()),
                    env: r.env,
                    user: Some(identity.user.as_str().into()),
                    tag: r.tag,
                    pty: Some(false),
                    exit: None,
                    ended_at: None,
                });
                record.notify.notify_waiters();
                let output = |mut file: File, stderr: bool, record: Arc<Record>| {
                    std::thread::Builder::new()
                        .name("windows-process-output".into())
                        .spawn(move || {
                            let mut buffer = [0; OUTPUT_CHUNK_BYTES];
                            loop {
                                match file.read(&mut buffer) {
                                    Ok(0) => break,
                                    Ok(n) => {
                                        let output = ProcessOutput {
                                            data: buffer
                                                .get(..n)
                                                .expect("read count bounded by buffer")
                                                .to_vec(),
                                        };
                                        record.output(if stderr {
                                            process_event::Event::Stderr(output)
                                        } else {
                                            process_event::Event::Stdout(output)
                                        });
                                    }
                                    Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {
                                        continue;
                                    }
                                    Err(e) => {
                                        tracing::warn!(%e,"Windows process output read failed");
                                        break;
                                    }
                                }
                            }
                        })
                };
                let Ok(out) = output(stdout, false, record.clone()) else {
                    return;
                };
                let Ok(err) = output(stderr, true, record.clone()) else {
                    return;
                };
                let start = Instant::now();
                let mut timed_out = false;
                let outcome = loop {
                    match launch::wait(&process) {
                        Ok(Some(code)) => {
                            break process_exit::Outcome::Exited(ProcessExited {
                                exit_code: code as i32,
                            });
                        }
                        Err(e) => {
                            tracing::warn!(%e,"Windows process wait failed");
                            break process_exit::Outcome::Unknown(ProcessUnknown {});
                        }
                        Ok(None) => {
                            if shutdown.is_cancelled()
                                || timeout.is_some_and(|t| start.elapsed() >= t)
                            {
                                timed_out = timeout.is_some_and(|t| start.elapsed() >= t);
                                if let Err(e) = launch::kill(&job) {
                                    tracing::error!(%e,"Windows job termination failed");
                                }
                            }
                        }
                    }
                };
                // Main-process exit never leaves descendants holding output pipes.
                let _ = launch::kill(&job);
                record.state.lock().expect("process journal").drain_deadline =
                    Some(Instant::now() + Duration::from_secs(1));
                record.consumed.notify_all();
                let _ = out.join();
                let _ = err.join();
                record.stdin.lock().expect("process stdin").take();
                record.job.lock().expect("process job").take();
                let exit = ProcessExit {
                    outcome: Some(if timed_out {
                        process_exit::Outcome::TimedOut(ProcessTimedOut { signal: None })
                    } else {
                        outcome
                    }),
                };
                let mut s = record.state.lock().expect("process journal");
                if let Some(info) = &mut s.info {
                    info.exit = Some(exit);
                    info.ended_at = Some(std::time::SystemTime::now().into());
                }
                s.exit = Some(exit);
                s.ended = Some(Instant::now());
                drop(s);
                record.notify.notify_waiters();
            });
        }
        Ok(Response::new(record.stream(Some(0), false, true).await?))
    }
    async fn connect_process(
        &self,
        r: Request<ConnectProcessRequest>,
    ) -> Result<Response<Stream>, Status> {
        let r = r.into_inner();
        validate_optional_duration("timeout", r.timeout)?;
        Ok(Response::new(
            self.get(&r.process_id)?
                .stream(r.output_cursor, r.resume_from_oldest_retained, false)
                .await?,
        ))
    }
    async fn list_processes(
        &self,
        r: Request<ListProcessesRequest>,
    ) -> Result<Response<Self::ListProcessesStream>, Status> {
        let r = r.into_inner();
        let records = self
            .records
            .lock()
            .map_err(|_| Status::internal("process records poisoned"))?;
        let mut result = Vec::new();
        for record in records.processes.values() {
            if r.process_id
                .as_ref()
                .is_some_and(|id| id != record.id.as_str())
            {
                continue;
            }
            if let Some(info) = &record.state.lock().expect("process journal").info
                && (r.include_exited || info.exit.is_none())
            {
                result.push(Ok(info.clone()));
            }
        }
        Ok(Response::new(Box::pin(tokio_stream::iter(result))))
    }
    async fn send_signal(
        &self,
        r: Request<SendSignalRequest>,
    ) -> Result<Response<SendSignalResponse>, Status> {
        let r = r.into_inner();
        // Linux-numbered SIGKILL requests forced termination of the Windows job.
        require_supported_signal(r.signal)?;
        let record = self.get(&r.process_id)?;
        let job = record
            .job
            .lock()
            .map_err(|_| Status::internal("process job poisoned"))?
            .clone()
            .ok_or_else(|| Status::failed_precondition("process is not running"))?;
        launch::kill(&job).map_err(process_io_status)?;
        Ok(Response::new(SendSignalResponse {}))
    }
    async fn send_input(
        &self,
        r: Request<SendInputRequest>,
    ) -> Result<Response<SendInputResponse>, Status> {
        let r = r.into_inner();
        if r.data.len() > 1024 * 1024 {
            return Err(Status::resource_exhausted("stdin chunk exceeds 1 MiB"));
        }
        let record = self.get(&r.process_id)?;
        let permit = record
            .input
            .clone()
            .try_acquire_owned()
            .map_err(|_| Status::resource_exhausted("stdin write pending"))?;
        self.tasks
            .spawn_blocking(move || {
                let _permit = permit;
                let mut stdin = record
                    .stdin
                    .lock()
                    .map_err(|_| Status::internal("stdin poisoned"))?;
                stdin
                    .as_mut()
                    .ok_or_else(|| Status::failed_precondition("stdin closed"))?
                    .write_all(&r.data)
                    .map_err(super::filesystem::io_status)
            })
            .await
            .map_err(|_| Status::internal("stdin worker failed"))??;
        Ok(Response::new(SendInputResponse {}))
    }
    async fn close_stdin(
        &self,
        r: Request<CloseStdinRequest>,
    ) -> Result<Response<CloseStdinResponse>, Status> {
        let record = self.get(&r.into_inner().process_id)?;
        let _permit = record
            .input
            .clone()
            .try_acquire_owned()
            .map_err(|_| Status::resource_exhausted("stdin write pending"))?;
        record
            .stdin
            .lock()
            .map_err(|_| Status::internal("stdin poisoned"))?
            .take();
        Ok(Response::new(CloseStdinResponse {}))
    }
    async fn resize_pty(
        &self,
        _: Request<ResizePtyRequest>,
    ) -> Result<Response<ResizePtyResponse>, Status> {
        Err(super::unsupported("Windows PTY is not qualified"))
    }
}

#[cfg(test)]
mod tests;
