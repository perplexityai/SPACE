//! Bounded output readers and pipe-drain supervision.

use super::*;

pub(super) const PROCESS_OUTPUT_DRAIN_TIMEOUT: Duration = Duration::from_secs(1);

pub(super) struct ProcessOutputTask {
    pub(super) task: tokio::task::JoinHandle<()>,
    pub(super) source: OwnedFd,
}

impl ProcessOutputTask {
    pub(super) fn writers_closed(&self) -> io::Result<bool> {
        use nix::poll::{PollFd, PollFlags, PollTimeout, poll};
        // POLLHUP observes the last writer closing even while unread bytes
        // remain. Waiting for the reader task instead confuses consumer
        // backpressure with a descendant keeping the pipe or PTY open.
        let mut sources = [PollFd::new(self.source.as_fd(), PollFlags::empty())];
        loop {
            match poll(&mut sources, PollTimeout::ZERO) {
                Ok(_) => {
                    let events = sources[0]
                        .revents()
                        .ok_or_else(|| io::Error::other("unrecognized output poll events"))?;
                    if events.contains(PollFlags::POLLHUP) {
                        return Ok(true);
                    }
                    if events.intersects(PollFlags::POLLERR | PollFlags::POLLNVAL) {
                        return Err(io::Error::other("output descriptor poll failed"));
                    }
                    return Ok(false);
                }
                Err(nix::errno::Errno::EINTR) => continue,
                Err(error) => return Err(error.into()),
            }
        }
    }
}

pub(super) fn spawn_process_output_reader<R>(
    mut reader: R,
    source: OwnedFd,
    record: Arc<ProcessRecord>,
    process_id: ProcessId,
    pid: u32,
    kind: ProcessOutputKind,
) -> ProcessOutputTask
where
    R: AsyncRead + Unpin + Send + 'static,
{
    let task = tokio::spawn(async move {
        let mut buffer = vec![0; 8192];
        loop {
            match reader.read(&mut buffer).await {
                Ok(0) => break,
                Ok(n) => {
                    let Some(data) = buffer.get(..n) else {
                        break;
                    };
                    let event = match kind {
                        ProcessOutputKind::Stdout => {
                            process_stdout_event(&process_id, Some(pid), data.to_vec())
                        }
                        ProcessOutputKind::Stderr => {
                            process_stderr_event(&process_id, Some(pid), data.to_vec())
                        }
                    };
                    record.journal.publish_output(event).await;
                }
                Err(error) => {
                    tracing::warn!(target: LOG_TARGET,
                        %error,
                        process_id = process_id.as_str(),
                        pid,
                        stream = kind.as_str(),
                        "process output reader failed"
                    );
                    break;
                }
            }
        }
    });
    ProcessOutputTask { task, source }
}

pub(super) async fn drain_process_output_tasks(
    output_tasks: &mut VecDeque<ProcessOutputTask>,
    process_id: &ProcessId,
    pid: u32,
    abort_on_timeout: bool,
) -> bool {
    let mut fully_drained = true;
    loop {
        let drain = async {
            while let Some(output) = output_tasks.front_mut() {
                match (&mut output.task).await {
                    Ok(()) => {}
                    Err(error) if error.is_cancelled() => {}
                    Err(error) => {
                        tracing::warn!(target: LOG_TARGET, %error, pid, "process output task failed")
                    }
                }
                output_tasks.pop_front();
            }
        };
        if tokio::time::timeout(PROCESS_OUTPUT_DRAIN_TIMEOUT, drain)
            .await
            .is_ok()
        {
            return fully_drained;
        }

        let mut writers_open = false;
        // Inspect every source, including stderr behind a backpressured stdout.
        // Closed sources can finish delivering their finite buffered output at
        // the consumer's pace. Open sources keep the inherited-writer deadline.
        for output in output_tasks.iter() {
            if output.task.is_finished() {
                continue;
            }
            match output.writers_closed() {
                Ok(true) => {}
                Ok(false) => {
                    writers_open = true;
                    if abort_on_timeout {
                        output.task.abort();
                        fully_drained = false;
                    }
                }
                Err(error) => {
                    // An observation failure is not evidence for killing the
                    // process group. Bound our own reader and report the error.
                    tracing::warn!(target: LOG_TARGET, %error, pid, "failed to observe process output closure");
                    output.task.abort();
                    fully_drained = false;
                }
            }
        }
        if writers_open {
            tracing::warn!(target: LOG_TARGET,
                process_id = process_id.as_str(),
                pid,
                drain_timeout_ms = PROCESS_OUTPUT_DRAIN_TIMEOUT.as_millis() as u64,
                abort_on_timeout,
                "process output writers remained open after drain deadline"
            );
        }
        if !abort_on_timeout {
            return !writers_open;
        }
    }
}
