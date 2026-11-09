//! SCM owns the daemon; Task Scheduler invokes this short recovery mode from
//! the committed slot. No recovery process survives a successful reconciliation.
use pplx_space_spaced_guest_update::{
    SlotStore, UPDATE_ROOT, UpdateError, boot_clock, record_start_deadline, start_deadline,
};
use pplx_space_spaced_update::JournalState;
use std::{
    fs::OpenOptions,
    io,
    os::windows::io::{AsRawHandle, FromRawHandle, OwnedHandle},
    path::Path,
    time::{Duration, Instant},
};
use windows_service::{
    service::{ServiceAccess, ServiceState},
    service_manager::{ServiceManager, ServiceManagerAccess},
};
use windows_sys::Win32::{
    Foundation::{ERROR_FILE_NOT_FOUND, WAIT_OBJECT_0},
    System::{
        JobObjects::*,
        Services::*,
        SystemServices::{JOB_OBJECT_QUERY, JOB_OBJECT_TERMINATE},
        Threading::*,
    },
};

const JOB: &str = r"Global\SPACE.GuestWorkloads";
const SERVICE: &str = "spaced";

#[derive(Debug, thiserror::Error)]
pub enum RecoveryError {
    #[error(transparent)]
    Storage(#[from] UpdateError),
    #[error("Windows guest recovery I/O failed: {0}")]
    Io(#[from] io::Error),
    #[error("Windows guest recovery service operation failed: {0}")]
    Service(#[from] windows_service::Error),
}
fn wide(value: &str) -> Vec<u16> {
    value.encode_utf16().chain(Some(0)).collect()
}
fn checked(ok: i32) -> io::Result<()> {
    if ok == 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}

/// Keep this handle until process exit. Every child (including nested workload
/// jobs) inherits membership; abrupt daemon death closes its last owning handle.
pub(super) fn contain_service() -> io::Result<OwnedHandle> {
    let name = wide(JOB);
    // SAFETY: NUL-terminated name, default SYSTEM security, owned result.
    let handle = unsafe { CreateJobObjectW(std::ptr::null(), name.as_ptr()) };
    if handle.is_null() {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: successful CreateJobObjectW transfers one handle.
    let job = unsafe { OwnedHandle::from_raw_handle(handle) };
    let limits = JOBOBJECT_EXTENDED_LIMIT_INFORMATION {
        BasicLimitInformation: JOBOBJECT_BASIC_LIMIT_INFORMATION {
            LimitFlags: JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE,
            ..Default::default()
        },
        ..Default::default()
    };
    // SAFETY: live job and initialized fixed-size limits.
    checked(unsafe {
        SetInformationJobObject(
            job.as_raw_handle(),
            JobObjectExtendedLimitInformation,
            (&limits as *const JOBOBJECT_EXTENDED_LIMIT_INFORMATION).cast(),
            std::mem::size_of_val(&limits) as u32,
        )
    })?;
    // SAFETY: pseudo-handle for this process, which joins before serving work.
    let process = unsafe { GetCurrentProcess() };
    // SAFETY: live job and current process, before any workload children exist.
    checked(unsafe { AssignProcessToJobObject(job.as_raw_handle(), process) })?;
    Ok(job)
}

fn terminate_generation(pid: Option<u32>) -> io::Result<()> {
    let name = wide(JOB);
    // SAFETY: query/terminate rights only, non-inherited handle, terminated name.
    let raw = unsafe { OpenJobObjectW(JOB_OBJECT_QUERY | JOB_OBJECT_TERMINATE, 0, name.as_ptr()) };
    let job = if raw.is_null() {
        let error = io::Error::last_os_error();
        if error.raw_os_error() != Some(ERROR_FILE_NOT_FOUND as i32) {
            return Err(error);
        }
        None
    } else {
        // SAFETY: successful OpenJobObjectW transfers one handle.
        Some(unsafe { OwnedHandle::from_raw_handle(raw) })
    };
    // Pin and validate SCM's process before terminating the job, so PID reuse
    // cannot turn a stale service observation into termination of another app.
    let process = if let Some(pid) = pid {
        // SAFETY: limited query, termination and synchronization for observed PID.
        let raw = unsafe {
            OpenProcess(
                PROCESS_TERMINATE | PROCESS_SYNCHRONIZE | PROCESS_QUERY_LIMITED_INFORMATION,
                0,
                pid,
            )
        };
        if raw.is_null() {
            let error = io::Error::last_os_error();
            if error.raw_os_error() != Some(87) {
                return Err(error);
            }
            None
        } else {
            // SAFETY: successful OpenProcess transfers one handle.
            let process = unsafe { OwnedHandle::from_raw_handle(raw) };
            let mut name = vec![0u16; 32768];
            let mut length = name.len() as u32;
            // SAFETY: queryable process and writable buffer with explicit length.
            checked(unsafe {
                QueryFullProcessImageNameW(
                    process.as_raw_handle(),
                    0,
                    name.as_mut_ptr(),
                    &mut length,
                )
            })?;
            let name = String::from_utf16(
                name.get(..length as usize)
                    .ok_or_else(|| io::Error::other("invalid image name length"))?,
            )
            .map_err(io::Error::other)?;
            if !["a", "b"].into_iter().any(|slot| {
                name.eq_ignore_ascii_case(&format!(r"{UPDATE_ROOT}\slots\{slot}\spaced-svc.exe"))
            }) {
                return Err(io::Error::other("SCM process is outside the agent slots"));
            }
            Some(process)
        }
    } else {
        None
    };
    if let Some(job) = &job {
        // SAFETY: live job with terminate access; terminates nested workload jobs.
        checked(unsafe { TerminateJobObject(job.as_raw_handle(), 1) })?;
    }
    if let Some(process) = process {
        // SAFETY: pinned, validated process; already-exited is harmless.
        unsafe { TerminateProcess(process.as_raw_handle(), 1) };
        // SAFETY: live synchronizable process, bounded wait.
        if unsafe { WaitForSingleObject(process.as_raw_handle(), 10_000) } != WAIT_OBJECT_0 {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "agent did not terminate",
            ));
        }
    }
    let until = Instant::now() + Duration::from_secs(10);
    if let Some(job) = job {
        loop {
            let mut accounting = JOBOBJECT_BASIC_ACCOUNTING_INFORMATION::default();
            // SAFETY: live queryable job and writable fixed-size accounting.
            checked(unsafe {
                QueryInformationJobObject(
                    job.as_raw_handle(),
                    JobObjectBasicAccountingInformation,
                    (&mut accounting as *mut JOBOBJECT_BASIC_ACCOUNTING_INFORMATION).cast(),
                    std::mem::size_of_val(&accounting) as u32,
                    std::ptr::null_mut(),
                )
            })?;
            if accounting.ActiveProcesses == 0 {
                break;
            }
            if Instant::now() >= until {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "workload descendants did not terminate",
                ));
            }
            std::thread::sleep(Duration::from_millis(25));
        }
    }
    Ok(())
}

/// Seed ABI. The periodic task uses `expired_only`; boot and SCM failure use
/// unconditional recovery of a stopped service. Running committed services are
/// never restarted by a stale task invocation.
pub fn recover(expired_only: bool) -> Result<(), RecoveryError> {
    let root = Path::new(UPDATE_ROOT);
    let recovery = OpenOptions::new()
        .read(true)
        .write(true)
        .open(root.join("recovery-lock"))?;
    match recovery.try_lock() {
        Ok(()) => {}
        Err(std::fs::TryLockError::WouldBlock) => return Ok(()),
        Err(std::fs::TryLockError::Error(error)) => return Err(error.into()),
    }
    let store = SlotStore::open(root)?;
    store.initialize_boot_epoch()?;
    let (boot, now) = boot_clock()?;
    let manager = ServiceManager::local_computer(None::<&str>, ServiceManagerAccess::CONNECT)?;
    let service = manager.open_service(
        SERVICE,
        ServiceAccess::QUERY_STATUS | ServiceAccess::CHANGE_CONFIG | ServiceAccess::START,
    )?;
    let status = service.query_status()?;
    let active = status.current_state != ServiceState::Stopped;
    let startup_expired = if status.current_state == ServiceState::StartPending {
        match start_deadline() {
            Ok(deadline) => now >= deadline,
            Err(UpdateError::Io(error)) if error.kind() == io::ErrorKind::NotFound => true,
            Err(error) => return Err(error.into()),
        }
    } else {
        false
    };
    let maintenance_due = match store.journal() {
        Ok(journal) => match journal.state {
            JournalState::Armed { .. } | JournalState::Restarting { .. } => true,
            JournalState::Trial { attempt, .. } => {
                !active || attempt.boot != boot || now >= attempt.confirmation_deadline_ms
            }
            JournalState::RolledBack { .. } => store.running_selection()? != journal.committed,
            _ => false,
        },
        // Match the selector's corruption policy. Never overwrite unreadable
        // authority: a transient I/O failure can hide a durable new commit.
        Err(UpdateError::Json(_) | UpdateError::Journal(_)) if !active || startup_expired => true,
        Err(UpdateError::Io(error))
            if error.kind() == io::ErrorKind::NotFound && (!active || startup_expired) =>
        {
            true
        }
        Err(error) => return Err(error.into()),
    };
    if !maintenance_due && !startup_expired && (active || expired_only) {
        return Ok(());
    }
    // The journal lock excludes Commit while termination/selection executes.
    // A commit that won first is observed above and never killed by a late task.
    terminate_generation(status.process_id)?;
    let stop_deadline = Instant::now() + Duration::from_secs(10);
    while service.query_status()?.current_state != ServiceState::Stopped {
        if Instant::now() >= stop_deadline {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "SCM did not finish stopping agent",
            )
            .into());
        }
        std::thread::sleep(Duration::from_millis(25));
    }
    let selected = store.select_with_clock(boot_clock)?;
    let executable = root
        .join("slots")
        .join(selected.slot.name())
        .join("spaced-svc.exe");
    let command = wide(&format!("\"{}\"", executable.display()));
    // SAFETY: live service handle and terminated path; preserve all configuration
    // except the executable. SCM starts only after the selector is durable.
    checked(unsafe {
        ChangeServiceConfigW(
            service.raw_handle(),
            SERVICE_NO_CHANGE,
            SERVICE_NO_CHANGE,
            SERVICE_NO_CHANGE,
            command.as_ptr(),
            std::ptr::null(),
            std::ptr::null_mut(),
            std::ptr::null(),
            std::ptr::null(),
            std::ptr::null(),
            std::ptr::null(),
        )
    })?;
    record_start_deadline(boot_clock()?.1.saturating_add(30_000))?;
    drop(store);
    service.start::<&str>(&[])?;
    Ok(())
}

pub(super) fn has_descendants() -> io::Result<bool> {
    let name = wide(JOB);
    // SAFETY: terminated name and query-only, non-inheritable handle.
    let raw = unsafe { OpenJobObjectW(JOB_OBJECT_QUERY, 0, name.as_ptr()) };
    if raw.is_null() {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: successful OpenJobObjectW transfers a handle.
    let job = unsafe { OwnedHandle::from_raw_handle(raw) };
    // SAFETY: pseudo-handle to the calling daemon.
    let process = unsafe { GetCurrentProcess() };
    let mut member = 0;
    // SAFETY: live process/job handles and writable BOOL result.
    checked(unsafe { IsProcessInJob(process, job.as_raw_handle(), &mut member) })?;
    if member == 0 {
        return Err(io::Error::other("daemon is outside its workload job"));
    }
    let mut accounting = JOBOBJECT_BASIC_ACCOUNTING_INFORMATION::default();
    // SAFETY: live queryable job and sized, writable accounting structure.
    checked(unsafe {
        QueryInformationJobObject(
            job.as_raw_handle(),
            JobObjectBasicAccountingInformation,
            (&mut accounting as *mut JOBOBJECT_BASIC_ACCOUNTING_INFORMATION).cast(),
            std::mem::size_of_val(&accounting) as u32,
            std::ptr::null_mut(),
        )
    })?;
    Ok(accounting.ActiveProcesses > 1)
}
