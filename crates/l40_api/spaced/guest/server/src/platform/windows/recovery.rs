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
    Foundation::{ERROR_ALREADY_EXISTS, ERROR_FILE_NOT_FOUND, WAIT_OBJECT_0},
    System::{
        JobObjects::*,
        Services::*,
        SystemServices::{JOB_OBJECT_QUERY, JOB_OBJECT_TERMINATE},
        Threading::*,
    },
};

mod namespace;

const JOB: &str = r"SPACE.GuestRecovery\Workloads";
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
    namespace::ensure()?;
    let name = wide(JOB);
    // SAFETY: NUL-terminated name, default SYSTEM security, owned result.
    let handle = unsafe { CreateJobObjectW(std::ptr::null(), name.as_ptr()) };
    if handle.is_null() {
        return Err(io::Error::last_os_error());
    }
    let already_exists =
        io::Error::last_os_error().raw_os_error() == Some(ERROR_ALREADY_EXISTS as i32);
    // SAFETY: successful CreateJobObjectW transfers one handle.
    let job = unsafe { OwnedHandle::from_raw_handle(handle) };
    if already_exists {
        return Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            "workload job already belongs to a generation",
        ));
    }
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
    namespace::ensure()?;
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

/// Seed ABI. Both periodic and event-driven invocations retry stopped services.
/// `--expired-only` remains accepted for installed tasks; healthy running
/// services are preserved, while expired startup remains independently bounded.
pub fn recover(_expired_only: bool) -> Result<(), RecoveryError> {
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
    let lock_deadline = Instant::now() + Duration::from_secs(5);
    let store = loop {
        match SlotStore::open(root) {
            Err(UpdateError::Busy) if Instant::now() < lock_deadline => {
                std::thread::sleep(Duration::from_millis(50))
            }
            result => break result?,
        }
    };
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
    let journal = match store.journal() {
        Ok(journal) => Some(journal),
        // Never overwrite unreadable authority: transient I/O may hide a commit.
        Err(UpdateError::Json(_) | UpdateError::Journal(_)) if !active || startup_expired => None,
        Err(UpdateError::Io(error))
            if error.kind() == io::ErrorKind::NotFound && (!active || startup_expired) =>
        {
            None
        }
        Err(error) => return Err(error.into()),
    };
    let rollback_mismatch = if let Some(journal) = &journal
        && matches!(journal.state, JournalState::RolledBack { .. })
    {
        match store.running_selection() {
            Ok(selected) => selected != journal.committed,
            Err(UpdateError::Io(error)) if error.kind() != io::ErrorKind::NotFound => {
                return Err(error.into());
            }
            // Malformed/missing current is repairable from journal authority.
            Err(_) => true,
        }
    } else {
        false
    };
    if !needs_recovery(
        journal.as_ref().map(|j| &j.state),
        active,
        startup_expired,
        rollback_mismatch,
        boot,
        now,
    ) {
        if let Some(journal) = &journal {
            let selected = match &journal.state {
                JournalState::Trial { attempt, .. } => &attempt.target,
                _ => &journal.committed,
            };
            configure_executable(&service, root, selected.slot)?;
        }
        return Ok(());
    }
    // The journal lock excludes Commit during termination/selection. A commit
    // that won first cannot be rolled back, but expired StartPending still
    // restarts committed bytes if readiness never reached SCM.
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
    configure_executable(&service, root, selected.slot)?;
    record_start_deadline(boot_clock()?.1.saturating_add(30_000))?;
    drop(store);
    service.start::<&str>(&[])?;
    Ok(())
}

pub(super) fn has_descendants() -> io::Result<bool> {
    namespace::ensure()?;
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

fn configure_executable(
    service: &windows_service::service::Service,
    root: &Path,
    slot: pplx_space_spaced_update::Slot,
) -> io::Result<()> {
    let executable = root.join("slots").join(slot.name()).join("spaced-svc.exe");
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
    Ok(())
}

fn needs_recovery(
    state: Option<&JournalState>,
    active: bool,
    startup_expired: bool,
    rollback_mismatch: bool,
    boot: uuid::Uuid,
    now: u64,
) -> bool {
    // A failed start (including dependency failure) must not lose its retry
    // owner once the journal has settled. The periodic task is that backstop.
    if !active || startup_expired {
        return true;
    }
    match state {
        Some(JournalState::Armed { .. } | JournalState::Restarting { .. }) => true,
        Some(JournalState::Trial { attempt, .. }) => {
            attempt.boot != boot || now >= attempt.confirmation_deadline_ms
        }
        Some(JournalState::RolledBack { .. }) => rollback_mismatch,
        _ => false,
    }
}

#[cfg(test)]
mod tests;
