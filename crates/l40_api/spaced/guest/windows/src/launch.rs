//! Create the child suspended, assign its Job Object, then allow execution.
use super::identity::Identity;
use std::{
    collections::{BTreeMap, HashMap},
    fs::File,
    io,
    os::windows::io::{AsRawHandle, FromRawHandle, OwnedHandle},
    path::Path,
};
use windows_sys::Win32::{
    Foundation::{HANDLE_FLAG_INHERIT, SetHandleInformation, WAIT_OBJECT_0},
    Security::SECURITY_ATTRIBUTES,
    System::{
        Environment::{CreateEnvironmentBlock, DestroyEnvironmentBlock},
        JobObjects::*,
        Pipes::CreatePipe,
        Threading::*,
    },
};

pub(super) struct Child {
    pub(super) process: OwnedHandle,
    pub(super) job: std::sync::Arc<OwnedHandle>,
    pub(super) pid: u32,
    pub(super) stdin: File,
    pub(super) stdout: File,
    pub(super) stderr: File,
}
fn wide(s: &str) -> io::Result<Vec<u16>> {
    super::identity::wide(s).map_err(io::Error::other)
}
fn checked(ok: i32) -> io::Result<()> {
    if ok == 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}
fn pipe() -> io::Result<(OwnedHandle, OwnedHandle)> {
    let mut read = std::ptr::null_mut();
    let mut write = std::ptr::null_mut();
    let security = SECURITY_ATTRIBUTES {
        nLength: std::mem::size_of::<SECURITY_ATTRIBUTES>() as u32,
        lpSecurityDescriptor: std::ptr::null_mut(),
        bInheritHandle: 1,
    };
    // SAFETY: two output handles and initialized security attributes are live.
    checked(unsafe { CreatePipe(&mut read, &mut write, &security, 0) })?;
    // SAFETY: successful CreatePipe returned the uniquely owned read handle.
    let read = unsafe { OwnedHandle::from_raw_handle(read) };
    // SAFETY: successful CreatePipe returned the uniquely owned write handle.
    let write = unsafe { OwnedHandle::from_raw_handle(write) };
    Ok((read, write))
}
struct Attributes(Vec<usize>);
impl Drop for Attributes {
    fn drop(&mut self) {
        // SAFETY: initialized attribute allocation remains live until this call.
        unsafe { DeleteProcThreadAttributeList(self.0.as_mut_ptr().cast()) }
    }
}

pub(super) fn quote(arg: &str) -> String {
    let mut result = String::from("\"");
    let mut slashes = 0;
    for ch in arg.chars() {
        if ch == '\\' {
            slashes += 1;
            continue;
        }
        if ch == '"' {
            result.extend(std::iter::repeat_n('\\', slashes * 2 + 1));
            result.push(ch);
        } else {
            result.extend(std::iter::repeat_n('\\', slashes));
            result.push(ch);
        }
        slashes = 0;
    }
    result.extend(std::iter::repeat_n('\\', slashes * 2));
    result.push('"');
    result
}
fn environment(identity: &Identity, overrides: &HashMap<String, String>) -> io::Result<Vec<u16>> {
    let mut block = std::ptr::null_mut();
    // SAFETY: token is live; successful call allocates a double-NUL Unicode block.
    unsafe {
        checked(CreateEnvironmentBlock(
            &mut block,
            identity.token().as_raw_handle(),
            0,
        ))?;
    }
    struct Environment(*mut std::ffi::c_void);
    impl Drop for Environment {
        fn drop(&mut self) {
            // SAFETY: unique block returned by CreateEnvironmentBlock.
            unsafe {
                DestroyEnvironmentBlock(self.0);
            }
        }
    }
    let block = Environment(block);
    let mut values = BTreeMap::new();
    let mut index = 0;
    // Windows guarantees a double-NUL terminated block. Advance only after
    // inspecting a valid element; no speculative reads beyond the terminator.
    let ptr = block.0.cast::<u16>();
    let mut string = Vec::new();
    loop {
        let current = ptr.wrapping_add(index);
        // SAFETY: index is the next element of the guaranteed terminated block.
        let unit = unsafe { *current };
        index += 1;
        if unit != 0 {
            string.push(unit);
            continue;
        }
        if string.is_empty() {
            break;
        }
        let text = String::from_utf16(&string).map_err(io::Error::other)?;
        string.clear();
        if let Some((key, value)) = text.split_once('=')
            && !key.is_empty()
        {
            values.insert(key.to_uppercase(), (key.to_owned(), value.to_owned()));
        }
    }
    for (key, value) in overrides {
        values.insert(key.to_uppercase(), (key.clone(), value.clone()));
    }
    let mut result = Vec::new();
    for (_, (key, value)) in values {
        result.extend(wide(&format!("{key}={value}"))?);
    }
    result.push(0);
    if result.len() > 32767 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "Windows environment exceeds limit",
        ));
    }
    Ok(result)
}
pub(super) fn spawn(
    identity: &Identity,
    argv: &[String],
    cwd: &Path,
    env: &HashMap<String, String>,
) -> io::Result<Child> {
    let program = argv
        .first()
        .ok_or_else(|| io::Error::from(io::ErrorKind::InvalidInput))?;
    let executable = Path::new(program);
    let executable = if executable.is_absolute() {
        executable.to_path_buf()
    } else if executable.components().count() == 1 {
        let system = std::env::var_os("SystemRoot")
            .ok_or_else(|| io::Error::from(io::ErrorKind::NotFound))?;
        let system = Path::new(&system).join("System32");
        if program.eq_ignore_ascii_case("powershell.exe") {
            system.join("WindowsPowerShell/v1.0/powershell.exe")
        } else {
            system.join(executable)
        }
    } else {
        cwd.join(executable)
    };
    let executable = super::path::LocalPath::parse(
        executable
            .to_str()
            .ok_or_else(|| io::Error::from(io::ErrorKind::InvalidInput))?,
        cwd,
    )
    .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error.message().to_owned()))?;
    let executable = executable.as_path();
    if !executable
        .extension()
        .is_some_and(|x| x.eq_ignore_ascii_case("exe"))
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "only direct .exe commands are supported",
        ));
    }
    // Check access under the selected account, not the privileged service.
    identity.run(|| File::open(executable).map(drop))?;
    let application = wide(&executable.to_string_lossy())?;
    let mut command = wide(&argv.iter().map(|s| quote(s)).collect::<Vec<_>>().join(" "))?;
    if command.len() > 32767 {
        return Err(io::Error::from(io::ErrorKind::InvalidInput));
    }
    let cwd = wide(&cwd.to_string_lossy())?;
    let environment = environment(identity, env)?;
    let (stdin_read, stdin_write) = pipe()?;
    let (stdout_read, stdout_write) = pipe()?;
    let (stderr_read, stderr_write) = pipe()?;
    // SAFETY: owned pipe handles; clearing inheritance affects only parent ends.
    unsafe {
        for h in [&stdin_write, &stdout_read, &stderr_read] {
            checked(SetHandleInformation(
                h.as_raw_handle(),
                HANDLE_FLAG_INHERIT,
                0,
            ))?;
        }
    }
    let handles = [
        stdin_read.as_raw_handle(),
        stdout_write.as_raw_handle(),
        stderr_write.as_raw_handle(),
    ];
    let mut size = 0;
    // SAFETY: null first call asks Windows for the required allocation size.
    unsafe {
        InitializeProcThreadAttributeList(std::ptr::null_mut(), 1, 0, &mut size);
    }
    let mut storage = vec![0usize; size.div_ceil(std::mem::size_of::<usize>())];
    // SAFETY: aligned allocation has at least the size Windows requested.
    unsafe {
        checked(InitializeProcThreadAttributeList(
            storage.as_mut_ptr().cast(),
            1,
            0,
            &mut size,
        ))?;
    }
    let mut attributes = Attributes(storage);
    // SAFETY: the attribute list and exact inherited handles live through create.
    unsafe {
        checked(UpdateProcThreadAttribute(
            attributes.0.as_mut_ptr().cast(),
            0,
            PROC_THREAD_ATTRIBUTE_HANDLE_LIST as usize,
            handles.as_ptr().cast(),
            std::mem::size_of_val(&handles),
            std::ptr::null_mut(),
            std::ptr::null(),
        ))?;
    }
    let startup = STARTUPINFOEXW {
        StartupInfo: STARTUPINFOW {
            cb: std::mem::size_of::<STARTUPINFOEXW>() as u32,
            dwFlags: STARTF_USESTDHANDLES,
            hStdInput: handles[0],
            hStdOutput: handles[1],
            hStdError: handles[2],
            ..Default::default()
        },
        lpAttributeList: attributes.0.as_mut_ptr().cast(),
    };
    // SAFETY: unnamed noninheritable job, owned by this function after success.
    let raw_job = unsafe { CreateJobObjectW(std::ptr::null(), std::ptr::null()) };
    if raw_job.is_null() {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: successful CreateJobObjectW transfers the owned handle.
    let job = unsafe { OwnedHandle::from_raw_handle(raw_job) };
    let mut limits = JOBOBJECT_EXTENDED_LIMIT_INFORMATION::default();
    limits.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;
    // SAFETY: correctly sized initialized information struct, live job handle.
    unsafe {
        checked(SetInformationJobObject(
            job.as_raw_handle(),
            JobObjectExtendedLimitInformation,
            (&limits as *const JOBOBJECT_EXTENDED_LIMIT_INFORMATION).cast(),
            std::mem::size_of_val(&limits) as u32,
        ))?;
    }
    let mut process = PROCESS_INFORMATION::default();
    // SAFETY: every pointer references initialized storage kept live for this call.
    unsafe {
        checked(CreateProcessAsUserW(
            identity.token().as_raw_handle(),
            application.as_ptr(),
            command.as_mut_ptr(),
            std::ptr::null(),
            std::ptr::null(),
            1,
            CREATE_SUSPENDED
                | CREATE_UNICODE_ENVIRONMENT
                | EXTENDED_STARTUPINFO_PRESENT
                | CREATE_NO_WINDOW,
            environment.as_ptr().cast(),
            cwd.as_ptr(),
            &startup.StartupInfo,
            &mut process,
        ))?;
    }
    // SAFETY: both handles are uniquely returned by successful process creation.
    let process_handle = unsafe { OwnedHandle::from_raw_handle(process.hProcess) };
    // SAFETY: successful process creation returned a uniquely owned thread handle.
    let thread = unsafe { OwnedHandle::from_raw_handle(process.hThread) };
    // SAFETY: child is still suspended; no instruction runs outside the job.
    if unsafe { AssignProcessToJobObject(job.as_raw_handle(), process_handle.as_raw_handle()) } == 0
    {
        let error = io::Error::last_os_error();
        // SAFETY: this is the exact owned suspended child, not a PID lookup.
        unsafe {
            TerminateProcess(process_handle.as_raw_handle(), 1);
        }
        // SAFETY: retain the process handle while waiting for termination.
        unsafe {
            WaitForSingleObject(process_handle.as_raw_handle(), 5000);
        }
        return Err(error);
    }
    // SAFETY: job assignment succeeded; resume the unique initial thread once.
    if unsafe { ResumeThread(thread.as_raw_handle()) } == u32::MAX {
        return Err(io::Error::last_os_error());
    }
    Ok(Child {
        process: process_handle,
        job: std::sync::Arc::new(job),
        pid: process.dwProcessId,
        stdin: File::from(stdin_write),
        stdout: File::from(stdout_read),
        stderr: File::from(stderr_read),
    })
}
pub(super) fn kill(job: &OwnedHandle) -> io::Result<()> {
    // SAFETY: caller owns a live job handle belonging only to this managed process tree.
    unsafe { checked(TerminateJobObject(job.as_raw_handle(), 1)) }
}
pub(super) fn wait(process: &OwnedHandle) -> io::Result<Option<u32>> {
    // SAFETY: process remains owned for the bounded wait.
    match unsafe { WaitForSingleObject(process.as_raw_handle(), 100) } {
        WAIT_OBJECT_0 => {
            let mut code = 0;
            // SAFETY: live process handle and writable exit-code storage.
            checked(unsafe { GetExitCodeProcess(process.as_raw_handle(), &mut code) })?;
            Ok(Some(code))
        }
        windows_sys::Win32::Foundation::WAIT_TIMEOUT => Ok(None),
        _ => Err(io::Error::last_os_error()),
    }
}

#[cfg(test)]
mod tests;
