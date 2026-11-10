//! Only SYSTEM may create or open the workload namespace. A tenant can create
//! the same textual alias with a different boundary without affecting this one.
use super::{checked, wide};
use std::{io, sync::OnceLock};
use windows_sys::Win32::{
    Foundation::{ERROR_ALREADY_EXISTS, LocalFree},
    Security::{
        Authorization::{ConvertStringSecurityDescriptorToSecurityDescriptorW, SDDL_REVISION_1},
        CreateWellKnownSid, SECURITY_ATTRIBUTES, WinLocalSystemSid,
    },
    System::Threading::{
        AddSIDToBoundaryDescriptor, CreateBoundaryDescriptorW, CreatePrivateNamespaceW,
        DeleteBoundaryDescriptor, OpenPrivateNamespaceW,
    },
};

pub(super) const ALIAS: &str = "SPACE.GuestRecovery";

pub(super) fn ensure() -> io::Result<()> {
    // The alias must remain mapped while any job handles are in use. Retain the
    // namespace until process exit, including in the short recovery process.
    // Store the opaque handle as an integer; no caller can use or close it.
    static NAMESPACE: OnceLock<Result<usize, i32>> = OnceLock::new();
    NAMESPACE
        .get_or_init(|| open().map_err(|error| error.raw_os_error().unwrap_or(1)))
        .as_ref()
        .map(|_| ())
        .map_err(|code| io::Error::from_raw_os_error(*code))
}

fn open() -> io::Result<usize> {
    let name = wide(ALIAS);
    // SAFETY: terminated descriptor name, no optional flags.
    let mut boundary = unsafe { CreateBoundaryDescriptorW(name.as_ptr(), 0) };
    if boundary.is_null() {
        return Err(io::Error::last_os_error());
    }
    let result = (|| {
        // A DWORD-aligned buffer larger than SECURITY_MAX_SID_SIZE.
        let mut sid = [0u32; 17];
        let mut size = std::mem::size_of_val(&sid) as u32;
        // SAFETY: bounded writable SID buffer, no domain SID for LocalSystem.
        checked(unsafe {
            CreateWellKnownSid(
                WinLocalSystemSid,
                std::ptr::null_mut(),
                sid.as_mut_ptr().cast(),
                &mut size,
            )
        })?;
        // SAFETY: live boundary and initialized SID; the API may replace boundary.
        checked(unsafe { AddSIDToBoundaryDescriptor(&mut boundary, sid.as_mut_ptr().cast()) })?;
        let sddl = wide("D:P(A;;GA;;;SY)");
        let mut descriptor = std::ptr::null_mut();
        // SAFETY: terminated SDDL and writable allocated-descriptor output.
        checked(unsafe {
            ConvertStringSecurityDescriptorToSecurityDescriptorW(
                sddl.as_ptr(),
                SDDL_REVISION_1,
                &mut descriptor,
                std::ptr::null_mut(),
            )
        })?;
        let attributes = SECURITY_ATTRIBUTES {
            nLength: std::mem::size_of::<SECURITY_ATTRIBUTES>() as u32,
            lpSecurityDescriptor: descriptor,
            bInheritHandle: 0,
        };
        // SAFETY: live boundary/descriptor and terminated alias.
        let mut handle = unsafe { CreatePrivateNamespaceW(&attributes, boundary, name.as_ptr()) };
        let error = handle.is_null().then(io::Error::last_os_error);
        // SAFETY: descriptor was allocated by ConvertStringSecurityDescriptor.
        unsafe { LocalFree(descriptor) };
        if let Some(error) = error {
            if error.raw_os_error() != Some(ERROR_ALREADY_EXISTS as i32) {
                return Err(error);
            }
            // SAFETY: matching SYSTEM boundary; never open a same-name namespace
            // with a weaker boundary or substitute a global object on failure.
            handle = unsafe { OpenPrivateNamespaceW(boundary, name.as_ptr()) };
            if handle.is_null() {
                return Err(io::Error::last_os_error());
            }
        }
        Ok(handle as usize)
    })();
    // SAFETY: uniquely owned boundary, after create/open no longer uses it.
    unsafe { DeleteBoundaryDescriptor(boundary) };
    result
}

#[cfg(test)]
mod tests;
