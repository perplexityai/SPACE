//! One explicitly provisioned local account; never execute tenant work as SYSTEM.
use pplx_space_guest_process::GuestUserName;
use std::{
    io,
    marker::PhantomData,
    os::windows::io::{AsHandle, AsRawHandle, BorrowedHandle, FromRawHandle, OwnedHandle},
    path::PathBuf,
    rc::Rc,
};
use windows_sys::Win32::{
    Foundation::HANDLE,
    Security::{
        ImpersonateLoggedOnUser, LOGON32_LOGON_INTERACTIVE, LOGON32_PROVIDER_DEFAULT, LogonUserW,
        RevertToSelf,
    },
    UI::Shell::{GetUserProfileDirectoryW, LoadUserProfileW, PROFILEINFOW, UnloadUserProfile},
};
use zeroize::{Zeroize, Zeroizing};

#[derive(Debug, thiserror::Error)]
pub enum IdentityError {
    #[error("reading provisioned Windows identity: {0}")]
    Read(#[source] io::Error),
    #[error("invalid provisioned Windows identity")]
    Invalid,
    #[error("Windows account operation failed: {0}")]
    Windows(#[source] io::Error),
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Credential {
    user: String,
    password: String,
}
impl Drop for Credential {
    fn drop(&mut self) {
        self.password.zeroize();
    }
}

pub(super) struct Identity {
    pub(super) user: GuestUserName,
    pub(super) home: PathBuf,
    token: OwnedHandle,
    profile: LoadedProfile,
}
struct LoadedProfile(HANDLE);
// SAFETY: token/profile are immutable OS references. Impersonation applies to
// the calling thread only; final profile unloading occurs after the last Arc.
unsafe impl Send for LoadedProfile {}
// SAFETY: operations never mutate these handles; Windows supports concurrent use.
unsafe impl Sync for LoadedProfile {}
impl Drop for Identity {
    fn drop(&mut self) {
        // SAFETY: the unique profile returned by LoadUserProfileW; token is live.
        if unsafe { UnloadUserProfile(self.token.as_raw_handle(), self.profile.0) } == 0 {
            tracing::warn!(error = %io::Error::last_os_error(), "unloading guest profile failed");
        }
    }
}

pub(super) fn wide(value: &str) -> Result<Vec<u16>, IdentityError> {
    if value.contains('\0') {
        return Err(IdentityError::Invalid);
    }
    Ok(value.encode_utf16().chain(Some(0)).collect())
}
impl Identity {
    pub(super) fn load() -> Result<Option<Self>, IdentityError> {
        let program_data = std::env::var_os("ProgramData").ok_or(IdentityError::Invalid)?;
        let path = PathBuf::from(program_data)
            .join("SPACE")
            .join("identity.json");
        let raw = match std::fs::read(path) {
            Ok(raw) if raw.len() <= 4096 => Zeroizing::new(raw),
            Ok(_) => return Err(IdentityError::Invalid),
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(IdentityError::Read(e)),
        };
        let credential: Credential =
            serde_json::from_slice(&raw).map_err(|_| IdentityError::Invalid)?;
        let user =
            GuestUserName::parse(credential.user.clone()).map_err(|_| IdentityError::Invalid)?;
        let mut name = wide(user.as_str())?;
        let password = Zeroizing::new(wide(&credential.password)?);
        let domain = wide(".")?;
        let mut token = std::ptr::null_mut();
        // SAFETY: terminated strings and writable token pointer live for the call.
        if unsafe {
            LogonUserW(
                name.as_ptr(),
                domain.as_ptr(),
                password.as_ptr(),
                LOGON32_LOGON_INTERACTIVE,
                LOGON32_PROVIDER_DEFAULT,
                &mut token,
            )
        } == 0
        {
            return Err(IdentityError::Windows(io::Error::last_os_error()));
        }
        // SAFETY: successful LogonUserW transfers ownership of a token handle.
        let token = unsafe { OwnedHandle::from_raw_handle(token) };
        let mut profile = PROFILEINFOW {
            dwSize: std::mem::size_of::<PROFILEINFOW>() as u32,
            dwFlags: 1,
            lpUserName: name.as_mut_ptr(),
            ..Default::default()
        };
        // SAFETY: profile buffer and token remain live; caller is the SYSTEM service.
        if unsafe { LoadUserProfileW(token.as_raw_handle(), &mut profile) } == 0 {
            return Err(IdentityError::Windows(io::Error::last_os_error()));
        }
        let mut identity = Self {
            user,
            token,
            profile: LoadedProfile(profile.hProfile),
            home: PathBuf::new(),
        };
        let mut home = vec![0u16; 32768];
        let mut length = home.len() as u32;
        // SAFETY: the buffer holds exactly length UTF-16 units and token is live.
        if unsafe {
            GetUserProfileDirectoryW(
                identity.token.as_raw_handle(),
                home.as_mut_ptr(),
                &mut length,
            )
        } == 0
        {
            return Err(IdentityError::Windows(io::Error::last_os_error()));
        }
        let end = home
            .iter()
            .position(|&c| c == 0)
            .ok_or(IdentityError::Invalid)?;
        identity.home = String::from_utf16(home.get(..end).ok_or(IdentityError::Invalid)?)
            .map_err(|_| IdentityError::Invalid)?
            .into();
        Ok(Some(identity))
    }
    pub(super) fn token(&self) -> BorrowedHandle<'_> {
        self.token.as_handle()
    }
    pub(super) fn check_user(&self, user: Option<&str>) -> Result<(), tonic::Status> {
        if user.is_some_and(|u| !u.eq_ignore_ascii_case(self.user.as_str())) {
            return Err(super::unsupported(
                "requested Windows identity is not provisioned",
            ));
        }
        Ok(())
    }
    /// Execute only synchronous work. The thread-bound guard cannot cross await.
    pub(super) fn run<T>(&self, work: impl FnOnce() -> io::Result<T>) -> io::Result<T> {
        // SAFETY: token belongs to a successful local logon and remains live.
        if unsafe { ImpersonateLoggedOnUser(self.token().as_raw_handle()) } == 0 {
            return Err(io::Error::last_os_error());
        }
        let _guard = Impersonation(PhantomData);
        work()
    }
}
struct Impersonation(PhantomData<Rc<()>>);
impl Drop for Impersonation {
    fn drop(&mut self) {
        // SAFETY: this guard never leaves the thread that installed impersonation.
        if unsafe { RevertToSelf() } == 0 {
            // Never return a pooled worker with the wrong security context.
            std::process::abort();
        }
    }
}
