#[cfg(unix)]
mod unix;
#[cfg(windows)]
mod windows;

#[cfg(unix)]
fn main() {
    unix::run();
}

#[cfg(windows)]
fn main() -> Result<(), pplx_space_spaced_guest_windows::RunError> {
    windows::run()
}
