#[cfg(unix)]
mod unix;
#[cfg(windows)]
mod windows;

#[cfg(unix)]
fn main() {
    unix::run();
}

#[cfg(windows)]
fn main() -> std::process::ExitCode {
    match windows::run() {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("ERROR: {error:#}");
            std::process::ExitCode::FAILURE
        }
    }
}
