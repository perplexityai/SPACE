//! Dedicated worker entrypoint; never starts an embedding application's main.
use std::process::ExitCode;

fn main() -> ExitCode {
    match pplx_space_spaced_guest_execution::run_filesystem_worker_from_environment() {
        Ok(true) => ExitCode::SUCCESS,
        Ok(false) => {
            eprintln!("this executable must be launched by an execution service");
            ExitCode::FAILURE
        }
        Err(error) => {
            eprintln!("{error}");
            ExitCode::FAILURE
        }
    }
}
