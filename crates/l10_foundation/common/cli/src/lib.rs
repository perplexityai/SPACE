use std::fmt;
use std::io::IsTerminal;

/// Bootstrap quiet logging for a SPACE CLI binary.
///
/// CLI tools default to a quiet `warn` level so library traces and startup
/// banners don't leak into user-visible output. The user can still raise the
/// global level with `RUST_LOG`.
///
/// Logs are written to **stderr** so they don't pollute stdout pipelines like
/// `space-tmplmgr list | grep foo`. ANSI colors are only emitted when that
/// same stderr stream is a TTY, so output captured into a pipe, file, or
/// systemd journal stays plaintext. (Whichever fd we write to is the fd we
/// check — checking a different fd was a bug in earlier versions.)
pub fn init_logging(service_name: &str) {
    init_logging_inner(service_name, false);
}

/// Bootstrap logging and emit the standard startup banner.
///
/// Use this for job-like CLIs where version/git metadata is useful in logs.
/// One-shot query/debug commands should use [`init_logging`] instead.
pub fn init_logging_with_startup_banner(service_name: &str) {
    init_logging_inner(service_name, true);
}

fn init_logging_inner(service_name: &str, emit_startup_banner: bool) {
    let env_filter = tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| {
        if emit_startup_banner {
            // `warn` keeps internal library logs silent; the second clause keeps
            // the canonical "<service> starting up …" banner visible by default.
            let directive = format!("warn,{}=info", pplx_space_observability::STARTUP_TARGET);
            tracing_subscriber::EnvFilter::new(directive)
        } else {
            tracing_subscriber::EnvFilter::new("warn")
        }
    });

    let use_ansi = std::io::stderr().is_terminal();

    match tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .with_target(true)
        .with_writer(std::io::stderr)
        .with_ansi(use_ansi)
        .try_init()
    {
        Ok(()) => {}
        Err(_already_initialized) => {}
    }

    if emit_startup_banner {
        pplx_space_observability::log_startup_banner(service_name);
    }
}

pub fn exit_error(error: impl fmt::Display) -> ! {
    eprintln!("ERROR: {error}");
    std::process::exit(1);
}

pub fn exit_message(message: impl fmt::Display) -> ! {
    eprintln!("{message}");
    std::process::exit(1);
}

pub fn unwrap_or_exit<T, E>(result: Result<T, E>, context: impl fmt::Display) -> T
where
    E: fmt::Display,
{
    match result {
        Ok(value) => value,
        Err(error) => exit_error(format!("{context}: {error}")),
    }
}
