//! `archcheck`: check repository structure rules declared in `ARCHITECTURE.toml`.
//!
//! Exit status: 0 when no required-mode finding exists, 1 when one does, and 2
//! for configuration, usage or I/O errors.

use std::{
    env,
    ffi::OsString,
    fmt,
    io::{self, Write},
    path::PathBuf,
    process::ExitCode,
};

use pplx_space_archcheck::{
    BASELINE_FILE, Baseline, Context, LoadError, WriteBaselineError, check,
    context::find_workspace_root,
    report::{Format, UnknownFormatError, render, summary},
    skipped, write_baseline,
};
use thiserror::Error;

const USAGE: &str = "\
usage: archcheck [--root <path>] [--format human|rdjson]
                 [--base-baseline <path>]
       archcheck [--root <path>] --write-baseline

  --root <path>       repository root (default: the Cargo workspace containing
                      the current directory)
  --format <format>   human (default), or rdjson for reviewdog, with the
                      summary on stderr
  --base-baseline <path>
                      the base branch's architecture-baseline.toml, which
                      file_size.no_new_entries and no_raised_caps compare
                      with (skipped without it)
  --write-baseline    update or remove the entries of architecture-baseline.toml
                      to match the current tree; never adds one";

const EXIT_REQUIRED_FINDINGS: u8 = 1;
const EXIT_ERROR: u8 = 2;

/// A command line that cannot be run; printed with the usage text.
#[derive(Debug, Error)]
enum UsageError {
    #[error("argument {0:?} is not valid UTF-8")]
    NonUtf8Argument(OsString),
    #[error("{0} needs a value")]
    MissingValue(String),
    #[error("{0} takes no value")]
    UnexpectedValue(String),
    #[error("unexpected argument {0:?}")]
    UnexpectedArgument(String),
    #[error("--base-baseline cannot be combined with --write-baseline")]
    BaseBaselineWithWriteBaseline,
    #[error(transparent)]
    Format(#[from] UnknownFormatError),
}

#[derive(Debug, Error)]
enum RunError {
    #[error("could not read the current directory: {0}")]
    CurrentDir(#[source] io::Error),
    #[error(transparent)]
    Load(#[from] LoadError),
    #[error(transparent)]
    WriteBaseline(#[from] WriteBaselineError),
    #[error("could not write the output: {0}")]
    Output(#[source] io::Error),
}

struct Options {
    root: Option<PathBuf>,
    format: Format,
    base_baseline: Option<PathBuf>,
    write_baseline: bool,
}

fn main() -> ExitCode {
    let result = match parse_args(env::args_os().skip(1)) {
        Ok(Some(options)) => run(options),
        Ok(None) => write(io::stdout(), format_args!("{USAGE}\n")).map(|()| ExitCode::SUCCESS),
        Err(err) => {
            report_error(format_args!("archcheck: {err}\n{USAGE}"));
            return ExitCode::from(EXIT_ERROR);
        }
    };
    match result {
        Ok(code) => code,
        Err(err) => {
            report_error(format_args!("archcheck: error: {err}"));
            ExitCode::from(EXIT_ERROR)
        }
    }
}

fn run(options: Options) -> Result<ExitCode, RunError> {
    let root = match options.root {
        Some(root) => root,
        None => find_workspace_root(&env::current_dir().map_err(RunError::CurrentDir)?)?,
    };

    if options.write_baseline {
        let written = write_baseline(&root)?;
        let not_added: String = written
            .not_added
            .iter()
            .map(|line| format!("{line}\n"))
            .collect();
        write(
            io::stdout(),
            format_args!(
                "{not_added}archcheck: wrote {} file_size entries to {}\n",
                written.baseline.file_size.len(),
                root.join(BASELINE_FILE).display()
            ),
        )?;
        return Ok(ExitCode::SUCCESS);
    }

    let mut ctx = Context::load(&root)?;
    if let Some(path) = &options.base_baseline {
        ctx.base_baseline = Baseline::load_base(path)?;
    }
    let findings = check(&ctx);
    let report = render(options.format, &findings, ctx.files.len());
    // A line per skip, or warning about a skip, after the summary line.
    let skipped: String = skipped(&ctx)
        .iter()
        .map(|line| format!("archcheck: {line}\n"))
        .collect();
    if options.format == Format::Rdjson {
        // stdout holds only the document.
        write(io::stdout(), format_args!("{report}"))?;
        let summary = summary(&findings, ctx.files.len());
        write(io::stderr(), format_args!("{summary}\n{skipped}"))?;
    } else {
        write(io::stdout(), format_args!("{report}{skipped}"))?;
    }
    if findings.iter().any(|finding| finding.mode.is_required()) {
        Ok(ExitCode::from(EXIT_REQUIRED_FINDINGS))
    } else {
        Ok(ExitCode::SUCCESS)
    }
}

/// Writes and flushes, so a full disk or a closed pipe exits 2 rather than
/// panicking in `print!` or being lost when stdout is flushed at exit.
fn write(mut out: impl Write, text: fmt::Arguments<'_>) -> Result<(), RunError> {
    out.write_fmt(text)
        .and_then(|()| out.flush())
        .map_err(RunError::Output)
}

/// An error message for stderr. If stderr cannot be written either, the exit
/// status is all that is left to report it.
fn report_error(message: fmt::Arguments<'_>) {
    let _ = writeln!(io::stderr(), "{message}");
}

/// `Ok(None)` asks for help.
fn parse_args(args: impl Iterator<Item = OsString>) -> Result<Option<Options>, UsageError> {
    let mut options = Options {
        root: None,
        format: Format::Human,
        base_baseline: None,
        write_baseline: false,
    };
    let mut args = args.map(|arg| arg.into_string().map_err(UsageError::NonUtf8Argument));
    while let Some(arg) = args.next() {
        let arg = arg?;
        let (flag, inline_value) = match arg.split_once('=') {
            Some((flag, value)) if flag.starts_with("--") => (flag.to_owned(), Some(value.into())),
            _ => (arg, None),
        };
        let mut value = || match inline_value.clone() {
            Some(value) => Ok(value),
            None => args
                .next()
                .unwrap_or_else(|| Err(UsageError::MissingValue(flag.clone()))),
        };
        match flag.as_str() {
            "-h" | "--help" => return Ok(None),
            "--root" => options.root = Some(PathBuf::from(value()?)),
            "--format" => options.format = value()?.parse()?,
            "--base-baseline" => options.base_baseline = Some(PathBuf::from(value()?)),
            "--write-baseline" if inline_value.is_none() => options.write_baseline = true,
            "--write-baseline" => return Err(UsageError::UnexpectedValue(flag.clone())),
            other => return Err(UsageError::UnexpectedArgument(other.to_owned())),
        }
    }
    if options.write_baseline && options.base_baseline.is_some() {
        return Err(UsageError::BaseBaselineWithWriteBaseline);
    }
    Ok(Some(options))
}
