//! Compile restricted app data into systemd syntax; no tenant text is shell code.
use pplx_space_common_core::types::{RuntimeReadiness, TemplateRuntime};
use std::collections::BTreeMap;
use std::io::{self, Read};

pub(super) type Files = BTreeMap<String, Vec<u8>>;
pub(super) const MAX_BUNDLE_BYTES: usize = 4 * 1024 * 1024;

// Unit strings use C escapes and specifiers, including in Environment=.
// Dollar expansion is disabled with ':' on command directives only.
fn quoted(value: &str) -> String {
    let mut output = String::from("\"");
    for ch in value.chars() {
        match ch {
            '%' => output.push_str("%%"),
            '\\' => output.push_str("\\\\"),
            '"' => output.push_str("\\\""),
            ch if ch.is_ascii_control() => output.push_str(&format!("\\x{:02x}", ch as u8)),
            ch => output.push(ch),
        }
    }
    output.push('"');
    output
}

fn command(argv: &[String], directory: Option<&str>) -> String {
    let args = argv.iter().map(|v| quoted(v)).collect::<Vec<_>>().join(" ");
    if let Some(directory) = directory {
        // Older systemd executor serialization trims trailing path whitespace.
        // Keep those bytes in argv, whose quoting is stable across versions.
        format!(":/usr/bin/env --chdir={} -- {args}", quoted(directory))
    } else if argv
        .first()
        .is_some_and(|executable| executable.starts_with('/'))
    {
        format!(":{args}")
    } else {
        // env execs in place, preserving MainPID and signal status. '--' protects
        // options; assignment-shaped executable names are rejected below.
        format!(":/usr/bin/env -- {args}")
    }
}

fn validate_command(argv: &[String]) -> io::Result<()> {
    if argv.first().is_none_or(|executable| {
        executable.is_empty() || (!executable.starts_with('/') && executable.contains('='))
    }) {
        return Err(io::Error::other(
            "runtime executable must be nonempty; use an absolute path for names containing '='",
        ));
    }
    Ok(())
}

pub(super) fn render(runtime: &TemplateRuntime) -> Result<Files, io::Error> {
    let mut files = Files::new();
    files.insert(
        "runtime.toml".into(),
        runtime.canonical_bytes().map_err(io::Error::other)?,
    );
    let mut target = String::from(
        "[Unit]\nDescription=SPACE optional template apps\nDefaultDependencies=no\nAfter=space.target\nWants=",
    );
    for app in runtime.apps() {
        validate_command(app.command().argv())?;
        if let RuntimeReadiness::Exec(probe) = app.readiness() {
            validate_command(probe.argv())?;
        }
        // User= is a raw directive, not a quoted word list. Restrict it to
        // conventional account names or numeric UIDs, never arbitrary unit text.
        if !app
            .user()
            .as_str()
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || b"_.-".contains(&c))
        {
            return Err(io::Error::other(
                "runtime user must be an account name or numeric UID",
            ));
        }
        let directory = app.working_directory().expose();
        let indirect_directory = directory
            .ends_with(|c: char| c.is_whitespace() || c == '\\')
            .then_some(directory);
        target.push_str(&app.name().unit());
        target.push(' ');
        let mut unit = format!(
            r#"[Unit]
Description=SPACE template app {}
DefaultDependencies=no
After=local-fs.target space.target
Before=shutdown.target
Conflicts=shutdown.target
ConditionKernelCommandLine=!systemd.mask=space-apps.target
StartLimitIntervalSec=30
StartLimitBurst=5

[Service]
Type=exec
User={}
WorkingDirectory={}/.
Environment=PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
UnsetEnvironment=NOTIFY_SOCKET WATCHDOG_PID WATCHDOG_USEC
ExecStart={}
Slice=space-sessions.slice
TasksMax=infinity
OOMScoreAdjust=0
OOMPolicy=continue
KillMode=control-group
Restart=on-failure
RestartSec=2
TimeoutStartSec={}
TimeoutStopSec=5
IgnoreSIGPIPE=no
StandardOutput=journal
StandardError=journal
LogRateLimitIntervalSec=30
LogRateLimitBurst=1000
"#,
            app.name().as_str(),
            app.user().as_str(),
            // GuestPath excludes control characters. '/.' prevents a trailing
            // space or backslash from being consumed by the unit-file parser.
            if indirect_directory.is_some() {
                "/".into()
            } else {
                directory.replace('%', "%%")
            },
            command(app.command().argv(), indirect_directory),
            app.startup_timeout().as_secs()
        );
        for (key, value) in app.env().as_map() {
            unit.push_str(&format!(
                "Environment={}\n",
                quoted(&format!("{}={}", key.as_str(), value.expose()))
            ));
        }
        if let RuntimeReadiness::Exec(probe) = app.readiness() {
            let mut args = vec![
                "/bin/sh".into(),
                "-c".into(),
                "until \"$0\" \"$@\"; do sleep 0.3; done".into(),
            ];
            args.extend_from_slice(probe.argv());
            unit.push_str(&format!(
                "ExecStartPost={}\n",
                command(&args, indirect_directory)
            ));
        }
        files.insert(app.name().unit(), unit.into_bytes());
    }
    target.push('\n');
    files.insert("space-apps.target".into(), target.into_bytes());
    Ok(files)
}

pub(super) fn archive(files: &Files) -> io::Result<Vec<u8>> {
    let mut archive = tar::Builder::new(Vec::new());
    for (name, bytes) in files {
        let mut header = tar::Header::new_gnu();
        header.set_size(bytes.len() as u64);
        header.set_mode(0o600);
        header.set_cksum();
        archive.append_data(&mut header, name, bytes.as_slice())?;
    }
    archive.into_inner()
}

pub(super) fn read_archive(bytes: &[u8]) -> io::Result<Files> {
    let mut files = Files::new();
    for entry in tar::Archive::new(bytes).entries()? {
        let mut entry = entry?;
        let path = entry.path()?.into_owned();
        if entry.header().entry_type().is_dir() && path == std::path::Path::new(".") {
            continue;
        }
        let path = path.strip_prefix(".").unwrap_or(&path);
        if !entry.header().entry_type().is_file() || path.components().count() != 1 {
            return Err(io::Error::other(
                "runtime bundle must contain only regular files",
            ));
        }
        let name = path
            .to_str()
            .ok_or_else(|| io::Error::other("invalid runtime bundle filename"))?
            .to_owned();
        let mut content = Vec::new();
        entry
            .by_ref()
            .take(MAX_BUNDLE_BYTES as u64 + 1)
            .read_to_end(&mut content)?;
        if content.len() > MAX_BUNDLE_BYTES
            || files.len() >= 18
            || files.insert(name, content).is_some()
        {
            return Err(io::Error::other("invalid runtime bundle"));
        }
    }
    Ok(files)
}
