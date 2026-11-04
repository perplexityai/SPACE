use std::{
    io::{self, IsTerminal, Write},
    time::Duration,
};

use reqwest::{StatusCode, Url, header};
use serde::Deserialize;

use crate::{
    admin_client::{http_client, is_loopback},
    cli::LoginArgs,
};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

fn origin(value: &str) -> Result<Url> {
    let url = Url::parse(value)?;
    let loopback = is_loopback(&url);
    if url.scheme() != "https" && !(url.scheme() == "http" && loopback) {
        return Err("Use HTTPS, or HTTP on localhost for a port forward".into());
    }
    if !url.username().is_empty()
        || url.password().is_some()
        || url.path() != "/"
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(
            "API URL must be an origin without credentials, path, query, or fragment".into(),
        );
    }
    Ok(url)
}

async fn authenticate(api_url: Url, auth_url: Url, email: &str, password: &str) -> Result<String> {
    let http = http_client(auth_url.as_str())
        .timeout(Duration::from_secs(30))
        .build()?;
    let response = http
        .post(auth_url.join("auth/v1/sessions")?)
        .json(&serde_json::json!({"email":email,"password":password}))
        .send()
        .await?;
    // Custom auth origins may return untrusted proxy error pages; do not echo their bodies.
    if !response.status().is_success() {
        if response.status() == StatusCode::SERVICE_UNAVAILABLE {
            return Err("Login temporarily unavailable: HTTP 503; retry the request".into());
        }
        return Err(format!("Login failed: HTTP {}", response.status()).into());
    }
    #[derive(Deserialize)]
    struct LoginResponse {
        token: String,
    }
    let login: LoginResponse = response
        .json()
        .await
        .map_err(|_| "Invalid login response")?;
    if login.token.trim().is_empty() {
        return Err("Login returned an empty token".into());
    }
    let mut authorization = header::HeaderValue::from_str(&format!("Bearer {}", login.token))
        .map_err(|_| "Login returned an invalid token")?;
    authorization.set_sensitive(true);
    let response = http_client(api_url.as_str())
        .timeout(Duration::from_secs(30))
        .build()?
        .get(api_url.join("admin/api/v1/templates")?)
        .header(header::AUTHORIZATION, authorization)
        .send()
        .await?;
    if !response.status().is_success() {
        return Err(format!(
            "Admin access check failed: HTTP {}; no token returned",
            response.status()
        )
        .into());
    }
    Ok(login.token)
}

async fn read_line() -> Result<String> {
    tokio::task::spawn_blocking(|| {
        let mut line = String::new();
        io::stdin().read_line(&mut line)?;
        Ok(line)
    })
    .await?
}

async fn credentials(email: Option<String>, password_stdin: bool) -> Result<(String, String)> {
    if password_stdin && io::stdin().is_terminal() {
        return Err("--password-stdin requires piped input; omit it for a hidden prompt".into());
    }
    let interactive = io::stdin().is_terminal() && io::stderr().is_terminal();
    let email = match email {
        Some(email) => email,
        None => {
            if !interactive {
                return Err("Use --email with --password-stdin for noninteractive login".into());
            }
            eprint!("SPACE email: ");
            io::stderr().flush()?;
            read_line().await?
        }
    };
    let email = email.trim().to_string();
    if email.is_empty() {
        return Err("Email must not be empty".into());
    }
    let password = if password_stdin {
        let password = read_line().await?;
        password.trim_end_matches(['\r', '\n']).to_string()
    } else {
        if !interactive {
            return Err("Use --password-stdin for noninteractive login".into());
        }
        read_password().await?
    };
    if password.is_empty() {
        return Err("Password must not be empty".into());
    }
    Ok((email, password))
}

#[cfg(unix)]
async fn read_password() -> Result<String> {
    use nix::sys::termios::{self, LocalFlags, SetArg, Termios};

    struct RestoreTerminal(Termios);
    impl Drop for RestoreTerminal {
        fn drop(&mut self) {
            if let Err(error) = termios::tcsetattr(io::stdin(), SetArg::TCSANOW, &self.0) {
                eprintln!("Cannot restore terminal settings: {error}");
            }
        }
    }

    let original = termios::tcgetattr(io::stdin())?;
    let mut hidden = original.clone();
    hidden
        .local_flags
        .remove(LocalFlags::ECHO | LocalFlags::ECHONL);
    termios::tcsetattr(io::stdin(), SetArg::TCSANOW, &hidden)?;
    let _restore = RestoreTerminal(original);
    eprint!("SPACE password: ");
    io::stderr().flush()?;
    let password = read_line().await?;
    eprintln!();
    Ok(password.trim_end_matches(['\r', '\n']).to_string())
}

#[cfg(not(unix))]
async fn read_password() -> Result<String> {
    Err("Interactive password input requires Unix; use --email with --password-stdin".into())
}

pub(crate) async fn login(args: LoginArgs) -> Result<String> {
    let api_url = origin(&args.api_url)?;
    let auth_url = match args.auth_url {
        Some(value) => origin(&value)?,
        None => api_url.clone(),
    };
    let login = async move {
        let (email, password) = credentials(args.email, args.password_stdin).await?;
        authenticate(api_url, auth_url, &email, &password).await
    };
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};

        let mut interrupt = signal(SignalKind::interrupt())?;
        let mut terminate = signal(SignalKind::terminate())?;
        let mut suspend = signal(SignalKind::from_raw(nix::libc::SIGTSTP))?;
        let mut login = Box::pin(login);
        let exit_code = tokio::select! {
            biased;
            _ = interrupt.recv() => 130,
            _ = terminate.recv() => 143,
            _ = suspend.recv() => 128 + nix::libc::SIGTSTP,
            result = &mut login => return result,
        };
        // Drop the terminal guard before exiting; a pending stdin read cannot be cancelled.
        drop(login);
        std::process::exit(exit_code);
    }
    #[cfg(not(unix))]
    login.await
}

#[cfg(test)]
mod tests;
