//! Emit fixtures for the explicitly invoked, disposable systemd acceptance test.
//! Run the emitted test.sh only in a container with private cgroups and no host mounts.
#[path = "../src/runtime/render.rs"]
mod render;
use pplx_space_common_core::types::TemplateRuntime;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let directory = std::path::PathBuf::from(
        std::env::args_os()
            .nth(1)
            .ok_or("expected fixture directory")?,
    );
    std::fs::create_dir_all(&directory)?;
    let literals = vec![
        "",
        "%n",
        "$HOME",
        "${HOME}",
        "$(touch /run/runtime-injected)",
        "a\nb\tc\r",
        "a\"b'c\\d",
        ";",
        "-@:+!",
        "caf\u{e9}",
    ];
    let cwd = "/fixtures/work %n $HOME \" \\ ";
    let worker_cwd = "/fixtures/normal %n $HOME \" \\ end";
    let mut argv = vec!["./capture.py"];
    argv.extend_from_slice(&literals);
    let mut probe = vec!["/usr/bin/python3", "/fixtures/probe.py"];
    probe.extend_from_slice(&literals);
    let runtime: TemplateRuntime = serde_json::from_value(
        serde_json::json!({"schema_version":1,"apps":[
            {"name":"app","command":argv,"user":"nobody","working_directory":cwd, "env":{"RUNTIME_TEST":"one","LITERAL":"%n $HOME ${HOME} a\nb\tc\r \\\"","PATH":"/usr/bin:/bin"},"readiness":{"type":"exec","command":probe},"startup_timeout_secs":5},
            {"name":"worker","command":["/bin/sleep","infinity"],"user":"root","working_directory":worker_cwd,"env":{},"readiness":{"type":"launch"},"startup_timeout_secs":5}
        ]}),
    )?;
    let mut replacement = serde_json::to_value(&runtime)?;
    replacement
        .get_mut("apps")
        .and_then(serde_json::Value::as_array_mut)
        .ok_or("missing fixture apps")?
        .remove(0);
    let replacement: TemplateRuntime = serde_json::from_value(replacement)?;
    std::fs::write(
        directory.join("replacement.tar"),
        render::archive(&render::render(&replacement)?)?,
    )?;
    let files = render::render(&runtime)?;
    let archive = render::archive(&files)?;
    assert_eq!(render::read_archive(&archive)?, files);
    std::fs::write(directory.join("apps.tar"), archive)?;
    std::fs::write(
        directory.join("expected.json"),
        serde_json::to_vec(
            &serde_json::json!({"cwd":cwd,"worker_cwd":worker_cwd,"argv":literals,"env":"%n $HOME ${HOME} a\nb\tc\r \\\""}),
        )?,
    )?;
    for (name, contents) in [
        ("install.sh", include_str!("../src/runtime/install.sh")),
        ("clear.sh", include_str!("../src/runtime/clear.sh")),
        ("read.sh", include_str!("../src/runtime/read.sh")),
        ("check.sh", include_str!("../src/runtime/check.sh")),
        ("test.sh", include_str!("runtime_systemd/test.sh")),
        (
            "observe.sh",
            include_str!("../../../sbx/mgr/server/src/service/template_apps/observe.sh"),
        ),
        ("capture.py", include_str!("runtime_systemd/capture.py")),
        ("probe.py", include_str!("runtime_systemd/probe.py")),
        (
            "check-observer.py",
            include_str!("runtime_systemd/check-observer.py"),
        ),
    ] {
        std::fs::write(directory.join(name), contents)?;
    }
    Ok(())
}
