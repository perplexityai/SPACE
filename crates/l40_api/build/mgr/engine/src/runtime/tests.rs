use super::*;

#[test]
fn runtime_archive_preserves_literal_fields_and_rejects_non_regular_payloads() {
    let declaration: TemplateRuntime = serde_json::from_value(serde_json::json!({
        "schema_version":1,"apps":[{"name":"web","command":["/bin/echo","%n $HOME ${HOME} \\\"\n\t", ""],
        "user":"root","working_directory":"/tmp/%n $HOME", "env":{"VALUE":"%n $HOME \\\"\n\t"},
        "readiness":{"type":"exec","command":["/bin/true"]},"startup_timeout_secs":3}]
    })).unwrap();
    let files = render::render(&declaration).unwrap();
    assert_eq!(
        render::read_archive(&render::archive(&files).unwrap()).unwrap(),
        files
    );
    assert_eq!(
        TemplateRuntime::from_bytes(&files["runtime.toml"]).unwrap(),
        declaration
    );
    let mut bad = tar::Builder::new(Vec::new());
    let mut header = tar::Header::new_gnu();
    header.set_entry_type(tar::EntryType::Symlink);
    header.set_size(0);
    header.set_mode(0o600);
    header.set_cksum();
    bad.append_link(&mut header, "runtime.toml", "/etc/shadow")
        .unwrap();
    render::read_archive(&bad.into_inner().unwrap()).unwrap_err();
}
