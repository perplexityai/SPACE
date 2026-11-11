use super::*;

fn create_request() -> CreateSandboxRequest {
    CreateSandboxRequest {
        template: "seed:latest".parse().unwrap(),
        runtime: None,
        name: None,
        env: BTreeMap::new(),
        metadata: PublicSandboxMetadata::new(),
        resources: None,
        network_policy: None,
        auto_lifecycle: None,
        idle_timeout: None,
        idle_action: None,
    }
}

fn update_request() -> UpdateSandboxRequest {
    UpdateSandboxRequest {
        metadata: None,
        resources: None,
        network_policy: None,
        auto_lifecycle: None,
    }
}

fn metadata_strings(entries: impl IntoIterator<Item = (String, String)>) -> PublicSandboxMetadata {
    entries
        .into_iter()
        .map(|(key, value)| (key, PublicSandboxMetadataValue::String(value)))
        .collect()
}

/// Distinct keys of exactly `key_bytes` bytes.
fn keys(count: usize, key_bytes: usize) -> impl Iterator<Item = String> {
    (0..count).map(move |index| format!("{index:0>key_bytes$}"))
}

fn with_metadata(metadata: PublicSandboxMetadata) -> Result<(), RequestLimitError> {
    let mut create = create_request();
    create.metadata = metadata.clone();
    let mut update = update_request();
    update.metadata = Some(metadata);
    let created = validate_create_request(&create);
    assert_eq!(
        created,
        validate_update_request(&update),
        "create and update agree"
    );
    created
}

fn with_env(env: BTreeMap<String, String>) -> Result<(), RequestLimitError> {
    let mut request = create_request();
    request.env = env;
    validate_create_request(&request)
}

fn with_name(name: String) -> Result<(), RequestLimitError> {
    let mut request = create_request();
    request.name = Some(name);
    validate_create_request(&request)
}

fn with_resources(resources: SandboxResources) -> Result<(), RequestLimitError> {
    let mut request = create_request();
    request.resources = Some(resources);
    validate_create_request(&request)
}

#[test]
fn empty_requests_are_accepted() {
    assert_eq!(validate_create_request(&create_request()), Ok(()));
    assert_eq!(validate_update_request(&update_request()), Ok(()));
}

#[test]
fn metadata_entry_count_is_bounded() {
    let at_max = metadata_strings(keys(MAX_METADATA_ENTRIES, 4).map(|key| (key, String::new())));
    assert_eq!(with_metadata(at_max), Ok(()));
    let over = metadata_strings(keys(MAX_METADATA_ENTRIES + 1, 4).map(|key| (key, String::new())));
    assert_eq!(
        with_metadata(over),
        Err(RequestLimitError::TooManyEntries {
            field: RequestField::Metadata,
            max_entries: MAX_METADATA_ENTRIES,
        })
    );
}

#[test]
fn metadata_key_length_is_bounded() {
    let at_max = metadata_strings(keys(1, MAX_METADATA_KEY_BYTES).map(|key| (key, String::new())));
    assert_eq!(with_metadata(at_max), Ok(()));
    let over =
        metadata_strings(keys(1, MAX_METADATA_KEY_BYTES + 1).map(|key| (key, String::new())));
    assert_eq!(
        with_metadata(over),
        Err(RequestLimitError::KeyTooLong {
            field: RequestField::Metadata,
            max_bytes: MAX_METADATA_KEY_BYTES,
        })
    );
}

#[test]
fn metadata_value_length_is_counted_in_bytes() {
    // "é" is two UTF-8 bytes, so a character count would accept the second map.
    let at_max = metadata_strings([("k".into(), "é".repeat(MAX_METADATA_VALUE_BYTES / 2))]);
    assert_eq!(with_metadata(at_max), Ok(()));
    let over = metadata_strings([("k".into(), "é".repeat(MAX_METADATA_VALUE_BYTES / 2) + "a")]);
    assert_eq!(
        with_metadata(over),
        Err(RequestLimitError::ValueTooLong {
            field: RequestField::Metadata,
            max_bytes: MAX_METADATA_VALUE_BYTES,
        })
    );
}

#[test]
fn metadata_total_bytes_are_bounded() {
    // 16 entries of 4-byte keys and 1020-byte values fill the 16 KiB budget.
    let value_bytes = MAX_METADATA_TOTAL_BYTES / 16 - 4;
    let at_max = || keys(16, 4).map(|key| (key, "v".repeat(value_bytes)));
    assert_eq!(with_metadata(metadata_strings(at_max())), Ok(()));
    let mut over = metadata_strings(at_max());
    over.insert(
        "x".into(),
        PublicSandboxMetadataValue::String(String::new()),
    );
    assert_eq!(
        with_metadata(over),
        Err(RequestLimitError::TotalTooLarge {
            field: RequestField::Metadata,
            max_bytes: MAX_METADATA_TOTAL_BYTES,
        })
    );
}

#[test]
fn metadata_numbers_and_booleans_count_only_towards_entries() {
    let metadata = keys(MAX_METADATA_ENTRIES, MAX_METADATA_KEY_BYTES)
        .enumerate()
        .map(|(index, key)| {
            let value = if index % 2 == 0 {
                PublicSandboxMetadataValue::Number(i64::MIN)
            } else {
                PublicSandboxMetadataValue::Bool(true)
            };
            (key, value)
        })
        .collect();
    assert_eq!(with_metadata(metadata), Ok(()));
}

#[test]
fn metadata_rejects_nul_in_keys_and_values() {
    for metadata in [
        metadata_strings([("a\0b".into(), "v".into())]),
        metadata_strings([("k".into(), "a\0b".into())]),
    ] {
        assert_eq!(
            with_metadata(metadata),
            Err(RequestLimitError::ContainsNul {
                field: RequestField::Metadata,
            })
        );
    }
}

#[test]
fn name_length_is_bounded_and_nul_is_rejected() {
    assert_eq!(with_name("n".repeat(MAX_SANDBOX_NAME_BYTES)), Ok(()));
    assert_eq!(
        with_name("n".repeat(MAX_SANDBOX_NAME_BYTES + 1)),
        Err(RequestLimitError::TooLong {
            field: RequestField::Name,
            max_bytes: MAX_SANDBOX_NAME_BYTES,
        })
    );
    assert_eq!(
        with_name("a\0b".into()),
        Err(RequestLimitError::ContainsNul {
            field: RequestField::Name,
        })
    );
}

#[test]
fn env_uses_the_command_environment_limits() {
    let entries = |count, key_bytes, value_bytes| {
        keys(count, key_bytes)
            .map(|key| (key, "v".repeat(value_bytes)))
            .collect::<BTreeMap<_, _>>()
    };
    let field = RequestField::Env;
    assert_eq!(with_env(entries(MAX_ENV_ENTRIES, 4, 0)), Ok(()));
    assert_eq!(
        with_env(entries(MAX_ENV_ENTRIES + 1, 4, 0)),
        Err(RequestLimitError::TooManyEntries {
            field,
            max_entries: MAX_ENV_ENTRIES,
        })
    );
    assert_eq!(with_env(entries(1, MAX_ENV_KEY_BYTES, 0)), Ok(()));
    assert_eq!(
        with_env(entries(1, MAX_ENV_KEY_BYTES + 1, 0)),
        Err(RequestLimitError::KeyTooLong {
            field,
            max_bytes: MAX_ENV_KEY_BYTES,
        })
    );
    assert_eq!(with_env(entries(1, 1, MAX_ENV_VALUE_BYTES)), Ok(()));
    assert_eq!(
        with_env(entries(1, 1, MAX_ENV_VALUE_BYTES + 1)),
        Err(RequestLimitError::ValueTooLong {
            field,
            max_bytes: MAX_ENV_VALUE_BYTES,
        })
    );
    // Eight 8 KiB values, less their 1-byte keys, fill the 64 KiB budget.
    let at_total = entries(8, 1, MAX_ENV_TOTAL_BYTES / 8 - 1);
    assert_eq!(with_env(at_total.clone()), Ok(()));
    let mut over_total = at_total;
    over_total.insert("x".into(), String::new());
    assert_eq!(
        with_env(over_total),
        Err(RequestLimitError::TotalTooLarge {
            field,
            max_bytes: MAX_ENV_TOTAL_BYTES,
        })
    );
    assert_eq!(
        with_env(BTreeMap::from([("K".into(), "a\0b".into())])),
        Err(RequestLimitError::ContainsNul { field })
    );
}

#[test]
fn env_keys_outside_the_shell_grammar_are_still_accepted() {
    assert_eq!(
        with_env(BTreeMap::from([("BAD-NAME".into(), "value".into())])),
        Ok(())
    );
}

#[test]
fn resources_are_range_checked_at_both_ends() {
    let cases = [
        (RequestField::Vcpu, MIN_SANDBOX_VCPU, MAX_SANDBOX_VCPU),
        (
            RequestField::MemoryMib,
            MIN_SANDBOX_MEMORY_MIB,
            MAX_SANDBOX_MEMORY_MIB,
        ),
        (
            RequestField::DiskMib,
            MIN_SANDBOX_DISK_MIB,
            MAX_SANDBOX_DISK_MIB,
        ),
    ];
    for (field, min, max) in cases {
        let resources = |value| {
            let mut resources = SandboxResources {
                vcpu: None,
                memory_mib: None,
                disk_mib: None,
            };
            *match field {
                RequestField::Vcpu => &mut resources.vcpu,
                RequestField::MemoryMib => &mut resources.memory_mib,
                _ => &mut resources.disk_mib,
            } = Some(value);
            resources
        };
        let rejected = Err(RequestLimitError::OutOfRange { field, min, max });
        assert_eq!(with_resources(resources(min)), Ok(()), "{field} at min");
        assert_eq!(with_resources(resources(max)), Ok(()), "{field} at max");
        assert_eq!(
            with_resources(resources(min - 1)),
            rejected,
            "{field} below min"
        );
        assert_eq!(
            with_resources(resources(max + 1)),
            rejected,
            "{field} above max"
        );
        // The value that wrapped to a negative `integer` column.
        assert_eq!(
            with_resources(resources(3_000_000_000)),
            rejected,
            "{field} wrap"
        );
    }
}

#[test]
fn update_checks_vcpu_and_memory() {
    let mut request = update_request();
    request.resources = Some(SandboxResources {
        vcpu: None,
        memory_mib: Some(16),
        disk_mib: None,
    });
    assert_eq!(
        validate_update_request(&request),
        Err(RequestLimitError::OutOfRange {
            field: RequestField::MemoryMib,
            min: MIN_SANDBOX_MEMORY_MIB,
            max: MAX_SANDBOX_MEMORY_MIB,
        })
    );
    request.resources = Some(SandboxResources {
        vcpu: Some(3_000_000_000),
        memory_mib: None,
        disk_mib: None,
    });
    assert_eq!(
        validate_update_request(&request),
        Err(RequestLimitError::OutOfRange {
            field: RequestField::Vcpu,
            min: MIN_SANDBOX_VCPU,
            max: MAX_SANDBOX_VCPU,
        })
    );
}

#[test]
fn field_names_match_the_request_body() {
    let names = [
        RequestField::Name,
        RequestField::Env,
        RequestField::Metadata,
        RequestField::Vcpu,
        RequestField::MemoryMib,
        RequestField::DiskMib,
    ]
    .map(RequestField::as_str);
    assert_eq!(
        names,
        [
            "name",
            "env",
            "metadata",
            "resources.vcpu",
            "resources.memory_mib",
            "resources.disk_mib",
        ]
    );
}

#[test]
#[allow(deprecated)]
fn validate_idempotency_key_applies_the_header_rule() {
    let at_max = "k".repeat(MAX_IDEMPOTENCY_KEY_BYTES);
    assert_eq!(
        validate_idempotency_key(at_max.as_bytes()),
        Ok(at_max.as_str())
    );
    assert_eq!(validate_idempotency_key(b"two words"), Ok("two words"));
    assert_eq!(
        validate_idempotency_key("k".repeat(MAX_IDEMPOTENCY_KEY_BYTES + 1).as_bytes()),
        Err(IdempotencyKeyError::TooLong)
    );
    assert_eq!(
        validate_idempotency_key(b""),
        Err(IdempotencyKeyError::Empty)
    );
    assert_eq!(
        validate_idempotency_key("ключ".as_bytes()),
        Err(IdempotencyKeyError::InvalidEncoding)
    );
}
