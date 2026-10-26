CREATE TABLE device_parents (
    id TEXT PRIMARY KEY NOT NULL,
    profile TEXT NOT NULL,
    mode TEXT NOT NULL,
    framebuffer_bytes INTEGER CHECK (framebuffer_bytes IS NULL OR framebuffer_bytes > 0),
    host_boot_id TEXT NOT NULL,
    qualification_digest TEXT NOT NULL,
    state TEXT NOT NULL,
    revision INTEGER NOT NULL CHECK (revision >= 0),
    qualification_evidence_json TEXT,
    last_error TEXT CHECK (last_error IS NULL OR length(last_error) <= 2048),
    updated_at INTEGER NOT NULL,
    CHECK (state != 'ready' OR qualification_evidence_json IS NOT NULL)
);
CREATE INDEX device_parents_profile_state ON device_parents(profile, state);

CREATE TABLE device_parent_members (
    id TEXT PRIMARY KEY NOT NULL,
    parent_id TEXT NOT NULL REFERENCES device_parents(id),
    host_device_id TEXT NOT NULL,
    enrolled_identity TEXT NOT NULL,
    UNIQUE(parent_id, host_device_id),
    UNIQUE(id, host_device_id),
    UNIQUE(id, parent_id)
);
CREATE INDEX device_parent_members_locator ON device_parent_members(host_device_id);

CREATE TABLE device_active_members (
    host_device_id TEXT PRIMARY KEY NOT NULL,
    member_id TEXT NOT NULL UNIQUE,
    FOREIGN KEY(member_id, host_device_id) REFERENCES device_parent_members(id, host_device_id)
);

CREATE TABLE device_allocations (
    lease_id TEXT PRIMARY KEY NOT NULL,
    parent_id TEXT NOT NULL REFERENCES device_parents(id),
    profile TEXT NOT NULL,
    holder_kind TEXT NOT NULL,
    execution_id TEXT NOT NULL,
    sandbox_id TEXT,
    slot_generation INTEGER CHECK (slot_generation IS NULL OR slot_generation >= 0),
    requirements_digest TEXT,
    procedure_digest TEXT,
    state TEXT NOT NULL,
    revision INTEGER NOT NULL CHECK (revision >= 0),
    assignment_json TEXT,
    last_error TEXT CHECK (last_error IS NULL OR length(last_error) <= 2048),
    updated_at INTEGER NOT NULL,
    UNIQUE(lease_id, parent_id),
    CHECK (
        (holder_kind = 'sandbox' AND sandbox_id IS NOT NULL AND slot_generation IS NOT NULL
         AND requirements_digest IS NOT NULL AND procedure_digest IS NULL)
        OR
        (holder_kind = 'sanitizer' AND sandbox_id IS NULL AND slot_generation IS NULL
         AND requirements_digest IS NULL AND procedure_digest IS NOT NULL)
    ),
    CHECK (state NOT IN ('attaching', 'attached') OR assignment_json IS NOT NULL)
);
CREATE INDEX device_allocations_parent ON device_allocations(parent_id);
CREATE INDEX device_allocations_owner ON device_allocations(sandbox_id, execution_id, slot_generation);
CREATE INDEX device_allocations_cleanup ON device_allocations(state, updated_at);

CREATE TABLE device_allocation_members (
    lease_id TEXT NOT NULL,
    parent_id TEXT NOT NULL,
    member_id TEXT NOT NULL,
    holding_kind TEXT NOT NULL,
    capacity_units INTEGER NOT NULL CHECK (capacity_units > 0),
    PRIMARY KEY(lease_id, member_id),
    FOREIGN KEY(lease_id, parent_id) REFERENCES device_allocations(lease_id, parent_id),
    FOREIGN KEY(member_id, parent_id) REFERENCES device_parent_members(id, parent_id),
    CHECK (holding_kind != 'exclusive' OR capacity_units = 1)
);
CREATE INDEX device_allocation_members_parent ON device_allocation_members(parent_id, member_id);

CREATE TABLE device_exclusive_member_claims (
    member_id TEXT PRIMARY KEY NOT NULL,
    lease_id TEXT NOT NULL,
    FOREIGN KEY(lease_id, member_id) REFERENCES device_allocation_members(lease_id, member_id)
);
CREATE INDEX device_exclusive_member_claims_lease ON device_exclusive_member_claims(lease_id);
