const SANDBOX_LABEL_LEN = 8;
const PUBLIC_SANDBOX_ID_PREFIX = 'sbx_';

const UUID_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const COMPACT_UUID_PATTERN = /^[0-9a-f]{32}$/;
// Sandbox pods are named `space-sbx-<uuid without dashes>-g<generation>`.
const POD_NAME_PATTERN = /^space-sbx-([0-9a-f]{32})(?:-g\d+)?$/;

export interface SandboxRef {
  // Lowercase dashed UUID, the form the admin debug and node routes take.
  uuid: string;
  // `sbx_<uuid>`, the form the admin sandbox routes take.
  publicId: string;
}

function dashed(compact: string): string {
  return [
    compact.slice(0, 8),
    compact.slice(8, 12),
    compact.slice(12, 16),
    compact.slice(16, 20),
    compact.slice(20),
  ].join('-');
}

// Resolve any identifier an operator may hold for a sandbox: the public
// `sbx_<uuid>` ID, a bare or undashed UUID, a `sandbox:<uuid>` log tag, or the
// sandbox pod name. Returns null for anything else, including partial IDs.
export function parseSandboxRef(input: string): SandboxRef | null {
  let value = input.trim().replace(/^['"`]+|['"`]+$/g, '').toLowerCase();
  if (value.startsWith(PUBLIC_SANDBOX_ID_PREFIX)) {
    value = value.slice(PUBLIC_SANDBOX_ID_PREFIX.length);
  } else if (value.startsWith('sandbox:')) {
    value = value.slice('sandbox:'.length);
  } else {
    const pod = POD_NAME_PATTERN.exec(value);
    if (pod) value = pod[1];
  }
  if (COMPACT_UUID_PATTERN.test(value)) value = dashed(value);
  if (!UUID_PATTERN.test(value)) return null;
  return { uuid: value, publicId: `${PUBLIC_SANDBOX_ID_PREFIX}${value}` };
}

// `decodeURIComponent` that returns its input unchanged when it is not valid
// percent-encoding, for identifiers taken from a URL path.
export function safeDecodeURIComponent(value: string): string {
  try {
    return decodeURIComponent(value);
  } catch {
    return value;
  }
}

export function sandboxUuidPart(sandboxId: string): string {
  return sandboxId.startsWith(PUBLIC_SANDBOX_ID_PREFIX)
    ? sandboxId.slice(PUBLIC_SANDBOX_ID_PREFIX.length)
    : sandboxId;
}

export function publicSandboxIdFromAny(sandboxId: string): string {
  const ref = parseSandboxRef(sandboxId);
  if (ref) return ref.publicId;
  return sandboxId.startsWith(PUBLIC_SANDBOX_ID_PREFIX)
    ? sandboxId
    : `${PUBLIC_SANDBOX_ID_PREFIX}${sandboxId}`;
}

export function sandboxLabelOrId(sandboxId: string): string {
  const compact = sandboxUuidPart(sandboxId).replaceAll('-', '').toLowerCase();
  if (!compact || !/^[0-9a-f]+$/.test(compact)) {
    return sandboxId;
  }
  if (compact.length <= SANDBOX_LABEL_LEN) {
    return compact;
  }
  return compact.slice(-SANDBOX_LABEL_LEN);
}
