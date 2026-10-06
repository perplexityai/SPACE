import assert from 'node:assert/strict';

// Presentation-only allowlist: requests/responses before their supporting values.
// Component keys and wire schemas are never renamed or removed.
export const modelGroups = [
  {
    name: 'Sandbox models',
    description: 'Create and inspect sandboxes, then follow their lifecycle and readiness. Start with the request and resource below.',
    models: [
      ['CreateSandboxRequest', 'Sandbox request', 'Template and optional configuration for creating a sandbox.'],
      ['PublicSandbox', 'Sandbox', 'Current sandbox state, readiness observations and stored configuration.'],
      ['SandboxOperationResponse', 'Accepted sandbox action', 'Identifiers returned when asynchronous sandbox work is accepted; poll the operation before relying on its outcome.'],
      ['PublicSandboxPage', 'Sandbox list', 'A page of visible sandboxes and its continuation cursor.'],
      ['SandboxCondition', 'Readiness condition', 'One readiness or failure observation, including its status and explanation.'],
      ['PublicSandboxMetadataValue', 'Metadata value', 'The string, integer or boolean value of a sandbox metadata entry.'],
      ['SandboxLifecycleState', 'Sandbox state', 'The sandbox lifecycle state, not an operation result or command exit status.'],
      ['SandboxLifecycleAction', 'Lifecycle action', 'Names for sandbox actions; values in this shared type do not make additional endpoints part of the supported interface.'],
      ['SandboxConditionType', 'Condition type', 'Identifies the kind of readiness or failure observation.'],
      ['ConditionStatus', 'Condition status', 'The reported truth value of a sandbox condition.'],
      ['SandboxFailureCode', 'Sandbox failure code', 'A machine-readable classification of an observed sandbox failure.'],
      ['SandboxIdleAction', 'Idle action', 'What happens when a sandbox stays idle past its idle timeout; tolerate unknown future values.'],
    ],
  },
  {
    name: 'Command and process models',
    description: 'Create a command session, attach to start execution, and inspect or control running processes. Session and process identifiers are not guest PIDs.',
    models: [
      ['CreateCommandRequest', 'Command request', 'Command arguments and optional execution settings. Creating the handle alone does not start execution.'],
      ['PublicCommand', 'Command session', 'The command handle, process identity and session-scoped attachment information.'],
      ['DataPlaneAttachInfo', 'Attachment credentials', 'Attachment destinations, credential transports and token expiry; validate the destination before sending the session token.'],
      ['PublicProcess', 'Running process', 'Metadata for a currently running process, not completed-command history.'],
      ['PublicProcessPage', 'Process list', 'A page of running processes and its continuation cursor.'],
      ['ProcessSignalRequest', 'Signal request', 'The Unix signal number to send to a process.'],
      ['ProcessMutationResponse', 'Process action result', 'Acknowledges a runtime action, not proof of process exit.'],
      ['SessionState', 'Session state', 'Attachment-session state; use the event stream to determine command success.'],
      ['DataPlaneTokenTransport', 'Token transport', 'Accepted ways to supply a session-scoped attachment token.'],
      ['TerminalSize', 'Terminal dimensions', 'Row and column dimensions for optional PTY settings; interactive WebSocket APIs remain outside this reference.'],
    ],
  },
  {
    name: 'Async operation models',
    description: 'An accepted lifecycle request is not a completed action. Poll its operation and inspect the terminal result before continuing.',
    models: [
      ['PublicOperationRecord', 'Operation status', 'Progress, terminal outcome and optional result of asynchronous work.'],
      ['PublicOperationResult', 'Operation result', 'A tagged result payload. Supported sandbox lifecycle operations return the sandbox variant.'],
      ['OperationState', 'Operation state', 'Progress and terminal outcomes for asynchronous operations.'],
      ['OperationKind', 'Operation kind', 'The kind of asynchronous work; this shared vocabulary also contains actions outside the supported endpoint set.'],
      ['OperationTargetType', 'Operation target', 'The kind of resource addressed by an operation.'],
    ],
  },
  {
    name: 'Stream event models',
    description: 'SSE events carry these frames. Streams are live, not replayable history; related WebSocket and file-watch types do not add supported creation endpoints.',
    models: [
      ['DataPlaneServerFrame', 'Stream event', 'The tagged event payload carried by the stream, including output, resynchronization, errors and process exit.'],
      ['DataPlaneProcessExitStatus', 'Process exit status', 'How a streamed process ended; also inspect its exit code and any output loss or error.'],
      ['DataPlaneResyncReason', 'Resynchronization reason', 'Why a client must resynchronize after a stream gap or reconnect.'],
      ['StreamCloseReason', 'Stream close reason', 'Why a stream closed; closure alone does not prove command success.'],
      ['ReconnectReason', 'Reconnect reason', 'The reported reason for a stream reconnection.'],
      ['SessionKind', 'Session kind', 'The session kind reported by stream metadata; not a list of supported session-creation endpoints.'],
      ['FileEventKind', 'File event kind', 'The kind of a file-watch event in the shared stream frame vocabulary.'],
    ],
  },
  {
    name: 'Error models',
    description: 'Management and data-plane HTTP errors use different envelopes and retry mappings. Error examples are illustrative; follow the returned retryable flag.',
    models: [
      ['PublicErrorEnvelope', 'API error', 'The management API error response, with a stable code, retryability and optional request diagnostics.'],
      ['PublicErrorDetail', 'Error detail', 'A structured key/value detail attached to a management API error.'],
      ['PublicErrorCode', 'API error code', 'Machine-readable management error vocabulary; do not parse messages as codes.'],
      ['DataPlaneHttpErrorEnvelope', 'Stream HTTP error', 'The nested error response when an SSE HTTP request fails.'],
      ['DataPlaneHttpError', 'Stream HTTP error detail', 'Code, message and retryability inside a data-plane HTTP error envelope.'],
      ['DataPlaneHttpErrorCode', 'Stream HTTP error code', 'HTTP admission or routing error codes for the data plane.'],
      ['DataPlaneErrorCode', 'Stream event error code', 'Error codes carried inside an established stream, distinct from HTTP error responses.'],
    ],
  },
  {
    name: 'Pagination',
    description: 'Continue a collection with its returned cursor until next_cursor is null. Keep cursor values opaque and do not reuse them across resource types.',
    models: [
      ['PageCursor', 'Page cursor', 'An opaque continuation token for a collection request.'],
    ],
  },
];

function references(value, found = new Set()) {
  if (!value || typeof value !== 'object') return found;
  if (typeof value.$ref === 'string' && value.$ref.startsWith('#/components/schemas/')) {
    found.add(value.$ref.slice('#/components/schemas/'.length));
  }
  Object.values(value).forEach(child => references(child, found));
  return found;
}

export function applySchemaCatalog(spec) {
  const models = modelGroups.flatMap(group => group.models);
  assert.deepEqual(models.map(([name]) => name).sort(), Object.keys(spec.components.schemas).sort(),
    'Review model navigation when the supported schema catalog changes');
  const titles = Object.fromEntries(models.map(([name, title]) => [name, title]));
  const operations = Object.values(spec.paths).flatMap(path => Object.values(path))
    .filter(operation => operation.operationId);
  const primaryOperations = ['createSandbox', 'createCommand', 'getOperation', 'streamSessionEvents'];
  operations.sort((left, right) =>
    (primaryOperations.indexOf(left.operationId) + 1 || 99) - (primaryOperations.indexOf(right.operationId) + 1 || 99));
  const endpoints = operations.map(operation => ({
    label: operation.summary, href: '#operation/' + operation.operationId, refs: references(operation),
  }));
  const parents = models.map(([name, title]) => ({
    name, label: title, href: '#schema/' + name, refs: references(spec.components.schemas[name]),
  }));
  const catalog = [];
  const ordered = {};
  const endpointTags = spec.tags.map(tag => tag.name);
  const index = {
    name: 'Schemas',
    'x-displayName': 'Model index',
    description: 'Start with the models for your task. Each entry keeps its canonical type name and links to related endpoints or models. '
      + 'Supporting values follow the main request and response objects; all fields and constraints remain available.\n\n'
      + modelGroups.map(group => `[${group.name}](#tag/${group.name.replaceAll(' ', '-')})`).join(' · '),
  };
  spec.tags.push(index, ...modelGroups.map(({ name, description }) => ({ name, description })));
  spec['x-tagGroups'] = [
    { name: 'Endpoints', tags: endpointTags },
    { name: 'Data models', tags: ['Schemas', ...modelGroups.map(group => group.name)] },
  ];
  for (const group of modelGroups) {
    for (const [name, title, purpose] of group.models) {
      const schema = spec.components.schemas[name];
      ordered[name] = { ...schema, title, 'x-tags': [group.name] };
      const direct = endpoints.filter(endpoint => endpoint.refs.has(name));
      const related = (direct.length ? direct : parents.filter(parent => parent.name !== name && parent.refs.has(name)))
        .slice(0, 2).map(({ label, href }) => ({ label, href }));
      assert.ok(related.length, `${name} must have a related endpoint or parent model`);
      catalog.push({
        name, title: titles[name], purpose, related,
        primitive: ['string', 'number', 'integer', 'boolean'].includes(schema.type),
      });
    }
  }
  spec.components.schemas = ordered;
  return catalog;
}
