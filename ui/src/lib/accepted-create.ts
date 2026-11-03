// What the console reports once `POST /sandboxes` has been accepted. The
// sandbox exists from that point on, so nothing after it may land in the
// create form's error path, where resubmitting creates a second sandbox.

/** The operation was still running when the console stopped polling. */
export class OperationTimeoutError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'OperationTimeoutError';
  }
}

/** The operation reached a terminal state other than `succeeded`. */
export class OperationFailedError extends Error {
  state: string;
  // `terminal_error.reason` and `terminal_error.message`, when the record has them.
  reason: string | null;
  detail: string | null;

  constructor(state: string, message: string, reason: string | null = null, detail: string | null = null) {
    super(message);
    this.name = 'OperationFailedError';
    this.state = state;
    this.reason = reason;
    this.detail = detail;
  }
}

// The create reasons the operation record names for a template the caller may
// use but that cannot be resolved to a release. Releases without them report
// the generic progress message, which this keeps.
const CREATE_FAILURE_REASONS: Record<string, string> = {
  template_has_no_release: 'the template has no published release; build it, or publish a release, first',
  template_release_not_found: 'no release of the template matches the requested tag or digest and runtime',
};

export function createFailureText(err: OperationFailedError): string {
  const reason = err.reason ? CREATE_FAILURE_REASONS[err.reason] : undefined;
  if (!reason) return `Sandbox create ${err.state}: ${err.message}`;
  return `Sandbox create ${err.state}: ${reason}${err.detail ? ` (${err.detail})` : ''}`;
}

// The reason and message of a failed operation record's `terminal_error`.
export function terminalErrorParts(terminalError: unknown): { reason: string | null; detail: string | null } {
  if (!terminalError || typeof terminalError !== 'object') return { reason: null, detail: null };
  const { reason, message } = terminalError as { reason?: unknown; message?: unknown };
  return {
    reason: typeof reason === 'string' && reason ? reason : null,
    detail: typeof message === 'string' && message ? message : null,
  };
}

export type CreateOutcome = { type: 'success' | 'warning' | 'error'; text: string };

/**
 * `waitForCreate` and `enableSnapshots` are only passed when the create asked
 * for snapshots, which need the sandbox to exist first. `snapshotsOff` says
 * whether an enable error means the feature is not enabled for the org.
 */
export async function settleAcceptedCreate(steps: {
  waitForCreate?: () => Promise<unknown>;
  enableSnapshots?: () => Promise<unknown>;
  snapshotsOff: (err: unknown) => boolean;
}): Promise<CreateOutcome> {
  if (steps.waitForCreate) {
    try {
      await steps.waitForCreate();
    } catch (err: any) {
      if (err instanceof OperationFailedError) {
        return { type: 'error', text: createFailureText(err) };
      }
      // Timed out, or polling itself failed: the create may still finish.
      return {
        type: 'warning',
        text: steps.enableSnapshots
          ? 'Sandbox creating; check its status. Snapshots were not enabled'
          : 'Sandbox creating; check its status',
      };
    }
  }
  if (steps.enableSnapshots) {
    try {
      await steps.enableSnapshots();
    } catch (err: any) {
      return {
        type: 'warning',
        text: steps.snapshotsOff(err)
          ? 'Sandbox created; snapshots are not enabled for your organization'
          : `Sandbox created; enabling snapshots failed: ${err?.message || 'request failed'}`,
      };
    }
  }
  return { type: 'success', text: 'Sandbox created' };
}
