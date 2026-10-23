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

  constructor(state: string, message: string) {
    super(message);
    this.name = 'OperationFailedError';
    this.state = state;
  }
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
        return { type: 'error', text: `Sandbox create ${err.state}: ${err.message}` };
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
