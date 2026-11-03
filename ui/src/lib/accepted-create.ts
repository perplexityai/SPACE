// What the console reports once an operation has been accepted: a 202 says
// only that it was queued, so its outcome is read from the operation record.
// For `POST /sandboxes`, the sandbox exists from that point on, so nothing
// after it may land in the create form's error path, where resubmitting
// creates a second sandbox.

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

/** The console stopped following the operation (its view closed); the operation itself goes on. */
export class OperationFollowStoppedError extends Error {
  constructor() {
    super('stopped following the operation');
    this.name = 'OperationFollowStoppedError';
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

const TERMINAL_STATES = ['succeeded', 'failed', 'canceled', 'expired'];

type OperationRecordLike = { state: string; progress_message: string; terminal_error?: unknown };

// Read an operation until it ends: its record when it succeeded,
// `OperationFailedError` when it ended otherwise, `OperationTimeoutError`
// when it is still running at `timeoutMs`, `OperationFollowStoppedError` once
// `signal` aborts. A failed read is thrown as it is. A read still unanswered
// at the deadline or when `signal` aborts is given up, and the signal `read`
// was given aborts, so a status request that never answers cannot hold the
// follower.
export async function pollOperation<T extends OperationRecordLike>(
  read: (signal: AbortSignal) => Promise<T>,
  {
    timeoutMs,
    intervalMs,
    now = Date.now,
    sleep = abortableSleep,
    timer = startTimer,
    signal,
  }: {
    timeoutMs: number;
    intervalMs: number;
    now?: () => number;
    sleep?: (ms: number, signal?: AbortSignal) => Promise<void>;
    // Calls `fire` after `ms`, unless the function it returns is called first.
    timer?: (ms: number, fire: () => void) => () => void;
    signal?: AbortSignal;
  },
): Promise<T> {
  const stopped = () => {
    if (signal?.aborted) throw new OperationFollowStoppedError();
  };
  const deadline = now() + timeoutMs;
  let last: T | null = null;
  while (now() < deadline) {
    stopped();
    const answer = await readWithin(read, deadline - now(), timer, signal);
    stopped();
    if (answer === GIVEN_UP) break;
    last = answer;
    if (TERMINAL_STATES.includes(last.state)) {
      if (last.state === 'succeeded') return last;
      const { reason, detail } = terminalErrorParts(last.terminal_error);
      throw new OperationFailedError(last.state, last.progress_message || `operation ${last.state}`, reason, detail);
    }
    await sleep(intervalMs, signal);
  }
  stopped();
  throw new OperationTimeoutError(last?.progress_message || 'operation still running');
}

const GIVEN_UP = Symbol('given up');

// One read, or `GIVEN_UP` once `ms` pass or `signal` aborts first. The read's
// own signal aborts once this settles, which cancels a request still open.
async function readWithin<T>(
  read: (signal: AbortSignal) => Promise<T>,
  ms: number,
  timer: (ms: number, fire: () => void) => () => void,
  signal?: AbortSignal,
): Promise<T | typeof GIVEN_UP> {
  const request = new AbortController();
  let cancelTimer = () => {};
  let giveUp = () => {};
  const givenUp = new Promise<typeof GIVEN_UP>((resolve) => {
    giveUp = () => resolve(GIVEN_UP);
    cancelTimer = timer(ms, giveUp);
    signal?.addEventListener('abort', giveUp);
  });
  try {
    return await Promise.race([read(request.signal), givenUp]);
  } finally {
    cancelTimer();
    signal?.removeEventListener('abort', giveUp);
    request.abort();
  }
}

function startTimer(ms: number, fire: () => void): () => void {
  const handle = setTimeout(fire, ms);
  return () => clearTimeout(handle);
}

// Waits `ms`, or less once `signal` aborts.
function abortableSleep(ms: number, signal?: AbortSignal): Promise<void> {
  return new Promise<void>((resolve) => {
    if (signal?.aborted) return resolve();
    const done = () => {
      clearTimeout(timer);
      signal?.removeEventListener('abort', done);
      resolve();
    };
    const timer = setTimeout(done, ms);
    signal?.addEventListener('abort', done);
  });
}

// What a rollback that was accepted came to, once its operation was followed.
export function rollbackOutcome(snapshotName: string, err: unknown | null): CreateOutcome {
  if (err === null) return { type: 'success', text: `Rolled back to ${snapshotName}` };
  if (err instanceof OperationFailedError) {
    const why = err.detail ?? err.message;
    return { type: 'error', text: `Rollback to ${snapshotName} ${err.state}: ${why}${err.reason ? ` (${err.reason})` : ''}` };
  }
  if (err instanceof OperationTimeoutError) {
    return {
      type: 'warning',
      text: `Rollback to ${snapshotName} is still running (${err.message}); the sandbox page's Operations card shows how it ends.`,
    };
  }
  const detail = err instanceof Error && err.message ? err.message : 'request failed';
  return {
    type: 'warning',
    text: `Rollback to ${snapshotName} was queued, but its progress could not be read (${detail}); the sandbox page's Operations card shows how it ends.`,
  };
}

export type CreateOutcome = { type: 'success' | 'warning' | 'error'; text: string };

// The notification a followed rollback shows: one per operation, so a later
// rollback's notification is never replaced by an earlier one's.
export interface RollbackNotice {
  key: string;
  type: 'loading' | 'info' | CreateOutcome['type'];
  text: string;
}

export function rollbackNoticeKey(operationId: string): string {
  return `rollback:${operationId}`;
}

// Follows an accepted rollback and reports how it ended. Once `signal`
// aborts (the view that sent it closed, or shows another sandbox), the
// console stops following: the notification says so and `onSettled`, which
// would read that view's snapshots again, does not run.
export async function followRollback({
  snapshotName,
  operationId,
  follow,
  signal,
  show,
  onSettled,
}: {
  snapshotName: string;
  operationId: string;
  follow: (signal: AbortSignal) => Promise<unknown>;
  signal: AbortSignal;
  show: (notice: RollbackNotice) => void;
  onSettled?: () => void;
}): Promise<void> {
  const key = rollbackNoticeKey(operationId);
  show({ key, type: 'loading', text: `Rollback to ${snapshotName} queued; following it` });
  let failure: unknown | null = null;
  try {
    await follow(signal);
  } catch (err) {
    failure = err;
  }
  if (signal.aborted || failure instanceof OperationFollowStoppedError) {
    show({
      key,
      type: 'info',
      text: `Rollback to ${snapshotName} was queued; the console stopped following it when its view closed. The sandbox page's Operations card shows how it ends.`,
    });
    return;
  }
  show({ key, ...rollbackOutcome(snapshotName, failure) });
  onSettled?.();
}

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
