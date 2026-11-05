// Why a stopped sandbox stopped when nobody asked it to.
//
// The admin API reports the cause its node published (`stop_cause` on the
// sandbox response: `pod_terminated` or `vm_exited`, with the node's stop
// time) while it renders the sandbox stopped. That is the only source: an API
// without the field, a null cause and a requested stop all read as a plain
// stop, as they did before the field existed.

import type { SandboxStopCause } from './types';

export type { SandboxStopCause };

const REASON_LABELS: Record<string, string> = {
  pod_terminated: 'pod terminated',
  vm_exited: 'VM exited',
};

// A reason a newer API adds reads as its own words rather than disappearing.
export function stopReasonLabel(reason: string): string {
  return REASON_LABELS[reason] ?? reason.replace(/_/g, ' ');
}

function isStopped(status: string | null | undefined): boolean {
  return status?.trim().toLowerCase() === 'stopped';
}

// The reported cause of a stopped sandbox's stop, or null. `stop_cause` is
// undefined from an API without the field and null when none is recorded.
export function reportedStopCause(sandbox: {
  status: string | null;
  stop_cause?: SandboxStopCause | null;
}): SandboxStopCause | null {
  if (!isStopped(sandbox.status)) return null;
  const cause = sandbox.stop_cause;
  if (!cause || typeof cause.reason !== 'string' || typeof cause.stopped_at !== 'string') return null;
  return cause;
}

function formatWhen(at: string): string {
  const ms = Date.parse(at);
  return Number.isNaN(ms) ? at : new Date(ms).toLocaleString();
}

// "Stopped unexpectedly: pod terminated at <time>".
export function stopCauseText(cause: SandboxStopCause): string {
  return `Stopped unexpectedly: ${stopReasonLabel(cause.reason)} at ${formatWhen(cause.stopped_at)}`;
}
