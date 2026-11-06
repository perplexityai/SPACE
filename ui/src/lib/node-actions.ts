import type { AdminNodeResponse } from './types';

export interface ForceUploadResponse {
  reuploaded: string[];
  failed: { sandbox_id: string; error: string }[];
  deferred?: string[];
  truncated?: boolean;
}

// Shown on both drain confirmations: the node page and the nodes list.
export const DRAIN_TERMINATES =
  'Once the node is drained it is terminated automatically wherever the drained-node terminator is enabled, unless the node carries the `space.pplx.net/drain-keep` annotation.';

// `:force-upload-local-snapshots` (`routes/admin/nodes.rs`): the handler lists
// a bounded number of pinned sandboxes, in ascending ID order, and asks the
// node to upload the first few of them; the node queues each upload on its
// durable worker and the call returns. Both bounds are the server's and are
// not repeated here: the answer's `deferred` names the listed sandboxes it
// did not attempt, and `truncated` says more are pinned than it listed. The
// node-drain reconciler also keeps going.
export const UPLOAD_LIMITS = 'One call lists a limited number of the pinned sandboxes and attempts uploads for only the first of them; the result says how many listed sandboxes were not attempted, and whether more are pinned than the call listed. The node-drain reconciler also continues uploading on its own.';
export const UPLOAD_ACCEPTED_MEANS = 'An accepted request means the node has queued the upload. The snapshot is durable only once the upload completes and registers it in the object store.';
export const UPLOAD_RETRY_ORDER = 'Running it again continues with the same list, in sandbox ID order, so sandboxes that failed are tried again first.';

// What one call left out, from its answer: the listed sandboxes it did not
// attempt, and whether more are pinned than it listed.
export function uploadLeftOut(result: Pick<ForceUploadResponse, 'deferred' | 'truncated'>): string[] {
  const notes: string[] = [];
  const deferred = result.deferred?.length ?? 0;
  if (deferred) {
    notes.push(`${deferred} listed sandbox${deferred === 1 ? '' : 'es'} not attempted in this call.`);
  }
  if (result.truncated) {
    notes.push('More sandboxes are pinned to this node than this call listed.');
  }
  return notes;
}

// What a forced drain does beyond a gentle one (`NodeDrainMode::Forced` and
// `space-admin-cli drain-node --forced`): it evacuates residents now, gates
// user calls against them, and enables the stale-transient sweep.
export const FORCED_DRAIN_EVACUATES =
  'Every running or paused sandbox on the node is suspended and restored on another node right away, and user calls against those sandboxes are refused until the drain ends; the disruption grows with the number of live sandboxes. Sandboxes stuck in a transitional state (creating, starting, resuming and similar) for longer than the cluster\'s abandon age are marked failed, which cannot be undone.';

// `:terminate` (`routes/admin/nodes.rs`) moves a drained node to
// `terminating`; the node-drain reconciler then terminates it through the
// cluster's configured terminator and deletes its row. `:undrain` refuses a
// terminating node, and the `drain-keep` annotation only holds back the
// automatic terminator.
export const TERMINATE_ENDS =
  'The node moves to terminating, and the node-drain reconciler then terminates it and removes it from SPACE. A terminating node cannot be undrained. The `space.pplx.net/drain-keep` annotation does not stop this. Whether a replacement node is started was fixed when the drain began. The API refuses while sandboxes still occupy the node or are held for it.';

export type NodeAction = {
  action: string;
  label: string;
  body?: unknown;
  dangerous?: boolean;
  // The operator types the node's name (or ID) before the action is sent.
  typed?: boolean;
  confirm: string;
};

// The node page's and the nodes list's actions: only worker and builder
// nodes, and drain-family actions only on substrates that support node drain
// (the Direct substrate answers 501), except undrain, which clears a phase an
// earlier drain left. Forced drain and terminate need a typed confirmation.
// Forced drain is offered on an idle node and on a gentle drain, which the
// API escalates in place; the API refuses to downgrade a forced drain.
// Terminate is offered only on a drained node, the one phase the API accepts
// it from.
export function nodeActions(
  n: Pick<AdminNodeResponse, 'role' | 'drain_phase' | 'drain_mode'>,
  substrate: string | null,
): NodeAction[] {
  const drainable = n.role === 'worker' || n.role === 'builder';
  if (!drainable) return [];
  const supportsDrain = substrate !== 'space';
  const actions: NodeAction[] = [];
  if (supportsDrain && !n.drain_phase) {
    actions.push(
      {
        action: 'cordon', label: 'Cordon',
        confirm: 'New sandboxes stop being placed on this node; running sandboxes are not touched. The admin API does not report whether a node is cordoned, so Cordon and Uncordon are both offered.',
      },
      { action: 'uncordon', label: 'Uncordon', confirm: 'The node accepts new sandboxes again. Has no effect if it is not cordoned.' },
      {
        action: 'drain', label: 'Drain', body: { mode: 'gentle' }, dangerous: true,
        confirm: `Cordons the node and waits for its sandboxes to leave on their own. ${DRAIN_TERMINATES} Use Forced drain to evacuate sandboxes immediately instead.`,
      },
      {
        action: 'drain', label: 'Forced drain', body: { mode: 'forced' }, dangerous: true, typed: true,
        confirm: `Cordons the node and evacuates its sandboxes now. ${FORCED_DRAIN_EVACUATES} ${DRAIN_TERMINATES}`,
      },
    );
  }
  // A missing mode on a draining node is gentle, as everywhere else.
  if (supportsDrain && n.drain_phase === 'draining' && n.drain_mode !== 'forced') {
    actions.push({
      action: 'drain', label: 'Forced drain', body: { mode: 'forced' }, dangerous: true, typed: true,
      confirm: `Switches this gentle drain to forced in place: the node stays cordoned and the drain keeps its start time. ${FORCED_DRAIN_EVACUATES} A forced drain cannot be switched back to gentle; Undrain cancels it. ${DRAIN_TERMINATES}`,
    });
  }
  if (n.drain_phase === 'draining' || n.drain_phase === 'drained') {
    actions.push({ action: 'undrain', label: 'Undrain', confirm: 'The drain is cancelled and the node accepts new sandboxes again.' });
  }
  if (supportsDrain && n.drain_phase === 'drained') {
    actions.push({ action: 'terminate', label: 'Terminate', dangerous: true, typed: true, confirm: TERMINATE_ENDS });
  }
  // The API accepts this only while the drain gate blocks wakes: drained,
  // terminating, or a forced drain.
  const uploadOpen =
    n.drain_phase === 'drained' || n.drain_phase === 'terminating' || (n.drain_phase === 'draining' && n.drain_mode === 'forced');
  if (supportsDrain && uploadOpen) {
    actions.push({
      action: 'force-upload-local-snapshots', label: 'Upload local snapshots', dangerous: true,
      confirm: `Asks this node to upload the snapshots of suspended sandboxes pinned to it to the object store, so they can restore elsewhere. ${UPLOAD_LIMITS}`,
    });
  }
  return actions;
}
