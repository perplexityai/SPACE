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

export type NodeAction = {
  action: string;
  label: string;
  body?: unknown;
  dangerous?: boolean;
  confirm: string;
};

// The node page's actions follow the nodes list's rules: only
// worker and builder nodes, and drain-family actions only on substrates that
// support node drain (the Direct substrate answers 501), except undrain,
// which clears a phase an earlier drain left. Forced drain and terminate stay
// CLI actions (`space-admin-cli drain-node --forced`, `terminate-node`), not
// dashboard buttons one misclick away from evacuating or ending a node.
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
        confirm: `Cordons the node and waits for its sandboxes to leave on their own. ${DRAIN_TERMINATES} Use \`space-admin-cli drain-node --forced\` to evacuate sandboxes immediately instead.`,
      },
    );
  }
  if (n.drain_phase === 'draining' || n.drain_phase === 'drained') {
    actions.push({ action: 'undrain', label: 'Undrain', confirm: 'The drain is cancelled and the node accepts new sandboxes again.' });
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
