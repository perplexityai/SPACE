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

// A v2 drain (`:drain {engine: v2, hold_seconds}`): the node drains itself.
// It takes no mode, and its deadline moves only earlier.
export const V2_DRAIN_STARTS =
  'Cordons the node and seals it against new VM activations: the node makes every sandbox on it durable elsewhere but a running or stopped one, keeps running sandboxes running until the deadline, the hold after now, and then suspends them the way an idle auto-pause does. A call that would start or restore a sandbox on the node is refused with a retryable 503 until the sandbox is durable, and then lands on another node; nothing restores a suspended sandbox before its next call. For the drain\'s whole life, its undrain\'s cancel included, users\' :stop, :kill and rollbacks without restart of its sandboxes get a retryable 503. The drain holds one of the cluster\'s evacuation slots until it reaches drained or is undrained, and is refused when every slot is held.';

export const V2_SHORTEN_HOLD =
  'Moves this v2 drain\'s deadline earlier, to the hold after the drain\'s start, so it may end at once. From the deadline the node suspends the running sandboxes it was holding. The deadline can\'t be moved later: the API refuses a longer hold.';

export const V2_END_HOLD =
  'Moves this v2 drain\'s deadline to now: the node suspends the running sandboxes it was holding and makes them durable elsewhere, and calls to them are refused until each restores on another node on its next call. The deadline can\'t be moved back; Undrain cancels the drain, and a sandbox it suspended stays suspended.';

// What :terminate does to a drained v2 node: it moves to terminating at
// once, and the cluster controller asks the node once more before it fences
// the instance.
export const V2_TERMINATE_ENDS =
  'The node moves to terminating. The cluster controller then asks the node once more, and terminates it only if nothing is pending or blocked and no sandbox or execution still names it. A failed or non-empty check returns the node to drained, with why in Last error, and you can terminate again once it is resolved; a sandbox or execution that still names the node keeps it terminating ("terminate held: ..." in Last error) until it clears. Until the instance is fenced the node can still be undrained.';

// What happens to a drained v2 node: the drained-node terminator skips v2
// drains, so only Terminate ends one.
// What :undrain does to a v2 node: the phase clears at once, and the node
// stays cordoned and sealed until the cluster controller has cancelled the
// drain on it.
export const V2_UNDRAIN_CANCELS =
  'The drain is cancelled: the phase clears at once, and the node stays cordoned until the cluster controller has cancelled the drain on it, which unseals it; then it accepts new sandboxes again. A sandbox the drain suspended stays suspended until its next call.';

export const V2_DRAIN_ENDS =
  'Once the node is drained, Terminate ends it; the automatic drained-node terminator leaves v2 drains alone.';

export type NodeAction = {
  action: string;
  label: string;
  body?: unknown;
  // The action asks for a v2 drain's hold, whose `:drain` body is
  // `v2DrainBodyFor(hold, seconds)` (`drain-progress.ts`): `start` counts it
  // from now and starts the drain, start-only; `shorten` counts it from the
  // drain's start and names no engine, so it never starts one.
  hold?: 'start' | 'shorten';
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
// API escalates in place; the API refuses to downgrade a forced drain. An
// idle worker also offers a v2 drain, beside the v1 choices; the plain Drain
// stays v1's gentle one. A v2 drain takes no mode: while it is draining,
// Shorten hold moves its deadline earlier and End hold now moves it to now.
// Terminate is offered only on a drained node, the one phase the API accepts
// it from. Upload local snapshots is offered only under a v1 drain.
export function nodeActions(
  n: Pick<AdminNodeResponse, 'role' | 'drain_phase' | 'drain_mode' | 'drain_engine'> & {
    drain?: Pick<NonNullable<AdminNodeResponse['drain']>, 'fenced'> | null;
  },
  substrate: string | null,
): NodeAction[] {
  const drainable = n.role === 'worker' || n.role === 'builder';
  if (!drainable) return [];
  const supportsDrain = substrate !== 'space';
  const actions: NodeAction[] = [];
  const v2 = n.drain_engine === 'v2';
  // An undrained v2 drain whose cancel hasn't reached the node yet keeps its
  // engine with no phase; the API refuses every drain until the cancel
  // finishes ("undrain still finishing").
  const v2Cancelling = v2 && !n.drain_phase;
  if (supportsDrain && !n.drain_phase) {
    actions.push(
      {
        action: 'cordon', label: 'Cordon',
        confirm: 'New sandboxes stop being placed on this node; running sandboxes are not touched. The admin API does not report whether a node is cordoned, so Cordon and Uncordon are both offered.',
      },
    );
    // A v2 cancel pending leaves the node cordoned until the controller has
    // cancelled the drain on it, which may still be sealed: the API refuses
    // Uncordon meanwhile.
    if (!v2Cancelling) {
      actions.push({ action: 'uncordon', label: 'Uncordon', confirm: 'The node accepts new sandboxes again. Has no effect if it is not cordoned.' });
    }
  }
  if (supportsDrain && !n.drain_phase && !v2Cancelling) {
    actions.push(
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
  if (supportsDrain && !n.drain_phase && !v2Cancelling) {
    // A v2 drain is a sandbox worker's; the API drains Kubernetes nodes only
    // and answers anything else with its refusal.
    if (n.role === 'worker') {
      actions.push({
        action: 'drain', label: 'v2 drain', hold: 'start', dangerous: true, typed: true,
        confirm: `${V2_DRAIN_STARTS} ${V2_DRAIN_ENDS}`,
      });
    }
  }
  if (supportsDrain && v2 && n.drain_phase === 'draining') {
    actions.push(
      {
        action: 'drain', label: 'Shorten hold', hold: 'shorten', dangerous: true, typed: true,
        confirm: `${V2_SHORTEN_HOLD} ${V2_DRAIN_ENDS}`,
      },
      {
        action: 'drain', label: 'End hold now', body: { hold_seconds: 0 }, dangerous: true, typed: true,
        confirm: `${V2_END_HOLD} ${V2_DRAIN_ENDS}`,
      },
    );
  }
  // A missing mode on a draining node is gentle, as everywhere else.
  if (supportsDrain && !v2 && n.drain_phase === 'draining' && n.drain_mode !== 'forced') {
    actions.push({
      action: 'drain', label: 'Forced drain', body: { mode: 'forced' }, dangerous: true, typed: true,
      confirm: `Switches this gentle drain to forced in place: the node stays cordoned and the drain keeps its start time. ${FORCED_DRAIN_EVACUATES} A forced drain cannot be switched back to gentle; Undrain cancels it. ${DRAIN_TERMINATES}`,
    });
  }
  // A v2 node in terminating can be undrained until its instance is fenced.
  const v2Unfenced = v2 && n.drain_phase === 'terminating' && n.drain?.fenced !== true;
  if (n.drain_phase === 'draining' || n.drain_phase === 'drained' || v2Unfenced) {
    actions.push({
      action: 'undrain', label: 'Undrain',
      confirm: v2 ? V2_UNDRAIN_CANCELS : 'The drain is cancelled and the node accepts new sandboxes again.',
    });
  }
  if (supportsDrain && n.drain_phase === 'drained') {
    actions.push({
      action: 'terminate', label: 'Terminate', dangerous: true, typed: true,
      confirm: v2 ? V2_TERMINATE_ENDS : TERMINATE_ENDS,
    });
  }
  // The API accepts this only under a v1 drain whose gate refuses wakes:
  // drained, terminating, or forced. It refuses it under a v2 drain, in every
  // phase and while its cancel is pending: the sealed node itself uploads
  // each local snapshot of a suspended resident that it has not recorded as
  // backed up.
  const uploadOpen =
    !v2 && (n.drain_phase === 'drained' || n.drain_phase === 'terminating' || (n.drain_phase === 'draining' && n.drain_mode === 'forced'));
  if (supportsDrain && uploadOpen) {
    actions.push({
      action: 'force-upload-local-snapshots', label: 'Upload local snapshots', dangerous: true,
      confirm: `Asks this node to upload the snapshots of suspended sandboxes pinned to it to the object store, so they can restore elsewhere. ${UPLOAD_LIMITS}`,
    });
  }
  return actions;
}
