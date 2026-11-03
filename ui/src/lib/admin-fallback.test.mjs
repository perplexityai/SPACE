import assert from 'node:assert/strict';
import { test } from 'node:test';
import {
  isMissingRoute,
  isUnknownFieldRejection,
  listSnapshots,
  listsSnapshotsOn,
  moveReleaseTag,
  pageRows,
  sendWithOptionalField,
  tagMoveFailureMessage,
} from './admin-fallback.ts';

// The shape `ApiError` gives the console: status, message, parsed JSON body
// (null when not JSON) and raw text.
const apiError = (status, body, text = body ? JSON.stringify(body) : '') =>
  Object.assign(new Error(body?.error || body?.message || 'Request failed'), { status, body, text });

// Axum's plain-text rejection of an unknown body field, as a release answers it.
const unknownField = (field, expected) => apiError(
  422,
  null,
  `Failed to deserialize the JSON body into the target type: ${field}: unknown field \`${field}\`, expected ${expected} at line 1 column 40`,
);

test('a route the release does not have is the admin fallback 404, an empty 404 or a 405', () => {
  for (const err of [
    apiError(404, { code: 404, message: 'Endpoint not found' }),
    apiError(404, null),
    apiError(405, null),
  ]) {
    assert.equal(isMissingRoute(err), true, JSON.stringify(err.body));
  }
});

test("a handler's own 404 is not a missing route", () => {
  for (const err of [
    apiError(404, { error: 'Sandbox not found' }),
    apiError(404, { code: 'not_found', message: 'sandbox not found', request_id: null, retryable: false, details: [] }),
    apiError(404, { error: "rollback is not enabled for the sandbox's organization", operation_id: null, retryable: false }),
    // The public API's hidden-operation answer, which no admin route sends.
    apiError(404, { code: 'not_found', message: 'route not found' }),
    apiError(400, null),
    new TypeError('Failed to fetch'),
  ]) {
    assert.equal(isMissingRoute(err), false, err.message);
  }
});

test('an unknown-field 422 is recognized only for the field it names', () => {
  const err = unknownField('user_id', '`name` or `allowed_surfaces`');
  assert.equal(isUnknownFieldRejection(err, 'user_id'), true);
  assert.equal(isUnknownFieldRejection(err, 'name'), false);
  // A 422 for a bad value, and a 400, are real refusals.
  assert.equal(isUnknownFieldRejection(apiError(422, null, 'Failed to deserialize the JSON body into the target type: user_id: UUID parsing failed'), 'user_id'), false);
  assert.equal(isUnknownFieldRejection(apiError(400, { error: 'user_id must name a user of the organization' }), 'user_id'), false);
});

test('a list reads as rows from a page object, keeping its cursor, or from a bare array', () => {
  assert.deepEqual(pageRows([{ id: 'a' }]), { rows: [{ id: 'a' }], nextCursor: null });
  assert.deepEqual(pageRows({ data: [{ id: 'a' }], next_cursor: 'c1' }), { rows: [{ id: 'a' }], nextCursor: 'c1' });
  assert.deepEqual(pageRows({ data: [], next_cursor: null }), { rows: [], nextCursor: null });
  assert.deepEqual(pageRows({ data: [], next_cursor: '' }), { rows: [], nextCursor: null });
  // No route this reads answers another page shape: it is refused, not read as empty.
  assert.throws(() => pageRows({ sandboxes: [], next_cursor: 'c1' }), /Unrecognized list response/);
  assert.throws(() => pageRows(null), /Unrecognized list response/);
});

test('a tag move sends the precondition once, to the release tag route', async () => {
  const calls = [];
  const put = async (path, body) => { calls.push([path, body]); };
  assert.deepEqual(await moveReleaseTag(put, 'rel-new', 'prod', 'rel-old'), { checked: true });
  assert.deepEqual(calls, [['/v1/template-releases/rel-new/tag', { tag: 'prod', expected_current_release_id: 'rel-old' }]]);
  // A release that refuses the field moves it unchecked, and says so.
  calls.length = 0;
  const old = async (path, body) => {
    calls.push([path, body]);
    if ('expected_current_release_id' in body) throw unknownField('expected_current_release_id', '`tag`');
  };
  assert.deepEqual(await moveReleaseTag(old, 'rel-new', 'prod', null), { checked: false });
  assert.deepEqual(calls.map(([, body]) => body), [{ tag: 'prod', expected_current_release_id: null }, { tag: 'prod' }]);
});

test("a refused precondition says the tag moved and names where it is now; other failures keep the API message", () => {
  const moved = apiError(412, {
    code: 'failed_precondition',
    message: 'tag does not point at expected_current_release_id',
    details: [{ key: 'current_release_id', value: 'rel-other' }],
  });
  assert.match(tagMoveFailureMessage(moved), /moved since it was read \(it now names release rel-other\); nothing was changed/);
  // A tag deleted meanwhile has no current release.
  assert.match(tagMoveFailureMessage(apiError(412, { message: 'x', details: [] })), /moved since it was read; nothing was changed/);
  assert.equal(tagMoveFailureMessage(apiError(404, { error: 'Template release not found' })), 'Template release not found');
});

test('the tag precondition is sent, and kept when the release accepts it', async () => {
  const sent = [];
  const outcome = await sendWithOptionalField(async (body) => { sent.push(body); return 'moved'; }, { tag: 'prod' }, 'expected_current_release_id', 'rel-1');
  assert.deepEqual(sent, [{ tag: 'prod', expected_current_release_id: 'rel-1' }]);
  assert.deepEqual(outcome, { result: 'moved', fieldAccepted: true });
  // `null` asks that the tag not exist yet, and is sent as null, not dropped.
  sent.length = 0;
  await sendWithOptionalField(async (body) => { sent.push(body); }, { tag: 'canary' }, 'expected_current_release_id', null);
  assert.deepEqual(sent, [{ tag: 'canary', expected_current_release_id: null }]);
});

test('a release that refuses the precondition gets one retry without it, reported as unchecked', async () => {
  const sent = [];
  const outcome = await sendWithOptionalField(async (body) => {
    sent.push(body);
    if ('expected_current_release_id' in body) throw unknownField('expected_current_release_id', '`tag`');
    return 'moved';
  }, { tag: 'prod' }, 'expected_current_release_id', 'rel-1');
  assert.deepEqual(sent, [{ tag: 'prod', expected_current_release_id: 'rel-1' }, { tag: 'prod' }]);
  assert.deepEqual(outcome, { result: 'moved', fieldAccepted: false });
});

test('any other failure of the tag move is not retried', async () => {
  for (const err of [
    apiError(412, { code: 'failed_precondition', message: 'tag does not point at expected_current_release_id' }),
    unknownField('tag', '`expected_current_release_id`'),
    apiError(500, { error: 'Internal server error' }),
  ]) {
    let calls = 0;
    await assert.rejects(
      sendWithOptionalField(async () => { calls += 1; throw err; }, { tag: 'prod' }, 'expected_current_release_id', 'rel-1'),
      (thrown) => thrown === err,
    );
    assert.equal(calls, 1, err.message);
  }
  // A retry that fails reports its own failure.
  const second = apiError(409, { message: 'tag changed while it was being moved; read it again and retry' });
  let calls = 0;
  await assert.rejects(
    sendWithOptionalField(async () => {
      calls += 1;
      throw calls === 1 ? unknownField('expected_current_release_id', '`tag`') : second;
    }, { tag: 'prod' }, 'expected_current_release_id', 'rel-1'),
    (thrown) => thrown === second,
  );
  assert.equal(calls, 2);
});

const reader = (rows, nextCursor = null) => {
  const calls = { count: 0 };
  const read = async () => { calls.count += 1; return nextCursor === undefined ? rows : { rows, nextCursor }; };
  return { read, calls };
};

test("the own organization's snapshots come from the public routes only, never the audited admin list", async () => {
  const admin = reader([{ id: 'snap_admin' }]);
  const listing = await listSnapshots({ ownOrg: true, readPublic: async () => [{ id: 'snap_1' }], readAdmin: admin.read });
  assert.deepEqual(listing, { via: 'public', rows: [{ id: 'snap_1' }] });
  assert.equal(admin.calls.count, 0);
  // Its failures, the hidden-operation gate included, reach the caller.
  const hidden = apiError(404, { code: 'not_found', message: 'route not found' });
  await assert.rejects(
    listSnapshots({ ownOrg: true, readPublic: async () => { throw hidden; }, readAdmin: admin.read }),
    (thrown) => thrown === hidden,
  );
  assert.equal(admin.calls.count, 0);
});

test("another organization's snapshots come from the admin list, which says when older ones were not read", async () => {
  let publicReads = 0;
  const readPublic = async () => { publicReads += 1; return []; };
  assert.deepEqual(
    await listSnapshots({ ownOrg: false, readPublic, readAdmin: reader([{ id: 'snap_2' }]).read }),
    { via: 'admin', rows: [{ id: 'snap_2' }], more: false },
  );
  assert.deepEqual(
    await listSnapshots({ ownOrg: false, readPublic, readAdmin: reader([{ id: 'snap_3' }], 'cursor-1').read }),
    { via: 'admin', rows: [{ id: 'snap_3' }], more: true },
  );
  assert.equal(publicReads, 0);
});

test("without the admin list, another organization's sandbox has no listing; a real failure is thrown", async () => {
  const missing = async () => { throw apiError(404, { code: 404, message: 'Endpoint not found' }); };
  assert.deepEqual(await listSnapshots({ ownOrg: false, readPublic: async () => [], readAdmin: missing }), { via: 'none' });
  const notFound = apiError(404, { code: 'not_found', message: 'sandbox not found' });
  await assert.rejects(
    listSnapshots({ ownOrg: false, readPublic: async () => [], readAdmin: async () => { throw notFound; } }),
    (thrown) => thrown === notFound,
  );
});

test("another organization's snapshot card has no timed read: the page's reloads never read the admin list", async () => {
  // What the sandbox page sends the card: it opens, the page reloads every
  // 20 s (and on its Refresh and each action), the operator presses the
  // card's Refresh, and a rollback it sent settles. The card asks
  // `listsSnapshotsOn` on each reload.
  const triggers = ['open', 'page-reload', 'page-reload', 'page-reload', 'refresh', 'page-reload', 'rollback-settled', 'page-reload'];
  const reads = async (ownOrg) => {
    const admin = reader([{ id: 'snap_admin' }]);
    let publicReads = 0;
    for (const trigger of triggers) {
      if (!listsSnapshotsOn(trigger, ownOrg)) continue;
      await listSnapshots({ ownOrg, readPublic: async () => { publicReads += 1; return []; }, readAdmin: admin.read });
    }
    return { admin: admin.calls.count, public: publicReads };
  };
  assert.deepEqual(await reads(false), { admin: 3, public: 0 });
  // The own organization's card lists through the public routes on every reload.
  assert.deepEqual(await reads(true), { admin: 0, public: triggers.length });
});
