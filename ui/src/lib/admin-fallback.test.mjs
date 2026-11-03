import assert from 'node:assert/strict';
import { test } from 'node:test';
import {
  isMissingRoute,
  isUnknownFieldRejection,
  listSnapshotsWithFallback,
  pageRows,
  sendWithOptionalField,
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

test('a route the release does not have is the fallback 404, an empty 404 or a 405', () => {
  for (const err of [
    apiError(404, { code: 404, message: 'Endpoint not found' }),
    apiError(404, null),
    apiError(405, null),
    apiError(404, { code: 'not_found', message: 'route not found' }),
  ]) {
    assert.equal(isMissingRoute(err), true, JSON.stringify(err.body));
  }
});

test("a handler's own 404 is not a missing route", () => {
  for (const err of [
    apiError(404, { error: 'Sandbox not found' }),
    apiError(404, { code: 'not_found', message: 'sandbox not found', request_id: null, retryable: false, details: [] }),
    apiError(404, { error: "rollback is not enabled for the sandbox's organization", operation_id: null, retryable: false }),
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

test('a list reads as rows from a page object or from a bare array', () => {
  assert.deepEqual(pageRows([{ id: 'a' }]), { rows: [{ id: 'a' }], nextCursor: null });
  assert.deepEqual(pageRows({ data: [{ id: 'a' }], next_cursor: 'c1' }), { rows: [{ id: 'a' }], nextCursor: 'c1' });
  assert.deepEqual(pageRows({ data: [], next_cursor: null }), { rows: [], nextCursor: null });
  assert.throws(() => pageRows({ sandboxes: [] }), /Unrecognized list response/);
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

test('snapshots come from the admin list whenever the release has it', async () => {
  let publicReads = 0;
  const listing = await listSnapshotsWithFallback(async () => [{ id: 'snap_1' }], async () => { publicReads += 1; return []; });
  assert.deepEqual(listing, { via: 'admin', rows: [{ id: 'snap_1' }] });
  assert.equal(publicReads, 0);
  // Another organization's sandbox too: no public reader is needed.
  assert.deepEqual(await listSnapshotsWithFallback(async () => [], null), { via: 'admin', rows: [] });
});

test('without the admin list, the own organization falls back to the public routes and another has none', async () => {
  const missing = async () => { throw apiError(404, { code: 404, message: 'Endpoint not found' }); };
  assert.deepEqual(await listSnapshotsWithFallback(missing, async () => [{ id: 'snap_2' }]), { via: 'public', rows: [{ id: 'snap_2' }] });
  assert.deepEqual(await listSnapshotsWithFallback(missing, null), { via: 'none' });
  // The public route's own failure (snapshots not enabled) reaches the caller.
  const hidden = apiError(404, { code: 'not_found', message: 'route not found' });
  await assert.rejects(listSnapshotsWithFallback(missing, async () => { throw hidden; }), (thrown) => thrown === hidden);
});

test('a real failure of the admin list does not fall back', async () => {
  const notFound = apiError(404, { code: 'not_found', message: 'sandbox not found' });
  let publicReads = 0;
  await assert.rejects(
    listSnapshotsWithFallback(async () => { throw notFound; }, async () => { publicReads += 1; return []; }),
    (thrown) => thrown === notFound,
  );
  assert.equal(publicReads, 0);
});
