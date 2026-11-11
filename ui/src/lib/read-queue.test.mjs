import assert from 'node:assert/strict';
import { test } from 'node:test';
import { ReadQueue } from './read-queue.ts';

test('reads go out a few at a time, each name once', () => {
  const queue = new ReadQueue(2);
  queue.add(['a', 'b', 'c', 'a']);
  assert.deepEqual(queue.take(), ['a', 'b']);
  assert.deepEqual(queue.take(), [], 'no slot until a read ends');
  queue.add(['a', 'c']);
  queue.finish('a');
  assert.deepEqual(queue.take(), ['c'], 'a name in flight or waiting is not queued again');
});

test("Strict Mode's cleanup and second setup do not send an in-flight read again", () => {
  const queue = new ReadQueue(2);
  queue.add(['a', 'b', 'c']);
  const sent = [...queue.take()];
  // Cleanup: only the waiting read is dropped; setup queues the view again.
  queue.dropWaiting();
  queue.add(['a', 'b', 'c']);
  sent.push(...queue.take());
  // The first reads end; their finish must not free a duplicate of them.
  queue.finish('a');
  sent.push(...queue.take());
  queue.finish('b');
  sent.push(...queue.take());
  assert.deepEqual(sent, ['a', 'b', 'c'], 'each template read once');
});

test('a view that stops wanting a template drops its unsent read only', () => {
  const queue = new ReadQueue(1);
  queue.add(['a', 'b', 'c']);
  assert.deepEqual(queue.take(), ['a']);
  queue.dropWaiting((name) => name === 'c');
  queue.finish('a');
  assert.deepEqual(queue.take(), ['c']);
  assert.equal(queue.has('b'), false);
});
