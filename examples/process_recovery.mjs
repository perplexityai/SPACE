#!/usr/bin/env node
// One explicit request per connection. Import recoveryRequest from this file;
// callers own retry decisions, durable cursors, sequences and unchanged fences.
import { pathToFileURL } from 'node:url';

export async function recoveryRequest({dataPlaneUrl, attach, request, timeoutMs = 30000, maxBytes = 1024 * 1024, onEvent = () => {}}) {
  const origin = new URL(dataPlaneUrl);
  if (!['http:', 'https:'].includes(origin.protocol) || origin.username || origin.password || origin.search || origin.hash || origin.pathname !== '/') {
    throw new Error('dataPlaneUrl must be an HTTP(S) origin');
  }
  if (origin.protocol === 'http:' && !['localhost', '127.0.0.1', '[::1]'].includes(origin.hostname)) {
    throw new Error('unencrypted connections are allowed only on loopback');
  }
  const target = new URL(attach.url, origin);
  if (target.protocol === 'ws:') target.protocol = 'http:';
  if (target.protocol === 'wss:') target.protocol = 'https:';
  if (target.origin !== origin.origin || target.username || target.password || target.search || target.hash) {
    throw new Error('attachment must stay on the configured data-plane origin');
  }
  target.protocol = target.protocol === 'https:' ? 'wss:' : 'ws:';
  for (const key of ['sequence', 'output_cursor']) {
    if (request[key] !== undefined && (!Number.isSafeInteger(request[key]) || request[key] < 0)) {
      throw new Error(`${key} must be a nonnegative safe integer`);
    }
  }
  if (!(timeoutMs > 0 && maxBytes > 0)) throw new Error('positive request bounds required');
  return new Promise((resolve, reject) => {
    let finished = false;
    let attached = false;
    let bytes = 0;
    const protocol = 'space.process-recovery.v1';
    const socket = new WebSocket(target, protocol);
    const finish = (error) => {
      if (finished) return;
      finished = true;
      clearTimeout(timer);
      socket.close();
      if (error) reject(error); else resolve();
    };
    const timer = setTimeout(() => finish(new Error('request deadline elapsed; outcome may be uncertain')), timeoutMs);
    socket.addEventListener('open', () => {
      if (socket.protocol !== protocol) return finish(new Error('data plane does not support process recovery'));
      socket.send(JSON.stringify({type: 'attach', token: attach.token}));
    });
    socket.addEventListener('message', ({data}) => {
      try {
        bytes += Buffer.byteLength(data);
        if (bytes > maxBytes) throw new Error('response limit exceeded; keep the last committed cursor');
        const frame = JSON.parse(data);
        for (const key of ['output_cursor', 'skipped_events', 'output_bytes_before', 'sequence', 'input_written']) {
          if (frame[key] != null && !Number.isSafeInteger(frame[key])) throw new Error(`unsafe ${key}`);
        }
        if (!attached) {
          if (frame.type !== 'attached') throw new Error('recovery attachment refused');
          attached = true;
          socket.send(JSON.stringify(request));
          return;
        }
        if (frame.type === 'recovery_complete') return finish();
        if (frame.type === 'recovery_error') {
          const error = new Error(frame.message);
          error.recovery = frame;
          return finish(error);
        }
        if (!['recovery_started', 'recovery_connected', 'recovery_stdout', 'recovery_stderr', 'recovery_output_gap', 'recovery_exited', 'recovery_accepted'].includes(frame.type)) {
          throw new Error('unexpected recovery frame');
        }
        onEvent(frame);
      } catch (error) { finish(error); }
    });
    socket.addEventListener('error', () => finish(new Error('transport failed; outcome may be uncertain')));
    socket.addEventListener('close', () => finish(new Error('stream ended without completion; outcome may be uncertain')));
  });
}

// Bounded JSON-in/JSON-out driver, also used by the public smoke scenario.
// Credentials arrive on stdin and are never printed or placed in argv.
if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  let input = '';
  for await (const chunk of process.stdin) {
    input += chunk;
    if (input.length > 2 * 1024 * 1024) throw new Error('request input too large');
  }
  const frames = [];
  try {
    await recoveryRequest({...JSON.parse(input), onEvent: frame => frames.push(frame)});
    process.stdout.write(JSON.stringify({frames, error: null}));
  } catch (error) {
    process.stdout.write(JSON.stringify({frames, error: error.recovery ?? {message: error.message}}));
  }
}
