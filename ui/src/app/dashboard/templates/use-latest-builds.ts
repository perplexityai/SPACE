'use client';

import { useEffect, useMemo, useRef, useState } from 'react';
import { adminApi } from '@/lib/api';
import { isMissingRoute, pageRows } from '@/lib/admin-fallback';
import { errorMessage } from '@/lib/template-format';
import { ReadQueue } from '@/lib/read-queue';
import { READ_TIMEOUT_MS } from '@/lib/template-reads';
import type { LatestBuild } from '@/lib/template-list';
import type { TemplateBuildListEntry } from '@/lib/types';
import { encodedPathSegment } from './use-templates-data';

// Each read asks the builder for the job's state, so a few at a time, and a
// read is reused for a minute before a refresh reads it again.
const CONCURRENCY = 4;
const STALE_MS = 60_000;

interface Entry {
  value: LatestBuild;
  readAt: number;
}

// The newest build of each template in `wanted`, read with
// `GET /templates/{name}/builds?limit=1` as the view asks for them. While the
// view stays open, each template is read at most once a minute. An API
// release without the route answers 405: every template then reads as
// `unsupported` without a request, until a refresh (`reloadKey`) a minute
// later asks again.
export function useLatestBuilds(wanted: string[], reloadKey: number) {
  const [entries, setEntries] = useState<ReadonlyMap<string, Entry>>(() => new Map());
  const [unsupported, setUnsupported] = useState(false);
  const entriesRef = useRef(entries);
  const unsupportedRef = useRef(false);
  // When the API last answered that it has no builds route.
  const unsupportedAt = useRef(0);
  const queue = useRef(new ReadQueue(CONCURRENCY));
  const mounted = useRef(false);
  const wantedRef = useRef(new Set(wanted));

  // Synced after each commit, before the effects below read them: a render
  // React discards must not leave them holding what no committed view showed.
  useEffect(() => {
    entriesRef.current = entries;
    wantedRef.current = new Set(wanted);
  });

  // Set in the effect, not at first render: Strict Mode runs the cleanup and
  // the setup again on a mounted component. The cleanup drops the reads not
  // sent yet; one in flight stays queued until it ends, so the second setup
  // does not send it again.
  useEffect(() => {
    mounted.current = true;
    return () => {
      mounted.current = false;
      queue.current.dropWaiting();
    };
  }, []);

  const store = (name: string, value: LatestBuild) => {
    if (!mounted.current) return;
    setEntries((current) => new Map(current).set(name, { value, readAt: Date.now() }));
  };

  const read = async (name: string) => {
    try {
      // Bounded, so a stalled read cannot hold one of the queue's slots.
      const body = await adminApi.get<unknown>(`/v1/templates/${encodedPathSegment(name)}/builds?limit=1`, {
        signal: AbortSignal.timeout(READ_TIMEOUT_MS),
      });
      const [build] = pageRows<TemplateBuildListEntry>(body).rows;
      unsupportedRef.current = false;
      if (mounted.current) setUnsupported(false);
      store(name, build ? { kind: 'build', build } : { kind: 'none' });
    } catch (err: unknown) {
      if (isMissingRoute(err)) {
        unsupportedRef.current = true;
        unsupportedAt.current = Date.now();
        if (mounted.current) setUnsupported(true);
        store(name, { kind: 'unsupported' });
      } else {
        store(name, { kind: 'error', message: errorMessage(err) });
      }
    }
  };

  const pump = () => {
    while (mounted.current) {
      const names = queue.current.take();
      if (names.length === 0) return;
      for (const name of names) {
        if (unsupportedRef.current) {
          queue.current.finish(name);
          store(name, { kind: 'unsupported' });
          continue;
        }
        void read(name).finally(() => {
          queue.current.finish(name);
          pump();
        });
      }
    }
  };

  const enqueue = (names: string[]) => {
    queue.current.add(names);
    pump();
  };

  // A read not sent yet for a template the view no longer shows is dropped:
  // each read asks a builder.
  const wantedKey = wanted.join('\n');
  useEffect(() => {
    queue.current.dropWaiting((name) => wantedRef.current.has(name));
    enqueue(wanted.filter((name) => !entriesRef.current.has(name)));
  }, [wantedKey]);

  // A refresh reads again what is shown and older than STALE_MS; on an API
  // without the route, the reads ask again once STALE_MS has passed.
  const lastReload = useRef(reloadKey);
  useEffect(() => {
    if (lastReload.current === reloadKey) return;
    lastReload.current = reloadKey;
    const now = Date.now();
    if (unsupportedRef.current) {
      if (now - unsupportedAt.current < STALE_MS) return;
      // The first reads go out; if they are refused again, the rest are
      // marked without a request.
      unsupportedRef.current = false;
      enqueue(wanted.filter((name) => entriesRef.current.get(name)?.value.kind === 'unsupported'));
      return;
    }
    enqueue(wanted.filter((name) => {
      const entry = entriesRef.current.get(name);
      return !entry || now - entry.readAt >= STALE_MS;
    }));
  }, [reloadKey]);

  const builds = useMemo(() => {
    const values = new Map<string, LatestBuild>();
    for (const [name, entry] of entries) values.set(name, entry.value);
    return values;
  }, [entries]);
  return { builds, unsupported };
}
