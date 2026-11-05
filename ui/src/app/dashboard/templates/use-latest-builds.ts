'use client';

import { useEffect, useMemo, useRef, useState } from 'react';
import { adminApi } from '@/lib/api';
import { isMissingRoute, pageRows } from '@/lib/admin-fallback';
import { errorMessage } from '@/lib/template-format';
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
// `GET /templates/{name}/builds?limit=1` as the view asks for them. An API
// release without the route answers 405: every template then reads as
// `unsupported` without a request, until a later refresh (`reloadKey`) asks
// again.
export function useLatestBuilds(wanted: string[], reloadKey: number) {
  const [entries, setEntries] = useState<ReadonlyMap<string, Entry>>(() => new Map());
  const [unsupported, setUnsupported] = useState(false);
  const entriesRef = useRef(entries);
  entriesRef.current = entries;
  const unsupportedRef = useRef(false);
  const queue = useRef<string[]>([]);
  const queued = useRef(new Set<string>());
  const running = useRef(0);
  const mounted = useRef(true);

  useEffect(() => () => {
    mounted.current = false;
  }, []);

  const store = (name: string, value: LatestBuild) => {
    if (!mounted.current) return;
    setEntries((current) => new Map(current).set(name, { value, readAt: Date.now() }));
  };

  const read = async (name: string) => {
    try {
      const body = await adminApi.get<unknown>(`/v1/templates/${encodedPathSegment(name)}/builds?limit=1`);
      const [build] = pageRows<TemplateBuildListEntry>(body).rows;
      unsupportedRef.current = false;
      if (mounted.current) setUnsupported(false);
      store(name, build ? { kind: 'build', build } : { kind: 'none' });
    } catch (err: unknown) {
      if (isMissingRoute(err)) {
        unsupportedRef.current = true;
        if (mounted.current) setUnsupported(true);
        store(name, { kind: 'unsupported' });
      } else {
        store(name, { kind: 'error', message: errorMessage(err) });
      }
    }
  };

  const pump = () => {
    while (running.current < CONCURRENCY && queue.current.length > 0) {
      const name = queue.current.shift()!;
      if (unsupportedRef.current) {
        queued.current.delete(name);
        store(name, { kind: 'unsupported' });
        continue;
      }
      running.current += 1;
      void read(name).finally(() => {
        running.current -= 1;
        queued.current.delete(name);
        pump();
      });
    }
  };

  const enqueue = (names: string[]) => {
    for (const name of names) {
      if (queued.current.has(name)) continue;
      queued.current.add(name);
      queue.current.push(name);
    }
    pump();
  };

  const wantedKey = wanted.join('\n');
  useEffect(() => {
    enqueue(wanted.filter((name) => !entriesRef.current.has(name)));
  }, [wantedKey]);

  // A refresh reads again what is shown and older than STALE_MS; on an API
  // without the route, the reads ask again.
  const lastReload = useRef(reloadKey);
  useEffect(() => {
    if (lastReload.current === reloadKey) return;
    lastReload.current = reloadKey;
    if (unsupportedRef.current) {
      // The first reads go out; if they are refused again, the rest are
      // marked without a request.
      unsupportedRef.current = false;
      enqueue(wanted.filter((name) => entriesRef.current.get(name)?.value.kind === 'unsupported'));
      return;
    }
    const now = Date.now();
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
