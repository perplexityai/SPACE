'use client';

import { useEffect, useRef, useState } from 'react';

const normalized = (search: string) => new URLSearchParams(search).toString();

// A view kept in the URL, which is authoritative: a URL the page did not
// write itself (a link, the menu, Back) replaces the view on screen. The
// page's changes are written with `replaceState`, or `pushState` for a step
// worth going back to, such as another tab; Next keeps `useSearchParams` in
// step with both, in a transition, so while a search is typed an older
// write can still arrive. Such a value no longer matches the address bar
// and is ignored.
export function useUrlView<V>(
  searchParams: { toString(): string },
  pathname: string,
  parse: (params: URLSearchParams) => V,
  serialize: (view: V) => URLSearchParams,
) {
  const query = searchParams.toString();
  const [view, setView] = useState(() => parse(new URLSearchParams(query)));
  const viewRef = useRef(view);
  viewRef.current = view;
  const lastWritten = useRef(query);

  useEffect(() => {
    if (query === lastWritten.current) return;
    if (query !== normalized(window.location.search)) return;
    lastWritten.current = query;
    const next = parse(new URLSearchParams(query));
    viewRef.current = next;
    setView(next);
  }, [query]);

  const update = (change: Partial<V>, mode: 'replace' | 'push' = 'replace') => {
    const next = { ...viewRef.current, ...change };
    viewRef.current = next;
    setView(next);
    const qs = serialize(next).toString();
    lastWritten.current = qs;
    const url = qs ? `${pathname}?${qs}` : pathname;
    if (mode === 'push') window.history.pushState(null, '', url);
    else window.history.replaceState(null, '', url);
  };

  return [view, update] as const;
}
