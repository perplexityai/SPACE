const UUID_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

// `?org=` from the URL is sent to the admin API as `org_id`; anything that is
// not a UUID would only make every list request fail with 400.
export function parseOrgParam(raw: string | null): string | null {
  const value = raw?.trim() ?? '';
  return UUID_PATTERN.test(value) ? value.toLowerCase() : null;
}

// Org names need not be unique; the slug is. Show it where a name repeats.
export function orgOptionLabels(orgs: { id: string; name: string; slug: string }[]): { value: string; label: string }[] {
  const counts = new Map<string, number>();
  for (const org of orgs) counts.set(org.name, (counts.get(org.name) ?? 0) + 1);
  return orgs.map((org) => ({
    value: org.id,
    label: (counts.get(org.name) ?? 0) > 1 ? `${org.name} (${org.slug})` : org.name,
  }));
}

// Windows share a view's refresh timestamp under one key, so one window's
// reload counts for all of them. Only the unscoped default-size list uses
// the plain key; any other org or size is a different view with its own key,
// or a window showing another scope would keep it "fresh" and it would never
// reload.
export function scopedRefreshKey(base: string, limit: number, defaultLimit: number, org: string | null): string {
  if (org) return `${base}:org-${org}:limit-${limit}`;
  return limit === defaultLimit ? base : `${base}:limit-${limit}`;
}

// What the direct-lookup notice says about a sandbox found outside the loaded
// rows, under the selected org filter.
export function lookupScopeText(
  sandbox: { org_id: string | null },
  selectedOrg: string | null,
  orgName: (id: string) => string | undefined,
  limit: number,
): string {
  if (!selectedOrg) return `It is not among the newest ${limit} sandboxes loaded below.`;
  const selected = orgName(selectedOrg) ?? selectedOrg;
  if (sandbox.org_id === selectedOrg) {
    return `It is not among the newest ${limit} sandboxes of ${selected} loaded below.`;
  }
  const owner = sandbox.org_id ? orgName(sandbox.org_id) ?? 'another organization' : 'no organization';
  return `It belongs to ${owner}, not ${selected}.`;
}
