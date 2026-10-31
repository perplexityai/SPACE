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
