// A refused branding change. Releases that validate branding answer
// `PATCH /orgs/{id}` with a 400 `{ error, field }`, `field` being
// `theme_primary`, `theme_secondary` or `logo_url`; older releases accept any
// value and never refuse.
const BRANDING_FIELDS = new Set(['theme_primary', 'theme_secondary', 'logo_url']);

export function brandingRefusal(err: unknown): { message: string; field: string | null } | null {
  if (typeof err !== 'object' || err === null) return null;
  const { status, body, message } = err as { status?: unknown; body?: unknown; message?: unknown };
  if (status !== 400) return null;
  const field = body && typeof body === 'object' ? (body as { field?: unknown }).field : undefined;
  return {
    message: typeof message === 'string' && message ? message : 'The branding values were refused',
    field: typeof field === 'string' && BRANDING_FIELDS.has(field) ? field : null,
  };
}
