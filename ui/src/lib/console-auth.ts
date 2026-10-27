// Console auth mode, discovered from `GET /auth/v1/config`.
//
// In `password` mode the console signs in with a SPACE email and password and
// sends the session token as a bearer credential. In `external` mode an
// identity-aware proxy in front of the console authenticates the browser and
// attaches a signed assertion to every request; the console holds no SPACE
// credential at all, so it sends no Authorization header and never stores a
// token.

export type ConsoleAuthMode =
  | { mode: 'password' }
  | { mode: 'external'; providerName: string; logoutUrl: string | null };

export const PASSWORD_MODE: ConsoleAuthMode = { mode: 'password' };

// Sent on every external-mode request. The API requires it, with a trusted
// `Origin`, on state-changing requests: a cross-site page cannot add it
// without a CORS preflight.
export const CONSOLE_REQUEST_HEADER = 'X-Space-Console-Request';

const DEFAULT_PROVIDER_NAME = 'Single sign-on';

// A logout destination the console may navigate to: a same-origin absolute
// path or an `https` URL. Anything else (protocol-relative, `javascript:`,
// plain `http`) is dropped.
export function safeLogoutUrl(value: unknown): string | null {
  if (typeof value !== 'string') return null;
  const url = value;
  if (!url || /[\s\u0000-\u001f\u007f]/.test(url)) return null;
  if (url.startsWith('/')) {
    return url.startsWith('//') || url.startsWith('/\\') ? null : url;
  }
  try {
    const parsed = new URL(url);
    if (parsed.protocol !== 'https:' || !parsed.host || parsed.username || parsed.password) {
      return null;
    }
    return url;
  } catch {
    return null;
  }
}

// Parse the discovery body. Returns `null` for anything the console does not
// understand, so the caller can surface an error instead of guessing a mode.
export function parseConsoleAuthConfig(body: unknown): ConsoleAuthMode | null {
  if (!body || typeof body !== 'object') return null;
  const config = body as Record<string, unknown>;
  if (config.mode === 'password') return PASSWORD_MODE;
  if (config.mode !== 'external') return null;
  const providerName =
    typeof config.provider_name === 'string' && config.provider_name.trim()
      ? config.provider_name.trim()
      : DEFAULT_PROVIDER_NAME;
  return { mode: 'external', providerName, logoutUrl: safeLogoutUrl(config.logout_url) };
}

// Why the console cannot show a signed-in user in external mode.
export type ConsoleAccessProblem = 'unauthenticated' | 'forbidden' | 'unavailable' | 'error';

export function accessProblemForStatus(status: number): ConsoleAccessProblem {
  if (status === 401) return 'unauthenticated';
  if (status === 403) return 'forbidden';
  if (status === 502 || status === 503 || status === 504) return 'unavailable';
  return 'error';
}

// Headers a request carries in the given mode, given the stored password
// session token (ignored in external mode).
export function consoleRequestHeaders(
  mode: ConsoleAuthMode,
  passwordToken: string | null,
): Record<string, string> {
  if (mode.mode === 'external') {
    // `X-Requested-With` asks identity-aware proxies that support it to
    // answer an expired session with 401 instead of a cross-origin redirect
    // that a fetch cannot follow.
    return { [CONSOLE_REQUEST_HEADER]: '1', 'X-Requested-With': 'XMLHttpRequest' };
  }
  return passwordToken ? { Authorization: `Bearer ${passwordToken}` } : {};
}

// Where a 401 sends the browser. A full navigation lets the proxy run its
// sign-in flow before the console loads again.
export const SIGN_IN_AGAIN_PATH = '/login?reauth=1';
