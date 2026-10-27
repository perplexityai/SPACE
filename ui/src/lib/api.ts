import {
  ConsoleAuthMode,
  PASSWORD_MODE,
  SIGN_IN_AGAIN_PATH,
  consoleRequestHeaders,
  parseConsoleAuthConfig,
} from './console-auth';

const API_BASE = '/api/v1';
const AUTH_BASE = '/auth/v1';
const ADMIN_BASE = '/admin/api';

type RequestMode = 'same-origin' | 'prefer-direct';

export class ApiError extends Error {
  constructor(public status: number, message: string) {
    super(message);
  }
}

// Set once by the auth provider from `GET /auth/v1/config`, before any
// authenticated request is made.
let consoleAuthMode: ConsoleAuthMode = PASSWORD_MODE;

export function setConsoleAuthMode(mode: ConsoleAuthMode) {
  consoleAuthMode = mode;
}

export function isExternalConsoleAuth(): boolean {
  return consoleAuthMode.mode === 'external';
}

// Discover the console auth mode. An API that predates discovery answers
// 404 and only supports passwords.
export async function fetchConsoleAuthMode(): Promise<ConsoleAuthMode> {
  const res = await fetch(`${AUTH_BASE}/config`, {
    credentials: 'same-origin',
    headers: { 'X-Requested-With': 'XMLHttpRequest' },
    cache: 'no-store',
  });
  if (res.status === 404) return PASSWORD_MODE;
  if (!res.ok) throw new ApiError(res.status, 'Could not load sign-in settings');
  const mode = parseConsoleAuthConfig(await res.json().catch(() => null));
  if (!mode) throw new ApiError(res.status, 'Unrecognized sign-in settings');
  return mode;
}

interface RequestBehavior {
  // Report a 401 to the caller instead of navigating to sign-in. Used by the
  // auth provider's own probe, which must not loop through the login page.
  probe?: boolean;
}

function directApiBase(base: string): string | null {
  if (typeof window === 'undefined') {
    return null;
  }
  // External sign-in is attached by the proxy in front of the console, so a
  // direct request to the API port would arrive without it.
  if (isExternalConsoleAuth()) {
    return null;
  }
  if (window.location.protocol !== 'http:' || window.location.port !== '3000') {
    return null;
  }
  const host = window.location.hostname;
  if (!host || host === 'localhost' || host === '127.0.0.1' || host === '[::1]') {
    return null;
  }
  return `http://${host}:8080${base}`;
}

async function responseOnce(
  base: string,
  path: string,
  options: RequestInit = {},
  behavior: RequestBehavior = {},
): Promise<Response> {
  const external = isExternalConsoleAuth();
  const token =
    typeof window !== 'undefined' && !external ? localStorage.getItem('token') : null;

  const headers: Record<string, string> = {
    'Content-Type': 'application/json',
    ...(options.headers as Record<string, string>),
    ...consoleRequestHeaders(consoleAuthMode, token),
  };

  const res = await fetch(`${base}${path}`, {
    ...options,
    headers,
    credentials: 'same-origin',
  });

  if (res.status === 401 && typeof window !== 'undefined' && !behavior.probe) {
    localStorage.removeItem('token');
    window.location.href = external ? SIGN_IN_AGAIN_PATH : '/login';
    throw new ApiError(401, 'Unauthorized');
  }

  if (!res.ok) {
    const body = await res.json().catch(() => ({}));
    // Admin handlers return `{ error }`; the public API envelope returns
    // `{ message }`. Prefer whichever is present so the real cause surfaces
    // instead of a generic "Request failed".
    throw new ApiError(res.status, body.error || body.message || 'Request failed');
  }

  return res;
}

async function requestOnce<T>(
  base: string,
  path: string,
  options: RequestInit = {},
  behavior: RequestBehavior = {},
): Promise<T> {
  const res = await responseOnce(base, path, options, behavior);

  if (res.status === 204) {
    return undefined as T;
  }

  return res.json();
}

async function request<T>(
  path: string,
  options: RequestInit = {},
  mode: RequestMode = 'same-origin',
  base: string = API_BASE,
): Promise<T> {
  if (mode === 'prefer-direct') {
    const directBase = directApiBase(base);
    if (directBase) {
      try {
        return await requestOnce<T>(directBase, path, options);
      } catch (err) {
        if (err instanceof ApiError) {
          throw err;
        }
      }
    }
  }

  return requestOnce<T>(base, path, options);
}

export const api = {
  get: <T>(path: string) => request<T>(path),
  post: <T>(path: string, body?: unknown, headers?: Record<string, string>) =>
    request<T>(path, { method: 'POST', body: body ? JSON.stringify(body) : undefined, headers }),
  postPreferDirect: <T>(path: string, body?: unknown) =>
    request<T>(
      path,
      { method: 'POST', body: body ? JSON.stringify(body) : undefined },
      'prefer-direct',
    ),
  patch: <T>(path: string, body?: unknown, headers?: Record<string, string>) =>
    request<T>(path, { method: 'PATCH', body: body ? JSON.stringify(body) : undefined, headers }),
  put: <T>(path: string, body?: unknown) =>
    request<T>(path, { method: 'PUT', body: body ? JSON.stringify(body) : undefined }),
  delete: <T>(path: string, headers?: Record<string, string>) =>
    request<T>(path, { method: 'DELETE', headers }),
};

export const authApi = {
  get: <T>(path: string) => request<T>(path, {}, 'same-origin', AUTH_BASE),
  // Like `get`, but a 401 is returned to the caller instead of navigating.
  probe: <T>(path: string) => requestOnce<T>(AUTH_BASE, path, {}, { probe: true }),
  post: <T>(path: string, body?: unknown) =>
    request<T>(
      path,
      { method: 'POST', body: body ? JSON.stringify(body) : undefined },
      'same-origin',
      AUTH_BASE,
    ),
  delete: <T>(path: string) =>
    request<T>(path, { method: 'DELETE' }, 'same-origin', AUTH_BASE),
};

export const adminApi = {
  get: <T>(path: string) => requestOnce<T>(ADMIN_BASE, path),
  post: <T>(path: string, body?: unknown, headers?: Record<string, string>) =>
    requestOnce<T>(ADMIN_BASE, path, { method: 'POST', body: body ? JSON.stringify(body) : undefined, headers }),
  patch: <T>(path: string, body?: unknown, headers?: Record<string, string>) =>
    requestOnce<T>(ADMIN_BASE, path, { method: 'PATCH', body: body ? JSON.stringify(body) : undefined, headers }),
  put: <T>(path: string, body?: unknown) =>
    requestOnce<T>(ADMIN_BASE, path, { method: 'PUT', body: body ? JSON.stringify(body) : undefined }),
  delete: <T>(path: string, headers?: Record<string, string>) =>
    requestOnce<T>(ADMIN_BASE, path, { method: 'DELETE', headers }),
};
