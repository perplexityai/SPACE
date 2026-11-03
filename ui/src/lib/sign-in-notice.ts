// A one-time message for the login page, carried across the full navigation
// that ends a session (sessionStorage survives it; nothing else does).
const KEY = 'space:sign-in-notice';

export function setSignInNotice(text: string): void {
  try {
    window.sessionStorage.setItem(KEY, text);
  } catch {
    // Storage unavailable: the login page simply shows no message.
  }
}

export function takeSignInNotice(): string | null {
  try {
    const text = window.sessionStorage.getItem(KEY);
    if (text !== null) window.sessionStorage.removeItem(KEY);
    return text;
  } catch {
    return null;
  }
}
