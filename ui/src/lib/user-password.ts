import type { ConsoleAuthMode } from './console-auth';

// What setting a local user's password does, by sign-in mode. The API ends
// every password session of that user either way. In password mode that
// includes the console's own session when the user is you. With external
// sign-in the console authenticates through the provider, not a password
// session, so it stays signed in; the password only lets the accounts the
// deployment lists for password sessions sign in without the provider.
export type PasswordChangeOutcome = {
  // The console's own session ends, so it signs in again afterwards.
  endsConsoleSession: boolean;
  // Shown in the set-password dialog before the change.
  notice: string;
  // Shown on the users page for the create form's password.
  createHint: string | null;
};

export function passwordChangeOutcome(mode: ConsoleAuthMode | null | undefined, isSelf: boolean): PasswordChangeOutcome {
  if (mode?.mode === 'external') {
    const unaffected = `Console sign-in through ${mode.providerName} is unaffected; the password is used only by accounts this deployment allows to sign in with a password.`;
    return {
      endsConsoleSession: false,
      notice: isSelf
        ? `Setting a password ends all of your password sessions. ${unaffected}`
        : `Setting a password ends all of this user's password sessions. ${unaffected}`,
      createHint: `Console sign-in uses ${mode.providerName}. ${PASSWORD_ONLY_FOR_LISTED}`,
    };
  }
  return {
    endsConsoleSession: isSelf,
    notice: isSelf
      ? 'Setting a password ends all of your sessions, including this one: you will sign in again with the new password.'
      : "Setting a password ends all of this user's sessions. Your own session is not affected.",
    createHint: null,
  };
}

const PASSWORD_ONLY_FOR_LISTED =
  'The password is used only if this deployment allows the account to sign in with a password; use a strong, random one.';
