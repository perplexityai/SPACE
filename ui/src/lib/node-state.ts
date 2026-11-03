import type { NodeRuntimeInstallation } from './types';

// Node state the admin node reads carry only on releases that report it, and
// some only on one substrate: Direct has no cordon. An absent field renders as
// nothing, never as "no" or "false".

// One line per installation: "fc 1.15.1 · linux/x86_64".
export function runtimeInstallationLines(installations: NodeRuntimeInstallation[] | undefined): string[] | null {
  if (installations === undefined) return null;
  return installations.map((installation) => {
    const guests = installation.guests.length ? ` · ${installation.guests.join(', ')}` : '';
    return `${installation.runtime} ${installation.runtime_version}${guests}`;
  });
}

// "yes" or "no" where the API said, null where it did not.
export function cordonedText(cordoned: boolean | undefined): string | null {
  if (cordoned === undefined) return null;
  return cordoned ? 'yes' : 'no';
}
