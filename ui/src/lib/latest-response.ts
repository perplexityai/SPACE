// Orders overlapping reads of one value: each read takes a ticket as it
// starts, and only the newest read started may write its answer. Without it,
// an older read that finishes last overwrites the newer read's answer.
export interface LatestResponse {
  // Starts a read; the returned check says whether it is still the newest.
  begin(): () => boolean;
}

export function latestResponse(): LatestResponse {
  let latest = 0;
  return {
    begin() {
      const ticket = ++latest;
      return () => ticket === latest;
    },
  };
}
