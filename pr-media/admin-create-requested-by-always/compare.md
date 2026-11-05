```text
$ npm run loop -- --label final --compare out/runs/2026-11-05T16-09-16-main-baseline
47 steps, 13 with events → /home/dev/.devbox/worktrees/SPACE-worktree-e17766b8/scripts/console-loop/out/runs/2026-11-05T16-37-11-final
2 new error type(s), 0 resolved, 0 not covered by the later run, 2 new step signature(s)
  NEW         http|GET /admin/api/v1/orgs/<uuid>/master-key|404
  NEW         http|GET /admin/api/v1/orgs/<uuid>|404
```

Both new signatures come from `org-detail-unknown`, a step in the `orgs` tag the scoped baseline did not run. It opens `/dashboard/orgs/00000000-0000-0000-0000-00000000dead`, an organization that does not exist, and passes when the console shows a not-found state; the 404s are the answers it expects. This change touches no organization route.
