# Console loop

A browser verification loop for the admin console (`ui/`) on the local Kind
stack. It seeds synthetic state through the public and admin APIs, signs in
through the real login page, visits every console route and runs the operator
flows, and records what happened at each step: a screenshot, console and page
errors, failed and slow requests, error toasts, and the step's own checks. Two
runs can be compared, for example `main` against a branch, and the comparison
fails when the branch has an error that `main` does not.

It produces development and review evidence, not a pass/fail gate: nothing in
CI runs it, and a clean comparison does not certify a change. Smoke tests
against a Kind or deployed stack are typed `space_smoke` scenarios (see the
`space-validation` skill's smoke-testing reference); typed console scenarios
are a separate follow-up.

It acts only on a stack on this host: `seed`, `clean`, and every `loop` run
that creates things refuse an API or console URL that is not loopback
(`localhost`, `127.0.0.0/8`, `::1`) unless `--allow-remote` is passed, and
print the target they act on.

## Prerequisites

- The Kind stack, brought up with `infra/space/kind/scripts/up.sh`
  ([docs/installation.md](../../../../docs/installation.md)). The loop uses the
  console at `http://localhost:3000` (Tilt's port-forward), the API at
  `http://localhost:8080`, and the console account `up.sh` provisions
  (`root@local`, with the password from `SPACE_CONSOLE_PASSWORD`, default
  `password`).
- Node 22.12 or later, then, in this directory:

  ```bash
  npm ci
  npx playwright install --with-deps chromium   # once per host
  ```

- `ffmpeg`, only to turn captured videos into GIFs.

## Seed, run, clean

```bash
npm run seed                  # rerunnable; reuses what exists
npm run loop                  # every step, against http://localhost:3000
npm run loop -- --no-mutate   # skip the steps that create and delete things
npm run loop -- --only 'sandboxes|nodes' --skip lifecycle
npm run clean                 # delete everything the seed created
```

The seed creates, under a name prefix (`loop` by default), three tenant
organizations (one nested), six users across roles and organizations, two API
keys in the managing organization, and sandboxes that are running, paused,
suspended and stopped, one in a tenant organization, and one created and
deleted. Every name and organization display name carries the prefix, so
runs under two prefixes can share a stack. Seeding sandboxes boots
Firecracker VMs and takes a minute or two; `--skip-sandboxes` leaves them out.

Seeding looks each object up by its prefixed name before creating it, so a
rerun after an interruption reuses what exists instead of duplicating it, and
it ends by looking every recorded fixture up by ID. Everything the seed
creates, and everything a `flows` step of the loop creates, is recorded in
`out/seed.json` as soon as it exists, together with the generated password
of the seeded users. The loop's first step checks that each recorded fixture
still exists.

`npm run clean` deletes exactly what `out/seed.json` records: sandboxes
(waiting for each delete operation to succeed), users and API keys, then
organizations, children first. It refuses to delete an organization that
still holds anything it did not record (a child organization, a user, an API
key or a live sandbox), and reports it. It exits 1 when anything recorded was
not deleted, and keeps those objects in `out/seed.json` for the next run.

`--only` and `--skip` take a regular expression matched against each step's
name and tags (`routes`, `orgs`, `users`, `api-keys`, `sandboxes`, `flows`,
`lifecycle`, `templates`, `nodes`, `roles`). Sign-in always runs. Steps tagged
`flows` create objects and delete them again; if one fails before deleting,
`npm run clean` removes what it recorded. `--no-mutate` skips them. The steps
are in [`src/steps.ts`](src/steps.ts).

Each run writes `out/runs/<time>[-<label>]/`:

- `summary.json`: per step, its URL, screenshot, duration, events (each with a
  `signature`) and notes, including the checks that passed;
- `summary.md`: the same, to read;
- one full-page PNG per step.

`out/runs/latest` links to the newest run. `out/` is git-ignored.

Every setting is a flag or an environment variable:

| Flag | Variable | Default |
|---|---|---|
| `--base-url` | `CONSOLE_LOOP_BASE_URL` | `http://localhost:3000` |
| `--api-url` | `CONSOLE_LOOP_API_URL` | `http://localhost:8080` |
| `--email` | `CONSOLE_LOOP_EMAIL` | `root@local` |
| `--password` | `CONSOLE_LOOP_PASSWORD`, then `SPACE_CONSOLE_PASSWORD` | `password` |
| `--out` | `CONSOLE_LOOP_OUT` | `./out` |
| `--prefix` | `CONSOLE_LOOP_PREFIX` | `loop` |
| `--template` (seed) | `CONSOLE_LOOP_TEMPLATE` | `seed:latest` |

`npm run loop` also takes `--label NAME`, `--slow-ms N` (default 1500),
`--compare RUN`, `--headed`, and `--video`, which records the run as
`run.webm` in its run directory. Run `npm run typecheck` after changing the
scripts.

## The fast loop: `next dev`

For quick iteration, run the console from your checkout with hot reload,
against the stack's port-forwarded backends. Tilt already serves on port
3000, so use another port:

```bash
cd ui
npm ci
NEXT_PUBLIC_API_URL=http://localhost:8080 npx next dev --port 3001
```

In development the data plane defaults to `http://localhost:8090`
(`ui/next.config.js`); `NEXT_PUBLIC_ADMIN_API_URL` and
`NEXT_PUBLIC_DATA_PLANE_URL` override the admin and data-plane backends (see
[ui/README.md](../../../../ui/README.md)). An edit shows within a second. Point
the loop at it:

```bash
npm run loop -- --base-url http://localhost:3001 --only users
```

A development build adds warnings that a production build does not print.
React 19 logs its development warnings with `console.error`, as format
strings whose `%s` placeholders reach the console unformatted, and antd logs
"Warning: …" messages. The loop records console errors of those two shapes
as `devwarn` events, which never fail a comparison; any other console error,
including a development-only one without placeholders, is a `console`
event.

## The real loop: the Tilt image

Tilt rebuilds the `space-ui` image whenever `ui/` changes in the checkout it
runs from, and rolls it out behind `http://localhost:3000`; that takes about
a minute. This is the production build the stack ships, and the one to show
in review. `NEXT_PUBLIC_*` values are fixed when the image is built (the
`Tiltfile` passes the in-cluster API and data-plane addresses), so setting
them at runtime has no effect.

## Comparing a branch with `main`

Run the loop once on `main` and once on the branch, against the same seeded
stack, then compare:

```bash
git switch main          # Tilt rebuilds space-ui; wait for it to roll out
npm run loop -- --label main
git switch my-branch     # and wait again
npm run loop -- --label branch --compare out/runs/<time>-main
```

`--compare` (or `npm run compare -- <before-run> <after-run>` for two existing
runs) compares error types, with IDs, counts and seeded names replaced by
placeholders. For a failed request, a console or page error, the type leaves
out the step, so a known error that shows up on another page is not new. A
failed check or an exception keeps its step: the same check failing in a
new step is new. The comparison lists the types the later run has and the
earlier one does not, the ones it resolved, and the ones the later run did
not cover (it did not run every step they appeared in, for example with
`--only` or `--no-mutate`), and exits 1 when there are new ones. `slow` and
`devwarn` events are reported but never fail a comparison.

Each event belongs to the step that issued its request. A response that
arrives after that step has ended is listed in the current step's notes as
"late", not counted against it.

To compare without switching the Tilt checkout, serve `main` with a production
build on another port and pass `--base-url` to each run. The backend addresses
are fixed at build time, so set them for the build:

```bash
# in the ui/ directory of a main worktree
npm ci
NEXT_PUBLIC_API_URL=http://localhost:8080 NEXT_PUBLIC_DATA_PLANE_URL=http://localhost:8090 npx next build
npx next start --port 3002
```

## Using runs as evidence

An agent or reviewer checks a change with the runs, not by guessing at the UI:

- Before changing a page, run the steps that cover it on the base branch.
  Afterwards, run them again with `--compare` on the earlier run. Treat a new
  error type as a regression to fix, not as noise.
- Read `summary.md` for each step's events, and open the step's screenshot to
  see the page. An event's `body` holds the response body of a failed request.
- A passing check is listed as `ok: …` under the step's notes; a failing one is
  a `check` event.
- To test an error path, a step can answer a request itself with
  `page.route(…, (route) => route.fulfill(…))`. Set the `x-console-loop-synthetic`
  header on such a response; the recorder does not report responses that
  carry it.
- Add a step to `src/steps.ts` for each behavior a change introduces, run it on
  the base branch and on the change, and keep it if it fails before and passes
  after.

## Capturing media for a pull request

```bash
npm run capture -- --name nodes-page --route /dashboard/nodes --route /dashboard/sandboxes --scroll
```

This signs in, records the routes at 1280×800, and writes
`out/media/nodes-page.webm`, plus `nodes-page.gif` when `ffmpeg` is installed.
Step screenshots from a run are in its run directory. Attach media to the pull
request instead of committing it.

A new API key's secret is shown once, in a dialog. The API-key step hides
that dialog's key before the dialog opens, so its screenshots and video
frames never show it, and the recorder replaces anything shaped like a key
(64 hex characters) in what it writes with `<redacted>`. Do not capture that
dialog with `npm run capture`, which does not hide it. With that, run media
show only synthetic data from the seed; look at each image before you share
it all the same.
