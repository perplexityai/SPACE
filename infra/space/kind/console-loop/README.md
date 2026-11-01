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

It acts only on one stack, on this host. `seed`, `clean`, and every `loop`
run that creates things:

- refuse an API or console URL that is not loopback (`localhost`,
  `127.0.0.0/8`, `::1`) unless `--allow-remote` is passed, and print the
  target they act on;
- read the stack's identity, the `id` and `created_at` of its cluster row
  (generated when its database is created; `managing_org_id` is the same on
  every stack), four ways: `/admin/api/v1/cluster` and `/auth/v1/cluster`,
  each signed in separately, through the API URL and through the console's
  origin, whose `/admin` and `/auth` proxies reach the backends its build
  names. They refuse when the readings differ, or differ from the identity
  the seed recorded. The tool's own API requests (the seeder's, `clean`'s
  and the steps' checks) and the requests it intercepts in the browser never
  follow a redirect; the console's own requests and navigations do.

A state recorded on a stack that is gone, for example after the Kind cluster
was recreated, is refused with the same message. If the stack the state was
recorded on no longer exists, delete `<out>/seed-<prefix>.json`; its objects
went with that stack. Otherwise point `--api-url` and `--base-url` at that
stack.

The check cannot prove more than that: the four readings came from one
database at that moment. It does not cover the console's other routes
(`/api`, the data plane's `/v1/sessions`) or requests the browser sends to
another origin, a proxy that routes differently later or per request, or two
stacks that share one database or a copy of it. `--allow-remote` drops only
the loopback check.

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
npm run loop -- --only sandboxes,nodes --skip lifecycle
npm run clean                 # delete what the seed and loop created, except organizations
```

The seed creates, under a name prefix (`loop` by default), three tenant
organizations (one nested), six users across roles and organizations, two API
keys in the managing organization, and sandboxes that are running, paused,
suspended and stopped, one in a tenant organization, and one created and
deleted. Every name and organization display name carries the prefix, so
runs under two prefixes can share a stack. Seeding sandboxes boots
Firecracker VMs and takes a minute or two; `--skip-sandboxes` leaves them out.

The state is `out/seed-<prefix>.json`: the stack it was seeded on, the
generated password of the seeded users, and every object the seed creates
and every object a `flows` step of the loop creates. `seed`, `loop` and
`clean` refuse a state that records another prefix than `--prefix`.

Each create is recorded before it is submitted, and its ID and operation as
soon as the API answers. Seeding looks each object up by its prefixed name
before creating it, and a rerun after an interruption first waits for the
operations recorded for it. Organizations, users and API keys have no
idempotent create, so a rerun finds them by slug, email or key name; their
creates take effect before the API answers, so a lookup by name is
authoritative. A sandbox create is recorded with its idempotency key and
request, and a rerun resubmits an unanswered one with the same key and the
recorded request, unchanged even when `--template` has changed since (the
API refuses a key reused with another body), which the API answers with the
original operation if the first submit arrived. Only a new create, after the
earlier one has resolved, uses the current options and a new key.

The outcome of each operation is recorded once it is seen finished, and not
asked for again. The API deletes finished operations a week after they are
created; one that is no longer there is recorded as `gone`, with an outcome
nobody can read, and the seeder and `clean` decide from the object itself,
looked up by its ID. Seeding fails
(exit 2) when it cannot discover or set up a fixture. It ends by looking up
every fixture the loop needs by ID: the organizations, users and API keys
and, unless `--skip-sandboxes` was given, each sandbox in its resting state
and the deleted one gone. The loop's `fixtures` step checks the same set
through the console.

`npm run clean` deletes what the state records:

- sandboxes, waiting first for every operation recorded for each, then for
  its delete and for it to leave `deleting`. A sandbox counts as gone only
  when it is deleted, or not found once nothing recorded for it is still
  running. A seed create that was submitted but never answered is
  resubmitted, unchanged, with its key to learn its operation. A loop create
  that left the browser with no answer, and has no sandbox of its name yet
  (a create can stay queued, without a sandbox, after the loop is gone), is
  unresolved: `clean` keeps it in the state, reports it, exits 1, and looks
  it up by name in its organization again on the next run. `--forget-unresolved`
  drops such creates on purpose. A loop create whose request never left the
  browser is forgotten;
- users and API keys, by ID, or by email or name when the create was never
  answered;
- the organizations loop runs created (`<prefix>-run-…`).

It keeps the seeded organizations (`<prefix>-acme`, `<prefix>-globex`,
`<prefix>-globex-research`), which the next seed reuses by slug, unless
`--delete-orgs` is given. Deleting an organization also deletes, by
`ON DELETE CASCADE`, its API keys, organization and user credentials,
credential references, master keys, storage vend identities, data-plane
revocation epochs, public operations and idempotency records, and template
build-base and publication grants. `clean` deletes organizations children
first, and refuses one that still holds a child organization, a user, an API
key or a sandbox that is not deleted, and reports it; it does not check the
rows that cascade. `clean` exits 1 when anything recorded was not deleted, and
keeps those objects in the state for the next run. It removes the state file
once nothing is left in it.

`--only` and `--skip` take comma-separated step names or tags, each matched
exactly; a name that is neither is an error. The tags are `routes`, `orgs`,
`users`, `api-keys`, `sandboxes`, `flows`, `lifecycle`, `templates`, `nodes`,
`roles`. Sign-in always runs. Steps tagged `flows` create objects and delete
them again; each records its object's name before it submits the create, the
ID from the create's response, and the operation of every sandbox action and
delete as its answer arrives, so if one fails before deleting, `npm run clean`
removes what it recorded. They need a seed state for the prefix, and
the identity check above passes before any runs. `--no-mutate` skips them.
The steps are in [`src/steps.ts`](src/steps.ts).

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

A prefix is up to 32 lowercase letters, digits and dashes. `npm run seed`
also takes `--skip-sandboxes`, and `npm run clean` takes `--delete-orgs` and
`--forget-unresolved`.
`npm run loop` also takes `--label NAME`, `--slow-ms N` (default 1500),
`--compare RUN`, `--headed`, and `--video`, which records the run as
`run.webm` in its run directory. Run `npm run typecheck` and `npm test` (unit
tests that need no stack) after changing the scripts.

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
arrives after that step has ended, including a body that finishes reading
after it, is listed in the current step's notes as "late", not counted
against it; a step's evidence is fixed once its result is taken.

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

A new API key's secret is shown once, in a dialog. The API-key step sends
every POST through `page.route`: it fetches the answer without following a
redirect (a 3xx is refused), records the key's ID from a create's answer (a
POST whose path ends in `/api-keys`), and hands the page the answer with
every `secret` replaced, so the browser never holds a live key, and the
dialog in its screenshots and video shows the placeholder
`console-loop-placeholder-not-a-key`. It fails closed: if a JSON answer with
a `secret` reaches the page some other way, the dialog opens before the
route answered the create, or the dialog shows anything shaped like a key,
the step opens `about:blank` before any screenshot and fails, and with
`--video` the run's video is deleted, which the run's output and summary
say. The recorder replaces anything shaped like a key (64 hex characters) in
what it writes with `<redacted>`. `npm run capture` only visits routes and creates
no keys.

The fixtures are synthetic, but screenshots and videos show everything the
console account can see on the stack: other organizations, users, sandboxes,
nodes and templates, whoever created them. Run the loop on a disposable
stack that holds nothing else, and look at each image before you share
it.
